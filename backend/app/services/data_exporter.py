"""分析导出领域服务：将查询结果转换为多种格式导出，消费联邦查询结果和缓存数据。

分区级续做改造（沿 导出 -> 集市 调用链）：
- ``refresh_mart_partitions`` 按分区遍历结果缓存取数，每个分区处理完即写检查点，
  中断重试只重建失败/未处理片段，成功分区（含成功零行）不再重复扫描；
- 成功零行（有扫描证据）与取数失败（缺证据）分别记录，二者在清单和集市口径里可区分；
- ``deliver_partition_files`` 按集市续做覆盖清单交付文件，缺口分区只出缺口记录、不空造数据；
- 旧的整批导出接口保持原样，历史集市可由结果集市服务标记核验缺口而不破坏已出结果。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.mart_resume import (
    PARTITION_EMPTY,
    PARTITION_FAILED,
    PARTITION_READY,
    CachePartitionReader,
    MartRefreshInterrupted,
    PartitionReader,
    mart_resume_service,
)
from app.services.result_mart import ResultMartService
from app.store import store

# 默认分区读取器为模块级单例：瞬时失败计数在“中断 -> 重试”之间保留并被消费
default_partition_reader = CachePartitionReader()


class DataExporterService(BaseModuleService):
    MODULE = "data_exporter"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["任务编号", "数据来源", "筛选条件", "输出格式", "字段列表", "排序规则", "导出上限", "接收地址"]
    REQUIRED_FIELDS = ["任务编号", "数据来源", "输出格式"]
    STATUS_ORDER = ["待执行", "导出中", "已完成", "导出失败"]
    ACTION_RULES = {"启动导出": "导出中", "取消导出": "已完成", "标记失败": "导出失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def __init__(self) -> None:
        # 已交付文件登记处：分区文件 / 零行标记 / 缺口记录三类分开
        self.delivered_files: list[dict[str, Any]] = []
        # 分区实际载荷，交付阶段按清单取数成文件
        self._partition_payloads: dict[str, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # 分区级续做刷新：导出任务 -> 集市分区写入
    # ------------------------------------------------------------------

    def refresh_mart_partitions(
        self,
        *,
        mart_id: int,
        partitions: list[str],
        batch_id: str | None = None,
        reader: PartitionReader | None = None,
        fail_after: int | None = None,
    ) -> dict[str, Any]:
        """按分区把遍历结果缓存的数据刷新进集市，支持中断续做。

        中断时抛出 ``MartRefreshInterrupted``，检查点已落清单，用同一 batch_id 再调一次即可续做。
        """
        reader = reader or default_partition_reader
        partitions = list(partitions)
        if not partitions:
            return {"ok": False, "message": "刷新分区列表不能为空"}

        mart_svc = ResultMartService()
        # 续做场景先复用既有批次：导出任务与集市都沿用首轮，不重新登记
        existing = (
            mart_resume_service.get_manifest(batch_id) if batch_id else None
        )
        if existing is not None:
            export_id = existing.export_id
            mart_id = existing.mart_id
            export_entry = store.find(self.MODULE, export_id)
            if export_entry is None:
                return {"ok": False, "message": "续做批次对应的导出任务已丢失"}
        else:
            # 1. 首轮：登记导出任务与目标集市（mart_id=0 时新建集市）
            export_entry = self._create_resume_export_task(partitions)
            export_id = export_entry["id"]
            if mart_id <= 0:
                mart = self._create_resume_mart(export_id)
                mart_id = mart["id"]
        mart = store.find("result_mart", mart_id)
        if mart is None:
            return {"ok": False, "message": "数据集市不存在"}

        # 2. 打开续做清单：已存在则复用，成功分区保持原终态
        manifest = mart_resume_service.open_manifest(
            batch_id, mart_id=mart_id, export_id=export_id, partitions=partitions
        )
        if set(manifest.partitions) != set(partitions):
            return {"ok": False, "message": "续做分区集合与原批次不一致，拒绝混批刷新"}

        existing_coverage = mart.get("分区覆盖")
        if not existing_coverage or existing_coverage.get("批次号") != manifest.batch_id:
            mart_svc.init_coverage(
                mart, batch_id=manifest.batch_id, export_id=export_id, partitions=partitions
            )

        export_entry["status"] = "导出中"
        export_entry["pending"] = True
        export_entry["续做批次号"] = manifest.batch_id

        # 3. 只处理未成功的分区：成功（含零行）跳过，不再重新扫描
        todo = mart_resume_service.pending_partitions(manifest.batch_id)
        skipped = [p for p in manifest.partitions if p not in todo]
        processed_now = 0

        for partition in manifest.partitions:
            if partition in skipped:
                continue

            mart_resume_service.mark_attempt(manifest.batch_id, partition)
            fetch = reader.read(partition)

            if fetch.error:
                # 取数失败 = 缺证据：与成功零行明确区分
                mart_resume_service.record_partition(
                    manifest.batch_id, partition,
                    state=PARTITION_FAILED, error=fetch.error,
                )
                mart_svc.commit_partition(
                    mart_id, batch_id=manifest.batch_id, partition=partition,
                    state=PARTITION_FAILED, row_count=0, file=None, checksum=None,
                    evidence=None, error=fetch.error,
                )
                processed_now += 1
            else:
                self._partition_payloads[f"{manifest.batch_id}:{partition}"] = list(fetch.rows)
                # 有数据 -> 有数据已提交；空列表且有证据 -> 成功零行。文件在交付阶段按清单生成
                state = PARTITION_READY if fetch.rows else PARTITION_EMPTY
                checksum = hashlib.md5(
                    json.dumps(fetch.rows, ensure_ascii=False, sort_keys=True).encode()
                ).hexdigest()[:16]
                mart_resume_service.record_partition(
                    manifest.batch_id, partition,
                    state=state, row_count=len(fetch.rows), file=None,
                    checksum=checksum, evidence=fetch.evidence,
                )
                mart_svc.commit_partition(
                    mart_id, batch_id=manifest.batch_id, partition=partition,
                    state=state, row_count=len(fetch.rows), file=None,
                    checksum=checksum, evidence=fetch.evidence,
                )
                processed_now += 1

            # 4. 模拟批次中断：已处理分区全部落检查点，可凭批次号续做
            if fail_after is not None and processed_now >= fail_after:
                mart_resume_service.mark_interruption(
                    manifest.batch_id, partition,
                    f"刷新在处理 {processed_now} 个分区后中断，已提交分区保留检查点",
                )
                mart_svc.sync_with_manifest(mart_id, manifest.batch_id)
                summary = mart_resume_service.summary(manifest.batch_id)
                self._writeback_export_task(export_entry, summary, interrupted=True)
                _audit_resume_refresh(manifest.batch_id, summary, interrupted=True)
                raise MartRefreshInterrupted(manifest.batch_id, summary)

        # 5. 全量跑完：集市覆盖账本与清单对齐，回写任务清单
        coverage = mart_svc.sync_with_manifest(mart_id, manifest.batch_id)
        summary = mart_resume_service.summary(manifest.batch_id)
        self._writeback_export_task(export_entry, summary, interrupted=False)
        _audit_resume_refresh(manifest.batch_id, summary, interrupted=False)

        return {
            "ok": True,
            "batch_id": manifest.batch_id,
            "mart_id": mart_id,
            "export_id": export_id,
            "processed_now": processed_now,
            "skipped_partitions": skipped,
            "summary": summary,
            "coverage": coverage,
            "readable_range": mart_svc.readable_range(mart_id),
        }

    def resume_mart_refresh(
        self,
        *,
        mart_id: int,
        batch_id: str,
        reader: PartitionReader | None = None,
        fail_after: int | None = None,
    ) -> dict[str, Any]:
        """按批次号续做：从清单恢复分区集合，只重建失败/未处理片段。"""
        manifest = mart_resume_service.get_manifest(batch_id)
        if manifest.mart_id != mart_id:
            return {"ok": False, "message": "批次与集市不匹配"}
        return self.refresh_mart_partitions(
            mart_id=mart_id,
            partitions=list(manifest.partitions),
            batch_id=batch_id,
            reader=reader,
            fail_after=fail_after,
        )

    # ------------------------------------------------------------------
    # 按集市续做覆盖清单交付文件
    # ------------------------------------------------------------------

    def deliver_partition_files(self, batch_id: str) -> dict[str, Any]:
        """按清单逐分区交付：有数据出数据文件、零行出零行标记、缺口出缺口记录。"""
        manifest = mart_resume_service.get_manifest(batch_id)
        export_entry = store.find(self.MODULE, manifest.export_id)
        if export_entry is None:
            return {"ok": False, "message": "导出任务不存在"}

        data_files: list[dict[str, Any]] = []
        empty_markers: list[dict[str, Any]] = []
        gap_records: list[dict[str, Any]] = []

        for partition in manifest.partitions:
            state = manifest.states[partition]

            if state.state == PARTITION_READY:
                rows = self._partition_payloads.get(f"{batch_id}:{partition}", [])
                payload = {"partition": partition, "batch_id": batch_id,
                           "rows": rows, "row_count": len(rows),
                           "evidence": state.evidence}
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                checksum = hashlib.md5(content.encode()).hexdigest()[:16]
                filename = f"{batch_id}_{partition}.json"
                record = self._register_file(
                    batch_id, partition, filename, "partition_data",
                    content, checksum, len(rows),
                )
                data_files.append(record)
                mart_resume_service.record_partition(
                    batch_id, partition, state=PARTITION_READY,
                    row_count=len(rows), file=filename, checksum=checksum,
                    evidence=state.evidence,
                )
                mart_resume_service.record_delivery(
                    batch_id, partition, filename,
                    f"交付数据文件 {len(rows)} 行，证据 {state.evidence}",
                )
                ResultMartService().attach_delivery_file(
                    manifest.mart_id, batch_id, partition, filename
                )

            elif state.state == PARTITION_EMPTY:
                # 成功零行：交付带扫描证据的零行标记，不与失败混淆
                payload = {"partition": partition, "batch_id": batch_id,
                           "rows": [], "row_count": 0,
                           "status": PARTITION_EMPTY, "evidence": state.evidence}
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                checksum = hashlib.md5(content.encode()).hexdigest()[:16]
                filename = f"{batch_id}_{partition}.EMPTY.json"
                record = self._register_file(
                    batch_id, partition, filename, "empty_marker",
                    content, checksum, 0,
                )
                empty_markers.append(record)
                mart_resume_service.record_partition(
                    batch_id, partition, state=PARTITION_EMPTY,
                    row_count=0, file=filename, checksum=checksum,
                    evidence=state.evidence,
                )
                mart_resume_service.record_delivery(
                    batch_id, partition, filename,
                    f"成功零行标记，证据 {state.evidence}",
                )
                ResultMartService().attach_delivery_file(
                    manifest.mart_id, batch_id, partition, filename
                )

            else:
                # 缺证据分区（待处理/取数失败）：只交付缺口记录，绝不空造数据文件
                reason = state.error or "分区尚未完成取数，缺少证据"
                payload = {"partition": partition, "batch_id": batch_id,
                           "status": state.state, "gap": True,
                           "reason": reason, "attempts": state.attempts}
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                checksum = hashlib.md5(content.encode()).hexdigest()[:16]
                filename = f"{batch_id}_{partition}.GAP.json"
                record = self._register_file(
                    batch_id, partition, filename, "gap_record",
                    content, checksum, 0,
                )
                gap_records.append(record)
                mart_resume_service.record_delivery(
                    batch_id, partition, None,
                    f"缺口记录：{reason}，未生成数据文件",
                )
                ResultMartService().attach_delivery_file(
                    manifest.mart_id, batch_id, partition, None
                )

        summary = mart_resume_service.summary(batch_id)
        # 提交结果回写任务清单（导出任务）
        export_entry["交付时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        export_entry["交付文件"] = [f["文件名"] for f in data_files + empty_markers]
        export_entry["缺口记录"] = [f["文件名"] for f in gap_records]
        export_entry["覆盖清单"] = summary
        export_entry["最终可读范围"] = summary["readable_partitions"]
        if not gap_records:
            export_entry["status"] = "已完成"
            export_entry["pending"] = False
        else:
            export_entry["status"] = "导出中"
            export_entry["pending"] = True

        _audit_delivery(batch_id, summary, len(data_files), len(empty_markers), len(gap_records))

        return {
            "ok": True,
            "batch_id": batch_id,
            "export_id": manifest.export_id,
            "data_files": data_files,
            "empty_markers": empty_markers,
            "gap_records": gap_records,
            "summary": summary,
        }

    def get_delivered_file(self, filename: str) -> dict[str, Any] | None:
        for record in self.delivered_files:
            if record["文件名"] == filename:
                return record
        return None

    # ------------------------------------------------------------------
    # 旧版整批接口：保持兼容，既有调用方与历史结果不受影响
    # ------------------------------------------------------------------

    def export_query_results(
        self,
        entry_id: int,
        query_result_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """导出查询结果为 csv/json：读取 result_cache 和 result_mart，写入审计。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "导出任务不存在"}

        fmt = entry.get("输出格式", "csv")

        # 获取数据源
        data = _collect_export_data(entry, query_result_ids)
        if not data:
            return {"ok": False, "message": "无可导出的数据"}

        # 格式转换
        t0 = time.time()
        converted, file_size = _format_export(data, fmt)
        elapsed_ms = (time.time() - t0) * 1000

        # 生成导出文件摘要
        file_hash = hashlib.md5(converted.encode()).hexdigest()[:16]
        filename = f"export_{entry_id}_{file_hash}.{fmt}"

        # 更新任务状态
        entry["status"] = "已完成"
        entry["pending"] = False

        # 写入 result_mart（将导出结果作为数据集市项）
        _write_mart_entry(entry_id, fmt, len(data), file_size)

        # 写入审计
        _audit_export(entry_id, fmt, elapsed_ms, len(data), file_size)

        return {
            "ok": True,
            "filename": filename,
            "format": fmt,
            "row_count": len(data),
            "file_size_bytes": file_size,
            "elapsed_ms": elapsed_ms,
            "checksum": file_hash,
            "sample": converted[:200] if len(converted) > 200 else converted,
        }

    def batch_export_to_mart(
        self, mart_id: int, filter_criteria: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """批量导出到 dataset_mart：读取 result_cache 和 query_audit，写入 result_mart。"""
        mart = store.find("result_mart", mart_id)
        if mart is None:
            return {"ok": False, "message": "数据集市不存在"}

        # 收集数据
        cached = _get_all_valid_cache()
        exported = 0
        for cache_entry in cached:
            data = cache_entry.get("result_data", cache_entry.get("cached_data", []))
            if not data:
                continue
            if filter_criteria:
                data = _filter_data(data, filter_criteria)
            exported += len(data)

        mart["行数统计"] = str(exported)
        mart["最后刷新"] = time.strftime("%Y-%m-%d %H:%M:%S")
        mart["status"] = "可用"
        mart["pending"] = False

        # 更新契约
        _verify_contract(mart_id, exported > 0)

        return {
            "ok": True,
            "mart_id": mart_id,
            "exported_rows": exported,
            "cached_sources": len(cached),
        }

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _create_resume_export_task(self, partitions: list[str]) -> dict[str, Any]:
        task_no = f"DATA-RESUME-{uuid.uuid4().hex[:6]}"
        entry, missing = self.create_entry({
            "任务编号": task_no,
            "数据来源": "result_cache:partitioned",
            "筛选条件": f"partitions={','.join(partitions)}",
            "输出格式": "json",
            "字段列表": "*",
            "排序规则": "partition,id",
            "导出上限": "100000",
            "接收地址": "local://exports",
        })
        if entry is None:
            raise RuntimeError(f"创建续做导出任务失败: {missing}")
        return entry

    def _create_resume_mart(self, export_id: int) -> dict[str, Any]:
        mart_svc = ResultMartService()
        mart, missing = mart_svc.create_entry({
            "集市编号": f"RESU-R{export_id:04d}",
            "集市名称": f"分区续做集市_{export_id}",
            "数据来源": f"data_exporter:{export_id}",
            "主题域": "partitioned",
            "行数统计": "0",
            "刷新周期": "manual",
            "下游消费": "dashboard",
            "最后刷新": "-",
        })
        if mart is None:
            raise RuntimeError(f"创建续做集市失败: {missing}")
        return mart

    def _register_file(
        self,
        batch_id: str,
        partition: str,
        filename: str,
        kind: str,
        content: str,
        checksum: str,
        row_count: int,
    ) -> dict[str, Any]:
        record = {
            "id": max((int(f.get("id", 0)) for f in self.delivered_files), default=0) + 1,
            "批次号": batch_id,
            "分区": partition,
            "文件名": filename,
            "类型": kind,
            "行数": row_count,
            "校验和": checksum,
            "内容": content,
            "交付时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.delivered_files.append(record)
        return record

    @staticmethod
    def _writeback_export_task(
        entry: dict[str, Any], summary: dict[str, Any], *, interrupted: bool
    ) -> None:
        entry["覆盖清单"] = summary
        entry["零行分区"] = [z["分区"] for z in summary["zero_row_partitions"]]
        entry["缺口分区"] = [g["分区"] for g in summary["gap_partitions"]]
        entry["可读分区"] = [r["分区"] for r in summary["readable_partitions"]]
        entry["中断次数"] = summary["interruptions"]
        if interrupted:
            entry["刷新状态"] = "已中断待续做"
        elif summary["fully_covered"]:
            entry["刷新状态"] = "整批完成"
            entry["status"] = "已完成"
            entry["pending"] = False
        else:
            entry["刷新状态"] = "局部完成"


def _audit_resume_refresh(
    batch_id: str, summary: dict[str, Any], *, interrupted: bool
) -> None:
    rows = store.rows("query_audit")
    audit = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"MART_REFRESH batch={batch_id} interrupted={interrupted}",
        "执行耗时": "-",
        "扫描行数": str(summary["readable_rows"]),
        "返回行数": str(summary["readable_rows"]),
        "数据来源": "data_exporter.resume",
        "操作用户": "exporter",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "慢查询" if interrupted else "正常",
        "pending": interrupted,
        "abnormal": bool(summary["gap_partitions"]),
    }
    rows.append(audit)


def _audit_delivery(
    batch_id: str, summary: dict[str, Any], n_data: int, n_empty: int, n_gap: int
) -> None:
    rows = store.rows("query_audit")
    audit = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"MART_DELIVER batch={batch_id} data={n_data} empty={n_empty} gap={n_gap}",
        "执行耗时": "-",
        "扫描行数": str(summary["readable_rows"]),
        "返回行数": str(summary["readable_rows"]),
        "数据来源": "data_exporter.delivery",
        "操作用户": "exporter",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常" if n_gap == 0 else "慢查询",
        "pending": n_gap > 0,
        "abnormal": n_gap > 0,
    }
    rows.append(audit)


def _collect_export_data(
    entry: dict[str, Any], query_result_ids: list[int] | None
) -> list[dict[str, Any]]:
    """从多种来源收集导出数据。"""
    data: list[dict[str, Any]] = []

    # 1. 从 result_cache 读取
    caches = store.rows("result_cache")
    for cache in caches:
        if query_result_ids and cache["id"] not in query_result_ids:
            continue
        if cache.get("status") == "有效":
            d = cache.get("result_data", cache.get("cached_data", []))
            if isinstance(d, list):
                data.extend(d)

    # 2. 如果没有缓存数据，生成模拟数据
    if not data:
        for i in range(3, 15):
            data.append({
                "row_id": i + 1,
                "metric": f"metric_{random.choice(['A','B','C'])}",
                "value": random.randint(1, 1000),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

    return data


def _format_export(data: list[dict[str, Any]], fmt: str) -> tuple[str, int]:
    """将数据转换为指定格式。"""
    if fmt == "csv":
        output = io.StringIO()
        if data:
            # 用第一行的字段作为列头，extrasaction='ignore'跳过额外字段
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(
                output, fieldnames=fieldnames, extrasaction='ignore', lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(data)
        result = output.getvalue()
    elif fmt == "json":
        result = json.dumps(data, ensure_ascii=False, indent=2)
    elif fmt == "parquet_info":
        result_dict = {
            "format": "parquet",
            "rows": len(data),
            "fields": list(data[0].keys()) if data else [],
            "compressed": True,
        }
        result = json.dumps(result_dict, ensure_ascii=False)
    else:
        result = json.dumps(data, ensure_ascii=False)

    return result, len(result.encode("utf-8"))


def _write_mart_entry(export_id: int, fmt: str, row_count: int, file_size: int) -> None:
    rows = store.rows("result_mart")
    mart_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    mart = {
        "id": mart_id,
        "集市编号": f"RESU-X{export_id:04d}",
        "集市名称": f"导出任务{export_id}_{fmt}",
        "数据来源": f"data_exporter:{export_id}",
        "主题域": fmt,
        "行数统计": str(row_count),
        "刷新周期": "manual",
        "下游消费": "external",
        "最后刷新": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "可用",
        "pending": False,
        "abnormal": False,
    }
    rows.append(mart)


def _audit_export(
    export_id: int, fmt: str, elapsed_ms: float, row_count: int, file_size: int
) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"EXPORT id={export_id} fmt={fmt}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(row_count),
        "返回行数": str(row_count),
        "数据来源": "data_exporter",
        "操作用户": "exporter",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)


def _get_all_valid_cache() -> list[dict[str, Any]]:
    return [c for c in store.rows("result_cache") if c.get("status") == "有效"]


def _filter_data(data: list[dict[str, Any]], criteria: dict[str, str]) -> list[dict[str, Any]]:
    result = data
    for key, value in criteria.items():
        result = [r for r in result if str(r.get(key, "")).find(value) >= 0]
    return result


def _verify_contract(mart_id: int, has_data: bool) -> None:
    contracts = store.rows("data_contract")
    for c in contracts:
        if c.get("status") == "已承诺" and str(mart_id) in str(c.get("数据集", "")):
            if not has_data:
                c["status"] = "已违约"
