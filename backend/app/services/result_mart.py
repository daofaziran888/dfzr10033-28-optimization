"""结果集市领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

在通用骨架之外承担集市侧的分区覆盖账本：
- 每次刷新按分区提交实际覆盖明细，不覆盖旧集市已可读的数据；
- 任何可读范围都能说明已完成的实际分区，未完成分区绝不空算为可读；
- 历史批次无法证实完整时，只标记核验缺口，不破坏既有已出结果。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.mart_resume import (
    GAP_STATES,
    PARTITION_EMPTY,
    PARTITION_READY,
    TERMINAL_OK,
    mart_resume_service,
)
from app.store import store


class ResultMartService(BaseModuleService):
    MODULE = "result_mart"
    ENTITY = "数据Mart"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["集市编号", "集市名称", "数据来源", "主题域", "行数统计", "刷新周期", "下游消费", "最后刷新"]
    REQUIRED_FIELDS = ["集市编号", "集市名称", "数据来源"]
    STATUS_ORDER = ["草稿", "刷新中", "可用", "刷新失败"]
    ACTION_RULES = {"启动刷新": "刷新中", "标记可用": "可用", "标记失败": "刷新失败"}
    NEGATIVE_ACTIONS = []

    # -- 分区覆盖账本 --

    def init_coverage(
        self,
        mart: dict[str, Any],
        *,
        batch_id: str,
        export_id: int,
        partitions: list[str],
    ) -> dict[str, Any]:
        """为刷新批次初始化覆盖账本；旧账本归档保留，保证旧读者看到的数据可解释。"""
        previous = mart.get("分区覆盖")
        if previous:
            history = mart.setdefault("覆盖历史", [])
            history.append(previous)
        mart["分区覆盖"] = {
            "批次号": batch_id,
            "导出任务": export_id,
            "计划分区": list(partitions),
            "完整覆盖": False,
            "覆盖状态": "待刷新",
            "可读范围说明": "本轮尚无分区完成，可读范围为空",
            "可读分区": [],
            "可读行数": 0,
            "零行分区": [],
            "缺口分区": list(partitions),
            "分区明细": [],
            "初始化时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        # 刷新期间旧集市仍可读：状态进刷新中，但不抹掉历史可读范围
        mart["status"] = "刷新中"
        mart["pending"] = True
        return mart["分区覆盖"]

    def commit_partition(
        self,
        mart_id: int,
        *,
        batch_id: str,
        partition: str,
        state: str,
        row_count: int,
        file: str | None,
        checksum: str | None,
        evidence: str | None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """把单个分区的实际提交结果写入集市覆盖明细，并回写任务清单。"""
        mart = store.find(self.MODULE, mart_id)
        if mart is None:
            raise KeyError(f"数据集市 {mart_id} 不存在")
        coverage = mart.get("分区覆盖")
        if coverage is None or coverage.get("批次号") != batch_id:
            raise KeyError(f"集市 {mart_id} 没有批次 {batch_id} 的覆盖账本")

        commit = {
            "提交序号": len(coverage["分区明细"]) + 1,
            "分区": partition,
            "状态": state,
            "行数": row_count,
            "文件": file,
            "校验和": checksum,
            "证据": evidence,
            "错误": error,
            "提交时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        coverage["分区明细"].append(commit)
        mart_resume_service.record_commit(batch_id, partition, commit)
        self._rebuild_coverage(mart)
        return commit

    def attach_delivery_file(
        self, mart_id: int, batch_id: str, partition: str, filename: str | None
    ) -> None:
        """交付阶段把实际文件名回贴到集市分区明细，保持集市与交付口径一致。"""
        mart = store.find(self.MODULE, mart_id)
        if mart is None:
            raise KeyError(f"数据集市 {mart_id} 不存在")
        coverage = mart.get("分区覆盖")
        if coverage is None or coverage.get("批次号") != batch_id:
            raise KeyError(f"集市 {mart_id} 没有批次 {batch_id} 的覆盖账本")
        for commit in reversed(coverage.get("分区明细", [])):
            if commit["分区"] == partition:
                commit["交付文件"] = filename
                commit["交付时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
                break

    def sync_with_manifest(self, mart_id: int, batch_id: str) -> dict[str, Any]:
        """按续做清单的汇总结果重建覆盖账本，使集市与任务清单口径一致。"""
        mart = store.find(self.MODULE, mart_id)
        if mart is None:
            raise KeyError(f"数据集市 {mart_id} 不存在")
        summary = mart_resume_service.summary(batch_id)
        coverage = mart.get("分区覆盖")
        if coverage is None or coverage.get("批次号") != batch_id:
            raise KeyError(f"集市 {mart_id} 没有批次 {batch_id} 的覆盖账本")

        coverage["完整覆盖"] = summary["fully_covered"]
        coverage["可读分区"] = [item["分区"] for item in summary["readable_partitions"]]
        coverage["可读行数"] = summary["readable_rows"]
        coverage["零行分区"] = [item["分区"] for item in summary["zero_row_partitions"]]
        coverage["缺口分区"] = [item["分区"] for item in summary["gap_partitions"]]
        self._apply_coverage_status(mart, summary)
        return coverage

    def readable_range(self, mart_id: int) -> dict[str, Any]:
        """说明集市当前实际可读的分区范围，未完成数据不参与计算。"""
        mart = store.find(self.MODULE, mart_id)
        if mart is None:
            raise KeyError(f"数据集市 {mart_id} 不存在")
        coverage = mart.get("分区覆盖")
        if coverage is None:
            # 历史集市没有分区级证据：只能整体标记核验缺口
            return {
                "mart_id": mart_id,
                "完整覆盖": False,
                "覆盖状态": "核验缺口",
                "可读范围说明": "历史批次缺少分区级覆盖证据，无法证实完整可读范围",
                "可读分区": [],
                "可读行数": 0,
                "零行分区": [],
                "缺口分区": [],
            }
        return {
            "mart_id": mart_id,
            "完整覆盖": coverage["完整覆盖"],
            "覆盖状态": coverage["覆盖状态"],
            "可读范围说明": coverage["可读范围说明"],
            "可读分区": list(coverage["可读分区"]),
            "可读行数": coverage["可读行数"],
            "零行分区": list(coverage["零行分区"]),
            "缺口分区": list(coverage["缺口分区"]),
            "分区明细": list(coverage.get("分区明细", [])),
        }

    def mark_legacy_gap(self, mart_id: int) -> dict[str, Any]:
        """无法证实完整的历史批：标出核验缺口，保留既有已出结果不破坏。"""
        mart = store.find(self.MODULE, mart_id)
        if mart is None:
            raise KeyError(f"数据集市 {mart_id} 不存在")
        if mart.get("分区覆盖"):
            return {"ok": False, "message": "该集市已有分区级覆盖账本，不属于无证据历史批"}

        legacy_rows = mart.get("行数统计", "0")
        mart["分区覆盖"] = {
            "批次号": None,
            "导出任务": None,
            "计划分区": [],
            "完整覆盖": False,
            "覆盖状态": "核验缺口",
            "可读范围说明": (
                f"历史批次仅保留整批行数 {legacy_rows}，无分区级证据，"
                "已标记核验缺口，既有结果保留可读但不宣称完整覆盖"
            ),
            "可读分区": [],
            "可读行数": 0,
            "零行分区": [],
            "缺口分区": [],
            "核验缺口": True,
            "标记时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        # 保留原 status / 行数统计：兼容既有已出结果，只追加缺口标记
        mart["abnormal"] = True
        return {"ok": True, "mart_id": mart_id, "coverage": mart["分区覆盖"]}

    # -- 内部 --

    def _rebuild_coverage(self, mart: dict[str, Any]) -> None:
        coverage = mart["分区覆盖"]
        details = coverage["分区明细"]
        # 同一分区多次提交只认最新一次（失败重试后成功的场景）
        latest: dict[str, dict[str, Any]] = {}
        for commit in details:
            latest[commit["分区"]] = commit

        readable = [c for c in latest.values() if c["状态"] in TERMINAL_OK]
        ready = [c for c in latest.values() if c["状态"] == PARTITION_READY]
        zero_row = [c for c in latest.values() if c["状态"] == PARTITION_EMPTY]
        gaps = [c for c in latest.values() if c["状态"] in GAP_STATES]
        planned = set(coverage["计划分区"])
        seen = set(latest)
        missing = sorted(planned - seen)  # 尚未尝试的分区同样是缺证据缺口

        coverage["可读分区"] = [c["分区"] for c in readable]
        coverage["零行分区"] = [c["分区"] for c in zero_row]
        coverage["缺口分区"] = [c["分区"] for c in gaps] + missing
        coverage["可读行数"] = sum(int(c["行数"]) for c in ready)
        coverage["完整覆盖"] = bool(planned) and not coverage["缺口分区"]

        if coverage["完整覆盖"]:
            coverage["覆盖状态"] = "整批完整"
            coverage["可读范围说明"] = (
                f"全部 {len(planned)} 个分区已完成，可读行数 {coverage['可读行数']}"
                + (f"，其中 {len(zero_row)} 个分区经扫描确认零行" if zero_row else "")
            )
            # 整批完整后才刷新权威行数；局部刷新期间保留历史行数，不空算
            mart["行数统计"] = str(coverage["可读行数"])
            mart["status"] = "可用"
            mart["pending"] = False
            mart["abnormal"] = False
            mart["最后刷新"] = time.strftime("%Y-%m-%d %H:%M:%S")
        elif readable:
            coverage["覆盖状态"] = "局部可读"
            coverage["可读范围说明"] = (
                f"仅 {len(readable)}/{len(planned)} 个分区完成可读"
                f"（可读行数 {coverage['可读行数']}），"
                f"缺口 {len(coverage['缺口分区'])} 个：{'、'.join(coverage['缺口分区'])}"
            )
            # 局部立即可读但显式标注非完整，避免把局部冒充整批可用
            mart["status"] = "可用"
            mart["pending"] = True
        else:
            coverage["覆盖状态"] = "无可用分区"
            coverage["可读范围说明"] = "本轮没有任何分区完成，集市无可读数据"
            mart["status"] = "刷新失败"
            mart["pending"] = True
        coverage["更新时间"] = time.strftime("%Y-%m-%d %H:%M:%S")

    def _apply_coverage_status(self, mart: dict[str, Any], summary: dict[str, Any]) -> None:
        coverage = mart["分区覆盖"]
        if summary["fully_covered"]:
            coverage["完整覆盖"] = True
            coverage["覆盖状态"] = "整批完整"
            coverage["可读范围说明"] = (
                f"全部 {summary['total_partitions']} 个分区已完成，"
                f"可读行数 {summary['readable_rows']}"
                + (f"，其中 {len(summary['zero_row_partitions'])} 个分区经扫描确认零行"
                   if summary["zero_row_partitions"] else "")
            )
            mart["行数统计"] = str(summary["readable_rows"])
            mart["status"] = "可用"
            mart["pending"] = False
            mart["abnormal"] = False
            mart["最后刷新"] = time.strftime("%Y-%m-%d %H:%M:%S")
        elif summary["completed_partitions"]:
            coverage["完整覆盖"] = False
            coverage["覆盖状态"] = "局部可读"
            coverage["可读范围说明"] = (
                f"仅 {len(summary['completed_partitions'])}/{summary['total_partitions']} "
                f"个分区完成可读（可读行数 {summary['readable_rows']}），"
                f"缺口 {len(summary['gap_partitions'])} 个："
                f"{'、'.join(g['分区'] for g in summary['gap_partitions'])}"
            )
            mart["status"] = "可用"
            mart["pending"] = True
        else:
            coverage["完整覆盖"] = False
            coverage["覆盖状态"] = "无可用分区"
            coverage["可读范围说明"] = "本轮没有任何分区完成，集市无可读数据"
            mart["status"] = "刷新失败"
            mart["pending"] = True
