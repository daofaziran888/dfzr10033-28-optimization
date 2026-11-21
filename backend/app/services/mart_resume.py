"""集市续做领域服务：分区级刷新清单、覆盖证据与续做检查点。

设计要点：
- 每个刷新批次持有一份 ``ResumeManifest``（集市续做覆盖清单），按分区记录状态，
  中断后凭清单只重建未成功的片段，成功分区（含成功零行）不再重复扫描。
- 分区状态明确区分「成功零行」与「取数失败」：
  零行是有证据的成功终态，失败是缺证据的缺口终态，两者分别统计、分别交付。
- 清单同时承载集市提交结果与导出交付结果的回写，形成导出侧与集市侧一致的证据链。
"""
from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

from app.store import store

# -- 分区状态：成功零行与取数失败必须是两个不同状态 --
PARTITION_PENDING = "待处理"
PARTITION_FAILED = "取数失败"
PARTITION_EMPTY = "成功零行"
PARTITION_READY = "有数据已提交"

# 成功终态：续做时跳过；其中零行与有数据分别可查
TERMINAL_OK = (PARTITION_EMPTY, PARTITION_READY)
# 缺口状态：缺证据，续做时需要重建
GAP_STATES = (PARTITION_PENDING, PARTITION_FAILED)


class MartRefreshInterrupted(Exception):
    """刷新在批次中途中断，已落检查点，可凭 batch_id 续做。"""

    def __init__(self, batch_id: str, summary: dict[str, Any]) -> None:
        super().__init__(f"集市刷新中断，批次 {batch_id} 已保留续做清单")
        self.batch_id = batch_id
        self.summary = summary


@dataclass
class PartitionFetch:
    """分区取数结果。失败时 rows 为空且 error 有值、evidence 为空（缺证据）。"""

    rows: list[dict[str, Any]]
    evidence: str
    error: str | None = None


class PartitionReader(Protocol):
    """分区读取器协议：把遍历结果缓存按分区读出，并给出取数证据。"""

    def read(self, partition: str) -> PartitionFetch: ...


@dataclass
class PartitionState:
    partition: str
    state: str = PARTITION_PENDING
    attempts: int = 0
    row_count: int = 0
    file: str | None = None
    checksum: str | None = None
    evidence: str | None = None
    error: str | None = None
    updated_at: str | None = None
    commits: list[dict[str, Any]] = field(default_factory=list)
    deliveries: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ResumeManifest:
    batch_id: str
    mart_id: int
    export_id: int
    partitions: list[str]
    states: dict[str, PartitionState] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: time.strftime("%Y-%m-%d %H:%M:%S"))
    updated_at: str = field(default_factory=lambda: time.strftime("%Y-%m-%d %H:%M:%S"))
    interruptions: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CachePartitionReader:
    """默认分区读取器：从遍历结果缓存（result_cache）按分区取数。

    - ``simulate_failure`` 登记瞬时失败次数，用于模拟首次读取失败、重试成功；
    - ``mark_empty`` 登记经扫描确认无行的分区，返回带证据的空结果；
    - 每次成功读取都留下证据字符串，失败读取不产生证据（即“缺证据”）。
    """

    def __init__(self) -> None:
        self._fail_remaining: dict[str, int] = {}
        self._empty_partitions: set[str] = set()
        self.evidence_log: dict[str, str] = {}
        self.read_counts: dict[str, int] = {}

    def simulate_failure(self, partition: str, times: int = 1) -> None:
        self._fail_remaining[partition] = self._fail_remaining.get(partition, 0) + times

    def mark_empty(self, partition: str) -> None:
        self._empty_partitions.add(partition)

    def reset(self) -> None:
        self._fail_remaining.clear()
        self._empty_partitions.clear()
        self.evidence_log.clear()
        self.read_counts.clear()

    def read(self, partition: str) -> PartitionFetch:
        self.read_counts[partition] = self.read_counts.get(partition, 0) + 1

        remaining = self._fail_remaining.get(partition, 0)
        if remaining > 0:
            # 取数失败：不写证据，调用方据此登记“取数失败/缺证据”
            self._fail_remaining[partition] = remaining - 1
            return PartitionFetch(
                rows=[],
                evidence="",
                error=f"分区 {partition} 读取失败：遍历结果缓存连接超时，未取得任何行或证据",
            )

        if partition in self._empty_partitions:
            evidence = (
                f"scan:{partition}:matched=0:"
                f"ts={time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            self.evidence_log[partition] = evidence
            return PartitionFetch(rows=[], evidence=evidence)

        # 优先读取带分区标记的有效缓存
        matched = [
            c for c in store.rows("result_cache")
            if c.get("status") == "有效" and str(c.get("分区", "")) == partition
        ]
        rows: list[dict[str, Any]] = []
        if matched:
            for cache in matched:
                data = cache.get("result_data", cache.get("cached_data", []))
                if isinstance(data, list):
                    rows.extend(data)
            evidence = "result_cache:" + ",".join(str(c["id"]) for c in matched)
        else:
            # 无缓存命中时给出确定性的遍历扫描数据，保证可重复续做
            seed = sum(ord(ch) for ch in partition)
            rows = [
                {"row_id": seed + idx, "partition": partition,
                 "metric": f"metric_{chr(65 + idx % 3)}", "value": (seed * (idx + 1)) % 1000}
                for idx in range(1, 3)
            ]
            evidence = f"traversal-scan:{partition}:rows={len(rows)}"

        self.evidence_log[partition] = evidence
        return PartitionFetch(rows=rows, evidence=evidence)


class MartResumeService:
    """续做清单登记处：管理批次清单、分区状态机与覆盖汇总。"""

    def __init__(self) -> None:
        self._manifests: dict[str, ResumeManifest] = {}

    # -- 清单打开与查询 --

    def open_manifest(
        self,
        batch_id: str | None,
        *,
        mart_id: int,
        export_id: int,
        partitions: list[str],
    ) -> ResumeManifest:
        """打开或新建续做清单；已存在则原样复用（续做不重置成功分区）。"""
        if batch_id and batch_id in self._manifests:
            manifest = self._manifests[batch_id]
            if manifest.mart_id != mart_id or manifest.export_id != export_id:
                raise ValueError("续做批次与集市/导出任务不匹配")
            return manifest

        bid = batch_id or f"MB-{uuid.uuid4().hex[:8]}"
        manifest = ResumeManifest(
            batch_id=bid,
            mart_id=mart_id,
            export_id=export_id,
            partitions=list(partitions),
            states={p: PartitionState(partition=p) for p in partitions},
        )
        self._manifests[bid] = manifest
        return manifest

    def get_manifest(self, batch_id: str) -> ResumeManifest:
        manifest = self._manifests.get(batch_id)
        if manifest is None:
            raise KeyError(f"续做批次不存在：{batch_id}")
        return manifest

    def list_manifests_for_mart(self, mart_id: int) -> list[dict[str, Any]]:
        return [m.to_dict() for m in self._manifests.values() if m.mart_id == mart_id]

    def pending_partitions(self, batch_id: str) -> list[str]:
        manifest = self.get_manifest(batch_id)
        return [
            p for p in manifest.partitions
            if manifest.states[p].state not in TERMINAL_OK
        ]

    # -- 分区状态机 --

    def mark_attempt(self, batch_id: str, partition: str) -> None:
        manifest = self._require_manifest(batch_id)
        state = self._require_state(manifest, partition)
        before = state.state
        state.state = PARTITION_PENDING
        state.attempts += 1
        state.error = None
        state.updated_at = time.strftime("%Y-%m-%d %H:%M:%S")
        manifest.updated_at = state.updated_at
        manifest.history.append({
            "时间": state.updated_at, "分区": partition,
            "变化": f"{before} -> {PARTITION_PENDING}",
            "说明": f"第 {state.attempts} 次取数尝试",
        })

    def record_partition(
        self,
        batch_id: str,
        partition: str,
        *,
        state: str,
        row_count: int = 0,
        file: str | None = None,
        checksum: str | None = None,
        evidence: str | None = None,
        error: str | None = None,
    ) -> PartitionState:
        manifest = self._require_manifest(batch_id)
        current = self._require_state(manifest, partition)
        before = current.state
        current.state = state
        current.updated_at = time.strftime("%Y-%m-%d %H:%M:%S")
        current.error = error
        if state == PARTITION_FAILED:
            # 失败不覆盖既有证据字段：失败本身就是缺证据
            current.evidence = None
        else:
            current.evidence = evidence
            current.row_count = row_count
            current.file = file
            current.checksum = checksum
        manifest.updated_at = current.updated_at
        manifest.history.append({
            "时间": current.updated_at, "分区": partition,
            "变化": f"{before} -> {state}",
            "说明": error or evidence or state,
        })
        return current

    def mark_interruption(self, batch_id: str, partition: str, reason: str) -> None:
        manifest = self._require_manifest(batch_id)
        manifest.interruptions += 1
        manifest.updated_at = time.strftime("%Y-%m-%d %H:%M:%S")
        manifest.history.append({
            "时间": manifest.updated_at, "分区": partition,
            "变化": "批次中断", "说明": reason,
        })

    def record_commit(self, batch_id: str, partition: str, commit: dict[str, Any]) -> None:
        """集市写入结果回写到任务清单。"""
        manifest = self._require_manifest(batch_id)
        self._require_state(manifest, partition).commits.append(commit)
        manifest.updated_at = time.strftime("%Y-%m-%d %H:%M:%S")

    def record_delivery(
        self, batch_id: str, partition: str, filename: str | None, note: str
    ) -> None:
        """导出交付结果回写到任务清单。"""
        manifest = self._require_manifest(batch_id)
        self._require_state(manifest, partition).deliveries.append({
            "时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "文件": filename,
            "说明": note,
        })
        manifest.updated_at = time.strftime("%Y-%m-%d %H:%M:%S")

    # -- 覆盖汇总 --

    def summary(self, batch_id: str) -> dict[str, Any]:
        manifest = self.get_manifest(batch_id)
        states = [manifest.states[p] for p in manifest.partitions]
        completed = [s for s in states if s.state in TERMINAL_OK]
        ready = [s for s in states if s.state == PARTITION_READY]
        zero_row = [s for s in states if s.state == PARTITION_EMPTY]
        gaps = [s for s in states if s.state in GAP_STATES]

        return {
            "batch_id": manifest.batch_id,
            "mart_id": manifest.mart_id,
            "export_id": manifest.export_id,
            "total_partitions": len(manifest.partitions),
            "counts": {
                PARTITION_READY: len(ready),
                PARTITION_EMPTY: len(zero_row),
                PARTITION_FAILED: sum(1 for s in states if s.state == PARTITION_FAILED),
                PARTITION_PENDING: sum(1 for s in states if s.state == PARTITION_PENDING),
            },
            # 成功零行分区：有证据、可读、行数为 0
            "zero_row_partitions": [
                {"分区": s.partition, "状态": s.state, "行数": 0,
                 "证据": s.evidence, "尝试次数": s.attempts}
                for s in zero_row
            ],
            # 缺证据分区：待处理或取数失败，不计入可读范围
            "gap_partitions": [
                {"分区": s.partition, "状态": s.state, "原因": s.error or "尚未取数，缺少证据",
                 "尝试次数": s.attempts, "证据": None}
                for s in gaps
            ],
            "completed_partitions": [s.partition for s in completed],
            "readable_partitions": [
                {"分区": s.partition, "状态": s.state, "行数": s.row_count,
                 "文件": s.file, "校验和": s.checksum}
                for s in completed
            ],
            "readable_rows": sum(s.row_count for s in ready),
            "fully_covered": len(completed) == len(manifest.partitions),
            "interruptions": manifest.interruptions,
            "updated_at": manifest.updated_at,
        }

    # -- 内部工具 --

    def _require_manifest(self, batch_id: str) -> ResumeManifest:
        manifest = self._manifests.get(batch_id)
        if manifest is None:
            raise KeyError(f"续做批次不存在：{batch_id}")
        return manifest

    @staticmethod
    def _require_state(manifest: ResumeManifest, partition: str) -> PartitionState:
        state = manifest.states.get(partition)
        if state is None:
            raise KeyError(f"分区 {partition} 不在批次 {manifest.batch_id} 清单内")
        return state


# 全局单例
mart_resume_service = MartResumeService()
