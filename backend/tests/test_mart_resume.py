"""集市分区续做测试：零行/失败区分、中断续做、按清单交付、历史核验缺口。"""
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store
from app.services.data_exporter import DataExporterService
from app.services.mart_resume import (
    PARTITION_EMPTY,
    PARTITION_FAILED,
    PARTITION_READY,
    CachePartitionReader,
    MartRefreshInterrupted,
    mart_resume_service,
)
from app.services.result_mart import ResultMartService

client = TestClient(app)

P1, P2, P3 = "dt=2026-11-19", "dt=2026-11-20", "dt=2026-11-21"
THREE_PARTITIONS = [P1, P2, P3]


@pytest.fixture
def exporter():
    return DataExporterService()


@pytest.fixture
def mart_svc():
    return ResultMartService()


def _state(batch_id: str, partition: str):
    return mart_resume_service.get_manifest(batch_id).states[partition]


class TestInterruptedResumeWithThreePartitions:
    """一批三个分区：一个确实无行、一个首次读取失败，中断后重试只重建失败片段。"""

    def test_interrupt_then_resume_only_rebuilds_failed_fragments(self, exporter, mart_svc):
        reader = CachePartitionReader()
        reader.mark_empty(P1)                 # P1 经扫描确实无行
        reader.simulate_failure(P2, times=1)  # P2 首次读取失败

        # 第一轮：处理 1 个分区后中断，P1 以成功零行落检查点
        with pytest.raises(MartRefreshInterrupted) as exc_info:
            exporter.refresh_mart_partitions(
                mart_id=0, partitions=THREE_PARTITIONS, reader=reader, fail_after=1
            )
        batch_id = exc_info.value.batch_id
        assert _state(batch_id, P1).state == PARTITION_EMPTY
        assert _state(batch_id, P1).evidence  # 零行是有证据的成功
        assert _state(batch_id, P2).state == "待处理"
        assert _state(batch_id, P3).state == "待处理"
        assert reader.read_counts[P1] == 1

        # 第二轮续做：P2 首次仍按 reader 状态失败一次（计数在同一 reader 内保留）
        with pytest.raises(MartRefreshInterrupted):
            exporter.resume_mart_refresh(
                mart_id=exc_info.value.summary["mart_id"],
                batch_id=batch_id, reader=reader, fail_after=1,
            )
        assert _state(batch_id, P2).state == PARTITION_FAILED
        assert _state(batch_id, P2).error is not None
        assert _state(batch_id, P2).evidence is None  # 失败 = 缺证据
        # P1 是成功终态，续做时不重新扫描
        assert reader.read_counts[P1] == 1
        assert reader.read_counts[P2] == 1

        # 第三轮：用新 reader（故障已恢复），只重建 P2、P3 两个失败/未处理片段
        healthy = CachePartitionReader()
        healthy.mark_empty(P1)  # 即便登记零行也不会被重新读取
        result = exporter.resume_mart_refresh(
            mart_id=exc_info.value.summary["mart_id"],
            batch_id=batch_id, reader=healthy,
        )
        assert result["ok"] is True
        assert result["processed_now"] == 2
        assert result["skipped_partitions"] == [P1]
        assert healthy.read_counts.get(P1, 0) == 0  # 成功零行分区不再扫描
        assert healthy.read_counts[P2] == 1
        assert healthy.read_counts[P3] == 1

        summary = result["summary"]
        assert summary["fully_covered"] is True
        # 成功零行与取数失败历史可分别追溯：P1 零行、P2 曾失败后恢复
        zero_rows = {z["分区"]: z for z in summary["zero_row_partitions"]}
        assert list(zero_rows) == [P1]
        assert zero_rows[P1]["证据"]
        assert _state(batch_id, P1).state == PARTITION_EMPTY
        assert _state(batch_id, P2).state == PARTITION_READY
        assert _state(batch_id, P2).attempts == 2  # 一次失败 + 一次成功
        assert _state(batch_id, P3).state == PARTITION_READY
        assert summary["counts"][PARTITION_FAILED] == 0
        assert summary["readable_rows"] > 0

        # 集市侧可读范围能说明实际完成的分区
        mart_id = summary["mart_id"]
        readable = mart_svc.readable_range(mart_id)
        assert readable["完整覆盖"] is True
        assert readable["覆盖状态"] == "整批完整"
        assert set(readable["可读分区"]) == {P1, P2, P3}
        assert readable["零行分区"] == [P1]
        assert readable["缺口分区"] == []
        assert P1 in readable["可读范围说明"] or "3 个分区" in readable["可读范围说明"]

    def test_zero_row_and_failure_are_distinct_records(self, exporter):
        """成功零行（有证据）与取数失败（缺证据）分别记录，不能混为一谈。"""
        reader = CachePartitionReader()
        reader.mark_empty(P1)
        reader.simulate_failure(P2, times=1)  # 故障不恢复：P2 始终缺证据

        with pytest.raises(MartRefreshInterrupted):
            exporter.refresh_mart_partitions(
                mart_id=0, partitions=THREE_PARTITIONS, reader=reader, fail_after=3
            )
        # fail_after=3 时三个分区都处理完才触发中断，清单里状态齐全
        manifest = list(mart_resume_service._manifests.values())[-1]
        batch_id = manifest.batch_id

        s1, s2, s3 = (_state(batch_id, p) for p in THREE_PARTITIONS)
        assert s1.state == PARTITION_EMPTY and s1.evidence and s1.row_count == 0
        assert s2.state == PARTITION_FAILED and s2.evidence is None and s2.error
        assert s3.state == PARTITION_READY and s3.row_count > 0

        summary = mart_resume_service.summary(batch_id)
        assert [z["分区"] for z in summary["zero_row_partitions"]] == [P1]
        gap = {g["分区"]: g for g in summary["gap_partitions"]}
        assert list(gap) == [P2]
        assert gap[P2]["状态"] == PARTITION_FAILED
        assert gap[P2]["证据"] is None
        assert summary["fully_covered"] is False


class TestPartialCoverageWhileOldMartReadable:
    def test_partial_refresh_exposes_only_completed_partitions(self, exporter, mart_svc):
        """一轮只落地部分分区时，集市标局部可读，未完成分区不空算。"""
        reader = CachePartitionReader()
        reader.mark_empty(P1)
        with pytest.raises(MartRefreshInterrupted):
            exporter.refresh_mart_partitions(
                mart_id=0, partitions=THREE_PARTITIONS, reader=reader, fail_after=1
            )
        manifest = list(mart_resume_service._manifests.values())[-1]
        mart_id = manifest.mart_id

        readable = mart_svc.readable_range(mart_id)
        assert readable["完整覆盖"] is False
        assert readable["覆盖状态"] == "局部可读"
        assert readable["可读分区"] == [P1]            # 零行分区也是已完成可读分区
        assert readable["可读行数"] == 0                # 零行不空算行数
        assert set(readable["缺口分区"]) == {P2, P3}
        # 集市仍可读（有已完成分区），但显式 pending，不冒充整批可用
        mart = store.find("result_mart", mart_id)
        assert mart["status"] == "可用" and mart["pending"] is True
        # 权威行数在局部阶段不被改写为 0，避免空算未完成数据
        assert mart["行数统计"] == "0"

    def test_failed_partition_does_not_pollute_readable_range(self, exporter, mart_svc):
        reader = CachePartitionReader()
        reader.simulate_failure(P2, times=99)  # 持续失败
        result = self._run_full(exporter, reader)
        summary = result["summary"]
        assert summary["fully_covered"] is False
        assert set(summary["completed_partitions"]) == {P1, P3}
        assert [g["分区"] for g in summary["gap_partitions"]] == [P2]

        readable = mart_svc.readable_range(summary["mart_id"])
        assert set(readable["可读分区"]) == {P1, P3}
        assert readable["缺口分区"] == [P2]
        mart = store.find("result_mart", summary["mart_id"])
        assert mart["status"] == "可用" and mart["pending"] is True

    @staticmethod
    def _run_full(exporter, reader):
        try:
            return exporter.refresh_mart_partitions(
                mart_id=0, partitions=THREE_PARTITIONS, reader=reader
            )
        except MartRefreshInterrupted as exc:
            return exporter.resume_mart_refresh(
                mart_id=exc.summary["mart_id"], batch_id=exc.batch_id, reader=reader
            )


class TestDeliverByCoverageManifest:
    def test_delivery_separates_data_empty_and_gap_files(self, exporter, mart_svc):
        """按续做覆盖清单交付：数据文件、零行标记、缺口记录三类分开且原因可追溯。"""
        reader = CachePartitionReader()
        reader.mark_empty(P1)
        reader.simulate_failure(P2, times=1)
        with pytest.raises(MartRefreshInterrupted):
            exporter.refresh_mart_partitions(
                mart_id=0, partitions=THREE_PARTITIONS, reader=reader, fail_after=3
            )
        manifest = list(mart_resume_service._manifests.values())[-1]
        batch_id = manifest.batch_id
        export_id = manifest.export_id

        delivery = exporter.deliver_partition_files(batch_id)
        # P3 数据文件、P1 零行标记、P2 缺口记录
        assert [f["分区"] for f in delivery["data_files"]] == [P3]
        assert [f["分区"] for f in delivery["empty_markers"]] == [P1]
        assert [f["分区"] for f in delivery["gap_records"]] == [P2]

        empty_file = exporter.get_delivered_file(delivery["empty_markers"][0]["文件名"])
        assert '"row_count": 0' in empty_file["内容"]
        assert PARTITION_EMPTY in empty_file["内容"]
        assert _state(batch_id, P1).evidence in empty_file["内容"]

        gap_file = exporter.get_delivered_file(delivery["gap_records"][0]["文件名"])
        assert '"gap": true' in gap_file["内容"]
        assert "读取失败" in gap_file["内容"]

        # 集市写入结果已回写任务清单
        export_entry = store.find("data_exporter", export_id)
        delivered_names = export_entry["交付文件"]
        assert any(f["文件名"] in delivered_names for f in delivery["data_files"])
        assert any(P3 in name for name in delivered_names)
        assert export_entry["缺口记录"] == [delivery["gap_records"][0]["文件名"]]
        assert export_entry["覆盖清单"]["gap_partitions"][0]["分区"] == P2
        assert export_entry["pending"] is True  # 还有缺口，任务不能算完成

    def test_full_batch_delivery_closes_task(self, exporter):
        reader = CachePartitionReader()
        reader.mark_empty(P1)
        result = exporter.refresh_mart_partitions(
            mart_id=0, partitions=THREE_PARTITIONS, reader=reader
        )
        batch_id = result["batch_id"]
        delivery = exporter.deliver_partition_files(batch_id)
        assert delivery["gap_records"] == []
        assert len(delivery["data_files"]) == 2
        assert len(delivery["empty_markers"]) == 1
        export_entry = store.find("data_exporter", result["export_id"])
        assert export_entry["status"] == "已完成"
        assert export_entry["pending"] is False
        readable_names = {item["分区"] for item in export_entry["最终可读范围"]}
        assert readable_names == {P1, P2, P3}


class TestLegacyMartVerificationGap:
    def test_legacy_batch_without_evidence_is_flagged_but_preserved(self, mart_svc):
        """兼容既有已出结果：无分区证据的历史批只标核验缺口，不破坏原数据。"""
        legacy, missing = mart_svc.create_entry({
            "集市编号": "RESU-LEGACY-GAP",
            "集市名称": "无证据历史集市",
            "数据来源": "legacy_export",
            "主题域": "history",
            "行数统计": "888",
            "刷新周期": "manual",
            "下游消费": "dashboard",
            "最后刷新": "2026-10-01 00:00:00",
        })
        assert legacy is not None and not missing
        legacy_id = legacy["id"]
        original_rows = legacy["行数统计"]
        original_status = legacy["status"]
        assert "分区覆盖" not in legacy

        result = mart_svc.mark_legacy_gap(legacy_id)
        assert result["ok"] is True
        coverage = store.find("result_mart", legacy_id)["分区覆盖"]
        assert coverage["覆盖状态"] == "核验缺口"
        assert coverage["完整覆盖"] is False
        assert original_rows in coverage["可读范围说明"]
        # 既有结果保留
        assert store.find("result_mart", legacy_id)["行数统计"] == original_rows
        assert store.find("result_mart", legacy_id)["status"] == original_status

        readable = mart_svc.readable_range(legacy_id)
        assert readable["覆盖状态"] == "核验缺口"
        assert readable["完整覆盖"] is False

    def test_cannot_mark_evidenced_mart_as_legacy_gap(self, exporter, mart_svc):
        reader = CachePartitionReader()
        result = exporter.refresh_mart_partitions(
            mart_id=0, partitions=THREE_PARTITIONS, reader=reader
        )
        response = mart_svc.mark_legacy_gap(result["mart_id"])
        assert response["ok"] is False


class TestBackwardCompatibility:
    def test_legacy_export_pipeline_still_works(self):
        resp = client.post(
            "/api/pipelines/export-to-mart",
            params={"export_no": "LEGACY-COMPAT-001", "fmt": "json"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] and body["export"]["ok"]

    def test_refresh_resume_and_coverage_api_end_to_end(self):
        # 第一轮中断：P1 零行成功、P2 首次失败
        resp = client.post(
            "/api/pipelines/export-to-mart-refresh",
            params={
                "partitions": ",".join(THREE_PARTITIONS),
                "empty_partitions": P1,
                "fail_partitions": P2,
                "fail_after": 3,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["interrupted"] is True
        batch_id = body["batch_id"]
        mart_id = body["summary"]["mart_id"]

        # 续做（不带失败注入，读取已恢复）：首轮三个分区均已处理，只剩 P2 缺口需重建
        resp = client.post(
            "/api/pipelines/export-to-mart-resume",
            params={"mart_id": mart_id, "batch_id": batch_id},
        )
        assert resp.status_code == 200
        resume = resp.json()
        assert resume["ok"] and resume["summary"]["fully_covered"]
        assert resume["skipped_partitions"] == [P1, P3]

        # 可读范围接口如实说明完成分区
        resp = client.get(f"/api/pipelines/mart-coverage/{mart_id}")
        coverage = resp.json()["readable_range"]
        assert set(coverage["可读分区"]) == {P1, P2, P3}
        assert coverage["零行分区"] == [P1]

        # 交付接口三类文件齐全
        resp = client.post(
            "/api/pipelines/export-to-mart-deliver",
            params={"batch_id": batch_id},
        )
        delivery = resp.json()
        assert len(delivery["data_files"]) == 2
        assert len(delivery["empty_markers"]) == 1
        assert delivery["gap_records"] == []
