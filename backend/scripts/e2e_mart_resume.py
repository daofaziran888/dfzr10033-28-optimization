"""端到端手工验证脚本：三分区批次，一个零行、一个首次读取失败、中断后续做。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)
P = ["dt=2026-11-19", "dt=2026-11-20", "dt=2026-11-21"]
P1, P2, P3 = P

print("=" * 70)
print("第 1 轮刷新：P1 成功零行、P2 首次读取失败，处理 1 个分区后中断")
print("=" * 70)
r = client.post("/api/pipelines/export-to-mart-refresh", params={
    "partitions": ",".join(P),
    "empty_partitions": P1,
    "fail_partitions": P2,
    "fail_after": 1,
})
body = r.json()
batch_id = body["batch_id"]
mart_id = body["summary"]["mart_id"]
print(json.dumps(body, ensure_ascii=False, indent=2))

print("=" * 70)
print("中断后立即查询集市可读范围：只能说明 P1（零行），P2/P3 是缺口")
print("=" * 70)
r = client.get(f"/api/pipelines/mart-coverage/{mart_id}")
print(json.dumps(r.json()["readable_range"], ensure_ascii=False, indent=2))

print("=" * 70)
print("第 2 轮续做：P2 仍失败一次后中断（fail_after=1），验证只重建缺口")
print("=" * 70)
r = client.post("/api/pipelines/export-to-mart-resume", params={
    "mart_id": mart_id, "batch_id": batch_id,
    "fail_partitions": P2, "fail_after": 1,
})
print(json.dumps(r.json(), ensure_ascii=False, indent=2)[:1200])

print("=" * 70)
print("第 3 轮续做：故障恢复，无失败注入，跑完剩余缺口")
print("=" * 70)
r = client.post("/api/pipelines/export-to-mart-resume", params={
    "mart_id": mart_id, "batch_id": batch_id,
})
final = r.json()
print("processed_now =", final["processed_now"], "| skipped =", final["skipped_partitions"])
print("counts =", json.dumps(final["summary"]["counts"], ensure_ascii=False))
print("零行分区 =", [z["分区"] for z in final["summary"]["zero_row_partitions"]])

print("=" * 70)
print("按集市续做覆盖清单交付文件")
print("=" * 70)
r = client.post("/api/pipelines/export-to-mart-deliver", params={"batch_id": batch_id})
delivery = r.json()
for key in ("data_files", "empty_markers", "gap_records"):
    print(key, "->", [f["文件名"] for f in delivery[key]])
print("零行标记内容：")
print(json.dumps(json.loads(delivery["empty_markers"][0]["内容"]), ensure_ascii=False, indent=2))

print("=" * 70)
print("最终集市可读范围")
print("=" * 70)
r = client.get(f"/api/pipelines/mart-coverage/{mart_id}")
print(json.dumps(r.json()["readable_range"], ensure_ascii=False, indent=2))

print("=" * 70)
print("历史集市核验缺口（种子集市 id=1）")
print("=" * 70)
r = client.post("/api/pipelines/mart-legacy-gap/1")
print(json.dumps(r.json(), ensure_ascii=False, indent=2))
