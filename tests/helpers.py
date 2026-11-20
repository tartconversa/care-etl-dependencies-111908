"""测试共享设施：紧凑合成资料、协议兼容的进程内启动器、基类。

进程内启动器与 support/job_worker.py 遵守同一协议（请求文件、产物、回执、
退出码 23/24），但省掉子进程开销；回执仍落盘并走同一条 validate_receipt
校验路径。运行授权相关测试使用真实子进程，见 test_run_authorization.py。
"""
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator.engine import Engine  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def inline_launcher(request_path, out_dir, mode):
    """与演练程序同协议的进程内实现。"""
    if mode == "fail-before":
        return 23, ""
    request = json.loads(Path(request_path).read_text(encoding="utf-8"))
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(request, ensure_ascii=False, sort_keys=True).encode()
    artifact = out / "artifact.json"
    artifact.write_bytes(payload)
    receipt = {
        "run_token": request["run_token"],
        "sha256": hashlib.sha256(payload).hexdigest(),
        "artifact": str(artifact.resolve()),
        "input_fingerprints": request["input_fingerprints"],
    }
    (out / "receipt.json").write_text(json.dumps(receipt, ensure_ascii=False), encoding="utf-8")
    if mode == "exit-after":
        return 24, ""
    return 0, ""


def _delivery(did, source, town, until, ready=True, fp=None, received="2026-11-01T08:00:00+08:00"):
    return {
        "delivery_id": did, "source": source, "town": town,
        "from_date": "2026-10-01", "until_date": until,
        "fingerprint": fp or f"{source}-{town}-v1",
        "ready": ready, "received_at": received,
    }


def base_events(towns=("T1", "T2"), until="2026-10-12"):
    """齐全的基础交付：每乡镇人口/站点/服务各一份。"""
    events = []
    n = 0
    for town in towns:
        for source in ("population", "site", "service"):
            n += 1
            events.append(_delivery(f"D{n}", source, town, until))
    return events


def correction_story(town="T1", source="population", frm="2026-10-05", until="2026-10-07",
                     did="D90", cid="C91", rid="C92", fp=None):
    """未齐全交付 → 修订内容 → 重新齐全声明，三步分开到达。"""
    return [
        {"delivery_id": did, "source": source, "town": town, "from_date": frm,
         "until_date": until, "fingerprint": fp or f"{source}-{town}-v2",
         "ready": False, "received_at": "2026-11-02T08:00:00+08:00"},
        {"change_id": cid, "source": source,
         "scope": {"town": town, "from_date": frm, "until_date": until},
         "new_values": {"record_id": "P1", "eligible": False},
         "received_at": "2026-11-02T08:00:00+08:00"},
        {"change_id": rid, "source": source,
         "scope": {"town": town, "from_date": frm, "until_date": until},
         "new_values": {"complete": True},
         "received_at": "2026-11-02T09:00:00+08:00"},
    ]


class DrillCase(unittest.TestCase):
    """每个用例一套独立临时目录与状态库。"""

    commitments = []
    towns = ("T1", "T2")
    until = "2026-10-12"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.data_dir = root / "data"
        self.data_dir.mkdir()
        for name in ("jobs.json", "publication_groups.json"):
            shutil.copy(REPO / "data" / name, self.data_dir / name)
        (self.data_dir / "commitments.json").write_text(
            json.dumps(self.commitments, ensure_ascii=False), encoding="utf-8"
        )
        self.eng = Engine(
            REPO,
            state_dir=root / "state",
            outputs_dir=root / "outputs",
            data_dir=self.data_dir,
        )
        self.eng.init()
        self.eng.ingest_events(base_events(self.towns, self.until))

    def tearDown(self):
        self.tmp.cleanup()

    def run_all(self, **kw):
        kw.setdefault("launcher", inline_launcher)
        return self.eng.run(**kw)

    def status_of(self, job_id, scope, part):
        return self.eng.explain(job_id, scope, part)["status"]

    def partitions_by_status(self, status):
        self.eng.refresh()
        with self.eng._db() as conn:
            rows = conn.execute(
                "SELECT job_id,scope_key,partition FROM partitions WHERE status=?", (status,)
            ).fetchall()
        return {f"{r['job_id']}@{r['scope_key']}@{r['partition']}" for r in rows}

    def satisfied_set(self):
        return self.partitions_by_status("satisfied")

    def published_dir(self, group, scope, period):
        return self.eng.outputs_dir / "published" / group / scope / period
