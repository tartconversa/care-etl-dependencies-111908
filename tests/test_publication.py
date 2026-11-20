"""成套发布：一组作业结果相互匹配才对外可见；失败不半套出门。"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import DrillCase, correction_story, inline_launcher  # noqa: E402


def read_manifest(path):
    return json.loads((path / "v1" / "manifest.json").read_text(encoding="utf-8")) \
        if (path / "v1" / "manifest.json").exists() else None


class Publication(DrillCase):
    def test_matched_set_published_and_manifest_consistent(self):
        self.run_all()
        published = self.eng.publish()
        keys = {(p["group"], p["scope"], p["period"]) for p in published}
        # 乡镇周两套（每周每镇一套），县控周两套
        self.assertIn(("town_week", "T1", "2026-10-05"), keys)
        self.assertIn(("town_week", "T2", "2026-10-05"), keys)
        self.assertIn(("county_week", "county", "2026-10-05"), keys)

        base = self.published_dir("town_week", "T1", "2026-10-05")
        self.assertEqual((base / "LATEST").read_text(), "v1")
        manifest = read_manifest(base)
        members = manifest["members"]
        # 闭包成员相互匹配：需求周实际读取的人员日维度摘要，
        # 必须与同套发布中的人员日维度产物一致
        with self.eng._db() as conn:
            row = conn.execute(
                "SELECT request_json FROM runs WHERE run_token=?",
                (members["demand_week@T1@2026-10-05"]["run_token"],),
            ).fetchone()
        request = json.loads(row["request_json"])
        for day in ("2026-10-05", "2026-10-09"):
            key = f"job:person_dim@T1@{day}"
            self.assertEqual(
                request["input_fingerprints"][key],
                members[f"person_dim@T1@{day}"]["artifact_sha256"],
            )
        # 重复发布内容未变时不出新版本
        self.assertEqual(self.eng.publish(), [])

    def test_partial_failure_blocks_whole_set_but_not_other_towns(self):
        # T2 的服务事实失败：T2 一套不出，T1 照常发布，县控等 T2
        self.run_all(mode_for={"service_fact": "fail-before"}, scope="T2")
        self.run_all(scope="T1")
        published = self.eng.publish()
        keys = {(p["group"], p["scope"], p["period"]) for p in published}
        self.assertIn(("town_week", "T1", "2026-10-05"), keys)
        self.assertFalse(any(k[1] == "T2" for k in keys))
        self.assertFalse(any(k[0] == "county_week" for k in keys))
        self.assertFalse(self.published_dir("town_week", "T2", "2026-10-05").exists())
        # T2 已产出的人员维度不会单独外流：没有半成品发布目录
        self.assertEqual(self.status_of("person_dim", "T2", "2026-10-01"), "satisfied")
        # 修复后 T2 与县控补发
        self.eng.retry()
        self.run_all()
        published = self.eng.publish()
        keys = {(p["group"], p["scope"], p["period"]) for p in published}
        self.assertIn(("town_week", "T2", "2026-10-05"), keys)
        self.assertIn(("county_week", "county", "2026-10-05"), keys)

    def test_new_version_only_after_whole_set_refreshes(self):
        self.run_all()
        self.eng.publish()
        base = self.published_dir("town_week", "T1", "2026-10-05")
        self.assertEqual((base / "LATEST").read_text(), "v1")
        v1 = read_manifest(base)

        self.eng.ingest_events(correction_story())
        # 重算未完成时旧版本保持可读
        self.eng.run(job_id="person_dim", scope="T1", partition="2026-10-05",
                     launcher=inline_launcher)
        self.eng.publish()
        self.assertEqual((base / "LATEST").read_text(), "v1")
        # 全套重算完成后出新版本，旧版本目录保留
        self.run_all()
        self.eng.publish()
        self.assertEqual((base / "LATEST").read_text(), "v2")
        self.assertTrue((base / "v1" / "manifest.json").exists())
        v2 = json.loads((base / "v2" / "manifest.json").read_text(encoding="utf-8"))
        self.assertNotEqual(
            v1["members"]["person_dim@T1@2026-10-05"]["artifact_sha256"],
            v2["members"]["person_dim@T1@2026-10-05"]["artifact_sha256"],
        )
        # 未受影响的 T1 09-28 周不出新版本
        other = self.published_dir("town_week", "T1", "2026-09-28")
        self.assertEqual((other / "LATEST").read_text(), "v1")


if __name__ == "__main__":
    unittest.main()
