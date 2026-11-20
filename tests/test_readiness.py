"""齐全声明、撤回与来源暂停：未齐全范围必须明确等待，不得跑出零需求。"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import DrillCase, correction_story  # noqa: E402


class Readiness(DrillCase):
    def test_not_ready_delivery_waits_then_correction_unblocks(self):
        d90, c91, c92 = correction_story()
        # 1. 未齐全交付到达：覆盖的两天进入等待，原因指向 D90
        self.eng.ingest_events([d90])
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "waiting")
        self.assertIn("D90", info["reasons"][0])
        self.assertIn("齐全", info["reasons"][0])
        # 2. 运行全套：等待分区不被运行，也不产生零需求产物
        self.run_all()
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "waiting")
        self.assertEqual(info["runs"], [])
        week = self.eng.explain("demand_week", "T1", "2026-10-05")
        self.assertEqual(week["status"], "waiting")
        self.assertEqual(week["runs"], [])
        # 相邻未受影响的天正常满足
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-04"), "satisfied")
        # 3. 只有修订内容、没有齐全声明时仍然等待
        self.eng.ingest_events([c91])
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "waiting")
        # 4. 齐全声明到达：转为可运行，指纹带修订标识
        self.eng.ingest_events([c92])
        self.run_all()
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "satisfied")
        version = info["inputs"][0]["version"]
        self.assertEqual(version, "population-T1-v2~C91")

    def test_retraction_after_ready_blocks_again(self):
        self.run_all()
        self.eng.publish()
        before = (self.published_dir("town_week", "T1", "2026-10-05") / "LATEST").read_text()
        # 来源撤回已齐全窗口的声明
        self.eng.ingest_events([{
            "change_id": "C99", "source": "population",
            "scope": {"town": "T1", "from_date": "2026-10-05", "until_date": "2026-10-07"},
            "new_values": {"complete": False},
            "received_at": "2026-11-03T08:00:00+08:00",
        }])
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "waiting")
        self.assertIn("撤回", info["reasons"][0])
        # 已发布集合保持旧版本，不被撤下也不出新版
        self.eng.publish()
        after = (self.published_dir("town_week", "T1", "2026-10-05") / "LATEST").read_text()
        self.assertEqual(before, after)

    def test_pause_source_gates_new_runs_but_keeps_artifacts(self):
        self.run_all()
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "satisfied")
        self.eng.pause_source("population")
        # 已满足的产物继续有效
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "satisfied")
        # 新变化需要重算时被暂停挡住，原因是暂停而不是零需求
        self.eng.ingest_events(correction_story(did="D95", cid="C95", rid="C96"))
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "waiting")
        self.assertTrue(any("暂停" in r for r in info["reasons"]))
        self.run_all()
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "waiting")
        # 恢复后即可运行
        self.eng.resume_source("population")
        self.run_all()
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "satisfied")


if __name__ == "__main__":
    unittest.main()
