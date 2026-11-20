"""撤销未发布的重算：不再调度、下游明确等待、被引用的产物保留。"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import DrillCase, correction_story  # noqa: E402


class Cancel(DrillCase):
    def test_cancel_unpublished_recompute(self):
        self.run_all()
        self.eng.publish()
        # 旧产物位置记下来，之后必须仍在
        old = self.eng.explain("person_dim", "T1", "2026-10-05")
        old_run = old["runs"][0]
        old_artifact = Path(old_run["artifact_path"])

        self.eng.ingest_events(correction_story())
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "pending")
        # 操作员撤销这次未发布的重算
        n = self.eng.cancel("person_dim", "T1", "2026-10-05")
        self.assertEqual(n, 1)
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "cancelled")
        # 撤销的范围不会被调度
        self.run_all()
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "cancelled")
        # 下游明确等待，原因可见
        down = self.eng.explain("service_fact", "T1", "2026-10-05")
        self.assertEqual(down["status"], "waiting")
        self.assertTrue(any("cancelled" in r for r in down["reasons"]))
        # 被其他有效范围引用的中间产物保留：旧运行与产物文件都在
        self.assertTrue(old_artifact.exists())
        self.assertEqual(self.eng.get_run(old_run["run_token"])["status"], "succeeded")
        # 已发布集合不受影响
        base = self.published_dir("town_week", "T1", "2026-10-05")
        self.assertEqual((base / "LATEST").read_text(), "v1")
        # 操作员再决定重试：恢复调度并完成
        self.eng.retry("person_dim", "T1", "2026-10-05")
        self.run_all()
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "satisfied")


if __name__ == "__main__":
    unittest.main()
