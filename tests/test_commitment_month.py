"""承诺月：跨月承诺影响其覆盖的后续月份；跨月需求周只算一份分区。"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import DrillCase  # noqa: E402


class CommitmentMonth(DrillCase):
    towns = ("T1",)
    until = "2026-12-07"
    commitments = [
        {"commitment_id": "K1", "town": "T1",
         "from_date": "2026-10-01", "until_date": "2026-12-01", "seats": 30},
    ]

    def test_cross_month_commitment_and_shared_week(self):
        self.run_all()
        # 承诺覆盖 10、11 两个自然月，两个月分区都满足
        self.assertEqual(self.status_of("commitment_month", "T1", "2026-10"), "satisfied")
        self.assertEqual(self.status_of("commitment_month", "T1", "2026-11"), "satisfied")

        # 跨月需求周 2026-10-26 被两个月份共同引用，但只有一份分区
        oct_info = self.eng.explain("commitment_month", "T1", "2026-10")
        nov_info = self.eng.explain("commitment_month", "T1", "2026-11")
        oct_weeks = {d["key"] for d in oct_info["inputs"] if d["kind"] == "job"
                     and d["job"] == "demand_week"}
        nov_weeks = {d["key"] for d in nov_info["inputs"] if d["kind"] == "job"
                     and d["job"] == "demand_week"}
        shared = oct_weeks & nov_weeks
        self.assertEqual(shared, {"job:demand_week@T1@2026-10-26"})
        with self.eng._db() as conn:
            rows = conn.execute(
                """SELECT COUNT(*) AS n FROM partitions
                   WHERE job_id='demand_week' AND scope_key='T1' AND partition='2026-10-26'"""
            ).fetchone()
        self.assertEqual(rows["n"], 1)

        # 修改承诺（跨月）：10 月与 11 月的承诺月同时失效
        before = self.satisfied_set()
        commitments = [
            {"commitment_id": "K1", "town": "T1",
             "from_date": "2026-10-01", "until_date": "2026-12-01", "seats": 36},
        ]
        (self.data_dir / "commitments.json").write_text(
            json.dumps(commitments, ensure_ascii=False), encoding="utf-8"
        )
        stale = before - self.satisfied_set()
        self.assertEqual(stale, {
            "commitment_month@T1@2026-10",
            "commitment_month@T1@2026-11",
        })
        # 重跑后两个月都恢复
        self.run_all()
        self.assertEqual(self.status_of("commitment_month", "T1", "2026-10"), "satisfied")
        self.assertEqual(self.status_of("commitment_month", "T1", "2026-11"), "satisfied")

    def test_month_reads_intersecting_weeks_and_month_site_days(self):
        self.run_all()
        info = self.eng.explain("commitment_month", "T1", "2026-11")
        keys = {d["key"] for d in info["inputs"]}
        # 读取与 11 月相交的需求周
        self.assertIn("job:demand_week@T1@2026-10-26", keys)
        self.assertIn("job:demand_week@T1@2026-11-23", keys)
        # 读取当月服务点日维度
        self.assertIn("job:site_dim@T1@2026-11-15", keys)
        self.assertNotIn("job:site_dim@T1@2026-10-31", keys)
        # 承诺来源指纹来自有效段
        src = [d for d in info["inputs"] if d["kind"] == "source"][0]
        self.assertTrue(src["version"].startswith("commitment[K1]:"))


if __name__ == "__main__":
    unittest.main()
