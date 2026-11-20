"""依赖传播：按真实读取范围扩展影响，指纹未变的产物一律复用。"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import DrillCase, correction_story  # noqa: E402


class DependencyExpansion(DrillCase):
    def test_two_day_population_correction_propagates_through_layers(self):
        self.run_all()
        before = self.satisfied_set()
        self.assertIn("county_control@county@2026-10-05", before)

        events = correction_story()  # population@T1 [10-05,10-07)
        self.eng.ingest_events(events)
        impact = set(self.eng.impact_of(["D90", "C91", "C92"]))

        # 预期：两天日维度 → 两天服务事实 → 需求周(10-05) → 县控周(10-05)
        expected = {
            "person_dim@T1@2026-10-05", "person_dim@T1@2026-10-06",
            "service_fact@T1@2026-10-05", "service_fact@T1@2026-10-06",
            "demand_week@T1@2026-10-05",
            "county_control@county@2026-10-05",
        }
        self.assertEqual(impact, expected)
        # 09-28 周的读取窗口 [10-01,10-05) 不含 10-05，不受影响
        self.assertNotIn("demand_week@T1@2026-09-28", impact)

        after = self.satisfied_set()
        stale = before - after
        self.assertEqual(stale, expected)
        # 与变化无关的范围全部复用：T2 与 T1 的其他天一个都不少
        self.assertTrue(all("T2" not in s for s in stale))
        self.assertIn("person_dim@T1@2026-10-04", after)
        self.assertIn("demand_week@T2@2026-10-05", after)

        # 重跑原因逐条可查，且只重跑受影响分区
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "pending")
        self.assertTrue(any("版本变化" in r for r in info["reasons"]))
        reruns = self.run_all()
        self.assertEqual(reruns, len(expected))
        self.assertEqual(self.satisfied_set(), before)

    def test_two_day_site_correction_changes_week_denominators(self):
        self.run_all()
        before = self.satisfied_set()
        events = correction_story(source="site", frm="2026-10-03", until="2026-10-05",
                                  did="D80", cid="C81", rid="C82")
        self.eng.ingest_events(events)
        impact = set(self.eng.impact_of(["D80", "C81", "C82"]))
        # 站点的两天修正同时落入 09-28 与 10-05 两周的读取窗口
        expected = {
            "site_dim@T1@2026-10-03", "site_dim@T1@2026-10-04",
            "service_fact@T1@2026-10-03", "service_fact@T1@2026-10-04",
            "demand_week@T1@2026-09-28", "demand_week@T1@2026-10-05",
            "county_control@county@2026-09-28", "county_control@county@2026-10-05",
        }
        self.assertEqual(impact, expected)
        stale = before - self.satisfied_set()
        self.assertEqual(stale, expected)

    def test_unrelated_delivery_does_not_invalidate_history(self):
        self.run_all()
        before = self.satisfied_set()
        # T2 收到一份与已覆盖窗口无关的新交付（新指纹但只影响自身两天）
        self.eng.ingest_events([{
            "delivery_id": "D70", "source": "service", "town": "T2",
            "from_date": "2026-10-08", "until_date": "2026-10-10",
            "fingerprint": "service-T2-v2", "ready": True,
            "received_at": "2026-11-02T10:00:00+08:00",
        }])
        stale = before - self.satisfied_set()
        # 只有 T2 的两天服务事实及其下游受影响，全县历史不被清空
        self.assertEqual(stale, {
            "service_fact@T2@2026-10-08", "service_fact@T2@2026-10-09",
            "demand_week@T2@2026-10-05",
            "county_control@county@2026-10-05",
        })
        self.assertIn("person_dim@T1@2026-10-08", self.satisfied_set())


if __name__ == "__main__":
    unittest.main()
