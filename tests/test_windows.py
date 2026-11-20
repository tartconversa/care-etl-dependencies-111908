"""窗口与读取范围的基础算术。"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import deps
from orchestrator.model import Window, month_add, parse_day, week_start


class Windows(unittest.TestCase):
    def test_week_starts_monday(self):
        self.assertEqual(week_start(date(2026, 10, 15)), date(2026, 10, 12))
        self.assertEqual(week_start(date(2026, 10, 12)), date(2026, 10, 12))
        self.assertEqual(week_start(date(2026, 10, 18)), date(2026, 10, 12))

    def test_window_half_open(self):
        w = Window(date(2026, 10, 1), date(2026, 10, 8))
        self.assertTrue(w.contains(date(2026, 10, 1)))
        self.assertFalse(w.contains(date(2026, 10, 8)))
        self.assertEqual(len(list(w.days())), 7)

    def test_month_add(self):
        self.assertEqual(month_add(date(2026, 10, 1)), date(2026, 11, 1))
        self.assertEqual(month_add(date(2026, 12, 1)), date(2027, 1, 1))

    def test_weekly_read_window_clips_to_horizon(self):
        jobs = {
            "demand_week": self._job("demand_week", 28, "weekly"),
        }
        ctx = deps.Ctx(jobs, ["T1"], {"T1": date(2026, 10, 1)}, {})
        win = deps.read_window(jobs["demand_week"], "T1", "2026-09-28", ctx)
        # 该周 09-28 起，回看 28 天到 08-31，但记录从 10-01 才开始
        self.assertEqual(win.start, date(2026, 10, 1))
        self.assertEqual(win.end, date(2026, 10, 5))

    def test_weekly_read_window_full_lookback(self):
        jobs = {"demand_week": self._job("demand_week", 28, "weekly")}
        ctx = deps.Ctx(jobs, ["T1"], {"T1": date(2026, 9, 1)}, {})
        win = deps.read_window(jobs["demand_week"], "T1", "2026-10-26", ctx)
        self.assertEqual(win.start, date(2026, 9, 28))
        self.assertEqual(win.end, date(2026, 11, 2))

    @staticmethod
    def _job(job_id, lookback, grain):
        from orchestrator.model import JobDef

        return JobDef(job_id=job_id, inputs=(), lookback_days=lookback,
                      output_grain=grain, scope="town")


if __name__ == "__main__":
    unittest.main()
