"""运行授权：退出码、回执有效、可发布是三个不同事实；重启不丢不重。

本文件使用真实子进程调用 support/job_worker.py，验证外部协议本身。
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import DrillCase, correction_story, inline_launcher  # noqa: E402
from orchestrator.engine import RunConflict  # noqa: E402
from orchestrator.worker import launch_worker  # noqa: E402


class RunAuthorization(DrillCase):
    def test_exit_after_adopts_receipt_despite_nonzero_exit(self):
        # 演练程序在回执落盘后以 24 退出：进程异常但产物有效（真实子进程）
        self.eng.run(mode="exit-after", job_id="person_dim", scope="T1",
                     partition="2026-10-01")
        info = self.eng.explain("person_dim", "T1", "2026-10-01")
        self.assertEqual(info["status"], "satisfied")
        run = info["runs"][0]
        self.assertEqual(run["exit_code"], 24)
        self.assertEqual(run["status"], "succeeded")
        self.assertTrue(Path(run["artifact_path"]).exists())

    def test_fail_before_marks_failed_and_retry_recovers(self):
        # 真实子进程在写入前以 23 退出：无回执，判失败
        self.eng.run(mode="fail-before", job_id="person_dim", scope="T1",
                     partition="2026-10-01")
        info = self.eng.explain("person_dim", "T1", "2026-10-01")
        self.assertEqual(info["status"], "failed")
        self.assertEqual(info["runs"][0]["exit_code"], 23)
        # 失败是粘滞的：不加干预不会被自动重跑
        self.run_all(job_id="person_dim", scope="T1", partition="2026-10-01")
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-01"), "failed")
        # 操作员重试后恢复
        self.eng.retry("person_dim", "T1", "2026-10-01")
        self.run_all(job_id="person_dim", scope="T1", partition="2026-10-01")
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-01"), "satisfied")

    def test_duplicate_authorization_rejected(self):
        token = self.eng.begin_run("person_dim", "T1", "2026-10-01")
        with self.assertRaises(RunConflict):
            self.eng.begin_run("person_dim", "T1", "2026-10-01")
        out_dir = self.eng.outputs_dir / "runs" / token
        exit_code, stderr = launch_worker(self.eng.worker_path,
                                          out_dir / "request.json", out_dir)
        self.assertEqual(exit_code, 0, stderr)
        self.eng.finalize_run(token, exit_code, stderr)
        # 已满足的分区不能重复签发
        with self.assertRaises(ValueError):
            self.eng.begin_run("person_dim", "T1", "2026-10-01")
        # 新变化使其重新待算后，可以签发新授权
        self.eng.ingest_events(correction_story())
        token2 = self.eng.begin_run("person_dim", "T1", "2026-10-05")
        self.assertNotEqual(token, token2)

    def test_recover_adopts_orphan_receipt_and_marks_lost(self):
        # 授权 A：演练程序写完回执，但编排进程"崩溃"在结案之前
        token_ok = self.eng.begin_run("person_dim", "T1", "2026-10-01")
        out_ok = self.eng.outputs_dir / "runs" / token_ok
        exit_code, _ = launch_worker(self.eng.worker_path, out_ok / "request.json", out_ok)
        self.assertEqual(exit_code, 0)
        # 授权 B：进程在产物落盘前停止，没有任何回执
        token_lost = self.eng.begin_run("person_dim", "T1", "2026-10-02")
        # 重启恢复：A 被采纳，B 标记丢失并可重跑
        result = self.eng.recover()
        self.assertEqual(result["adopted"], [token_ok])
        self.assertEqual(result["lost"], [token_lost])
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-01"), "satisfied")
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-02"), "pending")
        run = self.eng.get_run(token_ok)
        self.assertIn("重启", run["note"])
        # 丢失的授权不会被执行第二次：重跑会签发新授权
        token_new = self.eng.begin_run("person_dim", "T1", "2026-10-02")
        self.assertNotEqual(token_new, token_lost)

    def test_inflight_success_does_not_clear_newer_todo(self):
        self.run_all()
        # 第一批变化使分区进入待算
        self.eng.ingest_events(correction_story())
        token = self.eng.begin_run("person_dim", "T1", "2026-10-05")
        # 运行期间又到达新资料：在途授权读取的版本已经落后
        self.eng.ingest_events([{
            "delivery_id": "D97", "source": "population", "town": "T1",
            "from_date": "2026-10-05", "until_date": "2026-10-07",
            "fingerprint": "population-T1-v3", "ready": True,
            "received_at": "2026-11-02T10:00:00+08:00",
        }])
        out_dir = self.eng.outputs_dir / "runs" / token
        exit_code, stderr = inline_launcher(out_dir / "request.json", out_dir, "ok")
        self.eng.finalize_run(token, exit_code, stderr)
        # 旧授权本身记为成功，但分区的待办没有被它消除
        self.assertEqual(self.eng.get_run(token)["status"], "succeeded")
        info = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(info["status"], "pending")
        self.assertTrue(any("版本变化" in r for r in info["reasons"]))
        # 再次运行后才以新版本满足
        self.run_all(job_id="person_dim", scope="T1", partition="2026-10-05")
        self.assertEqual(self.status_of("person_dim", "T1", "2026-10-05"), "satisfied")
        final = self.eng.explain("person_dim", "T1", "2026-10-05")
        self.assertEqual(final["inputs"][0]["version"], "population-T1-v3")


if __name__ == "__main__":
    unittest.main()
