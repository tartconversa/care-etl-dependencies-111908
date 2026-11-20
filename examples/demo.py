"""端到端演练：来源到达 → 依赖重算 → 成套发布的完整故事。

用法：python examples/demo.py
在项目根目录下重建 state/ 与 outputs/（均为 gitignore 的演练目录），
使用真实的外部演练程序 support/job_worker.py。
"""
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from orchestrator.engine import Engine  # noqa: E402
from orchestrator.worker import launch_worker  # noqa: E402


def banner(text):
    print(f"\n=== {text} " + "=" * (60 - len(text)))


def show_status(eng, *keys):
    for job, scope, part in keys:
        info = eng.explain(job, scope, part)
        print(f"  {job}@{scope}@{part}: {info['status']}")
        for reason in info["reasons"][:2]:
            print(f"    - {reason}")


def main():
    state, outputs = ROOT / "state", ROOT / "outputs"
    shutil.rmtree(state, ignore_errors=True)
    shutil.rmtree(outputs, ignore_errors=True)
    eng = Engine(ROOT)
    eng.init()

    deliveries = json.loads((ROOT / "data" / "deliveries.json").read_text(encoding="utf-8"))
    changes = json.loads((ROOT / "data" / "changes.json").read_text(encoding="utf-8"))
    base = [d for d in deliveries if d["ready"]]
    d10 = next(d for d in deliveries if d["delivery_id"] == "D10")
    c1 = next(c for c in changes if c["change_id"] == "C1")
    c2 = next(c for c in changes if c["change_id"] == "C2")

    banner("阶段 0：先演示运行授权与重启恢复")
    eng.ingest_events(base)
    eng.refresh()
    token = eng.begin_run("person_dim", "T1", "2026-10-01")
    out_dir = outputs / "runs" / token
    print(f"  签发授权 {token}，随后假设编排进程在结案前停止")
    launch_worker(eng.worker_path, out_dir / "request.json", out_dir)
    result = eng.recover()
    print(f"  重启恢复：采纳孤儿回执 {result['adopted']}，标记丢失 {result['lost']}")
    show_status(eng, ("person_dim", "T1", "2026-10-01"))

    banner("阶段 1：基线资料齐全，运行并成套发布")
    print(f"  本次运行 {eng.run()} 个分区（真实调用 support/job_worker.py）")
    for p in eng.publish():
        print(f"  发布 {p['group']}@{p['scope']}@{p['period']} v{p['version']}（{p['members']} 个成员）")

    banner("阶段 2：D10 到达但声明未齐全")
    eng.ingest_events([d10])
    show_status(eng, ("person_dim", "T1", "2026-10-15"), ("demand_week", "T1", "2026-10-12"))
    print("  未齐全范围明确等待，而不是用旧资料或零需求跑出新分区")

    banner("阶段 3：修订 C1 到达但齐全声明未到，仍然等待")
    eng.ingest_events([c1])
    show_status(eng, ("person_dim", "T1", "2026-10-15"),)

    banner("阶段 4：C2 重新声明齐全，按真实依赖扩展影响范围")
    eng.ingest_events([c2])
    affected = eng.impact_of(["D10", "C1", "C2"])
    print(f"  影响 {len(affected)} 个分区（两天资格修订沿读取窗口扩散到周与月）：")
    for label in affected:
        print(f"    {label}")
    print(f"  本次运行 {eng.run()} 个分区（其余复用，不清空全县历史）")
    for p in eng.publish():
        print(f"  发布 {p['group']}@{p['scope']}@{p['period']} v{p['version']}")
    show_status(eng, ("person_dim", "T1", "2026-10-15"), ("person_dim", "T2", "2026-10-15"))

    banner("完成")
    print("  查询任一范围：python -m orchestrator explain person_dim T1 2026-10-15")
    print("  查看等待原因：python -m orchestrator status")
    print("  已发布集合：  python -m orchestrator published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
