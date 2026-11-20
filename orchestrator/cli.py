"""命令行入口：python -m orchestrator <命令>。

常用顺序：init → ingest → plan → run → publish → status。
"""
from __future__ import annotations

import argparse
import json
import sys

from .engine import Engine


def _engine(args):
    return Engine(
        args.root,
        state_dir=args.state_dir,
        outputs_dir=args.outputs_dir,
        data_dir=args.data_dir,
    )


def _print_json(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def cmd_init(args):
    _engine(args).init()
    print("状态库已就绪")


def cmd_ingest(args):
    added = _engine(args).ingest()
    print(f"新入库事件 {len(added)} 个：{', '.join(added) if added else '（无）'}")


def cmd_plan(args):
    eng = _engine(args)
    computed = eng.refresh()
    pending = [(k, r) for k, r in computed.items() if r["status"] == "pending"]
    waiting = [(k, r) for k, r in computed.items() if r["status"] == "waiting"]
    counts = {}
    for _, r in computed.items():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print("分区状态计数：" + json.dumps(counts, ensure_ascii=False, sort_keys=True))
    print(f"\n可运行 {len(pending)} 个：")
    for (j, s, p), r in sorted(pending)[: args.limit]:
        print(f"  {j}@{s}@{p}")
        for reason in r["reasons"]:
            print(f"    - {reason}")
    print(f"\n等待中 {len(waiting)} 个（前 {args.limit} 条原因）：")
    shown = 0
    for (j, s, p), r in sorted(waiting):
        if shown >= args.limit:
            break
        print(f"  {j}@{s}@{p}")
        for reason in r["reasons"][:2]:
            print(f"    - {reason}")
        shown += 1
    if args.impact:
        affected = eng.impact_of(args.impact)
        print(f"\n事件 {', '.join(args.impact)} 的影响范围（{len(affected)} 个分区）：")
        for label in affected:
            print(f"  {label}")


def cmd_run(args):
    eng = _engine(args)
    mode_for = {}
    for item in args.mode_for or []:
        job, _, m = item.partition(":")
        mode_for[job] = m
    total = eng.run(limit=args.limit, mode=args.mode, mode_for=mode_for,
                    job_id=args.job, scope=args.scope_key, partition=args.partition)
    print(f"本次执行运行 {total} 个分区")


def cmd_recover(args):
    result = _engine(args).recover()
    _print_json(result)


def cmd_publish(args):
    published = _engine(args).publish()
    if not published:
        print("没有新的可发布集合")
    for p in published:
        print(f"已发布 {p['group']}@{p['scope']}@{p['period']} v{p['version']}（{p['members']} 个成员）")


def cmd_status(args):
    st = _engine(args).status()
    print("分区状态计数：")
    for job, counts in sorted(st["summary"].items()):
        line = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"  {job}: {line}")
    if st["attention"]:
        print(f"\n需关注 {len(st['attention'])} 个：")
        for item in st["attention"][: args.limit]:
            print(f"  [{item['status']}] {item['partition']}")
            for reason in item["reasons"][:3]:
                print(f"    - {reason}")


def cmd_explain(args):
    out = _engine(args).explain(args.job, args.scope, args.partition)
    _print_json(out)


def cmd_retry(args):
    n = _engine(args).retry(args.job, args.scope_key, args.partition)
    print(f"已恢复 {n} 个分区待调度")


def cmd_cancel(args):
    n = _engine(args).cancel(args.job, args.scope_key, args.partition)
    print(f"已撤销 {n} 个未发布重算（产物保留）")


def cmd_pause(args):
    _engine(args).pause_source(args.source)
    print(f"来源 {args.source} 已暂停")


def cmd_resume(args):
    _engine(args).resume_source(args.source)
    print(f"来源 {args.source} 已恢复")


def cmd_published(args):
    eng = _engine(args)
    eng.init()
    with eng._db() as conn:
        rows = conn.execute(
            """SELECT group_id,scope_key,period,MAX(version) AS version,published_at
               FROM publications GROUP BY group_id,scope_key,period ORDER BY 1,2,3"""
        ).fetchall()
    for r in rows:
        print(f"  {r['group_id']}@{r['scope_key']}@{r['period']} v{r['version']}（{r['published_at']}）")
    if not rows:
        print("（尚无发布）")


def build_parser():
    p = argparse.ArgumentParser(prog="orchestrator", description="照护供需仓离线作业编排")
    p.add_argument("--root", default=".", help="项目根目录（默认当前目录）")
    p.add_argument("--state-dir", help="状态目录（默认 <root>/state）")
    p.add_argument("--outputs-dir", help="产物目录（默认 <root>/outputs）")
    p.add_argument("--data-dir", help="资料目录（默认 <root>/data）")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="初始化状态库").set_defaults(fn=cmd_init)
    sub.add_parser("ingest", help="装载来源交付与修订").set_defaults(fn=cmd_ingest)

    p_plan = sub.add_parser("plan", help="查看可运行/等待分区与原因")
    p_plan.add_argument("--limit", type=int, default=20)
    p_plan.add_argument("--impact", nargs="*", metavar="EVENT", help="展开指定事件的影响范围")
    p_plan.set_defaults(fn=cmd_plan)

    p_run = sub.add_parser("run", help="运行可运行分区")
    p_run.add_argument("--limit", type=int)
    p_run.add_argument("--mode", default="ok", choices=["ok", "fail-before", "exit-after"])
    p_run.add_argument("--mode-for", action="append", metavar="JOB:MODE", help="按作业指定演练模式")
    p_run.add_argument("--job", help="只运行指定作业")
    p_run.add_argument("--scope-key", dest="scope_key", help="只运行指定范围（乡镇或 county）")
    p_run.add_argument("--partition", help="只运行指定分区")
    p_run.set_defaults(fn=cmd_run)

    sub.add_parser("recover", help="重启恢复：采纳孤儿回执或标记丢失").set_defaults(fn=cmd_recover)
    sub.add_parser("publish", help="成套发布就绪的发布组").set_defaults(fn=cmd_publish)

    p_status = sub.add_parser("status", help="分区状态与等待原因")
    p_status.add_argument("--limit", type=int, default=30)
    p_status.set_defaults(fn=cmd_status)

    p_ex = sub.add_parser("explain", help="单个分区的输入、原因与运行史")
    p_ex.add_argument("job")
    p_ex.add_argument("scope")
    p_ex.add_argument("partition")
    p_ex.set_defaults(fn=cmd_explain)

    for name, fn, help_text in (
        ("retry", cmd_retry, "重试失败/已撤销范围"),
        ("cancel", cmd_cancel, "撤销未发布的重算（产物保留）"),
    ):
        sp = sub.add_parser(name, help=help_text)
        sp.add_argument("--job")
        sp.add_argument("--scope-key", dest="scope_key")
        sp.add_argument("--partition")
        sp.set_defaults(fn=fn)

    p_pause = sub.add_parser("pause-source", help="暂停一个来源")
    p_pause.add_argument("source")
    p_pause.set_defaults(fn=cmd_pause)
    p_resume = sub.add_parser("resume-source", help="恢复一个来源")
    p_resume.add_argument("source")
    p_resume.set_defaults(fn=cmd_resume)

    sub.add_parser("published", help="列出已发布集合").set_defaults(fn=cmd_published)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.fn(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
