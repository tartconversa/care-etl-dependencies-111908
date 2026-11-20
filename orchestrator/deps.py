"""作业读取声明、候选分区生成与影响范围扩展。

读取规则来自 jobs.json 的声明加上来源约定：
- 日粒度作业读取当天；
- demand_week 读取该周及其开始日前 lookback_days 天的日事实，
  起点不早于本乡镇有来源记录的第一天（记录开始之前不是"未知"）；
- county_control 读取全部乡镇同周的需求周；
- commitment_month 读取与月份相交的承诺有效段、与月份相交的需求周、
  以及当月服务点日维度；需求周跨月时被两个月份共同引用，但只算一份分区。
"""
from __future__ import annotations

from collections import namedtuple
from datetime import timedelta

from .model import Window, month_partition_of, parse_day, partition_window, week_partition

# kind: 'source' | 'job'；name: 来源名或作业号；scope: 乡镇或 county；
# partition: 作业分区（来源输入为 None）；window: 实际读取窗口。
InputRef = namedtuple("InputRef", ["kind", "name", "scope", "partition", "window"])


def ref_label(ref):
    if ref.kind == "source":
        return f"source:{ref.name}@{ref.scope}{ref.window.label()}"
    return f"job:{ref.name}@{ref.scope}@{ref.partition}"


class Ctx:
    """一次派生计算共享的上下文。"""

    def __init__(self, jobs, towns, horizons, candidates, paused=()):
        self.jobs = jobs
        self.towns = list(towns)
        self.horizons = dict(horizons)  # town -> date
        self.candidates = candidates  # {job_id: {(scope, partition)}}
        self.paused = set(paused)  # 被操作员暂停的来源（只挡新运行，不动既有产物）

    def horizon(self, town):
        return self.horizons.get(town)

    def has_candidate(self, job_id, scope, partition):
        return (scope, partition) in self.candidates.get(job_id, set())


def read_window(job, scope, partition, ctx):
    """作业分区的业务读取窗口。"""
    win = partition_window(job, partition)
    if job.output_grain == "weekly" and job.lookback_days:
        horizon = ctx.horizon(scope)
        start = win.start - timedelta(days=job.lookback_days)
        if horizon is not None and start < horizon:
            start = horizon
        return Window(start, win.end)
    return win


def input_refs(job_id, scope, partition, ctx):
    """声明读取的实体范围展开成输入引用列表。"""
    job = ctx.jobs[job_id]
    win = read_window(job, scope, partition, ctx)
    refs = []
    for upstream in job.inputs:
        if upstream.startswith("source:"):
            refs.append(InputRef("source", upstream.split(":", 1)[1], scope, None, win))
            continue
        up = ctx.jobs[upstream]
        if up.output_grain == "daily":
            days = list(win.days())
            refs += [
                InputRef("job", upstream, scope if up.scope != "county" else "county", d.isoformat(), Window(d, d + timedelta(days=1)))
                for d in days
            ]
        elif up.output_grain == "weekly":
            towns = ctx.towns if job.scope == "county" and up.scope == "town" else [scope]
            weeks = set()
            for d in win.days():
                weeks.add(week_partition(d))
            refs += [
                InputRef("job", upstream, t, w, partition_window(up, w))
                for t in towns
                for w in sorted(weeks)
            ]
        elif up.output_grain == "monthly":
            months = {month_partition_of(d) for d in win.days()}
            refs += [
                InputRef("job", upstream, scope, m, partition_window(up, m))
                for m in sorted(months)
            ]
        else:  # pragma: no cover - 防御未知粒度
            raise ValueError(f"未知粒度 {up.output_grain}")
    return refs


def generate_candidates(jobs, events, commitments, towns):
    """按来源覆盖生成候选分区：只对有来源声明覆盖的范围建分区。"""
    cand = {jid: set() for jid in jobs}
    days = {}  # (source, town) -> set[date]
    for e in events:
        key = (e["source"], e["town"])
        bucket = days.setdefault(key, set())
        d = parse_day(e["from_date"])
        end = parse_day(e["until_date"])
        while d < end:
            bucket.add(d)
            d += timedelta(days=1)

    def source_days(source, town):
        return days.get((source, town), set())

    for town in towns:
        for d in sorted(source_days("population", town)):
            cand["person_dim"].add((town, d.isoformat()))
        for d in sorted(source_days("site", town)):
            cand["site_dim"].add((town, d.isoformat()))
        for d in sorted(source_days("service", town)):
            cand["service_fact"].add((town, d.isoformat()))
        demand_days = source_days("population", town) | source_days("service", town)
        for d in sorted(demand_days):
            cand["demand_week"].add((town, week_partition(d)))
    for _town, w in sorted(cand["demand_week"]):
        cand["county_control"].add(("county", w))
    for c in commitments:
        win = Window(parse_day(c["from_date"]), parse_day(c["until_date"]))
        for d in win.days():
            cand["commitment_month"].add((c["town"], month_partition_of(d)))
    return cand


def impact_of_changes(changed, jobs, candidates, ctx):
    """依据真实依赖，把来源变化扩展成受影响的分区集合。

    changed: [(source, town, Window)]；返回 {"job@scope@partition"}。
    前向扩展：分区的任一声明输入与变化相交即受影响，再把该分区的
    输出窗口作为新的变化继续向下游传播。
    """
    frontier = [("source", name, town, win) for name, town, win in changed]
    affected = set()
    seen_frontier = set()
    while frontier:
        kind, name, scope, win = frontier.pop()
        mark = (kind, name, scope, win.start, win.end)
        if mark in seen_frontier:
            continue
        seen_frontier.add(mark)
        for job_id, parts in candidates.items():
            for p_scope, p_part in sorted(parts):
                label = f"{job_id}@{p_scope}@{p_part}"
                if label in affected:
                    continue
                for ref in input_refs(job_id, p_scope, p_part, ctx):
                    if ref.kind != kind or ref.name != name or ref.scope != scope:
                        continue
                    if ref.window.overlaps(win):
                        affected.add(label)
                        out = partition_window(jobs[job_id], p_part)
                        frontier.append(("job", job_id, p_scope, out))
                        break
    return affected
