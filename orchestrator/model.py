"""基础模型：业务窗口、作业定义与发布组。

约定：窗口一律左闭右开；周从周一开始；月用自然月；日期为 ISO 8601。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

DAY = timedelta(days=1)


def parse_day(text):
    return date.fromisoformat(text)


def week_start(day):
    """所在周的周一。"""
    return day - timedelta(days=day.weekday())


def month_start(day):
    return day.replace(day=1)


def month_add(month, count=1):
    year = month.year + (month.month - 1 + count) // 12
    mo = (month.month - 1 + count) % 12 + 1
    return date(year, mo, 1)


@dataclass(frozen=True)
class Window:
    """左闭右开业务窗口。"""

    start: date
    end: date

    def __post_init__(self):
        if self.end <= self.start:
            raise ValueError("窗口结束必须晚于开始")

    def overlaps(self, other):
        return self.start < other.end and other.start < self.end

    def contains(self, day):
        return self.start <= day < self.end

    def days(self):
        d = self.start
        while d < self.end:
            yield d
            d += DAY

    def label(self):
        return f"[{self.start.isoformat()}..{self.end.isoformat()})"


@dataclass(frozen=True)
class JobDef:
    """作业声明：读取的上游、回看天数、输出粒度与实体范围。"""

    job_id: str
    inputs: tuple
    lookback_days: int
    output_grain: str  # daily | weekly | monthly
    scope: str  # town | county


def load_jobs(path):
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    jobs = {}
    for r in rows:
        jobs[r["job_id"]] = JobDef(
            job_id=r["job_id"],
            inputs=tuple(r["inputs"]),
            lookback_days=int(r["lookback_days"]),
            output_grain=r["output_grain"],
            scope=r["scope"],
        )
    return jobs


def topo_order(jobs):
    """按声明的 inputs 拓扑排序；source: 输入不是作业。"""
    order = []
    seen = set()

    def visit(jid, stack=()):
        if jid in seen:
            return
        if jid in stack:
            raise ValueError(f"作业依赖成环: {' -> '.join(stack + (jid,))}")
        for ref in jobs[jid].inputs:
            if not ref.startswith("source:"):
                visit(ref, stack + (jid,))
        seen.add(jid)
        order.append(jid)

    for jid in jobs:
        visit(jid)
    return order


def load_groups(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def partition_window(job, partition):
    """作业输出分区对应的业务窗口。"""
    if job.output_grain == "daily":
        d = parse_day(partition)
        return Window(d, d + DAY)
    if job.output_grain == "weekly":
        w = parse_day(partition)
        return Window(w, w + 7 * DAY)
    if job.output_grain == "monthly":
        m = parse_day(partition + "-01")
        return Window(m, month_add(m))
    raise ValueError(f"未知粒度 {job.output_grain}")


def week_partition(day):
    return week_start(day).isoformat()


def month_partition_of(day):
    return day.isoformat()[:7]
