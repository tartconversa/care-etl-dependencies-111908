"""来源事件簿：把交付与修订解析成逐日的齐全状态与内容指纹。

规则（与 docs/source-contract.md 对齐）：
- 交付声明给出来源、乡镇、业务窗口、内容指纹与齐全标志；
- 交付 ready=false 表示该窗口进入修订、尚未齐全，覆盖范围内一律等待，
  不得用旧资料或零需求跑出新分区；
- 修订（change）与齐全声明分开到达：带内容的修订只追加内容标识，
  complete=true 才恢复齐全，complete=false 是撤回齐全声明；
- 同一日按 (received_at, 入库顺序) 取事件流终态；
- 就绪日的内容指纹 = 最近交付指纹，若窗口内应用过修订则追加 "~修订号"。
"""
from __future__ import annotations

import hashlib
import json

from .model import Window, parse_day


def sha_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_event(raw, seq_hint=""):
    """把交付或修订统一成事件簿记录。"""
    if "delivery_id" in raw:
        return {
            "event_id": raw["delivery_id"],
            "kind": "delivery",
            "source": raw["source"],
            "town": raw["town"],
            "from_date": raw["from_date"],
            "until_date": raw["until_date"],
            "ready": bool(raw["ready"]),
            "fingerprint": raw["fingerprint"],
            "complete": None,
            "correction": False,
            "received_at": raw["received_at"],
            "payload": raw,
        }
    if "change_id" in raw:
        scope = raw["scope"]
        values = raw.get("new_values") or {}
        return {
            "event_id": raw["change_id"],
            "kind": "change",
            "source": raw["source"],
            "town": scope["town"],
            "from_date": scope["from_date"],
            "until_date": scope["until_date"],
            "ready": None,
            "fingerprint": None,
            "complete": values.get("complete") if "complete" in values else None,
            "correction": any(k != "complete" for k in values),
            "received_at": raw["received_at"],
            "payload": raw,
        }
    raise ValueError(f"无法识别的事件: {seq_hint or raw!r}")


class WindowState:
    def __init__(self, ready, fingerprint=None, reasons=()):
        self.ready = ready
        self.fingerprint = fingerprint
        self.reasons = list(reasons)


class SourceBook:
    """按 (来源, 乡镇, 日) 解析齐全状态；承诺来源按月份窗口解析。"""

    def __init__(self, events, commitments):
        self.commitments = list(commitments)
        self._by_key = {}
        for e in events:
            self._by_key.setdefault((e["source"], e["town"]), []).append(e)
        for key in self._by_key:
            self._by_key[key].sort(key=lambda e: (e["received_at"], e["seq"]))
        self._day_cache = {}

    def day_state(self, source, town, day):
        key = (source, town, day)
        if key in self._day_cache:
            return self._day_cache[key]
        ready = False
        fp = None
        corrections = []
        reason = "无齐全交付覆盖"
        for e in self._by_key.get((source, town), ()):
            if not (parse_day(e["from_date"]) <= day < parse_day(e["until_date"])):
                continue
            if e["kind"] == "delivery":
                fp = e["fingerprint"]
                corrections = []
                ready = bool(e["ready"])
                reason = None if ready else f"交付 {e['event_id']} 声明尚未齐全"
            else:
                if e["correction"]:
                    corrections.append(e["event_id"])
                if e["complete"] is not None:
                    if bool(e["complete"]):
                        ready = True
                        reason = None
                    else:
                        ready = False
                        reason = f"齐全声明被 {e['event_id']} 撤回"
        if ready and fp is None:
            ready, reason = False, "无齐全交付覆盖"
        if ready and corrections:
            fp = fp + "~" + "+".join(corrections)
        state = WindowState(ready, fp if ready else None, [] if ready else [reason])
        self._day_cache[key] = state
        return state

    def window_state(self, source, town, window):
        """来源在整个读取窗口上的状态；任何一天未齐全则整体等待。"""
        if source == "commitment":
            return self._commitment_state(town, window)
        reasons = []
        fps = []
        open_from = None
        last_reason = None
        for day in window.days():
            st = self.day_state(source, town, day)
            if st.ready:
                fps.append((day.isoformat(), st.fingerprint))
                if open_from is not None:
                    reasons.append(f"{open_from}..{day.isoformat()} {last_reason}")
                    open_from = None
            else:
                if open_from is None:
                    open_from = day.isoformat()
                last_reason = st.reasons[0]
        if open_from is not None:
            reasons.append(f"{open_from}..{window.end.isoformat()} {last_reason}")
        if reasons:
            detail = "；".join(reasons)
            return WindowState(False, reasons=[f"等待来源 {source}@{town} {window.label()} 齐全：{detail}"])
        if len({fp for _, fp in fps}) == 1:
            return WindowState(True, fingerprint=fps[0][1])
        digest = sha_text(json.dumps(fps, ensure_ascii=False))
        return WindowState(True, fingerprint=f"mixed:{digest}")

    def _commitment_state(self, town, window):
        """承诺分区完整性由承诺有效段声明；指纹取自相交承诺行。"""
        rows = [
            c
            for c in self.commitments
            if c["town"] == town
            and Window(parse_day(c["from_date"]), parse_day(c["until_date"])).overlaps(window)
        ]
        if not rows:
            return WindowState(False, reasons=[f"承诺来源在 {town} {window.label()} 无有效段"])
        rows = sorted(rows, key=lambda c: c["commitment_id"])
        digest = sha_text(json.dumps(rows, ensure_ascii=False, sort_keys=True))
        ids = "+".join(c["commitment_id"] for c in rows)
        return WindowState(True, fingerprint=f"commitment[{ids}]:{digest[:16]}")
