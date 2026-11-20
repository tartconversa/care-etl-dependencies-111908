"""编排引擎：派生分区状态、驱动运行授权、成套发布。

核心不变量：
- 分区的"期望版本"由其声明读取范围内每个输入的版本指纹决定；
  来源输入取来源内容指纹，作业输入取上游期望版本加其当前产物摘要，
  因此任何上游变化会立即沿依赖链传导，而不是等同日分区逐层发现。
- 存在与期望版本一致的成功运行 → 复用；否则给出逐条重跑/等待原因。
- 运行授权（run_token）先入库再启动演练程序；回执有效即采纳产物，
  与进程退出码分开记录；运行期间输入再变化时，旧成功不抵新待办。
- 发布按发布组求依赖闭包，全部当前满足才原子推出新版本。
"""
from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from . import deps, store
from .model import (
    DAY,
    Window,
    load_groups,
    load_jobs,
    month_add,
    parse_day,
    partition_window,
    topo_order,
)
from .sources import SourceBook, normalize_event
from .worker import ReceiptError, launch_worker, validate_receipt


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha_json(obj):
    return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


class RunConflict(Exception):
    """同一分区已有进行中的运行授权。"""


class Engine:
    def __init__(self, root, state_dir=None, outputs_dir=None, data_dir=None, worker_path=None):
        root = Path(root)
        self.root = root
        self.data_dir = Path(data_dir) if data_dir else root / "data"
        self.state_dir = Path(state_dir) if state_dir else root / "state"
        self.outputs_dir = Path(outputs_dir) if outputs_dir else root / "outputs"
        self.worker_path = Path(worker_path) if worker_path else root / "support" / "job_worker.py"
        self.jobs = load_jobs(self.data_dir / "jobs.json")
        self.groups = load_groups(self.data_dir / "publication_groups.json")
        self.order = topo_order(self.jobs)
        self._topo_idx = {jid: i for i, jid in enumerate(self.order)}

    # ---------------------------------------------------------------- 基础
    def connect(self):
        return store.connect(self.state_dir / "orchestrator.db")

    @contextmanager
    def _db(self):
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init(self):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.outputs_dir / "runs").mkdir(parents=True, exist_ok=True)
        with self._db() as conn:
            store.init_db(conn)

    def _commitments(self):
        path = self.data_dir / "commitments.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def _events(self, conn):
        rows = conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        return [dict(r) for r in rows]

    def _paused(self, conn):
        rows = conn.execute("SELECT key FROM controls WHERE key LIKE 'paused:%'").fetchall()
        return {r["key"].split(":", 1)[1] for r in rows}

    # ---------------------------------------------------------------- 接收
    def ingest(self):
        """装载 data/deliveries.json 与 data/changes.json（按号幂等）。"""
        raw = []
        for name in ("deliveries.json", "changes.json"):
            path = self.data_dir / name
            if path.exists():
                raw += json.loads(path.read_text(encoding="utf-8"))
        return self.ingest_events(raw)

    def ingest_events(self, raw_events):
        """追加来源事件（交付或修订）；已入库的同号事件忽略。"""
        self.init()
        added = []
        with self._db() as conn:
            for raw in raw_events:
                e = normalize_event(raw)
                cur = conn.execute(
                    """INSERT OR IGNORE INTO events
                       (event_id,kind,source,town,from_date,until_date,ready,
                        fingerprint,complete,correction,received_at,payload)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        e["event_id"], e["kind"], e["source"], e["town"],
                        e["from_date"], e["until_date"],
                        None if e["ready"] is None else int(e["ready"]),
                        e["fingerprint"],
                        None if e["complete"] is None else int(bool(e["complete"])),
                        int(e["correction"]), e["received_at"],
                        json.dumps(e["payload"], ensure_ascii=False, sort_keys=True),
                    ),
                )
                if cur.rowcount:
                    added.append(e["event_id"])
        return added

    # ---------------------------------------------------------------- 派生
    def _build_ctx(self, conn):
        events = self._events(conn)
        commitments = self._commitments()
        paused = self._paused(conn)
        towns = sorted({e["town"] for e in events} | {c["town"] for c in commitments})
        horizons = {}
        for t in towns:
            starts = [parse_day(e["from_date"]) for e in events if e["town"] == t]
            if starts:
                horizons[t] = min(starts)
        book = SourceBook(events, commitments)
        candidates = deps.generate_candidates(self.jobs, events, commitments, towns)
        ctx = deps.Ctx(self.jobs, towns, horizons, candidates, paused)
        return book, ctx, candidates

    def refresh(self):
        """重算全部候选分区的状态与原因；返回 {key: row}。"""
        self.init()
        with self._db() as conn:
            book, ctx, candidates = self._build_ctx(conn)
            now = _now()
            computed = {}
            for job_id in self.order:
                for scope, part in sorted(candidates[job_id]):
                    row = self._derive(conn, book, ctx, computed, job_id, scope, part)
                    computed[(job_id, scope, part)] = row
            with conn:
                for (job_id, scope, part), row in computed.items():
                    conn.execute(
                        """INSERT INTO partitions
                           (job_id,scope_key,partition,status,desired_hash,content_digest,
                            inputs_json,reasons_json,run_token,sticky,updated_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(job_id,scope_key,partition) DO UPDATE SET
                            status=excluded.status, desired_hash=excluded.desired_hash,
                            content_digest=excluded.content_digest, inputs_json=excluded.inputs_json,
                            reasons_json=excluded.reasons_json, run_token=excluded.run_token,
                            sticky=excluded.sticky, updated_at=excluded.updated_at""",
                        (
                            job_id, scope, part, row["status"], row["desired_hash"],
                            row["content_digest"], json.dumps(row["inputs"], ensure_ascii=False),
                            json.dumps(row["reasons"], ensure_ascii=False), row["run_token"],
                            row["sticky"], now,
                        ),
                    )
        return computed

    def _derive(self, conn, book, ctx, computed, job_id, scope, part):
        refs = deps.input_refs(job_id, scope, part, ctx)
        waiting, vmap, cmap, details = [], {}, {}, []
        used_sources = set()
        for ref in refs:
            key = deps.ref_label(ref)
            if ref.kind == "source":
                used_sources.add(ref.name)
                st = book.window_state(ref.name, scope, ref.window)
                details.append({
                    "key": key, "kind": "source", "source": ref.name, "town": scope,
                    "ready": st.ready, "version": st.fingerprint,
                    "content": st.fingerprint, "reasons": st.reasons,
                })
                if st.ready:
                    vmap[key] = st.fingerprint
                    cmap[key] = st.fingerprint
                else:
                    waiting.extend(st.reasons)
            else:
                up = computed.get((ref.name, ref.scope, ref.partition))
                if up is None:
                    waiting.append(f"上游 {key} 无候选分区（来源未覆盖）")
                    details.append({"key": key, "kind": "job", "job": ref.name,
                                    "scope": ref.scope, "partition": ref.partition,
                                    "ready": False, "version": None, "content": None,
                                    "reasons": ["无候选分区"]})
                elif up["status"] != "satisfied":
                    waiting.append(f"上游 {key} 未就绪（{up['status']}）")
                    details.append({"key": key, "kind": "job", "job": ref.name,
                                    "scope": ref.scope, "partition": ref.partition,
                                    "ready": False, "version": None, "content": None,
                                    "reasons": [f"上游状态 {up['status']}"]})
                else:
                    token = f"{up['desired_hash']}:{up['content_digest']}"
                    vmap[key] = token
                    cmap[key] = up["content_digest"]
                    details.append({"key": key, "kind": "job", "job": ref.name,
                                    "scope": ref.scope, "partition": ref.partition,
                                    "ready": True, "version": token,
                                    "content": up["content_digest"], "reasons": []})
        desired = _sha_json(vmap) if not waiting else None
        stored = conn.execute(
            "SELECT * FROM partitions WHERE job_id=? AND scope_key=? AND partition=?",
            (job_id, scope, part),
        ).fetchone()
        running = conn.execute(
            """SELECT run_token FROM runs
               WHERE job_id=? AND scope_key=? AND partition=? AND status='running'""",
            (job_id, scope, part),
        ).fetchone()
        sticky = stored["sticky"] if stored else ""
        if running:
            status = "running"
            reasons = [f"运行 {running['run_token']} 进行中"] + waiting
            run_token = running["run_token"]
            content_digest = None
        elif waiting:
            status, reasons, run_token, content_digest = "waiting", waiting, None, None
        else:
            sat = conn.execute(
                """SELECT * FROM runs WHERE job_id=? AND scope_key=? AND partition=?
                   AND status='succeeded' AND desired_hash=?
                   ORDER BY finished_at DESC LIMIT 1""",
                (job_id, scope, part, desired),
            ).fetchone()
            if sat:
                status = "satisfied"
                reasons = [f"复用 {sat['run_token']}（输入指纹一致）"]
                run_token = sat["run_token"]
                content_digest = sat["artifact_sha256"]
            elif sticky == "cancelled":
                status, run_token, content_digest = "cancelled", None, None
                reasons = ["操作员撤销了本次重算（未发布）；retry 可恢复"]
            elif sticky == "failed" and stored and stored["desired_hash"] == desired:
                status, run_token, content_digest = "failed", None, None
                last = conn.execute(
                    """SELECT run_token,exit_code,note FROM runs
                       WHERE job_id=? AND scope_key=? AND partition=? AND status='failed'
                       ORDER BY finished_at DESC LIMIT 1""",
                    (job_id, scope, part),
                ).fetchone()
                note = f"退出码 {last['exit_code']}，{last['note']}" if last else ""
                reasons = [f"最近运行失败（{note}）；retry 后可重跑"]
            else:
                status, run_token, content_digest = "pending", None, None
                sticky = "" if sticky == "failed" else sticky
                paused_hit = sorted(used_sources & ctx.paused)
                if paused_hit:
                    status = "waiting"
                    reasons = [f"来源 {s} 已被操作员暂停" for s in paused_hit]
                else:
                    reasons = self._rerun_reasons(conn, job_id, scope, part, vmap)
        return {
            "status": status, "desired_hash": desired, "content_digest": content_digest,
            "inputs": details, "reasons": reasons, "run_token": run_token,
            "sticky": sticky, "content_map": cmap, "version_map": vmap,
        }

    def _rerun_reasons(self, conn, job_id, scope, part, vmap):
        prev = conn.execute(
            """SELECT run_token,version_map_json FROM runs
               WHERE job_id=? AND scope_key=? AND partition=? AND status='succeeded'
               ORDER BY finished_at DESC LIMIT 1""",
            (job_id, scope, part),
        ).fetchone()
        if not prev:
            return ["尚无成功运行，首次计算"]
        old = json.loads(prev["version_map_json"])
        reasons = []
        for key in sorted(set(old) | set(vmap)):
            if old.get(key) != vmap.get(key):
                reasons.append(f"输入 {key} 版本变化：{old.get(key)} → {vmap.get(key)}")
        return reasons or [f"相对 {prev['run_token']} 的输入版本已变化"]

    # ---------------------------------------------------------------- 运行
    def begin_run(self, job_id, scope, part, refresh_first=True):
        """签发运行授权：先落库再写请求文件。同一分区已有进行中授权则拒绝。"""
        if refresh_first:
            self.refresh()
        with self._db() as conn:
            conflict = conn.execute(
                """SELECT run_token FROM runs
                   WHERE job_id=? AND scope_key=? AND partition=? AND status='running'""",
                (job_id, scope, part),
            ).fetchone()
            if conflict:
                raise RunConflict(f"{job_id}@{scope}@{part} 已有进行中授权 {conflict['run_token']}")
            row = conn.execute(
                "SELECT * FROM partitions WHERE job_id=? AND scope_key=? AND partition=?",
                (job_id, scope, part),
            ).fetchone()
            if not row or row["status"] != "pending":
                raise ValueError(f"{job_id}@{scope}@{part} 当前不可运行（{row and row['status']}）")
            details = json.loads(row["inputs_json"])
            cmap = {d["key"]: d["content"] for d in details if d["ready"]}
            vmap = {d["key"]: d["version"] for d in details if d["ready"]}
            job = self.jobs[job_id]
            win = partition_window(job, part)
            token = "run-" + uuid.uuid4().hex[:12]
            scope_obj = {"grain": "county"} if job.scope == "county" else {"grain": "town", "town": scope}
            request = {
                "run_token": token,
                "job_id": job_id,
                "scope": scope_obj,
                "window": {"from_date": win.start.isoformat(), "until_date": win.end.isoformat()},
                "input_fingerprints": cmap,
            }
            out_dir = self.outputs_dir / "runs" / token
            out_dir.mkdir(parents=True, exist_ok=False)
            (out_dir / "request.json").write_text(
                json.dumps(request, ensure_ascii=False, indent=1), encoding="utf-8"
            )
            with conn:
                conn.execute(
                    """INSERT INTO runs
                       (run_token,job_id,scope_key,partition,desired_hash,request_json,
                        version_map_json,status,created_at)
                       VALUES (?,?,?,?,?,?,?,'running',?)""",
                    (token, job_id, scope, part, row["desired_hash"],
                     json.dumps(request, ensure_ascii=False, sort_keys=True),
                     json.dumps(vmap, ensure_ascii=False, sort_keys=True), _now()),
                )
        return token

    def finalize_run(self, token, exit_code, stderr=""):
        """按回执采纳或判败；退出码只记录不决定成败。"""
        with self._db() as conn:
            run = conn.execute("SELECT * FROM runs WHERE run_token=?", (token,)).fetchone()
            if not run:
                raise KeyError(f"未知运行授权 {token}")
            if run["status"] != "running":
                return dict(run)  # 幂等：已结案
            request = json.loads(run["request_json"])
            out_dir = self.outputs_dir / "runs" / token
            try:
                receipt = validate_receipt(out_dir, token, request)
            except ReceiptError as exc:
                status = "failed"
                note = f"回执无效：{exc}"
                receipt = None
            else:
                status = "succeeded"
                note = "回执有效，产物已采纳"
            if stderr:
                note += f"；stderr: {stderr}"
            with conn:
                conn.execute(
                    """UPDATE runs SET status=?, exit_code=?, receipt_json=?, artifact_path=?,
                       artifact_sha256=?, finished_at=?, note=? WHERE run_token=?""",
                    (
                        status, exit_code,
                        json.dumps(receipt, ensure_ascii=False) if receipt else None,
                        receipt["artifact"] if receipt else None,
                        receipt["sha256"] if receipt else None,
                        _now(), note, token,
                    ),
                )
                if status == "failed":
                    conn.execute(
                        """UPDATE partitions SET sticky='failed'
                           WHERE job_id=? AND scope_key=? AND partition=?""",
                        (run["job_id"], run["scope_key"], run["partition"]),
                    )
        return self.get_run(token)

    def run(self, limit=None, mode="ok", mode_for=None, job_id=None, scope=None,
            partition=None, launcher=None):
        """驱动全部可运行分区，直到没有进展或达到上限。返回运行次数。

        launcher(request_path, out_dir, mode) -> (exit_code, stderr)；
        默认启动外部演练程序，测试可注入协议兼容的进程内启动器，
        回执校验路径完全一致。
        """
        mode_for = dict(mode_for or {})
        launcher = launcher or self._subprocess_launcher
        total = 0
        while True:
            self.refresh()
            with self._db() as conn:
                rows = conn.execute(
                    "SELECT job_id,scope_key,partition FROM partitions WHERE status='pending'"
                ).fetchall()
            if job_id:
                rows = [r for r in rows if r["job_id"] == job_id]
            if scope:
                rows = [r for r in rows if r["scope_key"] == scope]
            if partition:
                rows = [r for r in rows if r["partition"] == partition]
            rows.sort(key=lambda r: (self._topo_idx[r["job_id"]], r["scope_key"], r["partition"]))
            if not rows:
                break
            progressed = False
            for r in rows:
                if limit is not None and total >= limit:
                    return total
                token = self.begin_run(r["job_id"], r["scope_key"], r["partition"],
                                       refresh_first=False)
                m = mode_for.get(r["job_id"], mode)
                out_dir = self.outputs_dir / "runs" / token
                exit_code, stderr = launcher(out_dir / "request.json", out_dir, m)
                self.finalize_run(token, exit_code, stderr)
                total += 1
                progressed = True
            if not progressed:
                break
        self.refresh()
        return total

    def _subprocess_launcher(self, request_path, out_dir, mode):
        return launch_worker(self.worker_path, request_path, out_dir, mode=mode)

    def recover(self):
        """重启恢复：进行中的授权有有效回执则采纳，否则标记丢失待重试。"""
        self.init()
        adopted, lost = [], []
        with self._db() as conn:
            rows = conn.execute("SELECT * FROM runs WHERE status='running'").fetchall()
            for run in rows:
                request = json.loads(run["request_json"])
                out_dir = self.outputs_dir / "runs" / run["run_token"]
                try:
                    receipt = validate_receipt(out_dir, run["run_token"], request)
                except ReceiptError as exc:
                    with conn:
                        conn.execute(
                            """UPDATE runs SET status='lost', finished_at=?, note=?
                               WHERE run_token=?""",
                            (_now(), f"进程中断且无可验证回执：{exc}", run["run_token"]),
                        )
                    lost.append(run["run_token"])
                else:
                    with conn:
                        conn.execute(
                            """UPDATE runs SET status='succeeded', receipt_json=?,
                               artifact_path=?, artifact_sha256=?, finished_at=?, note=?
                               WHERE run_token=?""",
                            (
                                json.dumps(receipt, ensure_ascii=False), receipt["artifact"],
                                receipt["sha256"], _now(),
                                "进程重启后采纳已有回执", run["run_token"],
                            ),
                        )
                    adopted.append(run["run_token"])
        self.refresh()
        return {"adopted": adopted, "lost": lost}

    def get_run(self, token):
        with self._db() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_token=?", (token,)).fetchone()
        return dict(row) if row else None

    # ---------------------------------------------------------------- 发布
    def publish(self):
        """按发布组求闭包，成套推出新版本；内容未变的组不重复发布。"""
        self.refresh()
        with self._db() as conn:
            _, ctx, candidates = self._build_ctx(conn)
            published = []
            for group in self.groups:
                gid = group["group_id"]
                grains = {self.jobs[m].output_grain for m in group["members"]}
                period_grain = "monthly" if "monthly" in grains else "weekly" if "weekly" in grains else "daily"
                scopes = ctx.towns if group["scope"] == "town" else ["county"]
                for scope in scopes:
                    periods = set()
                    for m in group["members"]:
                        if self.jobs[m].output_grain != period_grain:
                            continue
                        for (s, p) in candidates.get(m, set()):
                            if s == scope:
                                periods.add(p)
                    for period in sorted(periods):
                        result = self._publish_one(conn, candidates, group, scope, period, period_grain)
                        if result:
                            published.append(result)
        return published

    def _closure(self, conn, group, scope, period, period_grain, candidates):
        """发布闭包：期内成员分区 + 沿已满足输入链的全部作业分区。"""
        period_win = self._period_window(period, period_grain)
        required = {}
        stack = []
        for m in group["members"]:
            for (s, p) in candidates.get(m, set()):
                if s != scope:
                    continue
                if partition_window(self.jobs[m], p).overlaps(period_win):
                    stack.append((m, s, p))
        while stack:
            key = stack.pop()
            if key in required:
                continue
            row = conn.execute(
                "SELECT * FROM partitions WHERE job_id=? AND scope_key=? AND partition=?",
                (key[0], key[1], key[2]),
            ).fetchone()
            if row is None:
                continue
            required[key] = dict(row)
            if row["status"] == "satisfied":
                for d in json.loads(row["inputs_json"]):
                    if d["kind"] == "job" and d["ready"]:
                        stack.append((d["job"], d["scope"], d["partition"]))
        return required

    def _period_window(self, period, grain):
        if grain == "daily":
            d = parse_day(period)
            return Window(d, d + DAY)
        if grain == "weekly":
            w = parse_day(period)
            return Window(w, w + 7 * DAY)
        m = parse_day(period + "-01")
        return Window(m, month_add(m))

    def _publish_one(self, conn, candidates, group, scope, period, period_grain):
        required = self._closure(conn, group, scope, period, period_grain, candidates)
        if not required:
            return None
        blocked = [k for k, r in required.items() if r["status"] != "satisfied"]
        if blocked:
            return None
        members = {}
        for (job_id, s, p), r in sorted(required.items()):
            members[f"{job_id}@{s}@{p}"] = {
                "job_id": job_id, "scope": s, "partition": p,
                "run_token": r["run_token"], "artifact_sha256": r["content_digest"],
            }
        digests_hash = _sha_json({k: v["artifact_sha256"] for k, v in members.items()})
        row = conn.execute(
            """SELECT version, digests_hash FROM publications
               WHERE group_id=? AND scope_key=? AND period=?
               ORDER BY version DESC LIMIT 1""",
            (group["group_id"], scope, period),
        ).fetchone()
        if row and row["digests_hash"] == digests_hash:
            return None
        version = (row["version"] if row else 0) + 1
        gid = group["group_id"]
        base = self.outputs_dir / "published" / gid / scope / period
        vdir = base / f"v{version}"
        vdir.mkdir(parents=True, exist_ok=False)
        files = {}
        for label, meta in members.items():
            run = conn.execute(
                "SELECT artifact_path FROM runs WHERE run_token=?", (meta["run_token"],)
            ).fetchone()
            safe = label.replace("@", "__").replace("/", "_") + ".json"
            dest = vdir / "artifacts" / safe
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(run["artifact_path"], dest)
            files[label] = f"artifacts/{safe}"
            meta["file"] = files[label]
        manifest = {
            "group_id": gid, "scope": scope, "period": period, "version": version,
            "published_at": _now(), "members": members,
        }
        tmp = vdir / "manifest.json.tmp"
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(vdir / "manifest.json")
        latest_tmp = base / "LATEST.tmp"
        latest_tmp.write_text(f"v{version}", encoding="utf-8")
        latest_tmp.replace(base / "LATEST")
        with conn:
            conn.execute(
                """INSERT INTO publications
                   (group_id,scope_key,period,version,digests_hash,manifest_json,published_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (gid, scope, period, version, digests_hash,
                 json.dumps(manifest, ensure_ascii=False, sort_keys=True), _now()),
            )
        return {"group": gid, "scope": scope, "period": period, "version": version,
                "members": len(members)}

    # ---------------------------------------------------------------- 查询
    def status(self):
        computed = self.refresh()
        summary = {}
        waiting = []
        for (job_id, scope, part), row in sorted(computed.items()):
            summary.setdefault(job_id, {}).setdefault(row["status"], 0)
            summary[job_id][row["status"]] += 1
            if row["status"] in ("waiting", "failed", "cancelled"):
                waiting.append({
                    "partition": f"{job_id}@{scope}@{part}",
                    "status": row["status"], "reasons": row["reasons"],
                })
        return {"summary": summary, "attention": waiting}

    def explain(self, job_id, scope, part):
        self.refresh()
        with self._db() as conn:
            row = conn.execute(
                "SELECT * FROM partitions WHERE job_id=? AND scope_key=? AND partition=?",
                (job_id, scope, part),
            ).fetchone()
            if not row:
                raise KeyError(f"无分区 {job_id}@{scope}@{part}")
            runs = conn.execute(
                """SELECT run_token,status,exit_code,desired_hash,artifact_sha256,
                          artifact_path,created_at,finished_at,note FROM runs
                   WHERE job_id=? AND scope_key=? AND partition=? ORDER BY created_at""",
                (job_id, scope, part),
            ).fetchall()
        out = dict(row)
        out["inputs"] = json.loads(out.pop("inputs_json"))
        out["reasons"] = json.loads(out.pop("reasons_json"))
        out["runs"] = [dict(r) for r in runs]
        return out

    def impact_of(self, event_ids):
        """按真实依赖扩展给定事件的影响范围（用于计划与核对）。"""
        with self._db() as conn:
            _, ctx, candidates = self._build_ctx(conn)
            changed = []
            for eid in event_ids:
                row = conn.execute("SELECT * FROM events WHERE event_id=?", (eid,)).fetchone()
                if not row:
                    raise KeyError(f"未知事件 {eid}")
                changed.append((
                    row["source"], row["town"],
                    Window(parse_day(row["from_date"]), parse_day(row["until_date"])),
                ))
        return sorted(deps.impact_of_changes(changed, self.jobs, candidates, ctx))

    # ---------------------------------------------------------------- 操作
    def retry(self, job_id=None, scope=None, partition=None):
        """重试失败/已撤销范围：清除粘滞标记，下一轮 run 重新调度。"""
        self.init()
        sql = "UPDATE partitions SET sticky='' WHERE sticky!=''"
        args = []
        if job_id:
            sql += " AND job_id=?"
            args.append(job_id)
        if scope:
            sql += " AND scope_key=?"
            args.append(scope)
        if partition:
            sql += " AND partition=?"
            args.append(partition)
        with self._db() as conn, conn:
            cur = conn.execute(sql, args)
        self.refresh()
        return cur.rowcount

    def cancel(self, job_id=None, scope=None, partition=None):
        """撤销未发布的重算：仅作用于未满足且非进行中的分区；产物一律保留。"""
        self.refresh()
        sql = """UPDATE partitions SET sticky='cancelled'
                 WHERE status IN ('pending','failed','waiting')"""
        args = []
        if job_id:
            sql += " AND job_id=?"
            args.append(job_id)
        if scope:
            sql += " AND scope_key=?"
            args.append(scope)
        if partition:
            sql += " AND partition=?"
            args.append(partition)
        with self._db() as conn, conn:
            cur = conn.execute(sql, args)
        self.refresh()
        return cur.rowcount

    def pause_source(self, source):
        self.init()
        with self._db() as conn, conn:
            conn.execute(
                "INSERT OR REPLACE INTO controls(key,value) VALUES (?,?)",
                (f"paused:{source}", "1"),
            )
        self.refresh()

    def resume_source(self, source):
        self.init()
        with self._db() as conn, conn:
            conn.execute("DELETE FROM controls WHERE key=?", (f"paused:{source}",))
        self.refresh()
