"""持久状态：SQLite 事件、运行授权、分区状态、发布记录。

状态目录默认 state/，产物目录默认 outputs/，均可用 --state-dir/--outputs-dir
覆盖。所有写操作都在事务内完成，运行授权以主键保证唯一。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT UNIQUE NOT NULL,
  kind TEXT NOT NULL,
  source TEXT NOT NULL,
  town TEXT NOT NULL,
  from_date TEXT NOT NULL,
  until_date TEXT NOT NULL,
  ready INTEGER,
  fingerprint TEXT,
  complete INTEGER,
  correction INTEGER NOT NULL DEFAULT 0,
  received_at TEXT NOT NULL,
  payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs(
  run_token TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  partition TEXT NOT NULL,
  desired_hash TEXT NOT NULL,
  request_json TEXT NOT NULL,
  version_map_json TEXT NOT NULL,
  status TEXT NOT NULL,              -- running | succeeded | failed | lost
  exit_code INTEGER,
  receipt_json TEXT,
  artifact_path TEXT,
  artifact_sha256 TEXT,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS partitions(
  job_id TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  partition TEXT NOT NULL,
  status TEXT NOT NULL,              -- waiting|pending|running|satisfied|failed|cancelled
  desired_hash TEXT,
  content_digest TEXT,
  inputs_json TEXT NOT NULL,
  reasons_json TEXT NOT NULL,
  run_token TEXT,
  sticky TEXT NOT NULL DEFAULT '',   -- '' | failed | cancelled
  updated_at TEXT NOT NULL,
  PRIMARY KEY(job_id, scope_key, partition)
);
CREATE TABLE IF NOT EXISTS publications(
  group_id TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  period TEXT NOT NULL,
  version INTEGER NOT NULL,
  digests_hash TEXT NOT NULL,
  manifest_json TEXT NOT NULL,
  published_at TEXT NOT NULL,
  PRIMARY KEY(group_id, scope_key, period, version)
);
CREATE TABLE IF NOT EXISTS controls(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


def connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn):
    with conn:
        conn.executescript(SCHEMA)
