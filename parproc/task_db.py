"""
Task database for recording run history and estimating expected duration.

When task_db_path is set via set_options(), parproc records each task run
(start/end/status) in an SQLite DB. The live view uses get_expected_duration()
to show determinate progress bars when history exists.

Also stores a result cache for incremental builds: cached proc outputs and
input fingerprints so that unchanged procs can be auto-skipped.
"""

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

# Module-level current DB; set by set_path(), used by par.py and term.py
_current: Optional["TaskDB"] = None


def get_current() -> Optional["TaskDB"]:
    """Return the current task DB instance if one is set, else None."""
    return _current


def set_path(path: str | None) -> None:
    """
    Set the task database path. When path is set, open/create DB and create
    table if missing. When None, clear the current DB (hooks/reads become no-op).
    """
    global _current  # pylint: disable=global-statement  # Module-level singleton; required
    if path is None:
        if _current is not None:
            _current.close()
        _current = None
        return
    _current = TaskDB(path)


def on_task_start(task_name: str, started_at: datetime, run_id: str) -> None:
    """Record that a task has started. run_id identifies this run for matching on_task_end. No-op if no DB is set."""
    if _current is None:
        return
    _current.record_start(task_name, started_at, run_id)


def on_task_end(run_id: str, ended_at: datetime, status: str) -> None:
    """Record that a task has ended with the given status. run_id must match the one passed to on_task_start. No-op if no DB is set.
    status distinguishes failure types: 'success', 'failed', 'timeout' (and 'dep_failed' if ever recorded)."""
    if _current is None:
        return
    _current.record_end(run_id, ended_at, status)


def get_expected_duration(
    task_name: str,
    window: int = 10,
    decay: float = 0.8,
    success_only: bool = True,
) -> float | None:
    """
    Return expected duration in seconds for the task, or None if no history.

    Uses last `window` completed runs, optionally only successful ones,
    with exponential decay (most recent run has weight 1, then decay, decay^2, ...).
    """
    if _current is None:
        return None
    return _current.expected_duration(task_name, window=window, decay=decay, success_only=success_only)


@dataclass
class CachedResult:
    """A cached proc result stored in the task DB."""

    proc_name: str
    output: Any
    timestamp: float
    input_fingerprint: dict[str, float]


def save_result(proc_name: str, output: Any, input_fingerprint: dict[str, float]) -> None:
    """Persist a proc's output and input fingerprint. No-op if no DB is set."""
    if _current is None:
        return
    _current.save_cached_result(proc_name, output, input_fingerprint)


def load_result(proc_name: str) -> CachedResult | None:
    """Load a cached proc result. Returns None if not found or no DB is set."""
    if _current is None:
        return None
    return _current.load_cached_result(proc_name)


def clear_result(proc_name: str | None = None) -> None:
    """Clear cached result(s). If proc_name is None, clear all. No-op if no DB is set."""
    if _current is None:
        return
    _current.clear_cached_result(proc_name)


class TaskDB:
    """SQLite-backed store of task runs (started_at, ended_at, status)."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None
        self._open()

    def _open(self) -> None:
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._conn = sqlite3.connect(self._path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT NOT NULL,
                task_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS result_cache (
                proc_name TEXT PRIMARY KEY,
                output_json TEXT NOT NULL,
                timestamp REAL NOT NULL,
                input_fingerprint TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
        # Migrate existing DBs: add run_id column if missing (old rows get NULL)
        try:
            self._conn.execute("ALTER TABLE runs ADD COLUMN run_id TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def record_start(self, task_name: str, started_at: datetime, run_id: str) -> None:
        if self._conn is None:
            return
        started_str = started_at.isoformat()
        self._conn.execute(
            "INSERT INTO runs (run_id, task_name, started_at, ended_at, status) VALUES (?, ?, ?, NULL, NULL)",
            (run_id, task_name, started_str),
        )
        self._conn.commit()

    def record_end(self, run_id: str, ended_at: datetime, status: str) -> None:
        if self._conn is None:
            return
        ended_str = ended_at.isoformat()
        self._conn.execute(
            "UPDATE runs SET ended_at = ?, status = ? WHERE run_id = ?",
            (ended_str, status, run_id),
        )
        self._conn.commit()

    def save_cached_result(self, proc_name: str, output: Any, input_fingerprint: dict[str, float]) -> None:
        if self._conn is None:
            return
        import time  # pylint: disable=import-outside-toplevel

        output_json = json.dumps(output)
        fingerprint_json = json.dumps(input_fingerprint)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO result_cache (proc_name, output_json, timestamp, input_fingerprint)
            VALUES (?, ?, ?, ?)
            """,
            (proc_name, output_json, time.time(), fingerprint_json),
        )
        self._conn.commit()

    def load_cached_result(self, proc_name: str) -> CachedResult | None:
        if self._conn is None:
            return None
        cursor = self._conn.execute(
            "SELECT output_json, timestamp, input_fingerprint FROM result_cache WHERE proc_name = ?",
            (proc_name,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        output = json.loads(row[0])
        timestamp: float = row[1]
        input_fingerprint: dict[str, float] = json.loads(row[2])
        return CachedResult(
            proc_name=proc_name, output=output, timestamp=timestamp, input_fingerprint=input_fingerprint
        )

    def clear_cached_result(self, proc_name: str | None = None) -> None:
        if self._conn is None:
            return
        if proc_name is None:
            self._conn.execute("DELETE FROM result_cache")
        else:
            self._conn.execute("DELETE FROM result_cache WHERE proc_name = ?", (proc_name,))
        self._conn.commit()

    def expected_duration(
        self,
        task_name: str,
        window: int = 10,
        decay: float = 0.8,
        success_only: bool = True,
    ) -> float | None:
        if self._conn is None:
            return None
        cursor = self._conn.execute(
            """
            SELECT started_at, ended_at, status FROM runs
            WHERE task_name = ? AND ended_at IS NOT NULL
            ORDER BY ended_at DESC
            LIMIT ?
            """,
            (task_name, window),
        )
        rows = cursor.fetchall()
        if not rows:
            return None

        if success_only:
            rows = [r for r in rows if r[2] == "success"]  # "dep_failed" never counted
        if not rows:
            return None

        durations: list[float] = []
        for started_str, ended_str, _ in rows:
            started = datetime.fromisoformat(started_str)
            ended = datetime.fromisoformat(ended_str)
            durations.append((ended - started).total_seconds())

        # Weights: w_i = decay^i for i = 0..n-1 (0 = most recent)
        total_weight = 0.0
        weighted_sum = 0.0
        for i, d in enumerate(durations):
            w = decay**i
            total_weight += w
            weighted_sum += d * w
        if total_weight == 0:
            return None
        return weighted_sum / total_weight
