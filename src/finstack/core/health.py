"""Source health: a log of every fetch attempt plus a circuit breaker per source.

* Every attempt is logged to ~/.finstack/health.db (source, data type, ok, latency, error).
* Breaker: 3 failures in a row, or one 403/429-style block, opens the breaker for that source
  (15 min, then 30 min, then 60 min if it keeps failing). One success closes it.
* Demotion: if a source succeeded less than half of its last 10 attempts in the past day,
  the router tries it after the healthy sources instead of in its ranked position.
"""
from __future__ import annotations

import atexit
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

from . import config

_BLOCK_WORDS = ("403", "429", "forbidden", "too many requests", "rate limit", "paused", "blocked")


@dataclass
class _Breaker:
    fails: int = 0
    opens: int = 0
    open_until: float = 0.0
    last_error: str = ""


_lock = threading.Lock()
_breakers: Dict[str, _Breaker] = {}
_db: Optional[sqlite3.Connection] = None


def close() -> None:
    """Close the database connection (called automatically when Python exits)."""
    global _db
    if _db is not None:
        try:
            _db.close()
        except sqlite3.Error:
            pass
        _db = None


atexit.register(close)


def _conn() -> sqlite3.Connection:
    global _db
    if _db is None:
        _db = sqlite3.connect(str(config.home() / "health.db"), check_same_thread=False, timeout=10)
        _db.execute("PRAGMA journal_mode=WAL")
        _db.execute("""CREATE TABLE IF NOT EXISTS events (ts REAL, source TEXT, data_type TEXT, ok INTEGER,
                       latency REAL, rows INTEGER, error TEXT)""")
        _db.execute("CREATE INDEX IF NOT EXISTS ev_src ON events(source, ts)")
        _db.commit()
    return _db


def record(source: str, data_type: str, ok: bool, latency: float, rows: int = 0, error: str = "") -> None:
    """Log one attempt and update the breaker."""
    with _lock:
        try:
            c = _conn()
            c.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                      (time.time(), source, data_type, int(ok), round(latency, 3), rows, (error or "")[:300]))
            c.commit()
        except sqlite3.Error:
            pass
        b = _breakers.setdefault(f"{data_type}/{source}", _Breaker())
        if ok:
            b.fails, b.opens, b.open_until, b.last_error = 0, 0, 0.0, ""
            return
        b.fails += 1
        b.last_error = (error or "")[:200]
        blocked = any(w in (error or "").lower() for w in _BLOCK_WORDS)
        if blocked or b.fails >= 3:
            minutes = (15, 30, 60)[min(b.opens, 2)]
            b.open_until = time.time() + minutes * 60
            b.opens += 1
            b.fails = 0


def is_open(source: str, data_type: str) -> float:
    """Seconds left before a tripped breaker lets the source be tried again for this data type (0 = closed).
    Breakers are per (data type, source): NSE's option-chain endpoint failing does not stop NSE prices.
    Whole-website blocks are handled by the network governor instead."""
    b = _breakers.get(f"{data_type}/{source}")
    return max(0.0, b.open_until - time.time()) if b else 0.0


def reset(source: Optional[str] = None) -> None:
    """Close the breakers of one source (or all of them)."""
    with _lock:
        if source:
            for k in [k for k in _breakers if k.split("/", 1)[1] == source]:
                _breakers.pop(k, None)
        else:
            _breakers.clear()


def demoted(source: str, data_type: str, window: int = 10, since_s: float = 86400) -> bool:
    """True if the source succeeded in fewer than half of its last `window` attempts for this type (past day)."""
    try:
        with _lock:
            rows = _conn().execute("SELECT ok FROM events WHERE source=? AND data_type=? AND ts>? "
                                   "ORDER BY ts DESC LIMIT ?",
                                   (source, data_type, time.time() - since_s, window)).fetchall()
    except sqlite3.Error:
        return False
    return len(rows) >= 4 and sum(r[0] for r in rows) / len(rows) < 0.5


def report(days: float = 7):
    """Per-source health over the last `days`: attempts, success rate, median latency, breaker state."""
    import pandas as pd

    with _lock:
        df = pd.read_sql_query("SELECT * FROM events WHERE ts > ?", _conn(), params=(time.time() - days * 86400,))
    if df.empty:
        return pd.DataFrame(columns=["source", "data_type", "attempts", "success_rate", "median_latency_s",
                                     "last_error", "breaker_open_for_s"])
    g = df.groupby(["source", "data_type"])
    out = g.agg(attempts=("ok", "size"), success_rate=("ok", "mean"), median_latency_s=("latency", "median"))
    last_err = df[df.ok == 0].sort_values("ts").groupby(["source", "data_type"])["error"].last()
    out = out.join(last_err.rename("last_error")).reset_index()
    out["success_rate"] = out["success_rate"].round(2)
    out["breaker_open_for_s"] = [round(is_open(s, t)) for s, t in zip(out["source"], out["data_type"])]
    return out.sort_values(["data_type", "success_rate"], ascending=[True, False]).reset_index(drop=True)
