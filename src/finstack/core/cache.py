"""Local cache under ~/.finstack/cache so nothing is downloaded twice.

Series (prices, NAVs, macro ...): one Parquet file per (data type, instrument). The index remembers
the date range already covered and the date up to which the data is FINAL (e.g. NSE end-of-day data
is final after 18:30 IST). A later request only downloads what is missing: dates before the covered
range, and anything after the final date (today's still-changing bar is always refreshed).
Coverage is kept contiguous, so a gap between the cache and a newer request is filled too.

Snapshots (quotes, option chains, results tables, news ...): one file per (data type, request),
reused until its time-to-live runs out.

Quarantine: rows that failed validation are kept in ~/.finstack/quarantine/<type>/ for inspection
instead of silently disappearing.

Parquet needs pyarrow; without it files are stored as pickle.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import pickle
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd

from . import config

_lock = threading.RLock()
_db: Optional[sqlite3.Connection] = None
_db_home: Optional[Path] = None


def root() -> Path:
    p = config.home() / "cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _conn() -> sqlite3.Connection:
    global _db, _db_home
    home = config.home()
    if _db is None or _db_home != home:
        _db = sqlite3.connect(str(root() / "index.db"), check_same_thread=False, timeout=10)
        _db_home = home
        _db.execute("PRAGMA journal_mode=WAL")
        _db.execute("""CREATE TABLE IF NOT EXISTS series (type TEXT, entity TEXT, path TEXT, covered_from TEXT,
                       covered_to TEXT, min_date TEXT, max_date TEXT, rows INTEGER, updated REAL,
                       PRIMARY KEY (type, entity))""")
        _db.execute("""CREATE TABLE IF NOT EXISTS snaps (type TEXT, key TEXT, path TEXT, fetched REAL,
                       about TEXT, PRIMARY KEY (type, key))""")
        _db.commit()
    return _db


def _safe(text: str) -> str:
    h = hashlib.sha1(text.encode()).hexdigest()[:8]
    return f"{re.sub(r'[^A-Za-z0-9._=-]+', '_', text)[:80]}-{h}"


def _key(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


def _write(obj: Any, base: Path) -> Path:
    base.parent.mkdir(parents=True, exist_ok=True)
    tmp_suffix = f".tmp{threading.get_ident()}"
    if isinstance(obj, pd.DataFrame):
        try:
            path = base.with_suffix(".parquet")
            obj.to_parquet(str(path) + tmp_suffix, index=True)
            Path(str(path) + tmp_suffix).replace(path)
            base.with_suffix(".pkl").unlink(missing_ok=True)
            return path
        except Exception:   # no pyarrow, or a column parquet cannot store
            Path(str(base.with_suffix(".parquet")) + tmp_suffix).unlink(missing_ok=True)
    path = base.with_suffix(".pkl")
    with open(str(path) + tmp_suffix, "wb") as fh:
        pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
    Path(str(path) + tmp_suffix).replace(path)
    return path


def _read(path: str) -> Any:
    p = Path(path)
    if not p.exists():
        return None
    if p.suffix == ".parquet":
        return pd.read_parquet(p)
    with open(p, "rb") as fh:
        return pickle.load(fh)


def _d(x) -> Optional[dt.date]:
    if x is None or x == "":
        return None
    if isinstance(x, dt.datetime):
        return x.date()
    if isinstance(x, dt.date):
        return x
    return dt.date.fromisoformat(str(x)[:10])


# ------------------------------------------------------------------ series
def series_info(dtype: str, entity: str) -> Optional[dict]:
    with _lock:
        r = _conn().execute("SELECT path, covered_from, covered_to, min_date, max_date, rows, updated FROM series "
                            "WHERE type=? AND entity=?", (dtype, entity)).fetchone()
    if not r:
        return None
    return dict(zip(("path", "covered_from", "covered_to", "min_date", "max_date", "rows", "updated"), r))


def missing_ranges(dtype: str, entity: str, start: dt.date, end: dt.date) -> List[Tuple[dt.date, dt.date]]:
    """Date ranges that still have to be downloaded for [start, end]."""
    info = series_info(dtype, entity)
    if not info or not Path(info["path"]).exists():
        return [(start, end)]
    cf, ct = _d(info["covered_from"]), _d(info["covered_to"])
    out = []
    if start < cf:
        out.append((start, cf - dt.timedelta(days=1)))
    if end > ct:
        out.append((ct + dt.timedelta(days=1), end))
    return [(a, b) for a, b in out if a <= b]


def read_series(dtype: str, entity: str, start=None, end=None, date_col: str = "date") -> Optional[pd.DataFrame]:
    info = series_info(dtype, entity)
    if not info:
        return None
    df = _read(info["path"])
    if df is None or not isinstance(df, pd.DataFrame):
        return None
    if date_col in df.columns and (start or end):
        d = pd.to_datetime(df[date_col])
        if getattr(d.dt, "tz", None) is not None:
            d = d.dt.tz_localize(None)
        m = pd.Series(True, index=df.index)
        if start:
            m &= d >= pd.Timestamp(start)
        if end:
            m &= d < pd.Timestamp(_d(end) + dt.timedelta(days=1))
        df = df[m]
    return df.reset_index(drop=True)


def write_series(dtype: str, entity: str, df: pd.DataFrame, covered: Tuple[dt.date, dt.date],
                 final_through: dt.date, date_col: str = "date", keys: Tuple[str, ...] = ()) -> pd.DataFrame:
    """Merge new rows into the stored series and extend the covered range.

    covered: the date range that was requested from the source; final_through: last date whose data
    will not change any more. Only [covered_from, min(covered_to, final_through)] counts as cached.
    """
    with _lock:
        info = series_info(dtype, entity)
        old = _read(info["path"]) if info else None
        frames = [f for f in (old, df) if isinstance(f, pd.DataFrame) and not f.empty]
        merged = pd.concat(frames, ignore_index=True) if frames else df.iloc[0:0]
        if date_col in merged.columns and not merged.empty:
            subset = [date_col, *[k for k in keys if k in merged.columns]]
            merged = merged.drop_duplicates(subset=subset, keep="last").sort_values(subset).reset_index(drop=True)
        base = root() / "series" / dtype / _safe(entity)
        path = _write(merged, base)
        cf, ct = covered
        ct = min(ct, final_through)
        if info:
            cf = min(cf, _d(info["covered_from"]))
            ct = max(ct, _d(info["covered_to"]))
        if ct < cf:                      # nothing final yet: remember nothing as covered
            ct = cf - dt.timedelta(days=1)
        dmin = dmax = None
        if date_col in merged.columns and not merged.empty:
            d = pd.to_datetime(merged[date_col])
            dmin, dmax = str(d.min())[:10], str(d.max())[:10]
        _conn().execute("INSERT OR REPLACE INTO series VALUES (?,?,?,?,?,?,?,?,?)",
                        (dtype, entity, str(path), cf.isoformat(), ct.isoformat(), dmin, dmax, len(merged),
                         time.time()))
        _conn().commit()
        return merged


# ------------------------------------------------------------------ snapshots
def get_snapshot(dtype: str, key: Any, ttl: Optional[float]) -> Any:
    """Cached object if younger than ttl seconds (ttl=None: never expires; ttl=0: never cached)."""
    if ttl == 0:
        return None
    k = _key(key)
    with _lock:
        r = _conn().execute("SELECT path, fetched FROM snaps WHERE type=? AND key=?", (dtype, k)).fetchone()
    if not r or (ttl is not None and time.time() - r[1] > ttl):
        return None
    try:
        return _read(r[0])
    except Exception:
        return None


def snapshot_age(dtype: str, key: Any) -> Optional[float]:
    with _lock:
        r = _conn().execute("SELECT fetched FROM snaps WHERE type=? AND key=?", (dtype, _key(key))).fetchone()
    return time.time() - r[0] if r else None


def put_snapshot(dtype: str, key: Any, obj: Any) -> None:
    k = _key(key)
    with _lock:
        path = _write(obj, root() / "snap" / dtype / _safe(k))
        _conn().execute("INSERT OR REPLACE INTO snaps VALUES (?,?,?,?,?)", (dtype, k, str(path), time.time(), k[:200]))
        _conn().commit()


# ------------------------------------------------------------------ quarantine
def quarantine(dtype: str, df: pd.DataFrame, reason: str, source: str = "") -> Optional[Path]:
    if df is None or df.empty:
        return None
    q = config.home() / "quarantine" / dtype
    q.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["quarantine_reason"] = reason
    df["quarantine_source"] = source
    df["quarantined_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    path = q / f"{dt.date.today():%Y%m%d}-{int(time.time() * 1000)}.csv"
    df.to_csv(path, index=False)
    return path


# ------------------------------------------------------------------ housekeeping
def info() -> pd.DataFrame:
    """What is cached: one row per series or snapshot."""
    with _lock:
        s = pd.read_sql_query("SELECT 'series' AS kind, type, entity AS item, covered_from, covered_to, rows, "
                              "updated FROM series", _conn())
        n = pd.read_sql_query("SELECT 'snapshot' AS kind, type, about AS item, fetched AS updated FROM snaps", _conn())
    out = pd.concat([s, n], ignore_index=True)
    if not out.empty:
        out["updated"] = pd.to_datetime(out["updated"], unit="s")
    return out


def clear(dtype: Optional[str] = None, entity: Optional[str] = None) -> int:
    """Delete cached data (everything, one data type, or one instrument of a type). Returns items removed."""
    n = 0
    with _lock:
        c = _conn()
        q, args = "WHERE 1=1", []
        if dtype:
            q += " AND type=?"
            args.append(dtype)
        if entity:
            q += " AND entity=?"
            args.append(entity)
        for (p,) in c.execute(f"SELECT path FROM series {q}", args).fetchall():
            Path(p).unlink(missing_ok=True)
            n += 1
        c.execute(f"DELETE FROM series {q}", args)
        if not entity:
            q2, a2 = ("WHERE type=?", [dtype]) if dtype else ("", [])
            for (p,) in c.execute(f"SELECT path FROM snaps {q2}", a2).fetchall():
                Path(p).unlink(missing_ok=True)
                n += 1
            c.execute(f"DELETE FROM snaps {q2}", a2)
        c.commit()
    return n
