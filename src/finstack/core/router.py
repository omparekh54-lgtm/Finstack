"""The router: one call per data type, ranked fallbacks, cache, validation, provenance.

    fs.fetch("india_daily_prices", "RELIANCE", start="2020-01-01")
    fs.bulk("india_daily_prices", fs.fetch("india_indices", "NIFTY 500", what="constituents").symbol)

For each request the router
  1. resolves the symbol (NSE / BSE / ISIN / Yahoo / TradingView names),
  2. reads what is already cached and works out what is missing,
  3. orders the sources by their ranking for this data type, skipping sources that are not
     installed, need credentials you have not set, have a tripped breaker, or whose website is paused,
  4. calls them in turn (each request paced by the network governor) until one returns a table that
     has the right columns and passes the checks,
  5. stores the result and returns it with df.attrs["source"], ["attempts"], ["issues"].
"""
from __future__ import annotations

import concurrent.futures as cf
import dataclasses
import datetime as dt
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd

from . import cache, config, health, net, schema, validate
from .symbols import Instrument, resolve


class NoData(RuntimeError):
    """Every source failed (or none is available). .attempts lists what happened with each."""

    def __init__(self, dtype: str, what: str, attempts: List[Tuple[str, str]]):
        self.attempts = attempts
        lines = "\n  ".join(f"{s}: {o}" for s, o in attempts) or "no source is available for this request"
        super().__init__(f"No data for {dtype} {what}.\n  {lines}")


class _Empty(Exception):
    pass


@dataclass
class Req:
    dtype: str
    inst: Optional[Instrument] = None
    symbol: Optional[str] = None
    start: Optional[dt.date] = None
    end: Optional[dt.date] = None
    params: Dict[str, Any] = field(default_factory=dict)

    def p(self, key: str, default: Any = None) -> Any:
        v = self.params.get(key)
        return default if v is None else v

    def describe(self) -> str:
        bits = [self.symbol or ""]
        if self.start or self.end:
            bits.append(f"{self.start}..{self.end}")
        bits += [f"{k}={v}" for k, v in self.params.items() if v is not None]
        return " ".join(b for b in bits if b)


@dataclass
class Source:
    key: str                                     # ranking / registry key, e.g. "nse", "builtin:mfapi"
    fn: Callable[[Req], Any]                     # returns a table (DataFrame, list of dicts, polars ...)
    hosts: Tuple[str, ...] = ()                  # websites it calls (skipped while paused)
    libs: Optional[Tuple[str, ...]] = None       # registry keys that must be installed (default: key)
    creds: Optional[Tuple[str, ...]] = None      # settings it needs (default: config.CREDENTIALS[key])
    when: Optional[Callable[[Req], bool]] = None  # only for some requests (e.g. only indices)
    label: str = ""
    score: float = 0.0                           # rank when the key is not in the rankings table

    def __post_init__(self):
        self.label = self.label or self.key
        if self.libs is None:
            self.libs = () if self.key.startswith("builtin:") else (self.key,)
        if self.creds is None:
            self.creds = config.CREDENTIALS.get(self.key, ())


@dataclass
class Pipeline:
    name: str
    title: str
    kind: str                                    # "series" | "snapshot" | "daily_files"
    sources: List[Source]
    market: Optional[str] = "IN"                 # how to resolve symbols: IN | GLOBAL | None (guess) | "raw"
    needs_symbol: bool = True
    time_col: str = "date"
    required: Tuple[str, ...] = ("date", "close")
    tz: Optional[str] = None
    columns: Tuple[str, ...] = schema.OHLCV
    check: Callable = validate.nonempty
    ttl: Union[float, None, Callable[[Req], Optional[float]]] = 3600
    final: Optional[Callable[[Req], dt.date]] = None
    entity: Optional[Callable[[Req], str]] = None
    keys: Tuple[str, ...] = ()
    default_days: int = 365
    has_data: Optional[Callable[[dt.date, dt.date], bool]] = None  # skip ranges with no trading days
    bulk: Optional[Callable[["Pipeline", List[Req]], Dict[str, str]]] = None
    post: Optional[Callable[[pd.DataFrame, Req], pd.DataFrame]] = None
    finish: Optional[Callable[[pd.DataFrame, Req], pd.DataFrame]] = None   # after cache read (e.g. resample)
    normalize: bool = True
    extra_aliases: Optional[Dict[str, Iterable[str]]] = None
    params_doc: str = ""
    example: str = ""
    variants: Dict[str, "Pipeline"] = field(default_factory=dict)   # chosen by the `what=` parameter
    rank_as: Optional[str] = None                                      # rankings table to order by
    live_ttl: float = 300.0          # series: don't re-download the still-changing tail more often than this


PIPELINES: Dict[str, Pipeline] = {}


def register(p: Pipeline) -> Pipeline:
    PIPELINES[p.name] = p
    return p


def _load_pipelines():
    if not PIPELINES:
        import importlib

        importlib.import_module(__name__.rsplit(".", 2)[0] + ".adapters")   # registers every pipeline


# ------------------------------------------------------------------ ordering
def _scores(dtype: str) -> Dict[str, float]:
    from ..rankings import DATA_TYPES, _rows

    dtype = dtype.split(".", 1)[0]
    for d in DATA_TYPES:
        if d["key"] == dtype:
            return {r["key"]: r["overall"] for r in _rows(d)}
    return {}


def _skip_reason(src: Source, dtype: str) -> Optional[str]:
    from ..loader import is_installed
    from ..registry import CATALOG

    for lib in src.libs:
        if not is_installed(lib):
            extra = next((l.extra for l in CATALOG if l.key == lib), "")
            return f"not installed (pip install 'finstack[{extra}]')" if extra else "not installed"
    missing = [c for c in src.creds if not config.get(c)]
    if missing:
        return "needs " + ", ".join(missing)
    left = health.is_open(src.key, dtype)
    if left:
        return f"paused after repeated failures ({left / 60:.0f} min left)"
    for h in src.hosts:
        if net.is_blocked(h, more_than=10):         # paused websites: use another source instead of waiting
            return f"website {h} is paused by the rate limiter"
    return None


def plan(p: Pipeline, req: Req, only: Optional[Sequence[str]] = None,
         exclude: Sequence[str] = (), ignore_when: bool = False,
         skipped_last: bool = True) -> List[Tuple[Source, Optional[str]]]:
    """Sources in the order they will be tried, each with the reason it will be skipped (or None)."""
    scores = _scores(p.rank_as or p.name)
    rows = []
    for i, s in enumerate(p.sources):
        if only and s.key not in only and s.label not in only:
            continue
        if s.key in exclude or s.label in exclude:
            continue
        if s.when is not None and not ignore_when:
            try:
                if not s.when(req):
                    continue
            except Exception:
                continue
        why = _skip_reason(s, p.name)
        score = scores.get(s.key, s.score)
        if not why and health.demoted(s.key, p.name):
            score -= 10
        pos = list(only).index(s.key if s.key in only else s.label) if only else 0
        rows.append((pos, -score, i, s, why))
    rows.sort(key=lambda r: ((r[4] is not None) if skipped_last else False, r[0], r[1], r[2]))
    return [(r[3], r[4]) for r in rows]


# ------------------------------------------------------------------ calling sources
_pool = cf.ThreadPoolExecutor(max_workers=16, thread_name_prefix="finstack-src")


def _call(fn: Callable, req: Req, timeout: float):
    fut = _pool.submit(fn, req)
    try:
        return fut.result(timeout=timeout)
    except cf.TimeoutError:
        raise TimeoutError(f"no answer within {timeout:.0f}s") from None


def _next_day(d) -> dt.date:
    d = d.date() if isinstance(d, dt.datetime) else d
    return d + dt.timedelta(days=1)


def _shape(p: Pipeline, raw: Any, req: Req) -> pd.DataFrame:
    if p.normalize:
        df = schema.normalize(raw, required=p.required, time_col=p.time_col, tz=p.tz,
                              extra_aliases=p.extra_aliases)
    else:
        df = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw if isinstance(raw, list) else [raw])
        if df.empty:
            raise schema.SchemaError("empty table")
    if p.post:
        df = p.post(df, req)
    if df is None or df.empty:
        raise schema.SchemaError("empty table")
    if p.kind == "series" and p.time_col in df and req.start and req.end:
        t = pd.to_datetime(df[p.time_col])
        if getattr(t.dt, "tz", None) is not None:
            t = t.dt.tz_localize(None)
        df = df[(t >= pd.Timestamp(req.start)) & (t < pd.Timestamp(_next_day(req.end)))]
        if df.empty:
            raise schema.SchemaError("empty table (no rows in the requested dates)")
    return df


def run_sources(p: Pipeline, req: Req, only=None, exclude=(), timeout: float = 60.0,
                attempts: Optional[list] = None) -> pd.DataFrame:
    attempts = attempts if attempts is not None else []
    empties = tried = 0
    for src, why in plan(p, req, only, exclude):
        if why:
            attempts.append((src.label, f"skipped - {why}"))
            continue
        tried += 1
        t0 = time.monotonic()
        try:
            raw = _call(src.fn, req, timeout)
            df = _shape(p, raw, req)
            good, bad, notes = p.check(df)
            if len(bad):
                cache.quarantine(p.name, bad, "failed validation", src.label)
                if len(bad) > 0.2 * len(df):
                    raise ValueError(f"{len(bad)} of {len(df)} rows failed validation (quarantined)")
                notes = [f"{len(bad)} invalid row(s) quarantined"] + list(notes)
            if good.empty:
                raise schema.SchemaError("empty table after validation")
            good = good.copy()
            good["source"] = src.label
            health.record(src.key, p.name, True, time.monotonic() - t0, len(good))
            attempts.append((src.label, f"ok - {len(good)} rows"))
            good.attrs.update({"source": src.label, "attempts": list(attempts), "issues": list(notes),
                               "fetched_at": pd.Timestamp.now(tz="UTC").isoformat()})
            return good
        except schema.SchemaError as e:
            msg = str(e)
            if msg.startswith("empty") or msg == "no data":
                empties += 1
                health.record(src.key, p.name, True, time.monotonic() - t0, 0)
                attempts.append((src.label, "empty"))
                if empties >= 2:         # two independent sources agree there is nothing: stop asking
                    raise _Empty()
            else:
                health.record(src.key, p.name, False, time.monotonic() - t0, 0, f"schema: {msg}")
                attempts.append((src.label, f"wrong format - {msg[:160]}"))
        except Exception as e:  # noqa: BLE001 - any source failure means: try the next one
            msg = f"{type(e).__name__}: {e}"
            health.record(src.key, p.name, False, time.monotonic() - t0, 0, msg)
            attempts.append((src.label, f"failed - {msg[:200]}"))
    if empties and empties >= min(2, tried):
        raise _Empty()
    raise NoData(p.name, req.describe(), attempts)


# ------------------------------------------------------------------ single flight
_flights: Dict[tuple, threading.Lock] = defaultdict(threading.Lock)
_flights_lock = threading.Lock()


def _flight(*key) -> threading.Lock:
    with _flights_lock:
        return _flights[key]


# ------------------------------------------------------------------ public API
def _date(x) -> Optional[dt.date]:
    if x is None or x == "":
        return None
    if isinstance(x, dt.datetime):
        return x.date()
    if isinstance(x, dt.date):
        return x
    if isinstance(x, (int, float)):
        return dt.date.today() - dt.timedelta(days=int(x))
    s = str(x).strip()
    if s.endswith(("d", "y", "m")) and s[:-1].isdigit():         # "30d", "5y", "6m"
        n = int(s[:-1])
        return dt.date.today() - dt.timedelta(days=n * {"d": 1, "m": 31, "y": 366}[s[-1]])
    return pd.Timestamp(s).date() if s[:4].isdigit() else pd.to_datetime(s, dayfirst=True).date()


def _entity(p: Pipeline, req: Req) -> str:
    if p.entity:
        return p.entity(req)
    return req.inst.key if req.inst else (req.symbol or "all")


def _ttl(p: Pipeline, req: Req) -> Optional[float]:
    return p.ttl(req) if callable(p.ttl) else p.ttl


def _tag_cached(df: pd.DataFrame, attempts: list, issues: list) -> pd.DataFrame:
    srcs = list(dict.fromkeys(df["source"].dropna().astype(str))) if "source" in df else []
    df.attrs.update({"source": ", ".join(srcs) or "cache", "attempts": attempts, "issues": issues})
    return df


def _series(p: Pipeline, req: Req, refresh: bool, only, timeout: float, verify: bool) -> pd.DataFrame:
    today = dt.date.today()
    req.end = req.end or today
    req.start = req.start or (req.end - dt.timedelta(days=p.default_days))
    if req.start > req.end:
        raise ValueError(f"start {req.start} is after end {req.end}")
    entity = _entity(p, req)
    final = p.final(req) if p.final else today - dt.timedelta(days=1)
    attempts: list = []
    issues: list = []
    with _flight(p.name, entity):
        ranges = [(req.start, req.end)] if refresh else cache.missing_ranges(p.name, entity, req.start, req.end)
        info = cache.series_info(p.name, entity)
        if ranges and not refresh and info and time.time() - info["updated"] < p.live_ttl:
            ranges = [(a, b) for a, b in ranges if a <= final]     # the live tail was refreshed moments ago
        for a, b in ranges:
            if p.has_data and not p.has_data(a, b):
                cache.write_series(p.name, entity, pd.DataFrame(), (a, b), final, p.time_col, p.keys)
                continue
            sub = dataclasses.replace(req, start=a, end=b)
            try:
                df = run_sources(p, sub, only=only, timeout=timeout, attempts=attempts)
                issues += df.attrs.get("issues", [])
            except _Empty:
                df = pd.DataFrame()
                attempts.append(("all sources", f"no rows for {a}..{b} (not listed / no trading?)"))
            except NoData:
                if cache.series_info(p.name, entity) is None:
                    raise
                issues.append(f"could not download {a}..{b}; returning cached data only")
                continue
            cache.write_series(p.name, entity, df, (a, b), final, p.time_col, p.keys)
        out = cache.read_series(p.name, entity, req.start, req.end, p.time_col)
    if out is None or out.empty:
        raise NoData(p.name, req.describe(), attempts)
    if not ranges:
        attempts.append(("cache", f"ok - {len(out)} rows, nothing new to download"))
    out = _tag_cached(schema.order(out, p.columns), attempts, issues)
    if p.finish:
        out = p.finish(out, req)
    if verify:
        _verify(p, req, out, only, timeout)
    return out


def _verify(p: Pipeline, req: Req, df: pd.DataFrame, only, timeout: float) -> None:
    used = set(df["source"].dropna().astype(str)) if "source" in df else set()
    try:
        other = run_sources(p, dataclasses.replace(req), only=only, exclude=tuple(used), timeout=timeout)
        diff = validate.compare_close(df, other, p.time_col)
        df.attrs["verified"] = {"with": other.attrs.get("source"), "median_close_diff_pct": diff,
                                "agree": diff is not None and diff < 0.5}
    except Exception as e:  # noqa: BLE001
        df.attrs["verified"] = {"with": None, "error": str(e)[:200]}


def snapshot_key(req: Req) -> dict:
    return {"s": req.inst.key if req.inst else req.symbol, "start": req.start, "end": req.end, **req.params}


def _snapshot(p: Pipeline, req: Req, refresh: bool, only, timeout: float,
              max_age: Optional[float] = None) -> pd.DataFrame:
    key = snapshot_key(req)
    ttl = _ttl(p, req) if max_age is None else max_age
    if not refresh:
        hit = cache.get_snapshot(p.name, key, ttl)
        if isinstance(hit, pd.DataFrame):
            age = cache.snapshot_age(p.name, key) or 0
            return _tag_cached(hit, [("cache", f"ok - {age:.0f}s old")], [])
    attempts: list = []
    with _flight(p.name, str(key)):
        try:
            df = run_sources(p, req, only=only, timeout=timeout, attempts=attempts)
        except (NoData, _Empty) as e:
            stale = cache.get_snapshot(p.name, key, None)
            if isinstance(stale, pd.DataFrame):
                age = cache.snapshot_age(p.name, key) or 0
                out = _tag_cached(stale, attempts, [f"STALE: every source failed, returning data {age / 60:.0f} "
                                                    "min old"])
                out.attrs["stale"] = True
                return out
            if isinstance(e, _Empty):
                raise NoData(p.name, req.describe(), attempts) from None
            raise
        df = schema.order(df, p.columns).reset_index(drop=True)
        if ttl != 0:
            cache.put_snapshot(p.name, key, df)
    return df


def _daily_files(p: Pipeline, req: Req, refresh: bool, only, timeout: float) -> pd.DataFrame:
    from . import calendar

    end = req.end or calendar.now_ist().date()
    start = req.start or end
    days = calendar.trading_days(start, end)
    if not days:
        days = [calendar.previous_trading_day(end + dt.timedelta(days=1))]
    final = p.final(req) if p.final else end
    frames, attempts, issues = [], [], []
    for d in days:
        key = {"date": d.isoformat(), **req.params}
        ttl = None if d <= final else 900
        hit = None if refresh else cache.get_snapshot(p.name, key, ttl)
        if isinstance(hit, pd.DataFrame):
            frames.append(hit)
            continue
        sub = dataclasses.replace(req, start=d, end=d)
        try:
            df = run_sources(p, sub, only=only, timeout=timeout, attempts=attempts)
            df = schema.order(df, p.columns)
            cache.put_snapshot(p.name, key, df)
            frames.append(df)
            issues += df.attrs.get("issues", [])
        except (NoData, _Empty) as e:
            issues.append(f"{d}: no file ({str(e).splitlines()[0] if str(e) else 'not published yet?'})")
    if not frames:
        raise NoData(p.name, req.describe(), attempts)
    out = pd.concat(frames, ignore_index=True)
    return _tag_cached(out, attempts, issues)


def fetch(data_type: str, symbol: Optional[str] = None, start=None, end=None, *, refresh: bool = False,
          sources: Optional[Sequence[str]] = None, verify: bool = False, timeout: float = 60.0,
          max_age: Optional[float] = None, **params) -> pd.DataFrame:
    """One call for any data type. See fs.pipelines() for types and their parameters.

    symbol   NSE symbol, BSE code, ISIN, index name, Yahoo ticker, scheme code, series id ... (per type)
    start/end  'YYYY-MM-DD', a date, '5y' / '6m' / '30d', or None for the type's default window
    refresh  ignore the cache and download again
    sources  only use these sources, in this order (e.g. sources=["nse", "jugaad_data"])
    verify   also fetch from a second, independent source and compare closes (df.attrs["verified"])
    timeout  seconds to wait for one source before moving to the next
    max_age  snapshots (quotes, chains ...): accept a cached answer up to this many seconds old
    """
    _load_pipelines()
    if data_type not in PIPELINES:
        raise KeyError(f"Unknown data type '{data_type}'. Choose from: {', '.join(PIPELINES)}")
    p = _variant(PIPELINES[data_type], params)
    if symbol is None and p.needs_symbol:
        raise ValueError(f"{data_type} needs a symbol, e.g. {p.example}")
    inst = None
    if symbol is not None and p.market != "raw":
        inst = resolve(str(symbol), p.market)
    params = {k: v for k, v in params.items() if v is not None}
    req = Req(data_type, inst, None if symbol is None else str(symbol), _date(start), _date(end), params)
    if p.kind == "series":
        return _series(p, req, refresh, sources, timeout, verify)
    if p.kind == "daily_files":
        return _daily_files(p, req, refresh, sources, timeout)
    return _snapshot(p, req, refresh, sources, timeout, max_age)


def _variant(p: Pipeline, params: dict) -> Pipeline:
    what = params.get("what")
    if what is None or not p.variants:
        return p
    if what not in p.variants:
        raise ValueError(f"{p.name}: what= must be one of {sorted(p.variants)}")
    return p.variants[what]


def route(data_type: str, symbol: Optional[str] = None, **params) -> pd.DataFrame:
    """Dry run: the sources fetch() would try for this request, in order, and why any would be skipped."""
    _load_pipelines()
    p = _variant(PIPELINES[data_type], params)
    inst = resolve(str(symbol), p.market) if symbol is not None and p.market != "raw" else None
    req = Req(data_type, inst, symbol, None, None, params)
    scores = _scores(p.rank_as or p.name)
    rows = [{"order": i + 1, "source": s.label, "rank_score": scores.get(s.key, s.score),
             "status": "ready" if why is None else f"skip: {why}", "websites": ", ".join(s.hosts)}
            for i, (s, why) in enumerate(plan(p, req))]
    return pd.DataFrame(rows)


def bulk(data_type: str, symbols: Iterable[str], start=None, end=None, *, workers: int = 4,
         out: Optional[str] = None, fmt: str = "parquet", progress: bool = True, refresh: bool = False,
         **params) -> pd.DataFrame:
    """Fetch many symbols at once, politely and resumably.

    Uses the type's bulk route when it is cheaper (e.g. one NSE bhavcopy per day covers every stock),
    otherwise runs `workers` symbols in parallel; the network governor keeps every website within its
    limit no matter how many workers run. Already-cached data is not downloaded again, so re-running
    after an interruption continues where it stopped.

    Returns one long table with a `symbol` column (df.attrs["failed"] lists symbols that failed).
    out: folder to also write one file per symbol (fmt 'parquet' or 'csv').
    """
    _load_pipelines()
    p = _variant(PIPELINES[data_type], params)
    syms = list(dict.fromkeys(str(s) for s in symbols if s is not None and str(s).strip()))
    failed: Dict[str, str] = {}
    if p.bulk and len(syms) > 1 and not refresh:
        reqs = [Req(data_type, resolve(s, p.market) if p.market != "raw" else None, s, _date(start), _date(end),
                    dict(params)) for s in syms]
        try:
            note = p.bulk(p, reqs) or {}
            if progress and note:
                print(f"[finstack] bulk route: {note.get('route', '')}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001 - fall back to per-symbol fetching
            if progress:
                print(f"[finstack] bulk route failed ({e}); fetching symbol by symbol", file=sys.stderr)
    frames: Dict[str, pd.DataFrame] = {}
    done = 0

    extra = {"max_age": 300} if p.kind == "snapshot" and not refresh else {}

    def one(s):
        return s, fetch(data_type, s, start, end, refresh=refresh, **extra, **params)

    with cf.ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="finstack-bulk") as ex:
        futs = [ex.submit(one, s) for s in syms]
        for f in cf.as_completed(futs):
            done += 1
            try:
                s, df = f.result()
                frames[s] = df
            except Exception as e:  # noqa: BLE001
                s = syms[futs.index(f)]
                failed[s] = str(e).splitlines()[0][:200]
            if progress and (done % 10 == 0 or done == len(syms)):
                print(f"[finstack] {data_type}: {done}/{len(syms)} done, {len(failed)} failed", file=sys.stderr)
    if out:
        from pathlib import Path

        folder = Path(out)
        folder.mkdir(parents=True, exist_ok=True)
        for s, df in frames.items():
            name = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in s)
            (df.to_csv(folder / f"{name}.csv", index=False) if fmt == "csv"
             else df.to_parquet(folder / f"{name}.parquet", index=False))
    parts = []
    for s in syms:
        if s in frames:
            df = frames[s]
            df = df.assign(symbol=s) if "symbol" not in df or df["symbol"].isna().all() else df
            if "query" not in df:
                df = df.assign(query=s)
            parts.append(df)
    res = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    res.attrs["failed"] = failed
    return res


def pipelines() -> pd.DataFrame:
    """Every data type fs.fetch() understands, its parameters and its sources in ranked order."""
    _load_pipelines()
    rows = []
    for base in PIPELINES.values():
        for what, p in [(None, base)] + list(base.variants.items()):
            req = Req(p.name)
            steps = plan(p, req, ignore_when=True, skipped_last=False)
            order = [s.label for s, _ in steps]
            ready = [s.label for s, why in steps if why is None]
            rows.append({"type": base.name, "what": what or "", "title": p.title, "kind": p.kind,
                         "example": p.example, "params": p.params_doc, "sources": " > ".join(order),
                         "ready_now": ", ".join(ready)})
    return pd.DataFrame(rows)
