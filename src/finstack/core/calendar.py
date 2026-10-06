"""Trading calendar: Indian market days and hours, and when a day's data becomes final.

NSE holidays come from NSE's holiday master (cached for a week), then aynse's bundled list, then
the built-in list below (2025-2026, from NSE circulars). All times are IST unless stated.
"""
from __future__ import annotations

import datetime as dt
from functools import lru_cache
from typing import List, Optional, Set
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

_BUILTIN_HOLIDAYS = {
    # 2025 (NSE)
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01",
    "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22", "2025-11-05", "2025-12-25",
    # 2026 (NSE/CMTR/71775 as amended by NSE/CMTR/72260)
    "2026-01-15", "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14",
    "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20", "2026-11-10",
    "2026-11-24", "2026-12-25",
}

MARKET_OPEN = dt.time(9, 15)
MARKET_CLOSE = dt.time(15, 30)
PRE_OPEN = dt.time(9, 0)
EOD_FINAL = dt.time(18, 30)        # bhavcopy and EOD history are final after this time

# exchange suffix -> (timezone, local close time) for global "is this bar final" decisions
EXCHANGES = {
    "": ("America/New_York", dt.time(16, 0)), ".L": ("Europe/London", dt.time(16, 30)),
    ".DE": ("Europe/Berlin", dt.time(17, 30)), ".PA": ("Europe/Paris", dt.time(17, 30)),
    ".AS": ("Europe/Amsterdam", dt.time(17, 30)), ".SW": ("Europe/Zurich", dt.time(17, 30)),
    ".T": ("Asia/Tokyo", dt.time(15, 30)), ".HK": ("Asia/Hong_Kong", dt.time(16, 10)),
    ".SS": ("Asia/Shanghai", dt.time(15, 0)), ".SZ": ("Asia/Shanghai", dt.time(15, 0)),
    ".KS": ("Asia/Seoul", dt.time(15, 30)), ".TW": ("Asia/Taipei", dt.time(13, 30)),
    ".AX": ("Australia/Sydney", dt.time(16, 10)), ".TO": ("America/Toronto", dt.time(16, 0)),
    ".SI": ("Asia/Singapore", dt.time(17, 0)), ".NS": ("Asia/Kolkata", EOD_FINAL),
    ".BO": ("Asia/Kolkata", EOD_FINAL), ".ME": ("Europe/Moscow", dt.time(18, 50)),
}


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def _parse_nse_holidays(data) -> Set[dt.date]:
    out = set()
    rows = (data or {}).get("CM") or (data or {}).get("FO") or []
    for r in rows:
        try:
            out.add(dt.datetime.strptime(r["tradingDate"], "%d-%b-%Y").date())
        except Exception:
            pass
    return out


@lru_cache(maxsize=1)
def _holidays() -> frozenset:
    days = {dt.date.fromisoformat(d) for d in _BUILTIN_HOLIDAYS}
    try:
        from aynse import holidays as ay_holidays   # bundled list, no network
        days |= set(ay_holidays())
    except Exception:
        pass
    try:
        from . import cache

        live = cache.get_snapshot("calendar", "nse_holidays", ttl=7 * 86400)
        if live is None:
            from ..india import nse_client

            live = sorted(_parse_nse_holidays(nse_client().holidays("trading")))
            if live:
                cache.put_snapshot("calendar", "nse_holidays", live)
        days |= set(live or [])
    except Exception:
        pass
    return frozenset(days)


def refresh_holidays() -> None:
    _holidays.cache_clear()


def is_trading_day(d: dt.date) -> bool:
    return d.weekday() < 5 and d not in _holidays()


def trading_days(start: dt.date, end: dt.date) -> List[dt.date]:
    days, d = [], start
    while d <= end:
        if is_trading_day(d):
            days.append(d)
        d += dt.timedelta(days=1)
    return days


def previous_trading_day(d: dt.date) -> dt.date:
    d -= dt.timedelta(days=1)
    while not is_trading_day(d):
        d -= dt.timedelta(days=1)
    return d


def market_open(at: Optional[dt.datetime] = None) -> bool:
    at = (at or now_ist()).astimezone(IST)
    return is_trading_day(at.date()) and MARKET_OPEN <= at.time() <= MARKET_CLOSE


def in_session(at: Optional[dt.datetime] = None) -> bool:
    """Pre-open to close: quotes are moving and must not be cached long."""
    at = (at or now_ist()).astimezone(IST)
    return is_trading_day(at.date()) and PRE_OPEN <= at.time() <= MARKET_CLOSE


def india_final_through(at: Optional[dt.datetime] = None) -> dt.date:
    """Last date whose Indian end-of-day data will not change any more."""
    at = (at or now_ist()).astimezone(IST)
    d = at.date()
    if is_trading_day(d) and at.time() >= EOD_FINAL:
        return d
    return previous_trading_day(d)


def global_final_through(symbol: str = "", at: Optional[dt.datetime] = None) -> dt.date:
    """Last final daily bar for a Yahoo-style symbol: today once its exchange closed 3 hours ago, else yesterday.
    Weekends are fine either way (no bar exists, nothing to refetch)."""
    suffix = "." + symbol.rsplit(".", 1)[1].upper() if "." in symbol else ""
    if suffix in (".NS", ".BO"):
        return india_final_through(at)
    tz, close = EXCHANGES.get(suffix, EXCHANGES[""])
    local = (at or dt.datetime.now(dt.timezone.utc)).astimezone(ZoneInfo(tz))
    cutoff = (dt.datetime.combine(local.date(), close) + dt.timedelta(hours=3)).time()
    return local.date() if local.time() >= cutoff else local.date() - dt.timedelta(days=1)


def utc_final_through(at: Optional[dt.datetime] = None) -> dt.date:
    """For 24x7 markets (crypto) and UTC-dated series: yesterday (UTC) is final."""
    return (at or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).date() - dt.timedelta(days=1)
