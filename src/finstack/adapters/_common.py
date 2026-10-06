"""Shared helpers for the source adapters."""
from __future__ import annotations

import datetime as dt
import os
import tempfile
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from ..core import calendar, config
from ..core.router import Req
from ..loader import get

TMP = os.path.join(tempfile.gettempdir(), "finstack_dl")

NSE_HOSTS = ("www.nseindia.com",)
NSE_ARCHIVE_HOSTS = ("nsearchives.nseindia.com",)
BSE_HOSTS = ("api.bseindia.com",)
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
TV_HOSTS = ("data.tradingview.com",)


def nse():
    from ..india import nse_client

    return nse_client()


def bse():
    from ..company import bse_client

    return bse_client()


def dtm(d: dt.date) -> dt.datetime:
    return dt.datetime.combine(d, dt.time())


def chunks(a: dt.date, b: dt.date, days: int) -> List[Tuple[dt.date, dt.date]]:
    out, s = [], a
    while s <= b:
        e = min(b, s + dt.timedelta(days=days - 1))
        out.append((s, e))
        s = e + dt.timedelta(days=1)
    return out


def concat(frames: Iterable) -> pd.DataFrame:
    fs = [f for f in frames if f is not None and len(f)]
    return pd.concat(fs, ignore_index=True) if fs else pd.DataFrame()


def keep(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    return df[[c for c in cols if c in df.columns]]


def eq_series(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the normal EQ series when present (a stock in trade-to-trade trades as BE instead)."""
    if "series" in df.columns:
        s = df["series"].astype(str).str.strip().str.upper()
        if (s == "EQ").any():
            return df[s == "EQ"]
    return df


def india_final(req: Req) -> dt.date:
    return calendar.india_final_through()


def india_has_data(a: dt.date, b: dt.date) -> bool:
    return bool(calendar.trading_days(a, b))


def india_ttl(req: Req) -> float:
    """Live data: 5 s while the market is open, 1 h otherwise."""
    return 5 if calendar.in_session() else 3600


def bars_since(start: dt.date) -> int:
    return int((dt.date.today() - start).days * 5 / 7) + 20


def yf_history(ticker: str, start: Optional[dt.date], end: Optional[dt.date], interval: str = "1d",
               period: Optional[str] = None) -> pd.DataFrame:
    yf = get("yfinance")
    kw = dict(interval=interval, auto_adjust=False, actions=False)
    if period:
        kw["period"] = period
    else:
        kw.update(start=start.isoformat(), end=(end + dt.timedelta(days=1)).isoformat())
    df = yf.Ticker(ticker).history(**kw)
    if df is not None and len(df):
        df = df.drop(columns=[c for c in ("Dividends", "Stock Splits", "Capital Gains") if c in df.columns])
    return df


def yq_history(ticker: str, start: dt.date, end: dt.date, interval: str = "1d") -> pd.DataFrame:
    df = get("yahooquery").Ticker(ticker).history(start=start.isoformat(),
                                                    end=(end + dt.timedelta(days=1)).isoformat(),
                                                    interval=interval)
    if not isinstance(df, pd.DataFrame):
        raise RuntimeError(str(df)[:200])
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index(level=0, drop=True)
    return df.drop(columns=[c for c in ("dividends", "splits") if c in df.columns])


def tv_bars(symbol: str, exchange: str, interval: str, n_bars: int) -> pd.DataFrame:
    from ..india import tv_history

    return tv_history(symbol, exchange, interval, n_bars=min(max(n_bars, 10), 5000))


def need(cond, msg: str):
    if not cond:
        raise LookupError(msg)


def env(name: str) -> Optional[str]:
    return config.get(name)


def to_records(obj) -> list:
    """First list of row-dicts in a JSON-ish response."""
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        for v in obj.values():
            if isinstance(v, dict):
                r = to_records(v)
                if r:
                    return r
    return []
