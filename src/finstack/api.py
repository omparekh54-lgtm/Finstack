"""Unified helpers with automatic fallbacks.

Each function tries sources in order and returns the first that works, so a
Yahoo rate limit does not break your project.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

import pandas as pd
import requests

from .loader import get, is_installed

_TIMEOUT = 20


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise to columns Open/High/Low/Close/Volume with a DatetimeIndex."""
    if df is None or df.empty:
        raise ValueError("empty result")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns=str.title)
    keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[keep].sort_index()
    df.index = pd.to_datetime(df.index)
    df.index.name = "Date"
    return df


def history(symbol: str, start: Optional[str] = None, end: Optional[str] = None,
            interval: str = "1d",
            sources=("yfinance", "yahooquery", "tradingview", "stooq")) -> pd.DataFrame:
    """OHLCV history for any ticker, with fallback across sources.

    Indian stocks: use Yahoo style ("RELIANCE.NS", "TCS.BO"). For NSE-native sources
    (NSE website, jugaad-data) use finstack.nse_history("RELIANCE").
    """
    end = end or _dt.date.today().isoformat()
    start = start or (_dt.date.today() - _dt.timedelta(days=365)).isoformat()
    errors = []
    for src in sources:
        try:
            if src == "yfinance" and is_installed("yfinance"):
                yf = get("yfinance")
                return _norm(yf.download(symbol, start=start, end=end, interval=interval,
                                         progress=False, auto_adjust=False))
            if src == "yahooquery" and is_installed("yahooquery"):
                yq = get("yahooquery")
                df = yq.Ticker(symbol).history(start=start, end=end, interval=interval)
                if isinstance(df.index, pd.MultiIndex):
                    df = df.reset_index(level=0, drop=True)
                return _norm(df)
            if src == "tradingview" and is_installed("tvdatafeed"):
                from .india import tv_history
                sym, exch = symbol, "NASDAQ"
                if symbol.endswith(".NS"):
                    sym, exch = symbol[:-3], "NSE"
                elif symbol.endswith(".BO"):
                    sym, exch = symbol[:-3], "BSE"
                days = (pd.Timestamp(end) - pd.Timestamp(start)).days + 10
                return _norm(tv_history(sym, exch, interval, n_bars=days).loc[start:end])
            if src == "stooq" and is_installed("pandas_datareader"):
                pdr = get("pandas_datareader")
                s = symbol.lower()
                if "." not in s and not s.startswith("^"):
                    s += ".us"
                return _norm(pdr.DataReader(s, "stooq", start, end))
        except Exception as e:  # try next source
            errors.append(f"{src}: {e}")
    raise RuntimeError("All sources failed for %s:\n  %s" % (symbol, "\n  ".join(errors) or "no source installed"))


def quote(symbol: str) -> dict:
    """Latest price info. Uses yfinance fast_info, falls back to last close."""
    if is_installed("yfinance"):
        try:
            fi = get("yfinance").Ticker(symbol).fast_info
            return {"symbol": symbol, "price": float(fi["last_price"]),
                    "currency": fi.get("currency"), "source": "yfinance"}
        except Exception:
            pass
    df = history(symbol, start=(_dt.date.today() - _dt.timedelta(days=10)).isoformat())
    return {"symbol": symbol, "price": float(df["Close"].iloc[-1]), "currency": None, "source": "history"}


def fundamentals(symbol: str) -> dict:
    """Statements and key ratios from Yahoo (works for NSE as 'TCS.NS')."""
    yf = get("yfinance")
    t = yf.Ticker(symbol)
    return {
        "info": t.info,
        "income_statement": t.income_stmt,
        "balance_sheet": t.balance_sheet,
        "cash_flow": t.cashflow,
    }


def symbols(country: str = "India", asset: str = "equities", **filters) -> pd.DataFrame:
    """Symbol universe from FinanceDatabase (300k+ tickers).
    asset = equities | etfs | funds | indices | currencies | cryptos. Extra filters e.g. sector="Financials".
    """
    fd = get("financedatabase")
    cls = {"equities": fd.Equities, "etfs": fd.ETFs, "funds": fd.Funds, "indices": fd.Indices,
           "currencies": fd.Currencies, "cryptos": fd.Cryptos}[asset]
    kw = dict(filters)
    if country and asset in ("equities", "funds"):
        kw["country"] = country
    return cls().select(**kw)


def fx(base: str = "USD", target: str = "INR", date: str = "latest") -> float:
    """Currency rate from Frankfurter (ECB data). No key, no extra install."""
    r = requests.get(f"https://api.frankfurter.dev/v1/{date}",
                     params={"from": base.upper(), "to": target.upper()}, timeout=_TIMEOUT)
    r.raise_for_status()
    return float(r.json()["rates"][target.upper()])


def crypto_price(coin_id: str = "bitcoin", vs: str = "usd") -> float:
    """Crypto spot price from CoinGecko public API."""
    if is_installed("pycoingecko"):
        cg = get("pycoingecko").CoinGeckoAPI()
        return float(cg.get_price(ids=coin_id, vs_currencies=vs)[coin_id][vs])
    r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                     params={"ids": coin_id, "vs_currencies": vs}, timeout=_TIMEOUT)
    r.raise_for_status()
    return float(r.json()[coin_id][vs])


def macro(series_id: str, source: str = "fred", start: Optional[str] = None, api_key: Optional[str] = None):
    """Macro series. source='fred' (needs FRED_API_KEY or api_key) or 'worldbank' (indicator id)."""
    import os

    if source == "fred":
        key = api_key or os.environ.get("FRED_API_KEY")
        if key:
            return get("fredapi").Fred(api_key=key).get_series(series_id, observation_start=start)
        pdr = get("pandas_datareader")
        return pdr.DataReader(series_id, "fred", start or "2000-01-01")
    if source == "worldbank":
        wb = get("wbgapi")
        return wb.data.DataFrame(series_id, "IND", mrv=20)
    raise ValueError("source must be 'fred' or 'worldbank'")


def filings(ticker: str, form: str = "10-K", identity: Optional[str] = None):
    """SEC filings for a US ticker via edgartools. identity='Your Name you@email.com'."""
    ed = get("edgar")
    if identity:
        ed.set_identity(identity)
    return ed.Company(ticker).get_filings(form=form)


RSS_FEEDS = {
    # India (Business Standard verified live 2026-10-05; others are long-standing public feeds)
    "business_standard": "https://www.business-standard.com/rss/markets-106.rss",
    "economic_times": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "moneycontrol": "https://www.moneycontrol.com/rss/latestnews.xml",
    "livemint": "https://www.livemint.com/rss/markets",
    # Global (MarketWatch verified live 2026-10-05)
    "marketwatch": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "cnbc": "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "wsj": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
}


def news(sources=("business_standard", "economic_times", "moneycontrol", "livemint"),
         query: Optional[str] = None) -> pd.DataFrame:
    """Latest market headlines from RSS feeds (no key). Default = Indian sources.
    sources: names from finstack.api.RSS_FEEDS or any RSS URL. query: keep only headlines containing it.
    Feeds that fail are skipped; df.attrs['failed'] lists them.
    """
    import feedparser

    if isinstance(sources, str):
        sources = (sources,)
    rows, failed = [], []
    for src in sources:
        url = RSS_FEEDS.get(src, src)
        try:
            r = requests.get(url, timeout=_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 finstack"})
            r.raise_for_status()
            for e in feedparser.parse(r.content).entries:
                rows.append({"source": src, "title": e.get("title"), "published": e.get("published"),
                             "link": e.get("link"), "summary": e.get("summary")})
        except Exception as ex:
            failed.append(f"{src}: {str(ex)[:100]}")
    df = pd.DataFrame(rows, columns=["source", "title", "published", "link", "summary"])
    if query:
        df = df[df["title"].str.contains(query, case=False, na=False)]
    df.attrs["failed"] = failed
    return df.reset_index(drop=True)
