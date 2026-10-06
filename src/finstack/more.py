"""More data sources: global macro, rates, MCX commodities, other countries' markets,
news and alternative data.
"""
from __future__ import annotations

import datetime as _dt
import io
from typing import Iterable, Optional, Union  # noqa: F401

import pandas as pd
import requests

from .loader import get

_TIMEOUT = 30


# =============================================================== macro & rates
def dbnomics(series_id: str) -> pd.DataFrame:
    """Any series from DBnomics, which mirrors 80+ providers (IMF, OECD, ECB, BIS, World Bank,
    Eurostat, ILO, national statistics offices incl. RBI-sourced data).
    series_id looks like 'IMF/WEO:2025-10/IND.NGDP_RPCH.pcent_change' (copy it from db.nomics.world).
    """
    return get("dbnomics").fetch_series(series_id)


def sdmx_client(source: str = "IMF_DATA"):
    """SDMX client for official statistics. Sources include IMF_DATA, ECB, BIS, OECD, ESTAT,
    WB_WDI, ILO, UNSD. See sdmx1 docs for queries.
    """
    return get("sdmx").Client(source)


def imf_weo(version: Optional[tuple] = None) -> pd.DataFrame:
    """IMF World Economic Outlook database: GDP, inflation, debt, current account for ~190
    countries, with forecasts. version e.g. ('October', 2025); default = latest.
    """
    return get("imf_reader", "weo").fetch_data(version)


def ecb(series_key: str, start: Optional[str] = None) -> pd.DataFrame:
    """European Central Bank series, e.g. 'EXR.D.INR.EUR.SP00.A' (EUR/INR daily)."""
    return get("ecbdata").ecbdata.get_series(series_key, start=start)


def eurostat(code: str, **filters) -> pd.DataFrame:
    """Eurostat dataset by code, e.g. 'prc_hicp_manr' (EU inflation)."""
    return get("eurostat").get_data_df(code, **filters)


def us_yield_curve(year: Optional[int] = None) -> pd.DataFrame:
    """Daily US Treasury par yield curve (1M to 30Y) straight from treasury.gov. No install needed."""
    year = year or _dt.date.today().year
    url = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
           f"daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve"
           f"&field_tdr_date_value={year}&page&_format=csv")
    r = requests.get(url, timeout=_TIMEOUT, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/126.0 Safari/537.36", "Accept": "text/csv,*/*"})
    r.raise_for_status()
    if not r.text.lstrip().startswith("Date"):
        raise ValueError(f"treasury.gov did not return CSV (got: {r.text.strip()[:80]!r})")
    df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"]).set_index("Date").sort_index()
    return df


# =============================================================== MCX commodities (India)
def mcx(what: str = "market_watch", **kwargs) -> pd.DataFrame:
    """MCX India commodity data via mcxlib.

    what = market_watch | bhavcopy | history | option_chain | most_active | gainers | losers |
           expiries | contracts | pcr | indices
    e.g. fs.mcx("option_chain", commodity="GOLD", expiry="05DEC2026")
         fs.mcx("history", start_date="20260101", end_date="20260131")
    """
    md = get("mcxlib").market_data
    fn = {"market_watch": md.get_market_watch, "bhavcopy": md.get_bhav_copy,
          "history": md.get_historical_date_wise_data, "option_chain": md.get_option_chain,
          "most_active": md.get_most_active_contracts, "gainers": md.get_top_gainers,
          "losers": md.get_top_losers, "expiries": md.get_recent_expires,
          "contracts": md.get_available_contracts, "pcr": md.get_put_call_ratio,
          "indices": md.get_mcx_icomdex_indices}[what]
    return fn(**kwargs)


# =============================================================== other countries
def world_history(market: str, symbol: str, start: str = "2025-01-01", end: Optional[str] = None) -> pd.DataFrame:
    """Daily prices from a country's own exchange data.

    market = 'CN' (China A-shares, akshare) | 'KR' (Korea, pykrx; needs KRX_ID/KRX_PW env vars)
           | 'TW' (Taiwan, twstock) | 'RU' (Moscow Exchange, apimoex)
    For most other countries use fs.history() with Yahoo suffixes (.L, .T, .HK, .DE, .AX ...)
    or fs.tv_history(symbol, exchange).
    """
    end = end or _dt.date.today().isoformat()
    m = market.upper()
    if m == "CN":
        return get("akshare").stock_zh_a_hist(symbol=symbol, start_date=start.replace("-", ""),
                                              end_date=end.replace("-", ""), adjust="qfq")
    if m == "KR":
        return get("pykrx").stock.get_market_ohlcv(start.replace("-", ""), end.replace("-", ""), symbol)
    if m == "TW":
        s = get("twstock").Stock(symbol, initial_fetch=False)
        y, mo = int(start[:4]), int(start[5:7])
        return pd.DataFrame(s.fetch_from(y, mo))
    if m == "RU":
        with requests.Session() as sess:
            return pd.DataFrame(get("apimoex").get_board_history(sess, symbol, start=start, end=end))
    raise ValueError("market must be CN, KR, TW or RU")


# =============================================================== news & alternative data
def trends(keywords: Union[str, Iterable[str]], timeframe: str = "today 12-m", geo: str = "") -> pd.DataFrame:
    """Google Trends interest over time (geo='IN' for India)."""
    kw = [keywords] if isinstance(keywords, str) else list(keywords)
    return get("trendspy").Trends().interest_over_time(kw, timeframe=timeframe, geo=geo)


def gdelt(keyword: str, timespan: str = "7d", country: Optional[str] = None, n: int = 250) -> pd.DataFrame:
    """Worldwide news articles from the GDELT project (free, no key). country e.g. 'IN'."""
    g = get("gdeltdoc")
    f = g.Filters(keyword=keyword, timespan=timespan, num_records=n, country=country)
    return g.GdeltDoc().article_search(f)


def google_news(query: str, country: str = "IN", language: str = "en", period: str = "7d", n: int = 50) -> list:
    """Google News search results."""
    return get("gnews").GNews(language=language, country=country, period=period, max_results=n).get_news(query)
