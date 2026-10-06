"""Which sources may be used, given each website's terms.

finstack sorts its sources into two groups:

  permitted   official APIs, open-data publishers and your own broker/API accounts: their terms allow
              programmatic access (within their rate limits).
  restricted  public websites whose terms of use forbid automated data collection (NSE, BSE, TradingView,
              Moneycontrol, Tickertape, Screener, Yahoo, Google pages ...). finstack reads them slowly and
              politely, but that does not make it permitted. Personal research only; your IP can be blocked.

    fs.configure(policy="permitted")    # use only permitted sources
    fs.configure(policy="all")          # default: all sources, restricted ones after a one-time notice
    fs.terms()                          # the table
"""
from __future__ import annotations

import sys
import threading

from . import config

# source key -> (website, what its terms allow)
PERMITTED = {
    # your own accounts / keys with official APIs
    "upstox": ("Upstox API", "official broker API for your own account"),
    "kiteconnect": ("Zerodha Kite Connect", "official broker API for your own account"),
    "dhanhq": ("Dhan API", "official broker API for your own account"),
    "smartapi": ("Angel One SmartAPI", "official broker API for your own account"),
    "fyers": ("Fyers API", "official broker API for your own account"),
    "alpaca": ("Alpaca", "official API for your own account"),
    "tiingo": ("Tiingo", "official API with your key"),
    "polygon": ("Polygon.io", "official API with your key"),
    "twelvedata": ("Twelve Data", "official API with your key"),
    "alpha_vantage": ("Alpha Vantage", "official API with your key"),
    "eodhd": ("EODHD", "official API with your key"),
    "finnhub": ("Finnhub", "official API with your key"),
    "fredapi": ("FRED API", "official API with your key"),
    "tushare": ("Tushare", "official API with your token"),
    "newsapi": ("NewsAPI", "official API with your key"),
    "praw": ("Reddit API", "official API with your app credentials"),
    # official / open public APIs and files
    "builtin:sec": ("SEC EDGAR", "automated access allowed up to 10 requests/s with a declared User-Agent"),
    "edgar": ("SEC EDGAR", "automated access allowed up to 10 requests/s with a declared User-Agent"),
    "builtin:fred_csv": ("FRED (St. Louis Fed)", "public data downloads"),
    "pandas_datareader": ("FRED / Stooq downloads", "public CSV downloads"),
    "wbgapi": ("World Bank", "open data API (CC BY 4.0)"),
    "builtin:worldbank": ("World Bank", "open data API (CC BY 4.0)"),
    "ecbdata": ("European Central Bank", "open data API"),
    "builtin:ecb": ("European Central Bank", "open data API"),
    "eurostat": ("Eurostat", "open data API"),
    "dbnomics": ("DBnomics", "open data API"),
    "builtin:dbnomics": ("DBnomics", "open data API"),
    "builtin:treasury": ("US Treasury", "public data files"),
    "builtin:frankfurter": ("Frankfurter (ECB rates)", "free open API"),
    "currency_converter": ("ECB reference rates file", "public data file, bundled"),
    "builtin:amfi": ("AMFI", "NAVAll.txt is published for public download"),
    "builtin:mfapi": ("mfapi.in", "free open API over AMFI data"),
    "mftool": ("mfapi.in", "free open API over AMFI data"),
    "pycoingecko": ("CoinGecko", "public API within its rate limit"),
    "ccxt": ("crypto exchange public APIs", "public market-data APIs within their rate limits"),
    "binance": ("Binance", "public market-data API within its rate limit"),
    "gdeltdoc": ("GDELT", "open data API"),
    "feedparser": ("publishers' RSS feeds", "feeds published for syndication (headlines + links)"),
    "apimoex": ("Moscow Exchange ISS", "public exchange API"),
    "baostock": ("Baostock", "free public API"),
    "financedatabase": ("FinanceDatabase", "open dataset (MIT)"),
}

RESTRICTED_SITES = {
    "nse": "NSE", "nsepython": "NSE", "jugaad_data": "NSE", "nselib": "NSE", "nsefin": "NSE", "aynse": "NSE",
    "nsetools": "NSE", "indian_stock_market": "NSE", "indiaopt": "NSE", "builtin:xbrl": "NSE",
    "builtin:nse_fiidii": "NSE", "builtin:nse_preopen": "NSE", "builtin:niftyindices": "NSE Indices",
    "builtin:symbol_master": "NSE / BSE", "bse": "BSE", "bsedata": "BSE", "bseindia": "BSE",
    "bharat_sm_data": "Moneycontrol / Tickertape / Screener", "screener": "Screener.in",
    "tvdatafeed": "TradingView", "tvkit": "TradingView", "yfinance": "Yahoo Finance",
    "yahooquery": "Yahoo Finance", "mcxlib": "MCX", "gnews": "Google News", "GoogleNews": "Google News",
    "trendspy": "Google Trends", "finvizfinance": "Finviz", "cryptocmd": "CoinMarketCap",
    "akshare": "Eastmoney / Sina", "efinance": "Eastmoney", "pykrx": "KRX", "twstock": "TWSE",
    "finance_datareader": "various websites", "openbb": "OpenBB providers (Yahoo by default)",
}


def mode() -> str:
    return str(config.get("policy") or config.get("FINSTACK_POLICY") or "all").lower()


def is_permitted(key: str) -> bool:
    return key in PERMITTED


def skip_reason(key: str):
    if mode() == "permitted" and not is_permitted(key):
        site = RESTRICTED_SITES.get(key, "this website")
        return f"blocked by policy='permitted' ({site}'s terms do not allow automated collection)"
    return None


_NOTICE = """
[finstack] Notice (shown once): some of the data you just requested comes from public websites
({sites}) whose terms of use do not allow automated data collection. finstack reads them slowly and
politely (shared rate limits, back-off, caching), but that does not make it permitted.
  - Use it for personal research and learning only; do not redistribute or sell the data.
  - A site may block your IP address (usually temporarily) if it sees automated traffic.
  - To use only sources whose terms allow programmatic access:  fs.configure(policy="permitted")
  - See every source and its terms:  fs.terms()      Hide this notice:  FINSTACK_ACCEPT_TERMS=1
"""
_shown = False
_lock = threading.Lock()


def notice_once(key: str) -> None:
    """Print the restricted-source notice once per computer (remembered in ~/.finstack)."""
    global _shown
    if _shown or key in PERMITTED or key not in RESTRICTED_SITES:
        return
    if config.get("FINSTACK_ACCEPT_TERMS") or config.get("accept_terms"):
        _shown = True
        return
    with _lock:
        if _shown:
            return
        _shown = True
        flag = config.home() / "terms_notice_shown"
        if flag.exists():
            return
        sites = ", ".join(sorted(set(RESTRICTED_SITES.values()) - {"various websites"}))
        print(_NOTICE.format(sites=sites), file=sys.stderr)
        try:
            flag.write_text("shown\n")
        except OSError:
            pass


def terms():
    """Every source finstack can use, the website behind it and what its terms allow."""
    import pandas as pd

    from ..registry import CATALOG

    rows = []
    keys = set(PERMITTED) | set(RESTRICTED_SITES)
    names = {l.key: l.pip for l in CATALOG}
    for k in sorted(keys, key=lambda x: (x not in PERMITTED, x)):
        if k in PERMITTED:
            site, note = PERMITTED[k]
            rows.append({"source": names.get(k, k), "key": k, "website": site, "status": "permitted",
                         "terms": note})
        else:
            rows.append({"source": names.get(k, k), "key": k, "website": RESTRICTED_SITES[k],
                         "status": "restricted",
                         "terms": "website terms do not allow automated collection: personal research only"})
    return pd.DataFrame(rows)
