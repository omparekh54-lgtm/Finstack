"""Live check of every finstack data source.

Run on your own machine (needs normal internet access):

    python -m finstack.check                      # everything (takes a few minutes)
    python -m finstack.check --groups company     # only some groups
    python -m finstack.check --list               # list all tests

or from Python:  fs.check(groups=["company", "india"])

Writes finstack_report.html / .json / .csv with, for every source: whether it returned data,
how many rows, which fields, a sample, the time taken and the error if it failed.
"""
from __future__ import annotations

import argparse
import asyncio
import concurrent.futures as cf
import datetime as dt
import html
import json
import os
import tempfile
import time
from typing import Callable, List, NamedTuple, Optional

import pandas as pd

from .loader import get, is_installed

TODAY = dt.date.today()


def _days_ago(n: int) -> dt.date:
    return TODAY - dt.timedelta(days=n)


def _last_weekday(offset: int = 1) -> dt.date:
    d = TODAY - dt.timedelta(days=offset)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def _dmy(d: dt.date) -> str:
    return d.strftime("%d-%m-%Y")


class Test(NamedTuple):
    id: str
    group: str
    library: str              # finstack catalog key ("" = built in)
    what: str                 # what we expect back
    fn: Callable
    env: Optional[str] = None  # environment variable required (key / login)


def _tests() -> List[Test]:
    import finstack as fs

    T: List[Test] = []
    add = lambda *a, **k: T.append(Test(*a, **k))  # noqa: E731

    # ------------------------------------------------------------ company results & reports (India)
    add("quarterly_results.auto", "company", "", "Quarterly P&L for TCS (first source that works)",
        lambda: fs.quarterly_results("TCS"))
    for src, lib in [("nse", "nse"), ("bse", "bse"), ("moneycontrol", "bharat_sm_data"),
                     ("tickertape", "bharat_sm_data"), ("yahoo", "yfinance")]:
        add(f"quarterly_results.{src}", "company", lib, f"Quarterly P&L for TCS from {src} only",
            lambda s=src: fs.quarterly_results("TCS", sources=(s,)))
    add("quarterly_results.screener", "company", "bharat_sm_data", "Quarterly results from Screener.in",
        lambda: fs.quarterly_results("TCS", sources=("screener",)), env="SCREENER_USER")
    add("quarterly_results.bse_only_company", "company", "bse", "Quarterly results by BSE scrip code (532540 = TCS)",
        lambda: fs.quarterly_results("532540", sources=("bse",)))
    add("annual_results", "company", "", "Yearly P&L for INFY", lambda: fs.annual_results("INFY"))
    add("balance_sheet", "company", "", "Balance sheet for RELIANCE", lambda: fs.balance_sheet("RELIANCE"))
    add("cash_flow", "company", "", "Cash flow for RELIANCE", lambda: fs.cash_flow("RELIANCE"))
    add("annual_reports", "company", "nse", "Annual-report PDF links for RELIANCE", lambda: fs.annual_reports("RELIANCE"))
    add("result_filings", "company", "nse", "NSE results filings for TCS (last 120 days)",
        lambda: fs.result_filings("TCS", start=_days_ago(120).isoformat()))
    add("result_filings.all", "company", "nse", "All companies' results filings, last 7 days",
        lambda: fs.result_filings(start=_days_ago(7).isoformat()))
    add("latest_results.xbrl", "company", "nse", "Official figures from INFY's latest XBRL filing",
        lambda: fs.latest_results("INFY"))
    add("announcements", "company", "nse", "NSE announcements for INFY, 30 days", lambda: fs.announcements("INFY"))
    add("board_meetings", "company", "nse", "Forthcoming board meetings (all companies)", lambda: fs.board_meetings())
    add("upcoming_results", "company", "nsefin", "Companies announcing results soon", lambda: fs.upcoming_results())
    add("earnings_dates", "company", "yfinance", "EPS estimate vs actual for TCS", lambda: fs.earnings_dates("TCS"))
    add("company_lookup", "company", "bse", "Find 'Tata Consultancy' -> symbol, BSE code, ISIN",
        lambda: fs.company_lookup("Tata Consultancy"))
    add("bse_results", "company", "bse", "BSE results snapshot for Reliance (500325)", lambda: fs.bse_results("500325"))
    add("bse_result_calendar", "company", "bse", "Results due on BSE in 14 days", lambda: fs.bse_result_calendar(14))
    add("bse_announcements", "company", "bse", "BSE announcements for RELIANCE", lambda: fs.bse_announcements("RELIANCE"))
    add("bse_actions", "company", "bse", "Forthcoming corporate actions on BSE", lambda: fs.bse_actions(days=30))
    add("list_companies", "company", "bse", "BSE company list, industry = Banks", lambda: fs.list_companies("Banks", group=""))
    add("moneycontrol.ratios", "company", "bharat_sm_data", "Moneycontrol ratios for TCS", lambda: fs.moneycontrol("TCS", "ratios"))
    add("tickertape.scorecard", "company", "bharat_sm_data", "Tickertape scorecard for TCS", lambda: fs.tickertape("TCS", "scorecard"))
    add("shareholding", "company", "nse", "Shareholding pattern for TCS", lambda: fs.shareholding("TCS"))
    add("corporate_actions", "company", "nse", "Dividends/splits for INFY", lambda: fs.corporate_actions("INFY"))
    add("fundamentals.yahoo", "company", "yfinance", "Yahoo info + statements for INFY.NS", lambda: fs.fundamentals("INFY.NS"))
    add("nselib.financial_results", "company", "nselib", "nselib quarterly financial results, last 30 days",
        lambda: get("nselib", "capital_market").financial_results_for_equity(_dmy(_days_ago(30)), _dmy(TODAY)))
    add("quarterly_results.us", "company", "", "Quarterly P&L for AAPL (US)", lambda: fs.quarterly_results("AAPL", market="US"))
    add("annual_reports.us", "company", "edgar", "AAPL 10-K filings from SEC", lambda: fs.annual_reports("AAPL", market="US"),
        env="EDGAR_IDENTITY")

    # ------------------------------------------------------------ India market data
    add("nse_history", "india", "", "RELIANCE daily prices, 60 days", lambda: fs.nse_history("RELIANCE", _days_ago(60).isoformat()))
    add("nse_quote", "india", "", "Live quote SBIN", lambda: fs.nse_quote("SBIN"))
    add("bse_quote", "india", "bsedata", "Live BSE quote 500325", lambda: fs.bse_quote("500325"))
    add("bhavcopy.equity", "india", "nsefin", "Whole-market EOD file", lambda: fs.bhavcopy(_last_weekday().isoformat()))
    add("bhavcopy.fno", "india", "nsefin", "F&O EOD file", lambda: fs.bhavcopy(_last_weekday().isoformat(), segment="fno"))
    add("tv_history.nifty_15m", "india", "tvdatafeed", "NIFTY 15-minute candles (TradingView)", lambda: fs.tv_history("NIFTY", "NSE", "15m", 100))
    add("option_chain", "india", "", "NIFTY option chain", lambda: fs.option_chain("NIFTY"))
    add("fii_dii", "india", "", "FII/DII cash activity", lambda: fs.fii_dii())
    add("ipos.current", "india", "nse", "Current IPOs", lambda: fs.ipos("current"))
    add("ipos.upcoming", "india", "nse", "Upcoming IPOs", lambda: fs.ipos("upcoming"))
    add("index_constituents", "india", "nse", "NIFTY BANK constituents", lambda: fs.index_constituents("NIFTY BANK"))
    add("mcx.market_watch", "india", "mcxlib", "MCX commodity market watch", lambda: fs.mcx("market_watch"))
    add("mf_search", "india", "", "Find 'parag parikh flexi' schemes", lambda: fs.mf_search("parag parikh flexi"))
    add("mf_nav.latest", "india", "", "Latest NAV scheme 122639", lambda: fs.mf_nav("122639"))
    add("mf_nav.history", "india", "", "NAV history scheme 122639", lambda: fs.mf_nav("122639", history_nav=True))
    # direct library calls
    add("lib.nsepython", "india", "nsepython", "nse_eq('SBIN') full quote", lambda: get("nsepython").nse_eq("SBIN"))
    add("lib.jugaad_data", "india", "jugaad_data", "stock_df SBIN 30 days",
        lambda: __import__("jugaad_data.nse", fromlist=["stock_df"]).stock_df("SBIN", _days_ago(30), TODAY, "EQ"))
    add("lib.nselib", "india", "nselib", "price/volume/delivery SBIN",
        lambda: get("nselib", "capital_market").price_volume_and_deliverable_position_data("SBIN", _dmy(_days_ago(30)), _dmy(TODAY)))
    add("lib.nsefin.pre_market", "india", "nsefin", "Pre-market snapshot", lambda: get("nsefin").NSEClient().get_pre_market_info(category="All"))
    add("lib.nsefin.index", "india", "nsefin", "NIFTY 50 index details", lambda: get("nsefin").NSEClient().get_index_details(category="NIFTY 50"))
    add("lib.aynse", "india", "aynse", "stock_df RELIANCE 30 days",
        lambda: get("aynse").stock_df(symbol="RELIANCE", from_date=_days_ago(30).isoformat(), to_date=TODAY.isoformat()))
    add("lib.nsetools", "india", "nsetools", "get_quote('SBIN')", lambda: get("nsetools").Nse().get_quote("SBIN"))
    add("lib.indian_stock_market", "india", "indian_stock_market", "NIFTY 50 daily OHLC",
        lambda: get("indian_stock_market").NSE().get_ohlc_data("NIFTY 50", "1Day", True))
    add("lib.indiaopt", "india", "indiaopt", "Async NIFTY option chain + PCR",
        lambda: asyncio.run(_indiaopt()))
    add("lib.bseindia", "india", "bseindia", "BSE historical prices SBIN 1M",
        lambda: __import__("bseindia.equity", fromlist=["x"]).historical_stock_data("SBIN", period="1M"))
    add("lib.bharatfintrack.nps", "india", "bharatfintrack", "NPS scheme NAVs", lambda: get("bharatfintrack").NPS().schemes_latest_nav())
    add("lib.bharatfintrack.tri", "india", "bharatfintrack", "NIFTY 50 total return index, 30 days",
        lambda: get("bharatfintrack").NSETRI().download_daily_data("NIFTY 50", _dmy(_days_ago(30)), _dmy(TODAY)))
    add("lib.fundkit", "india", "fundkit", "Async AMFI NAV 122639", lambda: asyncio.run(_fundkit()))

    # ------------------------------------------------------------ global markets
    add("history.us", "global", "", "AAPL daily prices", lambda: fs.history("AAPL", _days_ago(60).isoformat()))
    add("history.nse", "global", "", "TCS.NS daily prices", lambda: fs.history("TCS.NS", _days_ago(60).isoformat()))
    add("history.stooq", "global", "pandas_datareader", "AAPL from Stooq only",
        lambda: fs.history("AAPL", _days_ago(60).isoformat(), sources=("stooq",)))
    add("quote", "global", "yfinance", "Last price AAPL", lambda: fs.quote("AAPL"))
    add("symbols.india", "global", "financedatabase", "Indian equities in FinanceDatabase", lambda: fs.symbols("India"))
    add("lib.yahooquery", "global", "yahooquery", "Ticker('AAPL').price", lambda: get("yahooquery").Ticker("AAPL").price)
    add("lib.yahoofinancials", "global", "yahoofinancials", "Quarterly income statement INFY.NS",
        lambda: get("yahoofinancials").YahooFinancials("INFY.NS").get_financial_stmts("quarterly", "income"))
    add("lib.finance_datareader", "global", "finance_datareader", "AAPL prices", lambda: get("finance_datareader").DataReader("AAPL", _days_ago(60).isoformat()))
    add("lib.tessa", "global", "tessa", "price_history AAPL", lambda: get("tessa").price_history("AAPL").df)
    add("lib.tvkit", "global", "tvkit", "NSE:RELIANCE 10 daily bars", lambda: asyncio.run(_tvkit()))
    add("world.cn", "global", "akshare", "China A-share 600519 (Moutai)", lambda: fs.world_history("CN", "600519", _days_ago(60).isoformat()))
    add("world.tw", "global", "twstock", "Taiwan 2330 (TSMC)", lambda: fs.world_history("TW", "2330", _days_ago(40).isoformat()))
    add("world.ru", "global", "apimoex", "Moscow SBER", lambda: fs.world_history("RU", "SBER", _days_ago(30).isoformat()))
    add("world.kr", "global", "pykrx", "Korea 005930 (Samsung)", lambda: fs.world_history("KR", "005930", _days_ago(30).isoformat()), env="KRX_ID")
    add("lib.efinance", "global", "efinance", "Eastmoney 600519 history", lambda: get("efinance").stock.get_quote_history("600519"))
    add("lib.baostock", "global", "baostock", "Baostock sh.600519 30 days", lambda: _baostock())

    # ------------------------------------------------------------ macro, fx, crypto
    add("macro.fred_no_key", "macro", "pandas_datareader", "US 10Y yield DGS10 (FRED via pandas-datareader)", lambda: fs.macro("DGS10", start=_days_ago(90).isoformat()))
    add("macro.fred_key", "macro", "fredapi", "US 10Y yield via fredapi", lambda: fs.macro("DGS10", start=_days_ago(90).isoformat()), env="FRED_API_KEY")
    add("macro.worldbank", "macro", "wbgapi", "India GDP (World Bank)", lambda: fs.macro("NY.GDP.MKTP.CD", source="worldbank"))
    add("imf_weo", "macro", "imf_reader", "IMF World Economic Outlook", lambda: fs.imf_weo())
    add("dbnomics", "macro", "dbnomics", "India real GDP growth (IMF WEO via DBnomics)",
        lambda: fs.dbnomics("IMF/WEO:2025-10/IND.NGDP_RPCH.pcent_change"))
    add("ecb", "macro", "ecbdata", "EUR/INR daily (ECB)", lambda: fs.ecb("EXR.D.INR.EUR.SP00.A", start=_days_ago(30).isoformat()))
    add("eurostat", "macro", "eurostat", "Eurostat tps00001 (EU population)", lambda: fs.eurostat("tps00001"))
    add("sdmx.ecb", "macro", "sdmx", "ECB EUR/INR monthly via SDMX", lambda: _sdmx())
    add("us_yield_curve", "macro", "", "US Treasury yield curve this year", lambda: fs.us_yield_curve())
    add("fx", "macro", "", "USD->INR (Frankfurter/ECB)", lambda: fs.fx("USD", "INR"))
    add("lib.forex_python", "macro", "forex_python", "USD->INR rate", lambda: get("forex_python", "converter").CurrencyRates().get_rate("USD", "INR"))
    add("lib.currency_converter", "macro", "currency_converter", "USD->INR offline ECB file",
        lambda: get("currency_converter").CurrencyConverter(fallback_on_missing_rate=True).convert(1, "USD", "INR"))
    add("crypto_price", "macro", "", "BTC in INR (CoinGecko)", lambda: fs.crypto_price("bitcoin", "inr"))
    add("lib.ccxt", "macro", "ccxt", "BTC/USDT ticker on Binance", lambda: get("ccxt").binance().fetch_ticker("BTC/USDT"))
    add("lib.python_binance", "macro", "binance", "BTCUSDT daily klines",
        lambda: get("binance").Client().get_klines(symbol="BTCUSDT", interval="1d", limit=5))
    add("lib.cryptocmd", "macro", "cryptocmd", "BTC history (CoinMarketCap)", lambda: get("cryptocmd").CmcScraper("BTC").get_dataframe())
    add("lib.defillama2", "macro", "defillama2", "TVL by chain (DefiLlama)", lambda: get("defillama2").DefiLlama().get_chains_curr_tvl())

    # ------------------------------------------------------------ filings, news, alt data
    add("filings.sec", "news", "edgar", "AAPL 10-K list", lambda: fs.filings("AAPL", "10-K", identity=os.environ["EDGAR_IDENTITY"]), env="EDGAR_IDENTITY")
    add("lib.sec_edgar_downloader", "news", "sec_edgar_downloader", "Download 1 AAPL 10-Q",
        lambda: _secdl(), env="EDGAR_IDENTITY")
    add("news.india_rss", "news", "feedparser", "Indian market headlines (RSS)", lambda: fs.news())
    add("news.global_rss", "news", "feedparser", "MarketWatch/CNBC/WSJ headlines", lambda: fs.news(("marketwatch", "cnbc", "wsj")))
    add("google_news", "news", "gnews", "Google News 'TCS results' India", lambda: fs.google_news("TCS results"))
    add("lib.GoogleNews", "news", "GoogleNews", "GoogleNews search 'Nifty'", lambda: _googlenews())
    add("gdelt", "news", "gdeltdoc", "GDELT articles 'Reserve Bank of India'", lambda: fs.gdelt("Reserve Bank of India", country="IN"))
    add("lib.moneycontrol_news", "news", "moneycontrol_news", "Moneycontrol latest news", lambda: get("moneycontrol_news", "moneycontrol_api").get_latest_news())
    add("lib.finvizfinance", "news", "finvizfinance", "Finviz news AAPL", lambda: get("finvizfinance", "quote").finvizfinance("AAPL").ticker_news())
    add("trends", "news", "trendspy", "Google Trends 'Nifty' in India", lambda: fs.trends("Nifty", geo="IN"))
    add("lib.pyipo", "news", "pyipo", "Nasdaq IPO calendar this month", lambda: get("pyipo").NasdaqClient().get_calendar(TODAY.strftime("%Y-%m")))

    # ------------------------------------------------------------ free-key APIs (only if key set)
    add("keyed.alpha_vantage", "keyed", "alpha_vantage", "Alpha Vantage quarterly income IBM",
        lambda: get("alpha_vantage", "fundamentaldata").FundamentalData(os.environ["ALPHAVANTAGE_API_KEY"], output_format="pandas").get_income_statement_quarterly("IBM")[0],
        env="ALPHAVANTAGE_API_KEY")
    add("keyed.finnhub", "keyed", "finnhub", "Finnhub quote AAPL",
        lambda: get("finnhub").Client(api_key=os.environ["FINNHUB_API_KEY"]).quote("AAPL"), env="FINNHUB_API_KEY")
    add("keyed.datagovindia", "keyed", "datagovindia", "data.gov.in search 'gdp'",
        lambda: get("datagovindia").DataGovIndia().search("gdp"), env="DATAGOVINDIA_API_KEY")
    return T


# -------------------------------------------------------------------- async / multi-step helpers
async def _indiaopt():
    from indiaopt import NSEClient

    async with NSEClient() as c:
        r = await c.fetch_option_chain("NIFTY")
        return {"spot_price": r.spot_price, "pcr": r.pcr, "atm_window": [vars(x) for x in r.atm_window(n=3)]}


async def _fundkit():
    from fundkit import NAVClient

    async with NAVClient() as c:
        return await c.get_nav(122639, df_format="pandas")


async def _tvkit():
    from tvkit.api.chart.ohlcv import OHLCV

    async with OHLCV() as c:
        bars = await c.get_historical_ohlcv("NSE:RELIANCE", interval="1D", bars_count=10)
        return [b.model_dump() if hasattr(b, "model_dump") else vars(b) for b in bars]


def _baostock():
    bs = get("baostock")
    bs.login()
    try:
        rs = bs.query_history_k_data_plus("sh.600519", "date,open,high,low,close,volume",
                                          start_date=_days_ago(30).isoformat(), end_date=TODAY.isoformat())
        return rs.get_data()
    finally:
        bs.logout()


def _sdmx():
    sdmx = get("sdmx")
    msg = sdmx.Client("ECB").data("EXR", key={"CURRENCY": "INR", "FREQ": "M"}, params={"startPeriod": str(TODAY.year - 1)})
    return sdmx.to_pandas(msg)


def _secdl():
    name, _, email = os.environ["EDGAR_IDENTITY"].rpartition(" ")
    folder = tempfile.mkdtemp()
    n = get("sec_edgar_downloader").Downloader(name or "finstack", email, folder).get("10-Q", "AAPL", limit=1)
    return {"downloaded": n, "folder": folder}


def _googlenews():
    from GoogleNews import GoogleNews

    g = GoogleNews(lang="en", region="IN", period="7d")
    g.search("Nifty")
    return g.results()


# -------------------------------------------------------------------- describing results
def _to_pandas(obj):
    if hasattr(obj, "to_pandas") and not isinstance(obj, pd.DataFrame):
        try:
            return obj.to_pandas()
        except Exception:
            pass
    return obj


def _short(v, n=120) -> str:
    s = str(v)
    return s if len(s) <= n else s[: n - 1] + "…"


def describe(obj) -> dict:
    """Shape, fields and a small sample of whatever a source returned."""
    obj = _to_pandas(obj)
    if obj is None:
        return {"kind": "None", "rows": 0, "fields": [], "sample": ""}
    if isinstance(obj, pd.Series):
        obj = obj.to_frame()
    if isinstance(obj, pd.DataFrame):
        d = obj.copy()
        if not isinstance(d.index, pd.RangeIndex):
            d = d.reset_index()
        d.columns = [str(c) for c in d.columns]
        sample = d.head(3).astype(str).map(lambda x: _short(x, 60)).to_dict("records")
        return {"kind": "DataFrame", "rows": int(len(obj)), "fields": list(d.columns),
                "sample": json.dumps(sample, ensure_ascii=False)[:1500]}
    if isinstance(obj, dict):
        fields = list(obj.keys())
        sizes = {k: len(v) for k, v in obj.items() if isinstance(v, (list, dict, pd.DataFrame))}
        rows = max(sizes.values()) if sizes else 1
        sample = {str(k): (_short(v) if not isinstance(v, (list, dict)) else f"<{type(v).__name__} of {len(v)}>")
                  for k, v in list(obj.items())[:25]}
        return {"kind": "dict", "rows": int(rows), "fields": [str(f) for f in fields],
                "sample": json.dumps(sample, ensure_ascii=False, default=str)[:1500]}
    if isinstance(obj, (list, tuple)):
        first = obj[0] if obj else None
        fields = list(first.keys()) if isinstance(first, dict) else []
        sample = [{str(k): _short(v, 60) for k, v in list(x.items())[:20]} if isinstance(x, dict) else _short(x)
                  for x in list(obj)[:3]]
        return {"kind": "list", "rows": len(obj), "fields": [str(f) for f in fields],
                "sample": json.dumps(sample, ensure_ascii=False, default=str)[:1500]}
    if isinstance(obj, (int, float, str)):
        return {"kind": type(obj).__name__, "rows": 1, "fields": [], "sample": _short(obj, 300)}
    try:
        n = len(obj)
    except Exception:
        n = 1
    return {"kind": type(obj).__name__, "rows": n, "fields": [], "sample": _short(repr(obj), 600)}


# -------------------------------------------------------------------- runner
def check(groups: Optional[List[str]] = None, only: Optional[List[str]] = None, timeout: int = 90,
          pause: float = 1.0, out: Optional[str] = "finstack_report", verbose: bool = True) -> pd.DataFrame:
    """Run the live checks and return a results table (also written to <out>.html/.json/.csv)."""
    import contextlib
    import io

    tests = [t for t in _tests() if (not groups or t.group in groups) and (not only or t.id in only)]
    results = []
    pool = cf.ThreadPoolExecutor(max_workers=1)
    for i, t in enumerate(tests, 1):
        row = {"id": t.id, "group": t.group, "library": t.library or "finstack", "what": t.what,
               "status": "", "seconds": 0.0, "kind": "", "rows": 0, "fields": "", "sample": "", "error": "", "source": ""}
        if t.library and not is_installed(t.library):
            row.update(status="SKIP", error=f"library '{t.library}' not installed")
        elif t.env and not os.environ.get(t.env):
            row.update(status="SKIP", error=f"set {t.env} to run (free key / login / SEC identity)")
        else:
            start = time.time()
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    fut = pool.submit(t.fn)
                    obj = fut.result(timeout=timeout)
                info = describe(obj)
                src = getattr(obj, "attrs", {}).get("source", "") if isinstance(obj, pd.DataFrame) else (
                    obj.get("source", "") if isinstance(obj, dict) else "")
                row.update(status="OK" if info["rows"] else "EMPTY", kind=info["kind"], rows=info["rows"],
                           fields=", ".join(info["fields"][:40]), sample=info["sample"], source=str(src))
            except cf.TimeoutError:
                row.update(status="FAIL", error=f"timed out after {timeout}s")
                pool.shutdown(wait=False, cancel_futures=True)
                pool = cf.ThreadPoolExecutor(max_workers=1)
            except Exception as e:  # noqa: BLE001
                row.update(status="FAIL", error=f"{type(e).__name__}: {_short(e, 400)}")
            row["seconds"] = round(time.time() - start, 1)
            time.sleep(pause)  # be gentle with NSE/BSE
        results.append(row)
        if verbose:
            print(f"[{i:>3}/{len(tests)}] {row['status']:<5} {t.id:<34} {row['rows']:>6} rows  "
                  f"{_short(row['error'] or row['fields'], 70)}", flush=True)
    pool.shutdown(wait=False)
    df = pd.DataFrame(results)
    if out:
        df.to_csv(f"{out}.csv", index=False)
        df.to_json(f"{out}.json", orient="records", indent=1, force_ascii=False)
        with open(f"{out}.html", "w", encoding="utf-8") as fh:
            fh.write(_html(df))
        if verbose:
            print(f"\nReport written: {out}.html  ({out}.json, {out}.csv)")
    if verbose:
        print(df.groupby("status").size().to_string())
    return df


def _html(df: pd.DataFrame) -> str:
    import platform

    import finstack

    counts = df["status"].value_counts().to_dict()
    rows = []
    for r in df.to_dict("records"):
        sample = html.escape(r["sample"] or "")
        detail = (f"<details><summary>fields &amp; sample</summary><p><b>Fields:</b> {html.escape(r['fields'])}</p>"
                  f"<pre>{sample}</pre></details>") if r["status"] in ("OK", "EMPTY") else ""
        rows.append(
            f"<tr class='{r['status']}'><td><span class='pill {r['status']}'>{r['status']}</span></td>"
            f"<td><code>{html.escape(r['id'])}</code><div class='what'>{html.escape(r['what'])}</div>{detail}</td>"
            f"<td>{html.escape(r['library'])}{'<div class=what>via ' + html.escape(r['source']) + '</div>' if r['source'] else ''}</td>"
            f"<td class='num'>{r['rows']}</td><td class='num'>{r['seconds']}</td>"
            f"<td class='err'>{html.escape(r['error'])}</td></tr>")
    summary = " · ".join(f"<b>{counts.get(k, 0)}</b> {k}" for k in ("OK", "EMPTY", "FAIL", "SKIP"))
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>finstack source check</title>
<style>
:root{{--bg:#fafaf9;--fg:#1c1917;--muted:#78716c;--line:#e7e5e4;--ok:#15803d;--okb:#dcfce7;--fail:#b91c1c;--failb:#fee2e2;--skip:#57534e;--skipb:#f5f5f4;--empty:#a16207;--emptyb:#fef9c3;--card:#fff}}
@media (prefers-color-scheme:dark){{:root{{--bg:#1c1917;--fg:#f5f5f4;--muted:#a8a29e;--line:#44403c;--ok:#86efac;--okb:#14532d;--fail:#fca5a5;--failb:#7f1d1d;--skip:#d6d3d1;--skipb:#292524;--empty:#fde047;--emptyb:#713f12;--card:#292524}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}}
main{{max-width:1200px;margin:0 auto;padding:24px 16px}}h1{{font-size:22px;margin:0 0 4px}}
.meta{{color:var(--muted);margin-bottom:16px}}.wrap{{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:8px 10px;border-bottom:1px solid var(--line);vertical-align:top;text-align:left}}
th{{font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}}.num{{text-align:right;font-variant-numeric:tabular-nums}}
.what{{color:var(--muted);font-size:12px}}.err{{color:var(--fail);font-size:12px;max-width:340px;word-break:break-word}}
.pill{{font-size:11px;font-weight:600;padding:2px 8px;border-radius:99px}}.OK.pill{{color:var(--ok);background:var(--okb)}}
.FAIL.pill{{color:var(--fail);background:var(--failb)}}.SKIP.pill{{color:var(--skip);background:var(--skipb)}}.EMPTY.pill{{color:var(--empty);background:var(--emptyb)}}
pre{{white-space:pre-wrap;word-break:break-word;font-size:12px;max-height:240px;overflow:auto}}code{{font-size:13px}}
</style></head><body><main><h1>finstack source check</h1>
<div class="meta">finstack {finstack.__version__} · Python {platform.python_version()} · {dt.datetime.now():%Y-%m-%d %H:%M} · {summary}</div>
<div class="wrap"><table><thead><tr><th>Status</th><th>Test</th><th>Library</th><th class=num>Rows</th><th class=num>Sec</th><th>Error</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div></main></body></html>"""


def main(argv=None):
    p = argparse.ArgumentParser(description="Live-check every finstack data source.")
    p.add_argument("--groups", help="comma list: company,india,global,macro,news,keyed")
    p.add_argument("--only", help="comma list of test ids")
    p.add_argument("--out", default="finstack_report", help="report file prefix")
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument("--list", action="store_true", help="list tests and exit")
    a = p.parse_args(argv)
    if a.list:
        for t in _tests():
            print(f"{t.group:<8} {t.id:<34} {t.what}" + (f"  [needs {t.env}]" if t.env else ""))
        return
    check(groups=a.groups.split(",") if a.groups else None, only=a.only.split(",") if a.only else None,
          timeout=a.timeout, out=a.out)


if __name__ == "__main__":
    main()
