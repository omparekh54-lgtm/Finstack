"""Which library to use for which kind of data, ranked best to worst.

    fs.data_types()                 # every type of data finstack can fetch
    fs.sources("india_options")     # ranked libraries for one type, with scores and notes

Scores are 1-5 per criterion, judged *within* each data type:
  quality      - how trustworthy the numbers are (official exchange/regulator API > broker > data vendor > scraper)
  speed        - how fresh the data is compared with what that data type allows (streaming > polling > delayed > EOD)
  availability - how reliably you can get it (stable API > maintained scraper > fragile/blocked; keys/accounts cost a point)
  coverage     - breadth of symbols, history and fields
  maintenance  - from the library's latest PyPI release (measured 2026-10-05)
overall = 30% quality + 25% availability + 20% coverage + 15% speed + 10% maintenance.

These are informed judgements from each library's data source, documentation and release history, not
benchmarks. Run `python -m finstack.check` to measure what actually works on your network.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

LAST_RELEASE = {
    "GoogleNews": "2026-03-20", "akshare": "2026-09-30", "alpaca": "2026-08-11", "alpha_vantage": "2024-07-18",
    "apimoex": "2026-06-05", "aynse": "2026-09-24", "baostock": "2026-09-21", "beaapi": "2026-02-17",
    "bharat_sm_data": "2025-07-22", "bharatfintrack": "2026-10-03", "binance": "2026-06-08", "breeze": "2026-04-14",
    "bse": "2026-09-24", "bsedata": "2024-03-14", "bseindia": "2026-04-23", "ccxt": "2026-10-01",
    "cryptocmd": "2026-04-26", "currency_converter": "2026-09-15", "datagovindia": "2024-09-20", "dbnomics": "2025-06-18",
    "defeatbeta": "2026-09-15", "defillama2": "2023-07-23", "dhanhq": "2026-07-07", "dune_client": "2026-10-05",
    "ecbdata": "2025-03-23", "edgar": "2026-10-02", "efinance": "2026-07-17", "eia": "2023-09-11",
    "eodhd": "2024-12-18", "eurostat": "2024-06-13", "feedparser": "2026-07-30", "finance_datareader": "2026-05-13",
    "financedatabase": "2026-06-02", "financetoolkit": "2026-10-01", "finnhub": "2026-06-24", "finvizfinance": "2026-08-29",
    "fmpsdk": "2026-08-24", "forex_python": "2025-05-22", "fredapi": "2024-05-05", "fundkit": "2026-06-04",
    "fyers": "2026-09-17", "gdeltdoc": "2025-04-03", "gnews": "2026-06-25", "ib_async": "2025-12-08",
    "imf_reader": "2026-08-14", "india_xbrl_filings": "2026-10-01", "indian_stock_market": "2026-02-11",
    "indiaopt": "2026-07-15", "jquants": "2026-09-30", "jugaad_data": "2026-09-23", "kiteconnect": "2026-09-15",
    "mcxlib": "2026-05-01", "mftool": "2026-09-21", "moexalgo": "2026-09-02", "moneycontrol_news": "2023-08-12",
    "mstarpy": "2026-06-24", "nasdaq_data_link": "2022-08-29", "newsapi": "2023-03-02", "nse": "2026-10-03",
    "nsefin": "2025-09-08", "nselib": "2026-05-01", "nsepython": "2025-05-26", "nsetools": "2025-03-18",
    "openbb": "2026-09-29", "openfigipy": "2022-04-13", "openscreener": "2026-04-15", "pandas_datareader": "2026-06-24",
    "polygon": "2025-10-30", "praw": "2026-08-12", "pycoingecko": "2024-11-13", "pyipo": "2026-08-04",
    "pykrx": "2026-09-19", "sdmx": "2026-08-07", "sec_api": "2026-04-13", "sec_edgar_downloader": "2026-02-02",
    "secfsdstools": "2025-09-20", "simfin": "2026-04-30", "smartapi": "2025-02-07", "stockdex": "2026-05-25",
    "stocksymbol": "2022-01-29", "tessa": "2026-04-09", "tiingo": "2025-04-05", "tradingeconomics": "2026-07-08",
    "trendspy": "2024-12-25", "tushare": "2026-03-25", "tvdatafeed": "2024-03-16", "tvkit": "2026-09-24",
    "twelvedata": "2026-04-27", "twstock": "2026-04-23", "upstox": "2026-09-07", "vnstock": "",
    "wbgapi": "2026-02-27", "yahoofinancials": "2023-12-17", "yahooquery": "2025-05-15", "yfinance": "2026-08-26",
}

# Built-in finstack sources (call a public endpoint directly, no extra library)
BUILTIN = {
    "builtin:xbrl": "fs.latest_results / fs.xbrl_results - reads the company's own XBRL results filing",
    "builtin:mfapi": "fs.mf_nav / fs.mf_search fallback - mfapi.in (community API over AMFI data)",
    "builtin:frankfurter": "fs.fx - Frankfurter API (ECB reference rates)",
    "builtin:treasury": "fs.us_yield_curve - treasury.gov daily par yield curve CSV",
}

# (key, quality, speed, availability, coverage, note)
def _E(*a):
    return a

DATA_TYPES: List[dict] = [
    # ------------------------------------------------------------------ INDIA
    dict(key="india_live_quotes", title="Indian stocks: live quotes (NSE/BSE)", region="IN",
         description="Last traded price, bid/ask, day OHLC, volume during market hours.",
         helpers="fs.nse_quote, fs.bse_quote, fs.get('upstox') ...",
         entries=[
             _E("upstox", 5, 5, 4, 5, "Official broker API: real-time websocket ticks, free with an Upstox account."),
             _E("smartapi", 5, 5, 4, 5, "Angel One official API: real-time websocket, free with an account. SDK updated less often."),
             _E("fyers", 5, 5, 4, 5, "Fyers official API: real-time websocket, free with an account."),
             _E("breeze", 5, 5, 4, 5, "ICICI Direct official API: real-time websocket, free with an account."),
             _E("dhanhq", 5, 5, 3, 5, "Dhan official API: real-time feed; data APIs may carry a fee - check pricing."),
             _E("kiteconnect", 5, 5, 3, 5, "Zerodha official API, best documented; market data is a paid subscription."),
             _E("nse", 4, 4, 3, 4, "NSE website JSON (~seconds old), no account; NSE throttles bursts. Best no-login option."),
             _E("nsepython", 4, 4, 3, 4, "Same NSE endpoints, very full quote fields; release cadence has slowed (May 2025)."),
             _E("indian_stock_market", 4, 4, 3, 3, "NSE charting feed with live candle streaming since 9:15; young library."),
             _E("bse", 4, 4, 3, 4, "BSE official site JSON; covers BSE-only companies."),
             _E("tvdatafeed", 3, 3, 3, 5, "TradingView, no login; anonymous NSE data may be delayed. PyPI build is from 2024."),
             _E("yfinance", 3, 3, 3, 5, "Yahoo .NS/.BO quotes; can lag and gets rate-limited."),
             _E("nsetools", 3, 4, 2, 3, "Classic NSE quotes, v2 rewrite (Mar 2025)."),
             _E("bsedata", 3, 4, 2, 3, "Scrapes BSE mobile site; not updated since Mar 2024."),
         ]),
    dict(key="india_daily_prices", title="Indian stocks: historical daily prices", region="IN",
         description="Daily OHLCV, delivery quantity, VWAP for NSE/BSE stocks over years.",
         helpers="fs.nse_history, fs.history('TCS.NS'), fs.tv_history",
         entries=[
             _E("nse", 5, 3, 4, 4, "NSE's own historical endpoint, unadjusted official prices, very actively maintained."),
             _E("jugaad_data", 5, 3, 4, 4, "NSE data incl. delivery %, threaded downloads; maintained (Sep 2026)."),
             _E("aynse", 5, 3, 4, 4, "Lean NSE client, stocks/indices/derivatives history; maintained (Sep 2026)."),
             _E("breeze", 5, 3, 3, 5, "ICICI Breeze: years of daily history free with an account."),
             _E("upstox", 5, 3, 3, 5, "Upstox historical candles API, free with an account."),
             _E("yfinance", 3, 3, 3, 5, "Decades of history and split/dividend-adjusted closes; occasional gaps, rate limits."),
             _E("nselib", 4, 3, 3, 4, "Price, volume and delivery data; some calls go to an NSE staging host."),
             _E("tvdatafeed", 3, 3, 3, 4, "Up to ~5,000 bars per call from TradingView."),
             _E("bse", 4, 3, 3, 3, "BSE prices; the way to get BSE-only companies."),
             _E("bseindia", 3, 3, 3, 3, "BSE historical data scraper (2 releases)."),
             _E("nsefin", 4, 3, 3, 3, "NSE candles and bulk history; last release Sep 2025."),
             _E("indian_stock_market", 3, 3, 3, 3, "NSE charting OHLC; Polars output."),
             _E("yahooquery", 3, 3, 3, 4, "Alternative Yahoo client; same data as yfinance."),
         ]),
    dict(key="india_intraday", title="Indian stocks: intraday (minute) candles", region="IN",
         description="1-minute to hourly bars for stocks, indices and futures.",
         helpers="fs.tv_history('NIFTY','NSE','5m'), broker SDKs",
         entries=[
             _E("upstox", 5, 5, 4, 5, "Official 1-minute history plus live ticks, free with account."),
             _E("fyers", 5, 5, 4, 5, "Official minute history plus websocket."),
             _E("smartapi", 5, 5, 4, 4, "Official minute candles, limited lookback per request."),
             _E("breeze", 5, 5, 4, 5, "Official 1-second and 1-minute history incl. options."),
             _E("dhanhq", 5, 5, 3, 4, "Official intraday history; check data fees."),
             _E("kiteconnect", 5, 5, 2, 5, "Best-in-class minute history but a paid add-on."),
             _E("tvkit", 3, 4, 3, 4, "TradingView async client, 5,000 bars anonymously; actively maintained."),
             _E("tvdatafeed", 3, 4, 3, 4, "TradingView 1m-1M bars, no login; older PyPI build."),
             _E("indian_stock_market", 4, 4, 3, 3, "NSE charting candles for today with live streaming."),
             _E("yfinance", 3, 3, 3, 3, "1-minute bars for the last 7 days only (5-min for 60 days)."),
         ]),
    dict(key="india_eod_files", title="Indian markets: bhavcopy / end-of-day files", region="IN",
         description="Exchange-published EOD files for every security (equity, F&O), delivery and index reports.",
         helpers="fs.bhavcopy",
         entries=[
             _E("nse", 5, 3, 4, 5, "Equity, F&O, delivery, index and price-band reports straight from NSE archives."),
             _E("jugaad_data", 5, 3, 4, 4, "Equity and F&O bhavcopy downloads."),
             _E("nsefin", 5, 3, 3, 4, "Equity and F&O bhavcopy as DataFrames."),
             _E("aynse", 5, 3, 4, 4, "Bhavcopy and archive files."),
             _E("bse", 5, 3, 4, 4, "BSE bhavcopy and delivery reports."),
             _E("bseindia", 4, 3, 3, 3, "BSE equity and derivative bhavcopy."),
             _E("indian_stock_market", 4, 3, 3, 3, "Equity and F&O bhavcopy downloads."),
         ]),
    dict(key="india_options", title="Indian derivatives: option chains and F&O", region="IN",
         description="Option chains (OI, IV, LTP per strike), futures, expiries, PCR, max pain, F&O history.",
         helpers="fs.option_chain, fs.bhavcopy(segment='fno')",
         entries=[
             _E("upstox", 5, 5, 4, 5, "Official option-chain API with greeks and live OI."),
             _E("dhanhq", 5, 5, 3, 5, "Official option chain with greeks; check data fees."),
             _E("breeze", 5, 4, 4, 5, "Official; unique free history of expired option contracts."),
             _E("nse", 5, 4, 3, 5, "NSE option chain (v3 endpoint), max pain, F&O lots, historical F&O data."),
             _E("nsepython", 5, 4, 3, 4, "NSE option chain, OI chain builder, F&O list, expiries."),
             _E("nsefin", 4, 4, 3, 4, "Option chain plus computed greeks, most-active contracts."),
             _E("indiaopt", 4, 4, 3, 4, "Async NSE and BSE chains with retries; brand new (v0.1.1)."),
             _E("bharat_sm_data", 3, 4, 3, 4, "NSE derivatives plus Sensibull data."),
             _E("jugaad_data", 4, 3, 4, 3, "Historical derivatives and F&O bhavcopy."),
             _E("yfinance", 2, 3, 2, 2, "Indian option chains are mostly missing on Yahoo."),
         ]),
    dict(key="india_commodities", title="Indian commodities (MCX)", region="IN",
         description="Gold, silver, crude, natural gas, base metals futures and options on MCX.",
         helpers="fs.mcx",
         entries=[
             _E("upstox", 5, 5, 4, 5, "MCX live and historical via official API (account)."),
             _E("kiteconnect", 5, 5, 2, 5, "MCX via Zerodha; data is paid."),
             _E("mcxlib", 4, 3, 3, 4, "Only free MCX library: market watch, bhavcopy, history, option chain, PCR."),
             _E("tvdatafeed", 3, 3, 3, 4, "MCX continuous futures (e.g. MCX:GOLD1!) from TradingView."),
             _E("yfinance", 2, 3, 3, 2, "Global futures only (GC=F, CL=F), not MCX prices."),
         ]),
    dict(key="india_company_financials", title="Indian companies: quarterly and annual results", region="IN",
         description="Revenue, expenses, profit, EPS, balance sheet, cash flow, ratios.",
         helpers="fs.quarterly_results, fs.annual_results, fs.latest_results, fs.balance_sheet, fs.cash_flow",
         entries=[
             _E("builtin:xbrl", 5, 5, 3, 4, "The company's own XBRL filing: official numbers the day results are filed."),
             _E("nse", 5, 5, 3, 4, "NSE results comparison and results filings with PDF/XBRL links."),
             _E("bse", 5, 5, 4, 4, "BSE results snapshot; also covers BSE-only companies."),
             _E("bharat_sm_data", 4, 4, 3, 5, "Screener.in (free login), Moneycontrol and Tickertape statements and ratios, 10+ years."),
             _E("openscreener", 4, 4, 2, 4, "Screener.in without login, but needs a headless browser."),
             _E("nselib", 4, 4, 3, 3, "NSE financial-results listings for all companies."),
             _E("yfinance", 3, 3, 3, 3, "Statements for .NS tickers; usually 4-5 years with gaps."),
             _E("yahoofinancials", 3, 3, 2, 3, "Yahoo statements as JSON; not updated since Dec 2023."),
         ]),
    dict(key="india_corporate_events", title="Indian companies: filings, announcements and events", region="IN",
         description="Announcements, board meetings, results dates, dividends/splits, shareholding, annual reports, insider trades, bulk/block deals.",
         helpers="fs.announcements, fs.board_meetings, fs.corporate_actions, fs.shareholding, fs.annual_reports, fs.bse_announcements, fs.upcoming_results",
         entries=[
             _E("nse", 5, 5, 4, 5, "Announcements, board meetings, actions, shareholding, annual reports, bulk/block deals, circulars."),
             _E("bse", 5, 5, 4, 5, "BSE announcements, results calendar, corporate actions; BSE-only companies."),
             _E("nsefin", 5, 4, 3, 4, "Corporate announcements, insider trading, upcoming results."),
             _E("nsepython", 5, 4, 3, 4, "Results calendar, events, block deals, FII/DII."),
             _E("indian_stock_market", 4, 4, 3, 3, "Corporate disclosures and block deals."),
             _E("aynse", 4, 3, 4, 3, "Bulk deals and IPO archives."),
             _E("india_xbrl_filings", 5, 3, 3, 4, "Bulk download of every XBRL results file (personal use only)."),
         ]),
    dict(key="india_ipos", title="IPOs", region="IN / US",
         description="Current, upcoming and past IPOs.",
         helpers="fs.ipos",
         entries=[
             _E("nse", 5, 4, 4, 4, "Current, upcoming and past Indian IPOs from NSE."),
             _E("aynse", 4, 3, 4, 3, "Indian IPO archive data."),
             _E("pyipo", 4, 4, 4, 3, "US (Nasdaq) and Shanghai IPO calendars."),
         ]),
    dict(key="india_mutual_funds", title="Mutual funds and pensions (India)", region="IN",
         description="Scheme list, latest and historical NAV, scheme details; NPS NAVs.",
         helpers="fs.mf_nav, fs.mf_search",
         entries=[
             _E("mftool", 5, 4, 4, 5, "AMFI official data: all schemes, latest/historical NAV, performance; maintained."),
             _E("fundkit", 5, 4, 4, 4, "AMFI parsers, async, Polars output; new (needs Python 3.12+)."),
             _E("builtin:mfapi", 4, 4, 5, 5, "mfapi.in JSON over AMFI data; verified live 2026-10-05; finstack's fallback."),
             _E("aynse", 4, 3, 4, 4, "AMFI NAVs inside a broader NSE library."),
             _E("bharatfintrack", 4, 3, 3, 2, "NPS pension scheme NAVs (its index-TRI feature is broken in 0.4.8)."),
             _E("mstarpy", 4, 3, 2, 4, "Morningstar ratings and holdings; needs Chrome + Selenium; India coverage unclear."),
         ]),
    dict(key="india_indices", title="Indices (Nifty, Sensex, sectoral, TRI)", region="IN",
         description="Index levels, history, constituents, P/E-P/B, total return indices.",
         helpers="fs.index_constituents, fs.history('^NSEI'), fs.tv_history('NIFTY','NSE')",
         entries=[
             _E("nse", 5, 4, 4, 5, "Index history, constituents, India VIX history, index names."),
             _E("aynse", 5, 3, 4, 4, "Index history and index valuation (P/E, P/B, dividend yield)."),
             _E("nsefin", 4, 4, 3, 4, "Index details and snapshots."),
             _E("nselib", 4, 3, 3, 4, "Index data and constituent lists."),
             _E("bse", 5, 3, 4, 4, "BSE index history and index names (Sensex family)."),
             _E("yfinance", 3, 3, 3, 4, "^NSEI, ^BSESN, ^NSEBANK with long history."),
             _E("tvdatafeed", 3, 4, 3, 4, "NIFTY, BANKNIFTY intraday bars."),
             _E("bharatfintrack", 3, 2, 1, 3, "Nifty TRI download currently broken (missing data file)."),
         ]),
    dict(key="india_market_breadth", title="Market-wide stats and flows (India)", region="IN",
         description="FII/DII flows, gainers/losers, pre-open, most active, advance-decline, 52-week highs, VIX.",
         helpers="fs.fii_dii",
         entries=[
             _E("nse", 5, 4, 4, 5, "Advance-decline, gainers/losers, volume gainers, VIX history."),
             _E("nsepython", 5, 4, 3, 5, "FII/DII, pre-open movers, most active."),
             _E("nsefin", 5, 4, 3, 4, "FII/DII, pre-market, 52-week high/low, most active."),
             _E("bse", 5, 4, 4, 4, "BSE gainers/losers, 52-week high/low, advance-decline."),
             _E("indian_stock_market", 4, 4, 3, 3, "India VIX, market status, turnover."),
             _E("nsetools", 3, 4, 3, 3, "Top gainers/losers, advances/declines."),
             _E("bsedata", 3, 4, 2, 2, "BSE gainers/losers; not updated since 2024."),
         ]),
    # ------------------------------------------------------------------ GLOBAL
    dict(key="global_live_quotes", title="Global stocks: live quotes", region="Global",
         description="Real-time or near-real-time prices for US and international stocks.",
         helpers="fs.quote, fs.get('alpaca') ...",
         entries=[
             _E("ib_async", 5, 5, 3, 5, "Interactive Brokers: real-time for 150+ markets incl. India; needs account + data subscriptions."),
             _E("alpaca", 5, 5, 4, 3, "Free real-time US (IEX feed) with a free account."),
             _E("finnhub", 4, 5, 3, 3, "Free key: real-time US quotes and websocket trades."),
             _E("twelvedata", 4, 4, 3, 4, "Free key: real-time for some exchanges, low daily limit."),
             _E("yfinance", 3, 3, 3, 5, "Near-real-time for many exchanges, delayed for some; rate-limited."),
             _E("tvkit", 3, 4, 3, 5, "TradingView quotes and scanner across 69 markets."),
             _E("yahooquery", 3, 3, 3, 5, "Yahoo batch quotes."),
             _E("tvdatafeed", 3, 3, 3, 5, "TradingView bars; older PyPI build."),
             _E("finvizfinance", 3, 2, 3, 2, "US only, delayed quotes."),
             _E("polygon", 4, 2, 3, 3, "Free tier is end-of-day only."),
         ]),
    dict(key="global_daily_prices", title="Global stocks: historical prices", region="Global",
         description="Daily and intraday OHLCV for stocks, ETFs and indices worldwide.",
         helpers="fs.history",
         entries=[
             _E("tiingo", 5, 3, 3, 4, "Free key: very clean, adjusted EOD for US/China; IEX intraday."),
             _E("yfinance", 4, 3, 3, 5, "Broadest free coverage (70+ exchanges), adjusted prices; rate limits."),
             _E("eodhd", 5, 3, 2, 5, "70+ exchanges incl. NSE/BSE; free key but only ~20 calls/day."),
             _E("polygon", 5, 3, 3, 3, "US only; free tier limited to EOD/2 years."),
             _E("twelvedata", 4, 3, 3, 4, "Free key, many exchanges incl. NSE, daily call limit."),
             _E("alpha_vantage", 4, 3, 2, 4, "Free key, ~25 calls/day; SDK not updated since 2024."),
             _E("yahooquery", 4, 3, 3, 5, "Same Yahoo data; good fallback."),
             _E("tvkit", 3, 3, 3, 5, "TradingView history, any exchange."),
             _E("pandas_datareader", 4, 3, 3, 3, "Stooq: free long daily history without keys."),
             _E("finance_datareader", 3, 3, 3, 4, "Strong for Korea/Asia, also US."),
             _E("tvdatafeed", 3, 3, 3, 4, "TradingView history; older PyPI build."),
             _E("tessa", 3, 3, 3, 3, "Wrapper over Yahoo + CoinGecko with caching."),
             _E("stockdex", 3, 3, 2, 3, "Multi-source; separate install (downgrades yfinance)."),
             _E("yahoofinancials", 3, 3, 2, 3, "Not updated since Dec 2023."),
         ]),
    dict(key="global_company_financials", title="Global companies: financials and filings", region="US / Global",
         description="Income statement, balance sheet, cash flow, ratios, SEC filings, insider and institutional holdings.",
         helpers="fs.quarterly_results(..., market='US'), fs.filings, fs.annual_reports(..., market='US')",
         entries=[
             _E("edgar", 5, 5, 4, 5, "edgartools: official SEC XBRL financials, every filing since 1994, insiders, 13F; very active."),
             _E("secfsdstools", 5, 3, 3, 5, "SEC bulk financial-statement datasets (separate install, numpy<2)."),
             _E("sec_edgar_downloader", 5, 4, 4, 4, "Downloads raw SEC filings to disk."),
             _E("financetoolkit", 4, 4, 3, 5, "FMP-based statements and 150+ ratios, global; free key."),
             _E("fmpsdk", 4, 4, 3, 5, "Financial Modeling Prep official SDK; free tier limited."),
             _E("simfin", 4, 3, 3, 3, "Bulk US fundamentals, free key."),
             _E("finnhub", 4, 4, 3, 4, "Fundamentals, earnings calendar, insider data; free key."),
             _E("yfinance", 3, 4, 3, 4, "Global statements, earnings dates, holders; ~4-5 years."),
             _E("alpha_vantage", 4, 3, 2, 3, "Statements and earnings; small free quota."),
             _E("yahooquery", 3, 4, 3, 4, "Yahoo statements and many modules in batch."),
             _E("defeatbeta", 3, 3, 4, 3, "US data incl. earnings-call transcripts; separate install."),
             _E("finvizfinance", 3, 3, 3, 3, "US fundamentals snapshot, insider trades, ratings."),
             _E("mstarpy", 3, 3, 2, 4, "Morningstar financials; needs Chrome."),
             _E("yahoofinancials", 3, 3, 2, 3, "Not updated since Dec 2023."),
             _E("sec_api", 5, 5, 1, 5, "sec-api.io: full-text search, insider and 13F APIs; paid after free trial."),
         ]),
    dict(key="world_markets", title="Other countries' exchanges", region="CN, KR, TW, JP, RU, VN",
         description="Prices and fundamentals straight from China, Korea, Taiwan, Japan, Russia and Vietnam sources.",
         helpers="fs.world_history",
         entries=[
             _E("akshare", 4, 4, 4, 5, "China A/HK/US stocks, futures, bonds, funds, macro; updated weekly."),
             _E("jquants", 5, 3, 3, 4, "Japan Exchange Group official API; free plan has delayed data."),
             _E("baostock", 4, 3, 4, 4, "China A-shares history and financials, free and stable."),
             _E("efinance", 4, 4, 3, 4, "Eastmoney: China/HK/US, funds, futures."),
             _E("tushare", 4, 3, 3, 5, "China markets platform; free token, more data with points."),
             _E("pykrx", 4, 3, 2, 4, "Korea Exchange; now needs a free KRX login."),
             _E("moexalgo", 5, 4, 3, 3, "Moscow Exchange AlgoPack (official)."),
             _E("apimoex", 5, 3, 4, 3, "Moscow Exchange ISS (official)."),
             _E("twstock", 4, 4, 3, 3, "Taiwan Stock Exchange."),
             _E("finance_datareader", 3, 3, 3, 4, "Korea, US, Japan, China listings and prices."),
             _E("vnstock", 4, 3, 2, 4, "Vietnam markets; off PyPI, installs from its own server."),
         ]),
    # ------------------------------------------------------------------ MACRO / RATES / FX / CRYPTO
    dict(key="macro", title="Macroeconomic data", region="Global / IN",
         description="GDP, inflation, unemployment, money supply, trade, government finance, forecasts.",
         helpers="fs.macro, fs.imf_weo, fs.dbnomics, fs.sdmx_client, fs.ecb, fs.eurostat",
         entries=[
             _E("sdmx", 5, 4, 4, 5, "Direct official SDMX APIs: IMF, ECB, BIS, OECD, Eurostat, World Bank, ILO, UN."),
             _E("wbgapi", 5, 2, 5, 5, "World Bank official API, every country incl. India."),
             _E("imf_reader", 5, 3, 4, 4, "IMF World Economic Outlook incl. forecasts (Python 3.12+)."),
             _E("fredapi", 5, 4, 4, 4, "FRED/ALFRED incl. revision history; free key; SDK quiet since 2024."),
             _E("dbnomics", 4, 3, 4, 5, "One API over 80+ providers; mirrors can lag the source slightly."),
             _E("pandas_datareader", 4, 4, 4, 4, "FRED without a key, OECD, Eurostat, Fama-French factors."),
             _E("ecbdata", 5, 4, 4, 3, "ECB Data Portal (euro area)."),
             _E("eurostat", 5, 3, 4, 3, "Eurostat datasets (EU)."),
             _E("datagovindia", 5, 2, 3, 4, "Government of India open data (free key); quiet since 2024."),
             _E("beaapi", 5, 3, 3, 3, "US Bureau of Economic Analysis (free key)."),
             _E("tradingeconomics", 4, 4, 2, 5, "196 countries + calendar, but guest access is very limited."),
             _E("eia", 5, 3, 2, 2, "US energy data (free key); library not updated since 2023."),
             _E("nasdaq_data_link", 3, 2, 2, 3, "Ex-Quandl; most free datasets retired, SDK from 2022."),
         ]),
    dict(key="rates", title="Interest rates and bond yields", region="Global / IN",
         description="Policy rates, government bond yields, yield curves, T-bills.",
         helpers="fs.us_yield_curve, fs.macro('DGS10'), fs.ecb",
         entries=[
             _E("builtin:treasury", 5, 4, 5, 3, "US Treasury par yield curve 1M-30Y; verified live 2026-10-05."),
             _E("fredapi", 5, 4, 4, 5, "US and many international rate series (free key)."),
             _E("sdmx", 5, 3, 4, 4, "BIS policy rates for 40+ central banks; ECB rates."),
             _E("ecbdata", 5, 4, 4, 3, "Euro area rates and yield curves."),
             _E("jugaad_data", 4, 3, 3, 3, "RBI policy, deposit and T-bill rates."),
             _E("aynse", 4, 3, 3, 3, "RBI economic indicators."),
             _E("pandas_datareader", 4, 4, 4, 4, "FRED rates without a key."),
             _E("dbnomics", 4, 3, 4, 4, "Rates from many central banks via one API."),
         ]),
    dict(key="forex", title="Currency exchange rates", region="Global",
         description="Spot and historical FX rates incl. INR pairs.",
         helpers="fs.fx",
         entries=[
             _E("ecbdata", 5, 3, 4, 3, "Official ECB reference rates (daily, ~30 currencies)."),
             _E("builtin:frankfurter", 5, 3, 5, 3, "ECB rates via Frankfurter; verified live 2026-10-05 (USD/INR 96.32)."),
             _E("currency_converter", 5, 2, 5, 3, "ECB history back to 1999 bundled offline; no network needed."),
             _E("yfinance", 3, 4, 3, 5, "Intraday FX (INR=X) incl. many crosses."),
             _E("tvdatafeed", 3, 4, 3, 5, "Intraday FX bars."),
             _E("twelvedata", 4, 4, 3, 5, "Real-time FX with free key."),
             _E("alpha_vantage", 4, 4, 2, 4, "FX intraday/daily, small free quota."),
             _E("forex_python", 3, 3, 3, 3, "Uses third-party theratesapi.com."),
         ]),
    dict(key="crypto", title="Crypto and DeFi", region="Global",
         description="Prices, order books, trades, market caps, DeFi TVL, on-chain data.",
         helpers="fs.crypto_price",
         entries=[
             _E("ccxt", 5, 5, 5, 5, "100+ exchanges' live tickers, order books, trades, candles; released constantly."),
             _E("binance", 5, 5, 4, 3, "Binance REST and websockets."),
             _E("pycoingecko", 4, 4, 4, 5, "Market caps and prices for 10k+ coins; SDK quiet since 2024 but API stable."),
             _E("dune_client", 5, 3, 3, 5, "On-chain SQL over all major chains; free key."),
             _E("cryptocmd", 3, 2, 3, 4, "CoinMarketCap historical OHLCV."),
             _E("defillama2", 4, 3, 3, 4, "DeFi TVL, fees, yields, stablecoins; library from 2023."),
             _E("yfinance", 3, 3, 3, 3, "BTC-USD etc."),
             _E("tessa", 3, 3, 3, 3, "CoinGecko wrapper with caching."),
         ]),
    # ------------------------------------------------------------------ NEWS / ALT / REFERENCE
    dict(key="news", title="Financial news", region="IN / Global",
         description="Headlines and articles for markets, companies and the economy.",
         helpers="fs.news, fs.google_news, fs.gdelt",
         entries=[
             _E("feedparser", 5, 4, 4, 3, "Direct RSS from Business Standard, ET, Moneycontrol, Livemint, MarketWatch."),
             _E("gnews", 4, 4, 4, 5, "Google News search by keyword and country."),
             _E("gdeltdoc", 4, 4, 4, 5, "GDELT: global news in 65 languages with tone; free."),
             _E("finvizfinance", 4, 4, 3, 2, "US ticker news."),
             _E("GoogleNews", 3, 4, 3, 4, "Google News scraper with date ranges."),
             _E("newsapi", 4, 4, 2, 4, "80k sources; free key, delayed on free tier; SDK from 2023."),
             _E("moneycontrol_news", 3, 4, 2, 2, "Moneycontrol scraper; not updated since 2023."),
         ]),
    dict(key="alt_data", title="Alternative data and sentiment", region="Global",
         description="Search interest, social media, news tone.",
         helpers="fs.trends, fs.gdelt",
         entries=[
             _E("trendspy", 4, 4, 3, 4, "Google Trends (replacement for archived pytrends)."),
             _E("praw", 4, 4, 4, 4, "Reddit posts/comments; free app credentials."),
             _E("gdeltdoc", 4, 4, 4, 4, "News tone timelines by country/keyword."),
         ]),
    dict(key="reference", title="Symbols and reference data", region="Global / IN",
         description="Ticker lists, ISIN/FIGI mapping, company master data, sectors.",
         helpers="fs.symbols, fs.company_lookup, fs.list_companies",
         entries=[
             _E("financedatabase", 4, 2, 5, 5, "300k+ symbols with sector/country; works here (6,520 Indian equities)."),
             _E("bse", 5, 3, 4, 4, "BSE securities master with industry and ISIN; company lookup by name/ISIN."),
             _E("nselib", 4, 3, 3, 4, "NSE equity, F&O and index constituent lists."),
             _E("openfigipy", 5, 3, 3, 4, "OpenFIGI identifier mapping; SDK from 2022."),
             _E("stocksymbol", 3, 2, 2, 3, "Symbol lists, free key; not updated since 2022."),
         ]),
    dict(key="all_in_one", title="All-in-one platforms", region="Global",
         description="One interface over many providers.",
         helpers="fs.get('openbb')",
         entries=[
             _E("openbb", 4, 4, 3, 5, "Dozens of providers behind one API; large install, many need keys."),
         ]),
]

_W = dict(quality=0.30, availability=0.25, coverage=0.20, speed=0.15, maintenance=0.10)


def _maintenance(key: str) -> int:
    if key.startswith("builtin:"):
        return 4
    d = LAST_RELEASE.get(key, "")
    if not d:
        return 2
    if d >= "2026-07-01":
        return 5
    if d >= "2026-01-01":
        return 4
    if d >= "2025-01-01":
        return 3
    if d >= "2024-01-01":
        return 2
    return 1


def _rows(dt: dict) -> List[Dict]:
    from .registry import CATALOG

    by_key = {l.key: l for l in CATALOG}
    out = []
    for key, q, s, a, c, note in dt["entries"]:
        m = _maintenance(key)
        lib = by_key.get(key)
        out.append({
            "library": BUILTIN[key].split(" - ")[0] if key.startswith("builtin:") else (lib.pip if lib else key),
            "key": key, "quality": q, "speed": s, "availability": a, "coverage": c, "maintenance": m,
            "overall": round(q * _W["quality"] + a * _W["availability"] + c * _W["coverage"]
                             + s * _W["speed"] + m * _W["maintenance"], 2),
            "needs": ("account" if (lib and lib.category in ("india_broker", "broker")) else
                      "key" if (lib and lib.api_key) else "-"),
            "last_release": LAST_RELEASE.get(key, "built in") if not key.startswith("builtin:") else "built in",
            "note": note,
        })
    out.sort(key=lambda r: (-r["overall"], -r["quality"], -r["availability"]))
    for i, r in enumerate(out, 1):
        r["rank"] = i
    return out


def data_types() -> pd.DataFrame:
    """Every type of data finstack can fetch, with the best source for each."""
    rows = []
    for dt in DATA_TYPES:
        r = _rows(dt)
        free = [x for x in r if x["needs"] == "-"]
        rows.append({"type": dt["key"], "title": dt["title"], "region": dt["region"], "libraries": len(r),
                     "best": r[0]["library"], "best_free_no_signup": free[0]["library"] if free else "",
                     "helpers": dt["helpers"]})
    return pd.DataFrame(rows)


def sources(data_type: Optional[str] = None, free_only: bool = False) -> pd.DataFrame:
    """Ranked libraries (best first) for one data type, or for all types if data_type is None.
    free_only=True hides sources that need an account or key.
    """
    types = [d for d in DATA_TYPES if data_type in (None, d["key"])]
    if not types:
        raise KeyError(f"Unknown data type '{data_type}'. See fs.data_types().")
    frames = []
    for dt in types:
        df = pd.DataFrame(_rows(dt))
        if free_only:
            df = df[df["needs"] == "-"].assign(rank=lambda d: range(1, len(d) + 1))
        df.insert(0, "type", dt["key"])
        frames.append(df)
    cols = ["type", "rank", "library", "overall", "quality", "speed", "availability", "coverage",
            "maintenance", "needs", "last_release", "note", "key"]
    return pd.concat(frames, ignore_index=True)[cols]
