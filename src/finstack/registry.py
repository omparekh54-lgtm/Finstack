"""Catalog of the free finance-data libraries that finstack knows about."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Lib:
    key: str            # short name used with finstack.get("key")
    pip: str            # name on PyPI
    module: str         # import name
    category: str       # see CATEGORIES
    region: str         # global | US | IN
    api_key: bool       # needs a key / account?
    extra: str          # finstack extra that installs it
    about: str
    install: str = ""   # custom install command when it is not a plain pip install


CATEGORIES = [
    "india_exchange", "india_derivatives", "india_fundamentals", "india_mf", "india_broker",
    "india_commodities", "india_macro",
    "equities", "world", "symbols", "multi", "broker", "macro", "crypto", "forex", "filings",
    "news", "alt_data", "ipo", "keyed",
]

CATALOG: List[Lib] = [
    # ================= INDIA: exchanges (NSE / BSE) =================
    Lib("nse", "nse", "nse", "india_exchange", "IN", False, "india",
        "NseIndiaApi: quotes, bhavcopies, historical equity/index/F&O/VIX, corporate actions, "
        "announcements, results, shareholding, IPOs (current/upcoming/past), bulk & block deals, "
        "holidays, option chain + max pain. Actively maintained; built-in 3 req/s throttle."),
    Lib("nsepython", "nsepython", "nsepython", "india_exchange", "IN", False, "india",
        "NSE quotes, option chain, F&O, FII/DII, indices, pre-open, most active, events."),
    Lib("jugaad_data", "jugaad-data", "jugaad_data", "india_exchange", "IN", False, "india",
        "NSE live and historical stock/index/derivative data, bhavcopy, plus RBI rates."),
    Lib("aynse", "aynse", "aynse", "india_exchange", "IN", False, "india",
        "Lean NSE + AMFI + RBI library: historical stocks/indices/derivatives, index P/E, "
        "bhavcopy, bulk deals, IPOs, MF NAVs."),
    Lib("nselib", "nselib", "nselib", "india_exchange", "IN", False, "india",
        "NSE equity, index and derivative data into pandas."),
    Lib("nsefin", "nsefin", "nsefin", "india_exchange", "IN", False, "india",
        "NSE bhavcopy (equity & F&O), pre-market, index details, FII/DII, corporate actions, "
        "candles, option chain with Greeks."),
    Lib("nsetools", "nsetools", "nsetools", "india_exchange", "IN", False, "india",
        "Classic NSE live quotes, index quotes, top gainers/losers, stock codes (v2 rewrite)."),
    Lib("indian_stock_market", "indian-stock-market", "indian_stock_market", "india_exchange", "IN", False, "india",
        "NSE OHLC incl. intraday, live streaming candles, ETFs, SGBs, SME stocks, India VIX."),
    Lib("bse", "bse", "bse", "india_exchange", "IN", False, "india",
        "BseIndiaApi: results snapshot (revenue, profit, EPS), results calendar, announcements, "
        "corporate actions, company lookup by name/ISIN, full securities list with industry, quotes, "
        "bhavcopy, index history. Covers BSE-only companies. Actively maintained."),
    Lib("bharatfintrack", "BharatFinTrack", "BharatFinTrack", "india_exchange", "IN", False, "india",
        "National Pension System (NPS) scheme NAVs. Its Nifty Total Return Index download is broken in "
        "v0.4.8 (the package ships without its base_data file)."),
    Lib("bsedata", "bsedata", "bsedata", "india_exchange", "IN", False, "india",
        "BSE live quotes, gainers/losers, index list."),
    Lib("bseindia", "bseindia", "bseindia", "india_exchange", "IN", False, "india",
        "BSE historical stock data, equity & derivative bhavcopy, holiday calendar."),

    # ================= INDIA: derivatives =================
    Lib("indiaopt", "indiaopt", "indiaopt", "india_derivatives", "IN", False, "india",
        "Async NSE & BSE option chains: spot, ATM window, OI, PCR; retries and circuit breaker."),

    # ================= INDIA: fundamentals =================
    Lib("bharat_sm_data", "Bharat-sm-data", "Fundamentals", "india_fundamentals", "IN", False, "india-fundamentals",
        "Moneycontrol statements & ratios, Tickertape (scorecard, peers, shareholding, MF holdings, "
        "dividends), Screener.in tables (free Screener login), BSE; Derivatives module adds NSE + Sensibull."),
    Lib("openscreener", "openscreener", "openscreener", "india_fundamentals", "IN", False, "screener",
        "Screener.in fundamentals without login, via Playwright (python -m playwright install chromium)."),

    # ================= INDIA: mutual funds =================
    Lib("mftool", "mftool", "mftool", "india_mf", "IN", False, "mf",
        "AMFI scheme list, latest and historical NAV, scheme details and performance."),
    Lib("fundkit", "fundkit", "fundkit", "india_mf", "IN", False, "mf",
        "Async AMFI NAV client: latest/historical NAV, scheme metadata (Python 3.12+, Polars)."),

    # ================= INDIA: broker APIs (account needed) =================
    Lib("kiteconnect", "kiteconnect", "kiteconnect", "india_broker", "IN", True, "brokers",
        "Zerodha Kite Connect official SDK. Historical data is a paid add-on; check current pricing."),
    Lib("smartapi", "smartapi-python", "SmartApi", "india_broker", "IN", True, "brokers",
        "Angel One SmartAPI official SDK: quotes, historical candles, websocket. Free with an account."),
    Lib("upstox", "upstox-python-sdk", "upstox_client", "india_broker", "IN", True, "brokers",
        "Upstox official SDK: quotes, historical candles, option chain, websocket."),
    Lib("dhanhq", "dhanhq", "dhanhq", "india_broker", "IN", True, "brokers",
        "Dhan official SDK: market quotes, historical & intraday data, option chain."),
    Lib("fyers", "fyers-apiv3", "fyers_apiv3", "india_broker", "IN", True, "fyers",
        "Fyers API v3 official SDK: quotes, history, market depth, websocket. Separate extra: it pins the "
        "old PyPI 'asyncio' package, which breaks Python's own asyncio - install it in its own venv."),
    Lib("breeze", "breeze-connect", "breeze_connect", "india_broker", "IN", True, "brokers",
        "ICICI Direct Breeze official SDK: historical data incl. options, quotes, websocket."),

    # ================= GLOBAL: equities =================
    Lib("yfinance", "yfinance", "yfinance", "equities", "global", False, "global",
        "Prices, fundamentals, options, news from Yahoo Finance (NSE as .NS, BSE as .BO)."),
    Lib("yahooquery", "yahooquery", "yahooquery", "equities", "global", False, "global",
        "Alternative Yahoo client with batch endpoints."),
    Lib("yahoofinancials", "yahoofinancials", "yahoofinancials", "equities", "global", False, "global",
        "Yahoo fundamentals and price history as JSON."),
    Lib("stockdex", "stockdex", "stockdex", "equities", "global", False, "stockdex",
        "Multi-source stock data: Yahoo API + web, Macrotrends, Digrin, Finviz, JustETF. Separate extra: pins an old curl_cffi (downgrades yfinance)."),
    Lib("defeatbeta", "defeatbeta-api", "defeatbeta_api", "equities", "US", False, "defeatbeta",
        "Yahoo-style data served from an open Hugging Face dataset: prices, statements, "
        "earnings call transcripts. No rate limits. Separate extra: needs pandas>=3."),
    Lib("finance_datareader", "finance-datareader", "FinanceDataReader", "equities", "global", False, "global",
        "Prices and stock listings for many markets (KRX, US, JP, CN...), indices, FX, crypto."),
    Lib("tvdatafeed", "tradingview-datafeed", "tvDatafeed", "equities", "global", False, "tradingview",
        "TradingView OHLCV for any exchange incl. NSE/BSE, intraday intervals, futures. No login needed."),
    Lib("tvkit", "tvkit", "tvkit", "equities", "global", False, "tradingview",
        "Async TradingView client: OHLCV, 69-market scanner, 100+ fundamentals (Python 3.11+)."),
    Lib("pandas_datareader", "pandas-datareader", "pandas_datareader", "multi", "global", False, "global",
        "Readers for Stooq, FRED, World Bank, OECD, Eurostat, Fama-French."),
    Lib("tessa", "tessa", "tessa", "multi", "global", False, "global",
        "Unified price history over Yahoo and CoinGecko with caching and rate limiting."),

    # ================= Symbol databases =================
    Lib("financedatabase", "financedatabase", "financedatabase", "symbols", "global", False, "global",
        "300k+ symbols (equities, ETFs, funds, indices, currencies, crypto) with sector/country filters."),

    # ================= Macro =================
    Lib("fredapi", "fredapi", "fredapi", "macro", "US", True, "macro",
        "FRED/ALFRED economic series. Free API key from fred.stlouisfed.org."),
    Lib("wbgapi", "wbgapi", "wbgapi", "macro", "global", False, "macro",
        "World Bank indicators for every country (India GDP, inflation, etc.)."),

    # ================= Crypto / FX =================
    Lib("pycoingecko", "pycoingecko", "pycoingecko", "crypto", "global", False, "crypto",
        "CoinGecko prices, market caps, history. Free public tier."),
    Lib("ccxt", "ccxt", "ccxt", "crypto", "global", False, "crypto",
        "Unified public market data for 100+ crypto exchanges (incl. Indian ones)."),
    Lib("forex_python", "forex-python", "forex_python", "forex", "global", False, "forex",
        "ECB-based currency rates and conversion."),

    # ================= Filings / news / IPO =================
    Lib("edgar", "edgartools", "edgar", "filings", "US", False, "filings",
        "SEC EDGAR 10-K/10-Q/8-K, XBRL financials, insider trades, 13F. Needs set_identity()."),
    Lib("sec_edgar_downloader", "sec-edgar-downloader", "sec_edgar_downloader", "filings", "US", False, "filings",
        "Bulk download of SEC filings to disk."),
    Lib("feedparser", "feedparser", "feedparser", "news", "global", False, "news",
        "RSS reader behind finstack.news(): Business Standard, Economic Times, Moneycontrol, Livemint, "
        "MarketWatch, CNBC, WSJ, or any feed URL."),
    Lib("finvizfinance", "finvizfinance", "finvizfinance", "news", "US", False, "news",
        "Finviz screener, quotes, news, insider data."),
    Lib("moneycontrol_news", "moneycontrol-api", "moneycontrol", "news", "IN", False, "news",
        "Moneycontrol news headlines as JSON (Python 3.11+)."),
    Lib("pyipo", "pyipo", "pyipo", "ipo", "US", False, "ipo",
        "Nasdaq IPO calendar (US). For Indian IPOs use finstack.ipos()."),

    # ================= Free-tier keyed APIs =================
    Lib("alpha_vantage", "alpha_vantage", "alpha_vantage", "keyed", "global", True, "keyed",
        "Alpha Vantage: stocks (incl. BSE), FX, crypto, indicators. Small free daily quota."),
    Lib("finnhub", "finnhub-python", "finnhub", "keyed", "global", True, "keyed",
        "Finnhub: quotes, news, earnings, fundamentals. Free tier ~60 calls/min."),
    Lib("twelvedata", "twelvedata", "twelvedata", "keyed", "global", True, "keyed",
        "Twelve Data: broad exchange coverage incl. NSE, free tier with daily limit."),
    Lib("tiingo", "tiingo", "tiingo", "keyed", "global", True, "keyed",
        "Tiingo: clean EOD prices and fundamentals for personal use."),
    Lib("polygon", "polygon-api-client", "polygon", "keyed", "US", True, "polygon",
        "Polygon.io (Massive): US market data, limited free tier. Separate extra (pins certifi against ccxt)."),
    Lib("nasdaq_data_link", "nasdaq-data-link", "nasdaqdatalink", "keyed", "global", True, "keyed",
        "Nasdaq Data Link (ex-Quandl): free macro and alternative datasets."),
    Lib("financetoolkit", "financetoolkit", "financetoolkit", "keyed", "global", True, "keyed",
        "150+ ratios, models and statements on top of Financial Modeling Prep (free FMP key)."),
    Lib("stocksymbol", "stocksymbol", "stocksymbol", "keyed", "global", True, "keyed",
        "Symbol lists for all major exchanges incl. NSE/BSE (free API key)."),

    # ================= INDIA: commodities & government data =================
    Lib("mcxlib", "mcxlib", "mcxlib", "india_commodities", "IN", False, "commodities",
        "MCX India: market watch, bhavcopy, date-wise history, option chains, most active, gainers/losers, "
        "put-call ratio, iCOMDEX indices (gold, silver, crude, natural gas, base metals)."),
    Lib("datagovindia", "datagovindia", "datagovindia", "india_macro", "IN", True, "keyed",
        "Search and download 100k+ Government of India datasets from data.gov.in (free API key)."),

    Lib("india_xbrl_filings", "india-xbrl-filings", "india_xbrl", "india_fundamentals", "IN", False, "xbrl-bulk",
        "Bulk download of every NSE/BSE financial-results XBRL file with checksums (personal/educational "
        "use only per its terms). Read files with finstack.xbrl_results()."),

    # ================= OTHER COUNTRIES =================
    Lib("akshare", "akshare", "akshare", "world", "CN", False, "world",
        "Huge Chinese library: China A-shares, HK, US stocks, futures, options, bonds, funds, FX, macro "
        "(Python 3.11+)."),
    Lib("efinance", "efinance", "efinance", "world", "CN", False, "world",
        "Eastmoney data: China/HK/US stocks, funds, futures, intraday and daily."),
    Lib("baostock", "baostock", "baostock", "world", "CN", False, "world",
        "Free China A-share history, financials, index constituents (call bs.login() first)."),
    Lib("tushare", "tushare", "tushare", "world", "CN", True, "keyed",
        "China markets data platform (free token; more data with points)."),
    Lib("pykrx", "pykrx", "pykrx", "world", "KR", True, "world",
        "Korea Exchange (KOSPI/KOSDAQ) prices, fundamentals, investor flows. Needs a free KRX account "
        "set via KRX_ID / KRX_PW environment variables."),
    Lib("twstock", "twstock", "twstock", "world", "TW", False, "world",
        "Taiwan Stock Exchange prices and real-time quotes."),
    Lib("jquants", "jquants-api-client", "jquantsapi", "world", "JP", True, "keyed",
        "Japan Exchange Group J-Quants: prices, financials, indices (free plan with delayed data)."),
    Lib("apimoex", "apimoex", "apimoex", "world", "RU", False, "world",
        "Moscow Exchange ISS: securities list, board history, candles."),
    Lib("moexalgo", "moexalgo", "moexalgo", "world", "RU", False, "world",
        "Moscow Exchange AlgoPack: candles, order book stats, trades (Python 3.12+)."),
    Lib("vnstock", "vnstock", "vnstock", "world", "VN", False, "manual",
        "Vietnam stocks, indices, warrants, derivatives, funds, financials. No longer on PyPI.",
        install="pip install -U --extra-index-url https://vnstocks.com/api/simple vnstock vnai"),
    Lib("mstarpy", "mstarpy", "mstarpy", "equities", "global", False, "morningstar",
        "Morningstar funds and stocks: NAV, returns, holdings, financials, screener. Needs Chrome + Selenium."),

    # ================= GLOBAL BROKERS (account needed) =================
    Lib("alpaca", "alpaca-py", "alpaca", "broker", "US", True, "brokers-global",
        "Alpaca official SDK: free US stock and crypto data (IEX feed) with a free account, paper trading."),
    Lib("ib_async", "ib_async", "ib_async", "broker", "global", True, "brokers-global",
        "Interactive Brokers API: data for 150+ markets incl. India (needs IBKR account + TWS/Gateway)."),

    # ================= MACRO aggregators =================
    Lib("dbnomics", "dbnomics", "dbnomics", "macro", "global", False, "macro",
        "DBnomics: one API for 80+ providers (IMF, OECD, ECB, BIS, Eurostat, World Bank, ILO, national "
        "statistics offices). Python 3.11+."),
    Lib("sdmx", "sdmx1", "sdmx", "macro", "global", False, "macro",
        "SDMX client for official statistics: IMF, ECB, BIS, OECD, Eurostat, World Bank, ILO, UN."),
    Lib("imf_reader", "imf-reader", "imf_reader", "macro", "global", False, "macro",
        "IMF World Economic Outlook and other IMF datasets (Python 3.12+)."),
    Lib("ecbdata", "ecbdata", "ecbdata", "macro", "EU", False, "macro",
        "ECB Data Portal: euro rates, FX (incl. EUR/INR), money supply, bank lending."),
    Lib("eurostat", "eurostat", "eurostat", "macro", "EU", False, "macro",
        "Eurostat datasets: EU inflation, GDP, employment, trade."),
    Lib("beaapi", "beaapi", "beaapi", "macro", "US", True, "keyed",
        "US Bureau of Economic Analysis: GDP, personal income, trade (free key)."),
    Lib("eia", "EIAOpenData", "EIAOpenData", "macro", "US", True, "keyed",
        "US Energy Information Administration: oil, gas, electricity prices and stocks (free key)."),
    Lib("tradingeconomics", "tradingeconomics", "tradingeconomics", "macro", "global", True, "keyed",
        "Trading Economics: indicators, forecasts, economic calendar for 196 countries (limited guest access)."),

    # ================= FX / crypto extras =================
    Lib("currency_converter", "currencyconverter", "currency_converter", "forex", "global", False, "forex",
        "Offline ECB rates back to 1999 bundled in the package (incl. INR); no network needed."),
    Lib("binance", "python-binance", "binance", "crypto", "global", False, "crypto",
        "Binance public market data: klines, order book, trades (keys only for trading)."),
    Lib("cryptocmd", "cryptocmd", "cryptocmd", "crypto", "global", False, "crypto",
        "CoinMarketCap historical OHLCV for any coin."),
    Lib("defillama2", "defillama2", "defillama2", "crypto", "global", False, "crypto",
        "DefiLlama: DeFi TVL, protocol fees/revenue, stablecoins, yields, DEX volumes."),
    Lib("dune_client", "dune-client", "dune_client", "crypto", "global", True, "keyed",
        "Dune Analytics: run on-chain SQL queries (free key, Python 3.11+)."),

    # ================= Filings extras =================
    Lib("secfsdstools", "secfsdstools", "secfsdstools", "filings", "US", False, "secfsdstools",
        "SEC Financial Statement Data Sets: every US-listed company's statements in bulk. Separate extra: needs numpy<2."),
    Lib("sec_api", "sec-api", "sec_api", "filings", "US", True, "keyed",
        "sec-api.io: full-text search, XBRL-to-JSON, insider and 13F APIs (free trial key)."),

    # ================= News / alternative data =================
    Lib("gnews", "gnews", "gnews", "news", "global", False, "news",
        "Google News search by keyword, topic, country (India, US, ...)."),
    Lib("GoogleNews", "GoogleNews", "GoogleNews", "news", "global", False, "news",
        "Alternative Google News scraper with date ranges."),
    Lib("gdeltdoc", "gdeltdoc", "gdeltdoc", "news", "global", False, "news",
        "GDELT: worldwide news articles and tone timelines, filter by country/language. Free, no key."),
    Lib("newsapi", "newsapi-python", "newsapi", "news", "global", True, "keyed",
        "NewsAPI.org headlines from 80k+ sources (free developer key)."),
    Lib("praw", "praw", "praw", "alt_data", "global", True, "keyed",
        "Reddit posts and comments (r/IndianStockMarket, r/wallstreetbets...). Free Reddit app credentials."),
    Lib("trendspy", "trendspy", "trendspy", "alt_data", "global", False, "alt-data",
        "Google Trends interest over time, by region, related queries, trending now. "
        "Replaces pytrends (archived April 2025)."),





    # ================= More free-key APIs =================
    Lib("eodhd", "eodhd", "eodhd", "keyed", "global", True, "keyed",
        "EODHD: EOD prices for 70+ exchanges incl. NSE/BSE, fundamentals (small free daily quota)."),
    Lib("fmpsdk", "fmpsdk", "fmpsdk", "keyed", "global", True, "keyed",
        "Financial Modeling Prep official SDK: statements, ratios, prices (free tier)."),
    Lib("simfin", "simfin", "simfin", "keyed", "US", True, "keyed",
        "SimFin bulk fundamentals and share prices (free key, US focus)."),
    Lib("openfigipy", "openfigipy", "openfigipy", "symbols", "global", False, "keyed",
        "OpenFIGI: map tickers/ISINs/CUSIPs across exchanges (works without a key at lower limits)."),

    # ================= All-in-one =================
    Lib("openbb", "openbb", "openbb", "multi", "global", False, "openbb",
        "OpenBB platform: one interface to many providers. Large install."),
]

_BY_KEY = {l.key: l for l in CATALOG}


def lookup(key: str) -> Lib:
    try:
        return _BY_KEY[key]
    except KeyError:
        raise KeyError(f"Unknown library '{key}'. Try finstack.catalog() to see all keys.") from None


def as_dicts(category: Optional[str] = None, region: Optional[str] = None):
    out = []
    for l in CATALOG:
        if category and not l.category.startswith(category):
            continue
        if region and l.region != region:
            continue
        out.append(asdict(l))
    return out
