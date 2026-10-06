# finstack

One import for **free financial data**, with a focus on Indian markets. 95 data libraries: stock prices, F&O, commodities, mutual funds, **company quarterly results and annual reports**, 10+ other countries, global macro, crypto, forex, filings and news. finstack only fetches data. Analysis tools (indicators, backtesting, portfolio maths) are deliberately left out.

## Install

```bash
pip install -e ".[all]"        # every data source that needs no account, key or browser
pip install -e ".[company]"    # just company results, statements and reports (India + US)
```

| Extra | Data |
|---|---|
| `india` | NSE / BSE prices, F&O, option chains, IPOs, corporate actions, results filings, results snapshots, Nifty TRI, NPS NAVs (13 libraries) |
| `india-fundamentals` | Moneycontrol, Tickertape, Screener.in statements and ratios |
| `commodities` | MCX India |
| `mf` | AMFI mutual fund NAVs |
| `global`, `tradingview` | Yahoo, TradingView, Stooq, 300k-symbol database |
| `world` | China, Korea, Taiwan, Russia exchanges |
| `macro`, `forex` | FRED, World Bank, IMF, ECB, BIS, OECD, Eurostat, DBnomics, ECB FX |
| `crypto` | CoinGecko, 100+ exchanges, Binance, CoinMarketCap history, DefiLlama |
| `filings`, `news`, `alt-data`, `ipo` | SEC EDGAR, RSS news, Google News, GDELT, Google Trends, US IPOs |
| `brokers` / `brokers-global` | Zerodha, Angel One, Upstox, Dhan, ICICI / Alpaca, IBKR (your account) |
| `keyed` | Free-key APIs: Alpha Vantage, Finnhub, FMP, EODHD, SimFin, data.gov.in, NewsAPI, Reddit ... |
| separate | `fyers` (its SDK pins the old PyPI `asyncio` package, which breaks Python's asyncio: use its own venv), `polygon` (certifi pin clashes with ccxt), `stockdex`, `defeatbeta`, `secfsdstools`, `morningstar`, `screener`, `openbb` (conflicts or extra setup); `xbrl-bulk` (bulk XBRL downloader, personal use only) |

## One call per data type

`fs.fetch(type, symbol, ...)` gets any of the 24 data types. Behind each call is a pipeline that tries
the ranked sources for that type in order and returns the first answer that passes the checks.

```python
import finstack as fs

fs.fetch("india_daily_prices", "RELIANCE", start="2015-01-01")              # NSE symbol, BSE code or ISIN
fs.fetch("india_daily_prices", "RELIANCE", start="10y", adjust=True)        # + split/bonus-adjusted adj_close
fs.fetch("india_live_quotes", "BANKNIFTY")
fs.fetch("india_intraday", "NIFTY", start="5d", interval="5m")
fs.fetch("india_options", "NIFTY", expiry="2026-10-27")
fs.fetch("india_company_financials", "TCS", statement="quarterly")         # INR crore, every source mapped
fs.fetch("india_corporate_events", "INFY", what="actions", start="10y")
fs.fetch("india_eod_files", start="2026-09-01", end="2026-09-30", segment="fno")
fs.fetch("india_mutual_funds", "122639", start="5y"); fs.fetch("india_mutual_funds", what="latest")
fs.fetch("global_daily_prices", "AAPL", start="10y"); fs.fetch("global_company_financials", "AAPL")
fs.fetch("macro", "fred:CPIAUCSL"); fs.fetch("forex", "USD/INR", start="5y"); fs.fetch("crypto", "BTC/USDT")

fs.bulk("india_daily_prices", fs.fetch("india_indices", "NIFTY 500", what="constituents").symbol, start="5y")
```

Every result has the same columns whatever source answered (`date, open, high, low, close, volume ...`),
a `source` column per row, and `df.attrs` with `source`, `attempts` (what each source did) and `issues`.

| Type | What | Example |
|---|---|---|
| `india_daily_prices` | Indian stocks: daily OHLCV (official, unadjusted; `adjust=True` adds adj_close) | `fs.fetch("india_daily_prices", "RELIANCE", start="2015-01-01")` |
| `india_live_quotes` | Indian stocks and indices: live quote | `fs.fetch("india_live_quotes", "SBIN")` |
| `india_intraday` | Intraday candles, 1m to 1h | `fs.fetch("india_intraday", "BANKNIFTY", start="5d", interval="5m")` |
| `india_eod_files` | Bhavcopy for every security on each day (equity, fno, delivery, index; NSE or BSE) | `fs.fetch("india_eod_files", start="2026-09-30", segment="fno")` |
| `india_options` | Option chain, nearest or chosen expiry | `fs.fetch("india_options", "NIFTY")` |
| `india_commodities` | MCX futures history (continuous front month); `what="quotes"` / `"option_chain"` | `fs.fetch("india_commodities", "GOLD", start="2y")` |
| `india_company_financials` | Quarterly / annual results, balance sheet, cash flow (INR crore) | `fs.fetch("india_company_financials", "TCS")` |
| `india_corporate_events` | Actions, announcements, board meetings, results dates, result filings, shareholding, annual reports | `fs.fetch("india_corporate_events", what="results_calendar")` |
| `india_ipos` | IPOs open now, upcoming, past | `fs.fetch("india_ipos", what="upcoming")` |
| `india_mutual_funds` | NAV history per scheme; `what="latest"` (all schemes) / `"search"` | `fs.fetch("india_mutual_funds", "122639")` |
| `india_indices` | Index history (Nifty family, India VIX, Sensex); `what="constituents"` | `fs.fetch("india_indices", "NIFTY BANK", start="10y")` |
| `india_market_breadth` | Advance/decline, gainers, losers, FII/DII, pre-open, 52-week | `fs.fetch("india_market_breadth", what="fii_dii")` |
| `global_live_quotes` | Latest quote | `fs.fetch("global_live_quotes", "MSFT")` |
| `global_daily_prices` | Daily OHLCV for stocks, ETFs, indices worldwide | `fs.fetch("global_daily_prices", "7203.T")` |
| `global_company_financials` | Results and statements; SEC XBRL first for US companies | `fs.fetch("global_company_financials", "AAPL")` |
| `world_markets` | China, Hong Kong, Korea, Taiwan, Japan, Russia from local sources | `fs.fetch("world_markets", "CN:600519")` |
| `macro` | FRED, World Bank, ECB, Eurostat, DBnomics (IMF, OECD, BIS ...) | `fs.fetch("macro", "wb:NY.GDP.MKTP.KD.ZG/IND")` |
| `rates` | US Treasury yield curve; `what="rbi"` | `fs.fetch("rates", start="2020-01-01")` |
| `forex` | Any currency pair, daily | `fs.fetch("forex", "USD/INR", start="5y")` |
| `crypto` | Daily OHLCV (UTC days) | `fs.fetch("crypto", "BTC/USDT", start="3y")` |
| `news` | Headlines from publisher RSS, Google News, GDELT ... | `fs.fetch("news", query="RBI policy")` |
| `alt_data` | Google Trends, Reddit, GDELT timelines | `fs.fetch("alt_data", what="trends", query="Nifty", geo="IN")` |
| `reference` | Symbol lists: every Indian equity (NSE + BSE + ISIN), global, SEC tickers | `fs.fetch("reference", what="india")` |
| `all_in_one` | OpenBB pass-through for anything else | `fs.fetch("all_in_one", "AAPL", route="equity.fundamental.income")` |

`fs.pipelines()` lists every type with its parameters and source order. `fs.route(type, symbol)` shows,
before anything is downloaded, which source will be tried first and why others will be skipped
(not installed, needs a key, paused after failures, website paused).

### New in 0.8 (beta)

Marked `beta` in `fs.pipelines()` until verified on more machines. They are separate data types: nothing
above changed.

```python
fs.fetch("india_deals", what="bulk", start="30d")             # bulk deals (what="block" / "short")
fs.fetch("india_deals", "TCS", what="bulk", start="1y")       # one stock (filters the cached all-stocks answer)
fs.fetch("india_delivery", "TCS", start="3m")                 # delivery qty and delivery % per day
fs.fetch("india_options", "NIFTY", what="analytics")          # max pain, put-call ratio, OI by strike
fs.fetch("india_index_valuation", "NIFTY 50", start="10y")    # index P/E, P/B, dividend yield
fs.fetch("india_ratios", "TCS")                               # P/E, P/B, ROE, ROCE, margins, debt/equity
fs.fetch("analyst_estimates", "INFY", what="price_targets")   # also recommendations, earnings_estimate,
                                                              # revenue_estimate, earnings_history
fs.fetch("analyst_estimates", "AAPL", what="earnings_history", market="US")

fs.fetch("india_price_bands", "ADANIENT")                     # circuit limit (2/5/10/20 %, no band)
fs.fetch("india_fno_reference", what="lots")                  # F&O lot sizes; what="expiries" for an index
fs.fetch("india_fno_history", "NIFTY", expiry="2026-10-27", option_type="CE", strike=25000)  # one contract
fs.fetch("india_market_breadth", what="volume_gainers")       # also what="most_active"
fs.fetch("india_margins", "TCS")                              # VaR / ELM / applicable margin %
fs.fetch("company_profile", "TCS")                            # sector, industry, ISIN, listing date ...
fs.fetch("company_profile", "AAPL", market="US")
fs.fetch("india_peers", "TCS")                                # peer comparison (Tickertape)
fs.fetch("india_scorecard", "TCS")                            # Tickertape scorecard
```

Request cost: deals and delivery are one request/file for every stock (per day or window) and are kept
for good once the day is over; option analytics re-use the option chain already fetched (no extra
request); ratios and analyst data are cached for a day.

### How it stays fast and avoids getting blocked

* **One speed limit per website, shared by every library.** Ten of the bundled libraries call
  nseindia.com. finstack paces requests where they leave your machine (it wraps requests, httpx,
  curl_cffi, urllib and aiohttp when you `import finstack`), so all of them together stay within one
  budget, across threads too. Default budgets per second: NSE 3, NSE archives 1, niftyindices 1, BSE 2,
  MCX 1, AMFI 1, Yahoo 1, TradingView 0.5, Moneycontrol / Tickertape / Screener 0.5, SEC 8 (its limit
  is 10), CoinGecko 0.1, Google Trends 0.15, GDELT 0.2, official statistics sites 2. Other websites (including
  your own APIs) are not slowed down unless they answer 429 or 403.
* **Backs off when a site pushes back.** A 429 or 503 pauses that website for its Retry-After time and
  halves its rate; a 403 pauses it for 30 s, doubling up to 15 minutes. The rate recovers slowly after
  successes. While a website is paused, the router uses a source on a different website instead of waiting.
* **Breakers per source.** Three failures in a row (or one block) take a source out of rotation for
  15, then 30, then 60 minutes; one success puts it back. Sources that failed more than half their
  recent attempts are tried last. See `fs.health()` and `fs.net_stats()`.
* **Nothing is downloaded twice.** Results are cached in `~/.finstack/cache` (Parquet). A request for
  a longer range only downloads the missing days; data becomes "final" after the market's end-of-day
  time (18:30 IST for NSE), and only the still-changing part is refreshed. Quotes are reused for a few
  seconds, option chains for a minute, results tables for 12 hours.
* **Bulk downloads pick the cheapest route.** `fs.bulk("india_daily_prices", 500_symbols)` compares one
  NSE bhavcopy per day (all stocks in one file) with per-symbol history calls and uses whichever needs
  fewer requests; global symbols are fetched 50 at a time from Yahoo; quotes for many stocks come from
  one NSE index snapshot. Re-running after an interruption continues from the cache.
* No proxy rotation, no fake identities: finstack only slows down. NSE/BSE data is for personal use;
  redistributing it commercially needs an exchange licence.

Change a budget with `fs.configure(budgets={"nse": 2})`, or with `FINSTACK_GOVERNOR=0` turn pacing off.

### How it keeps the data correct

* Every source is mapped to the same column names and units (prices in rupees; company figures in
  INR crore; dates on the exchange's calendar, IST for India).
* Rows that fail checks (high below low, close outside the day's range, negative prices, duplicate
  dates, invalid option rows) are set aside in `~/.finstack/quarantine/`; if more than 20% of a source's
  rows fail, the next source is used. Big unexplained jumps, gaps and results that don't add up
  (PBT - tax vs net profit) are listed in `df.attrs["issues"]`.
* `verify=True` downloads the same range from a second, independent source and reports the median
  difference in closes (`df.attrs["verified"]`).
* Prices are the exchange's official unadjusted prices; `adjust=True` adds `adj_close` adjusted for
  splits and bonuses parsed from NSE corporate actions (dividends are not adjusted).

### Settings, keys and broker accounts

Set keys in code (`fs.configure(UPSTOX_ACCESS_TOKEN="...")`), as environment variables, or in
`~/.finstack/config.toml`:

```toml
[keys]
UPSTOX_ACCESS_TOKEN = "..."     # Upstox: official quotes, history, minute candles, option chains
FRED_API_KEY = "..."
EDGAR_IDENTITY = "Your Name you@example.com"   # required by the SEC

[budgets]                       # requests per second for a website
nse = 2
```

Broker APIs are used first when their credentials are set: Upstox (`UPSTOX_ACCESS_TOKEN`), Zerodha
(`KITE_API_KEY`, `KITE_ACCESS_TOKEN`), Dhan (`DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`), Angel One
(`ANGEL_API_KEY`, `ANGEL_CLIENT_CODE`, `ANGEL_PIN`, `ANGEL_TOTP_SECRET`), Fyers (`FYERS_CLIENT_ID`,
`FYERS_ACCESS_TOKEN`). Without them the free website sources are used.

MCX commodity data is the one Indian data type no anonymous free source serves: MCX blocks scripts and
TradingView shows MCX only to logged-in users. A free TradingView account works: set
`TRADINGVIEW_USERNAME` and `TRADINGVIEW_PASSWORD`.

Other helpers: `fs.resolve("500325")` (one company's NSE / BSE / ISIN / Yahoo / TradingView names),
`fs.search_symbols("tata")`, `fs.cache_info()`, `fs.clear_cache("india_daily_prices", "TCS")`.

## Terms and responsible use

finstack only fetches data that is publicly visible, but **some of the websites behind it do not allow
automated data collection in their terms of use**. finstack reads them slowly and politely (one shared
speed limit per website, back-off when a site pushes back, a cache so nothing is downloaded twice, no
proxies or fake identities). That lowers the chance of being blocked; it does not make the access
permitted.

| Sources | Website | What its terms say | Status |
|---|---|---|---|
| nse, jugaad-data, nselib, nsepython, aynse, nsefin, nsetools, indian-stock-market | NSE | Terms of use, clause 9: "any systematic or automated data collection activities (including scraping...)" are prohibited; clause 8: no copying, aggregating or redistributing without written permission | restricted |
| bse, bsedata, bseindia | BSE | Assume the same as NSE | restricted |
| tvDatafeed, tvkit | TradingView | Data is "licensed for exclusive display-only use"; non-display use is prohibited; accounts can be banned | restricted |
| yfinance, yahooquery | Yahoo Finance | Personal use only | restricted |
| Bharat-sm-data, openscreener | Moneycontrol, Tickertape, Screener | Website pages, not an API | restricted |
| mcxlib, gnews, trendspy, finvizfinance, cryptocmd, akshare, efinance | MCX, Google, Finviz, CoinMarketCap, Eastmoney | Website pages, not an API | restricted |
| Broker SDKs, Tiingo, Polygon, Twelve Data, Alpha Vantage, EODHD, Finnhub, FRED API | your own account / key | Official APIs | permitted |
| SEC EDGAR | sec.gov | Automated access allowed, max 10 requests/second, with your name and email (`EDGAR_IDENTITY`) | permitted |
| AMFI, mfapi.in, FRED, World Bank, ECB, Eurostat, DBnomics, US Treasury, Frankfurter | public publishers | Open data / public files and APIs | permitted |
| CoinGecko, Binance, exchanges via ccxt, GDELT, RSS feeds | public APIs | Allowed within their rate limits | permitted |

`fs.terms()` prints this list for every source.

**What this means for you**

* Use data from restricted sources for **personal research and learning only**. Do not redistribute it
  or build a commercial product on it: NSE/BSE data for commercial use needs an exchange licence or a
  licensed vendor.
* A restricted website can block your IP address, usually for minutes to hours, if it sees automated
  traffic. If you log in to TradingView through finstack, use a spare account: TradingView can ban accounts.
* The first time finstack uses a restricted source it prints a one-time notice. Hide it with
  `FINSTACK_ACCEPT_TERMS=1` once you have read it.
* To use **only sources whose terms allow programmatic access**:

  ```python
  fs.configure(policy="permitted")      # back to everything: fs.configure(policy="all")
  ```

  In this mode Indian stock prices, results and NSE/BSE data need a broker account (Upstox, Zerodha,
  Dhan, Angel One, Fyers); mutual funds, macro, forex, crypto, US filings and news still work for free.
  Set it permanently in `~/.finstack/config.toml` with `policy = "permitted"` under `[keys]`.

## Which source to use: data types and rankings

finstack covers 24 types of data. Every library is ranked best to worst within each type it serves
(quality 30%, availability 25%, coverage 20%, speed 15%, maintenance 10%):

```python
fs.data_types()                          # all 24 types with the best source and best free no-sign-up source
fs.sources("india_options")              # ranked libraries for one type, with scores and notes
fs.sources("india_live_quotes", free_only=True)   # hide sources that need an account or key
```

Types: india_live_quotes, india_daily_prices, india_intraday, india_eod_files, india_options,
india_commodities, india_company_financials, india_corporate_events, india_ipos, india_mutual_funds,
india_indices, india_market_breadth, global_live_quotes, global_daily_prices, global_company_financials,
world_markets, macro, rates, forex, crypto, news, alt_data, reference, all_in_one.

## Check which sources work on your machine

```bash
python -m finstack.check                  # all ~108 tests, a few minutes
python -m finstack.check --groups company # company | india | global | macro | news | keyed
python -m finstack.check --list           # see every test
```

or `fs.check()` in Python. It calls every helper, every fallback source on its own and every library
in `[all]`, then writes **finstack_report.html** (plus .json and .csv) showing for each source:
OK / EMPTY / FAIL / SKIP, rows returned, the field names, a sample of the data, time taken and the error.
Tests that need a key or login are skipped unless the variable is set (SCREENER_USER, EDGAR_IDENTITY,
FRED_API_KEY, KRX_ID, ALPHAVANTAGE_API_KEY, FINNHUB_API_KEY, DATAGOVINDIA_API_KEY).

## Company results and reports

Indian companies use NSE symbols (default `market="IN"`); US companies pass `market="US"`.
Every function tries several sources; `df.attrs["source"]` tells you which one answered.

```python
import finstack as fs

fs.quarterly_results("TCS")              # NSE -> BSE -> Moneycontrol -> Tickertape -> Screener -> Yahoo
fs.quarterly_results("532540")           # BSE scrip code also works (incl. BSE-only companies)
fs.quarterly_results("AAPL", market="US")  # Yahoo -> SEC EDGAR
fs.annual_results("INFY")                # yearly P&L: Moneycontrol -> Tickertape -> Yahoo
fs.balance_sheet("HDFCBANK"); fs.cash_flow("RELIANCE", quarterly=True)
fs.annual_reports("RELIANCE")            # annual-report PDF links from NSE (10-Ks for US)
fs.result_filings("TCS")                 # results filings with PDF + XBRL links
fs.result_filings()                      # every company's results filed in the last 30 days
fs.announcements("INFY", days=60)        # all corporate announcements
fs.board_meetings(); fs.upcoming_results()   # when results are coming
fs.earnings_dates("TCS")                 # EPS estimate vs actual, surprise %
fs.moneycontrol("TCS", "ratios"); fs.tickertape("TCS", "scorecard"); fs.shareholding("TCS")
fs.screener("TCS", "quarterly")          # set SCREENER_USER / SCREENER_PASS (free account)
```

**Official numbers straight from the filing.** Every listed Indian company files its results in XBRL
(a machine-readable format) along with the PDF. finstack reads it into a table:

```python
fs.latest_results("INFY")                # revenue, other income, expenses, PBT, tax, net profit, EPS
fs.xbrl_results("RES_123.xml")           # same, from a file or URL you already have (see result_filings)
fs.xbrl_facts("RES_123.xml")             # every number in the filing incl. segment breakdowns
```

**BSE** (about 5,000 companies list only on BSE, so NSE tools miss them):

```python
fs.company_lookup("Tata Consultancy")    # name / symbol / ISIN -> NSE symbol, BSE code, ISIN
fs.bse_results("500325")                 # results snapshot: revenue, net profit, EPS
fs.bse_result_calendar(days=14)          # who declares results in the next 2 weeks
fs.bse_announcements("RELIANCE", days=30); fs.bse_actions(days=30)
fs.list_companies(industry="Banks", group="")   # BSE company master with industry and ISIN
```

For US SEC data set `EDGAR_IDENTITY="Your Name you@email.com"` (SEC requires it).

## Market data

```python
fs.nse_history("RELIANCE", "2025-01-01"); fs.nse_quote("SBIN"); fs.bse_quote("500325")
fs.tv_history("BANKNIFTY", "NSE", "5m", 500); fs.bhavcopy("2025-08-14", segment="fno")
fs.option_chain("NIFTY"); fs.fii_dii(); fs.ipos("upcoming"); fs.corporate_actions("INFY")
fs.index_constituents("NIFTY BANK"); fs.mcx("market_watch")
fs.mf_search("parag parikh"); fs.mf_nav("122639", history_nav=True)
fs.history("AAPL"); fs.world_history("CN", "600519"); fs.symbols("India", sector="Financials")
fs.imf_weo(); fs.dbnomics("IMF/WEO:2025-10/IND.NGDP_RPCH.pcent_change"); fs.ecb("EXR.D.INR.EUR.SP00.A")
fs.macro("DGS10"); fs.us_yield_curve(2026); fs.fx("USD", "INR"); fs.crypto_price("bitcoin", "inr")
fs.news(); fs.news(query="results")      # Business Standard, ET, Moneycontrol, Livemint RSS
fs.google_news("TCS results", country="IN"); fs.gdelt("RBI repo rate", country="IN"); fs.trends("Nifty", geo="IN")
fs.catalog(region="IN"); fs.get("nsepython")   # browse / raw access to any library
```

## Claude connectors

Connectors work inside Claude conversations, not inside this Python package. Use them to pull data while you chat with Claude (for example, "get Apple's last 8 quarters of income statements and save them as CSV"), then load the CSV into your project. Their underlying APIs can also be called from Python with your own key.

| Connector | Company data | India coverage |
|---|---|---|
| Financial Datasets | Income/balance/cash-flow statements, earnings, SEC filings, insider trades, 13F | US-listed only (Infosys via its US listing INFY) |
| Alpha Vantage | Statements, earnings, SEC filings, prices | Some (BSE) |
| FMP | Statements, ratios, earnings calendar, transcripts | Some |
| Twelve Data | Fundamentals, earnings, prices | Some (NSE) |
| FactSet / LSEG / Moody's | Institutional fundamentals, estimates, credit | Yes (paid subscriptions) |
| Bigdata.com / Pinegap / Kepler | Filings, earnings calls, research | Varies |

There is no Claude connector for Indian company financials yet. For Indian quarterly results the free Python sources above (NSE, BSE, XBRL filings, Moneycontrol, Tickertape, Screener) are the best route. For Indian **economic** data there is the free **MoSPI** connector (official GDP, CPI inflation, IIP, employment statistics; no sign-in needed).

## Notes

- Most Indian sources read public website endpoints and can break when sites change; every pipeline tries several. The network governor keeps requests within each site's limits; stay within each site's terms.
- Tests: `pytest` runs offline (49 tests: the core, the rate limiter against a local server, and the adapters against recorded NSE/BSE responses). Live endpoints were not reachable from the machine this release was built on, so run `python -m finstack.check` on yours.
- Free-key APIs need your own key; broker SDKs need your account (Zerodha charges for historical data). Korea's pykrx needs a free KRX login (KRX_ID / KRX_PW).
- Python 3.10+. A full install uses pandas 2.x.
