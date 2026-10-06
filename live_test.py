"""Live test: does each data type return CORRECT data on this computer, using free sources only?

    python live_test.py            # everything (about 5-10 minutes the first time)
    python live_test.py india      # only the Indian data types
    python live_test.py extra      # only the data types added in 0.8 (beta)
    python live_test.py fresh      # ignore the cache and download everything again

A test passes only if data came back AND it passed a sanity check (right columns, sensible values).
Broker sources are never used here: the test hides any broker credentials you may have set.
Writes live_test_report.csv next to this file.
"""
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

# free sources only: hide broker credentials for this run
_BROKER_KEYS = ("UPSTOX_ACCESS_TOKEN", "KITE_API_KEY", "KITE_ACCESS_TOKEN", "DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN",
                "ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "FYERS_CLIENT_ID", "FYERS_ACCESS_TOKEN", "ALPACA_API_KEY")
for _k in _BROKER_KEYS:
    os.environ.pop(_k, None)

import pandas as pd  # noqa: E402

import finstack as fs  # noqa: E402

fs.configure(**{k: "" for k in _BROKER_KEYS})      # also overrides ~/.finstack/config.toml for this run


def has(*cols):
    return lambda df: all(c in df.columns for c in cols) or f"missing columns {[c for c in cols if c not in df]}"


def between(col, lo, hi):
    def check(df):
        if col not in df:
            return f"no '{col}' column"
        v = pd.to_numeric(df[col], errors="coerce").dropna()
        return (len(v) > 0 and bool(v.between(lo, hi).all())) or f"{col} outside {lo}-{hi}: {v.min()}..{v.max()}"
    return check


def at_least(n):
    return lambda df: len(df) >= n or f"only {len(df)} rows (expected {n}+)"


def all_of(*checks):
    def check(df):
        for c in checks:
            r = c(df)
            if r is not True:
                return r
        return True
    return check


def anything(df):
    return True


# Tests whose data simply doesn't exist at some times, or needs a (free) login, are reported as
# [~] instead of [ ] when they fail for that reason.
ONLY_IN_SESSION = {"Pre-open"}
NEEDS_LOGIN = {"MCX gold": "MCX blocks scripts and TradingView only serves MCX to logged-in users: set "
                           "TRADINGVIEW_USERNAME / TRADINGVIEW_PASSWORD (free account) or a broker token"}

# (group, label, data type, symbol, parameters, sanity check)
TESTS = [
    # ---------------- India: prices and market data
    ("india", "Daily prices", "india_daily_prices", "RELIANCE", dict(start="30d"),
     all_of(has("date", "open", "high", "low", "close", "volume"), at_least(15))),
    ("india", "Daily prices, adjusted", "india_daily_prices", "RELIANCE", dict(start="3y", adjust=True),
     all_of(has("adj_close", "close"), at_least(600))),
    ("india", "Live quote (stock)", "india_live_quotes", "TCS", {}, between("last", 100, 100000)),
    ("india", "Live quote (index)", "india_live_quotes", "NIFTY", {}, between("last", 10000, 100000)),
    ("india", "Intraday 5m", "india_intraday", "INFY", dict(start="3d", interval="5m"),
     all_of(has("ts", "close"), between("close", 500, 5000))),
    ("india", "Bhavcopy equity", "india_eod_files", None, dict(start="7d"), at_least(1000)),
    ("india", "Bhavcopy F&O", "india_eod_files", None, dict(start="7d", segment="fno"),
     all_of(has("symbol", "expiry", "close"), at_least(1000))),
    ("india", "Option chain", "india_options", "NIFTY", {},
     all_of(has("strike", "option_type", "oi"), at_least(20))),
    ("india", "Index history", "india_indices", "NIFTY BANK", dict(start="30d"),
     all_of(between("close", 20000, 150000), at_least(15))),
    ("india", "India VIX", "india_indices", "INDIA VIX", dict(start="30d"), between("close", 5, 100)),
    ("india", "Sensex", "india_indices", "SENSEX", dict(start="30d"), between("close", 30000, 200000)),
    ("india", "Index constituents", "india_indices", "NIFTY 50", dict(what="constituents"), at_least(50)),
    ("india", "FII / DII", "india_market_breadth", None, dict(what="fii_dii"), at_least(1)),
    ("india", "Advance / decline", "india_market_breadth", None, dict(what="advance_decline"), at_least(1)),
    ("india", "Top gainers", "india_market_breadth", None, dict(what="gainers"), at_least(3)),
    ("india", "Top losers", "india_market_breadth", None, dict(what="losers"), at_least(3)),
    ("india", "Pre-open", "india_market_breadth", None, dict(what="pre_open"), at_least(1)),     # 09:00-15:30 only
    ("india", "52-week high/low", "india_market_breadth", None, dict(what="52w"), at_least(1)),
    ("india", "Bulk: 10 stocks", "BULK", None, dict(start="30d"), all_of(has("symbol"), at_least(150))),
    # ---------------- India: company data
    ("india", "Quarterly results", "india_company_financials", "TCS", dict(statement="quarterly"),
     all_of(has("period_end", "revenue", "net_profit"), at_least(2))),
    ("india", "Annual results", "india_company_financials", "TCS", dict(statement="annual"),
     all_of(has("period_end", "net_profit"), at_least(2))),
    ("india", "Balance sheet", "india_company_financials", "TCS", dict(statement="balance_sheet"), at_least(2)),
    ("india", "Cash flow", "india_company_financials", "TCS", dict(statement="cash_flow"), at_least(2)),
    ("india", "Corporate actions", "india_corporate_events", "INFY", dict(what="actions", start="5y"),
     has("ex_date", "purpose")),
    ("india", "Results calendar", "india_corporate_events", None, dict(what="results_calendar"), at_least(1)),
    ("india", "Announcements", "india_corporate_events", "INFY", dict(what="announcements"), at_least(1)),
    ("india", "Board meetings", "india_corporate_events", "INFY", dict(what="board_meetings", start="1y"),
     at_least(1)),
    ("india", "Shareholding", "india_corporate_events", "INFY", dict(what="shareholding"), at_least(1)),
    ("india", "Annual reports", "india_corporate_events", "INFY", dict(what="annual_reports"), at_least(1)),
    ("india", "Result filings (XBRL)", "india_corporate_events", "TCS", dict(what="result_filings"), at_least(1)),
    ("india", "IPOs", "india_ipos", None, dict(what="upcoming"), anything),
    ("india", "Company list", "reference", None, dict(what="india"), at_least(4000)),
    # ---------------- India: mutual funds and commodities
    ("india", "MF NAV history", "india_mutual_funds", "122639", dict(start="1y"),
     all_of(has("date", "nav"), at_least(200))),
    ("india", "All MF NAVs (AMFI)", "india_mutual_funds", None, dict(what="latest"), at_least(5000)),
    ("india", "MF search", "india_mutual_funds", None, dict(what="search", query="parag parikh"), at_least(1)),
    ("india", "MCX gold", "india_commodities", "GOLD", dict(start="30d"), at_least(10)),
    # ---------------- New in 0.8 (beta): python live_test.py extra
    ("extra", "Bulk deals", "india_deals", None, dict(what="bulk", start="30d"),
     all_of(has("date", "symbol", "quantity"), at_least(1))),
    ("extra", "Block deals", "india_deals", None, dict(what="block", start="90d"), has("date", "symbol")),
    ("extra", "Short selling", "india_deals", None, dict(what="short", start="30d"), has("date", "symbol")),
    ("extra", "Delivery % (one stock)", "india_delivery", "TCS", dict(start="10d"),
     all_of(has("delivery_pct"), between("delivery_pct", 0, 100), at_least(3))),
    ("extra", "Option analytics", "india_options", "NIFTY", dict(what="analytics"),
     all_of(has("max_pain", "pcr_total"), between("pcr_total", 0.05, 20))),
    ("extra", "Index valuation", "india_index_valuation", "NIFTY 50", dict(start="1y"),
     all_of(between("pe", 5, 60), at_least(100))),
    ("extra", "Company ratios", "india_ratios", "TCS", {}, all_of(has("pe"), at_least(1))),
    ("extra", "Analyst price targets", "analyst_estimates", "INFY", dict(what="price_targets"), has("mean")),
    ("extra", "Analyst recommendations", "analyst_estimates", "INFY", dict(what="recommendations"),
     at_least(1)),
    ("extra", "Earnings surprises", "analyst_estimates", "AAPL", dict(what="earnings_history", market="US"),
     at_least(1)),
    ("extra", "Price bands", "india_price_bands", None, dict(start="7d"), all_of(has("symbol", "band"), at_least(1000))),
    ("extra", "F&O lot sizes", "india_fno_reference", None, dict(what="lots"), at_least(50)),
    ("extra", "Futures expiries", "india_fno_reference", "NIFTY", dict(what="expiries"), at_least(1)),
    ("extra", "F&O contract history", "FNO", "NIFTY", dict(start="20d"), all_of(has("oi", "settle"), at_least(5))),
    ("extra", "Volume gainers", "india_market_breadth", None, dict(what="volume_gainers"), at_least(1)),
    ("extra", "Most active", "india_market_breadth", None, dict(what="most_active"), at_least(1)),
    ("extra", "Margins (VaR/ELM)", "india_margins", "TCS", dict(start="7d"), at_least(1)),
    ("extra", "Company profile (India)", "company_profile", "TCS", {}, has("name")),
    ("extra", "Company profile (US)", "company_profile", "AAPL", dict(market="US"), has("name", "sector")),
    ("extra", "Peers", "india_peers", "TCS", {}, at_least(2)),
    ("extra", "Tickertape scorecard", "india_scorecard", "TCS", {}, at_least(1)),
    ("extra", "Segments (XBRL)", "india_segments", "RELIANCE", {}, all_of(has("segment", "value_cr"), at_least(2))),
    ("extra", "ETF list", "india_instruments", None, dict(what="etf"), at_least(20)),
    ("extra", "Gold bonds list", "india_instruments", None, dict(what="sgb"), at_least(1)),
    ("extra", "Exchange circulars", "india_instruments", None, dict(what="circulars"), at_least(1)),
    ("extra", "Insider trades (India)", "insider_trades", "TCS", dict(start="1y"), at_least(1)),
    ("extra", "Insider trades (US)", "insider_trades", "AAPL", dict(market="US"), at_least(1)),
    ("extra", "Holders (US)", "holders", "AAPL", dict(market="US"), at_least(3)),
    ("extra", "Dividend history", "dividends", "ITC", {}, all_of(has("date", "dividend"), at_least(5))),
    ("extra", "Company news", "company_news", "INFY", {}, all_of(has("title"), at_least(1))),
    ("extra", "ESG scores (US)", "esg", "AAPL", dict(market="US"), at_least(1)),
    # ---------------- Global
    ("global", "US quote", "global_live_quotes", "AAPL", {}, between("last", 10, 10000)),
    ("global", "US daily prices", "global_daily_prices", "AAPL", dict(start="30d"), at_least(15)),
    ("global", "US financials", "global_company_financials", "AAPL", {}, has("period_end", "revenue")),
    ("global", "China stock", "world_markets", "CN:600519", dict(start="30d"), at_least(10)),
    ("global", "Macro (FRED)", "macro", "fred:DGS10", dict(start="1y"), at_least(200)),
    ("global", "US yield curve", "rates", None, dict(start="30d"), at_least(10)),
    ("global", "USD/INR", "forex", "USD/INR", dict(start="30d"), between("rate", 60, 120)),
    ("global", "Bitcoin", "crypto", "BTC/USDT", dict(start="30d"), at_least(25)),
    ("global", "News", "news", None, dict(query="Nifty"), anything),
    ("global", "Google Trends", "alt_data", None, dict(what="trends", query="Nifty", geo="IN"), at_least(10)),
]

args = [a.lower() for a in sys.argv[1:]]
group = next((a for a in args if a in ("india", "global", "extra")), None)
fresh = "fresh" in args

rows = []
for grp, label, dtype, sym, kw, check in TESTS:
    if group and grp != group:
        continue
    t0 = time.time()
    err, df = None, None
    try:
        if dtype == "FNO":         # history of the nearest NIFTY futures contract
            exp = fs.fetch("india_fno_reference", "NIFTY", what="expiries")["expiry"].min()
            df = fs.fetch("india_fno_history", sym, expiry=str(exp.date()), refresh=fresh, timeout=30, **kw)
        elif dtype == "BULK":
            syms = fs.fetch("india_indices", "NIFTY 50", what="constituents")["symbol"].head(10).tolist()
            df = fs.bulk("india_daily_prices", syms, refresh=fresh, progress=False, **kw)
            if df.attrs.get("failed"):
                raise RuntimeError(f"failed symbols: {df.attrs['failed']}")
        if dtype not in ("FNO", "BULK"):
            df = fs.fetch(dtype, sym, timeout=30, refresh=fresh, **kw)
        verdict = check(df)
        if verdict is True:
            status, note = "OK", "; ".join(df.attrs.get("issues", []))[:150]
        else:
            status, note = "WRONG", str(verdict)
    except Exception as e:  # noqa: BLE001
        err = e
        status = "FAIL"
        note = " | ".join(f"{s}: {o}" for s, o in getattr(e, "attempts", [])[:5])[:400] or str(e)[:400]
    secs = round(time.time() - t0, 1)
    if status == "FAIL":
        from finstack.core import calendar

        if label in ONLY_IN_SESSION and not calendar.in_session():
            status, note = "LATER", "NSE publishes pre-open data 09:00-09:08 IST and clears it after hours"
        elif label in NEEDS_LOGIN and not fs.core.config.get("TRADINGVIEW_USERNAME"):
            status, note = "LOGIN", NEEDS_LOGIN[label]
    n = 0 if df is None else len(df)
    src = "" if df is None else str(df.attrs.get("source", ""))[:40]
    rows.append({"group": grp, "test": label, "type": dtype, "status": status, "rows": n, "source": src,
                 "seconds": secs, "details": note})
    mark = {"OK": "[x]", "WRONG": "[?]", "FAIL": "[ ]", "LATER": "[~]", "LOGIN": "[~]"}[status]
    print(f"{mark} {status:5} {label:24} rows={n:<6} {secs:>5}s  {src}")
    if status == "WRONG":
        print(f"        - check failed: {note}")
    if status in ("LATER", "LOGIN"):
        print(f"        - {note}")
        err = None
    for s, o in (getattr(err, "attempts", []) or []):
        print(f"        - {s}: {o[:160]}")
    if err is not None and not getattr(err, "attempts", None):
        print(f"        - {type(err).__name__}: {str(err)[:200]}")

report = pd.DataFrame(rows)
report.to_csv("live_test_report.csv", index=False)
ok = int((report.status == "OK").sum())
waiting = int(report.status.isin(["LATER", "LOGIN"]).sum())
print(f"\n{ok}/{len(report) - waiting} passed ([x] = data came back and looks right)"
      + (f"; {waiting} [~] not available right now or need a free login" if waiting else "")
      + ". Details: live_test_report.csv")
