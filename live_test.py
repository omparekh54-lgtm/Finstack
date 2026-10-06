"""Live test: does each data type actually return data on this computer?

    python live_test.py           # about 2-5 minutes
Writes live_test_report.csv next to this file.
"""
import time
import warnings

warnings.filterwarnings("ignore")

import pandas as pd  # noqa: E402

import finstack as fs  # noqa: E402

TESTS = [
    ("India daily prices", "india_daily_prices", "RELIANCE", dict(start="30d")),
    ("India live quote", "india_live_quotes", "TCS", {}),
    ("India index quote", "india_live_quotes", "NIFTY", {}),
    ("India intraday 5m", "india_intraday", "INFY", dict(start="3d", interval="5m")),
    ("NSE bhavcopy", "india_eod_files", None, dict(start="7d")),
    ("Option chain", "india_options", "NIFTY", {}),
    ("MCX gold", "india_commodities", "GOLD", dict(start="30d")),
    ("Quarterly results", "india_company_financials", "TCS", dict(statement="quarterly")),
    ("Balance sheet", "india_company_financials", "TCS", dict(statement="balance_sheet")),
    ("Corporate actions", "india_corporate_events", "INFY", dict(what="actions", start="5y")),
    ("Results calendar", "india_corporate_events", None, dict(what="results_calendar")),
    ("IPOs", "india_ipos", None, dict(what="upcoming")),
    ("Mutual fund NAV", "india_mutual_funds", "122639", dict(start="1y")),
    ("All MF NAVs (AMFI)", "india_mutual_funds", None, dict(what="latest")),
    ("Nifty Bank history", "india_indices", "BANKNIFTY", dict(start="30d")),
    ("Nifty 50 constituents", "india_indices", "NIFTY 50", dict(what="constituents")),
    ("FII / DII", "india_market_breadth", None, dict(what="fii_dii")),
    ("US quote", "global_live_quotes", "AAPL", {}),
    ("US daily prices", "global_daily_prices", "AAPL", dict(start="30d")),
    ("US financials", "global_company_financials", "AAPL", {}),
    ("China stock", "world_markets", "CN:600519", dict(start="30d")),
    ("Macro (FRED)", "macro", "fred:DGS10", dict(start="1y")),
    ("US yield curve", "rates", None, dict(start="30d")),
    ("USD/INR", "forex", "USD/INR", dict(start="30d")),
    ("Bitcoin", "crypto", "BTC/USDT", dict(start="30d")),
    ("News", "news", None, dict(query="Nifty")),
    ("Google Trends", "alt_data", None, dict(what="trends", query="Nifty", geo="IN")),
    ("Symbol list", "reference", None, dict(what="india")),
]

rows = []
err = None
for label, dtype, sym, kw in TESTS:
    t0 = time.time()
    try:
        df = fs.fetch(dtype, sym, timeout=30, **kw)
        status, src, n, note = "OK", df.attrs.get("source", ""), len(df), "; ".join(df.attrs.get("issues", []))[:120]
    except Exception as e:  # noqa: BLE001
        err = e
        status, src, n = "FAIL", "", 0
        note = " | ".join(f"{s}: {o}" for s, o in getattr(e, "attempts", [])[:4])[:300] or str(e)[:300]
    secs = round(time.time() - t0, 1)
    rows.append({"test": label, "type": dtype, "status": status, "rows": n, "source": src, "seconds": secs,
                 "details": note})
    print(f"{status:4}  {label:24} rows={n:<6} {secs:>5}s  {src}")
    if status == "FAIL":                       # show why each source failed, right here
        for s, o in getattr(err, "attempts", []) or []:
            print(f"        - {s}: {o[:160]}")

report = pd.DataFrame(rows)
report.to_csv("live_test_report.csv", index=False)
ok = (report.status == "OK").sum()
print(f"\n{ok}/{len(report)} data types returned data. Details: live_test_report.csv")
print(fs.net_stats().to_string())
