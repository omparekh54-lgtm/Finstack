"""Offline tests for the pipeline core: symbols, schema, validation, calendar, parsers."""
import datetime as dt

import pandas as pd
import pytest
from conftest import load

from finstack.core import calendar, schema, validate
from finstack.core.symbols import isin_valid, resolve


def test_isin_check_digit():
    assert isin_valid("US0378331005")      # Apple
    assert isin_valid("INE002A01018")      # Reliance
    assert isin_valid("INE040A01034")      # HDFC Bank
    assert not isin_valid("INE002A01019")  # wrong check digit
    assert not isin_valid("RELIANCE")


def test_resolve_names():
    i = resolve("banknifty")
    assert (i.kind, i.nse, i.yahoo, i.tv) == ("index", "NIFTY BANK", "^NSEBANK", "NSE:BANKNIFTY")
    t = resolve("TCS.NS")
    assert (t.nse, t.yahoo, t.tv, t.key) == ("TCS", "TCS.NS", "NSE:TCS", "NSE:TCS")
    assert resolve("AAPL", "GLOBAL").key == "G:AAPL"
    assert resolve("NIFTY TOTAL MARKET", "IN_INDEX").kind == "index"


@pytest.mark.parametrize("text,factor", [
    ("Bonus 1:1", 2.0),
    ("Bonus 2:1", 3.0),
    ("Bonus 1:2", 1.5),
    ("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share", 10.0),
    ("Stock  Split From Rs.5/- to Rs.2/-", 2.5),
    ("Interim Dividend - Rs. - 18.0000", None),
])
def test_action_factor(text, factor):
    assert validate.action_factor(text) == factor


def test_adjust_for_actions():
    px = pd.DataFrame({"date": pd.to_datetime(["2023-10-16", "2023-10-17", "2023-10-18"]),
                       "close": [300.0, 100.0, 101.0], "volume": [10, 30, 30]})
    acts = pd.DataFrame({"ex_date": ["17-Oct-2023"], "purpose": ["Bonus 2:1"]})
    out = validate.adjust_for_actions(px, acts)
    assert out["adj_close"].round(2).tolist() == [100.0, 100.0, 101.0]
    assert out["adj_volume"].tolist() == [30, 30, 30]


def test_normalize_nse_next_api():
    df = schema.normalize(load("fetch_equity_historical_data.json"))
    r = df.iloc[0]
    assert r["date"] == pd.Timestamp("2025-04-01")       # not the UTC '2025-03-31T18:30Z' column
    assert (r["open"], r["high"], r["low"], r["close"]) == (1802, 1809.9, 1765.35, 1767.85)
    assert r["volume"] == 14255503 and r["isin"] == "INE040A01034" and r["vwap"] == 1781.74


def test_normalize_index_history():
    df = schema.normalize(load("fetch_historical_index_data.json"))
    assert df.iloc[0]["date"] == pd.Timestamp("2025-12-15") and df.iloc[0]["close"] == 59461.8


def test_normalize_text_numbers_and_dayfirst():
    raw = pd.DataFrame({"Date": ["01-04-2025", "02-04-2025"], "OpenPrice": ["1,771.0", "1,780"],
                        "ClosePrice": ["1,776.5", "-"], "TotalTradedQuantity": ["1,234", "99"],
                        "%DlyQttoTradedQty": ["48.6", "50"]})
    df = schema.normalize(raw)
    assert df["date"].tolist() == [pd.Timestamp("2025-04-01"), pd.Timestamp("2025-04-02")]
    assert df["open"].tolist() == [1771.0, 1780.0]
    assert pd.isna(df["close"].iloc[1]) and df["delivery_pct"].iloc[0] == 48.6


def test_normalize_udiff_and_old_bhavcopy():
    udiff = pd.DataFrame({"TradDt": ["2025-01-02"], "TckrSymb": ["SBIN"], "SctySrs": ["EQ"], "ISIN": ["INE062A01020"],
                          "OpnPric": [760.0], "HghPric": [770.0], "LwPric": [755.0], "ClsPric": [765.0],
                          "PrvsClsgPric": [758.0], "TtlTradgVol": [1000], "TtlTrfVal": [765000.0]})
    a = schema.normalize(udiff, required=("symbol", "close"))
    assert {"date", "symbol", "series", "isin", "open", "close", "prev_close", "volume", "value"} <= set(a.columns)
    old = pd.DataFrame({"SYMBOL": ["SBIN"], "SERIES": ["EQ"], "OPEN": [1.0], "HIGH": [2.0], "LOW": [0.5],
                        "CLOSE": [1.5], "LAST": [1.5], "PREVCLOSE": [1.0], "TOTTRDQTY": [5], "TOTTRDVAL": [7.5],
                        "TIMESTAMP": ["02-JAN-2023"], "TOTALTRADES": [3], "ISIN": ["INE062A01020"]})
    b = schema.normalize(old, required=("symbol", "close"))
    assert b.iloc[0]["date"] == pd.Timestamp("2023-01-02") and b.iloc[0]["trades"] == 3


def test_normalize_missing_column_raises():
    with pytest.raises(schema.SchemaError):
        schema.normalize(pd.DataFrame({"date": ["2025-01-01"], "foo": [1]}))


def test_normalize_daily_dates_keep_exchange_calendar():
    tokyo = pd.DataFrame({"Close": [1.0]}, index=pd.DatetimeIndex(["2025-01-06"], tz="Asia/Tokyo", name="Date"))
    assert schema.normalize(tokyo)["date"].iloc[0] == pd.Timestamp("2025-01-06")
    epoch = pd.DataFrame({"date": [1743445800000], "close": [1.0]})     # 2025-03-31T18:30Z = 1 Apr IST
    assert schema.normalize(epoch)["date"].iloc[0] == pd.Timestamp("2025-04-01")


def test_ohlcv_validation_quarantines_bad_rows():
    df = pd.DataFrame({"date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-03"]),
                       "open": [10, 10, 10, 10], "high": [11, 9, 11, 11], "low": [9, 9.5, 9, 9],
                       "close": [10.5, 10, 10.2, 10.3]})
    good, bad, notes = validate.ohlcv(df)
    assert len(good) == 2 and len(bad) == 1          # high < low row rejected, duplicate date collapsed
    assert any("duplicate" in n for n in notes)
    assert good["close"].tolist() == [10.5, 10.3]


def test_calendar():
    assert not calendar.is_trading_day(dt.date(2026, 10, 2))    # Gandhi Jayanti
    assert not calendar.is_trading_day(dt.date(2026, 10, 3))    # Saturday
    assert calendar.is_trading_day(dt.date(2026, 10, 5))
    ist = calendar.IST
    assert calendar.india_final_through(dt.datetime(2026, 10, 5, 12, 0, tzinfo=ist)) == dt.date(2026, 10, 1)
    assert calendar.india_final_through(dt.datetime(2026, 10, 5, 19, 0, tzinfo=ist)) == dt.date(2026, 10, 5)
    assert len(calendar.trading_days(dt.date(2026, 9, 28), dt.date(2026, 10, 4))) == 4
    ny = dt.datetime(2026, 10, 5, 23, 30, tzinfo=dt.timezone.utc)     # 19:30 New York: final
    assert calendar.global_final_through("AAPL", ny) == dt.date(2026, 10, 5)


def test_period_parsing_and_wide_tables():
    from finstack.adapters.india_company import period_end, periods_from_wide

    assert period_end("Dec-25") == pd.Timestamp("2025-12-31")
    assert period_end("FY24-25") == pd.Timestamp("2025-03-31")
    assert period_end("Mar '24") == pd.Timestamp("2024-03-31")
    assert period_end("2024-04-01 to 2024-06-30") == pd.Timestamp("2024-06-30")
    snap = load("bse_resultsSnapshot.json")["results_in_crores"]
    df = periods_from_wide(pd.DataFrame(snap["data"], columns=snap["fields"]), 1.0)
    dec = df[df["period_end"] == pd.Timestamp("2025-12-31")].iloc[0]
    assert dec["revenue"] == 2883.0 and dec["net_profit"] == 657.0 and dec["eps"] == 0.72


def test_navall_parser():
    from finstack.adapters.india_company import parse_navall

    text = """Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

Open Ended Schemes(Equity Scheme - Flexi Cap Fund)

PPFAS Mutual Fund

122639;INF879O01027;-;Parag Parikh Flexi Cap Fund - Direct Plan - Growth;92.1234;03-Oct-2026
"""
    df = parse_navall(text)
    r = df.iloc[0]
    assert (r["scheme_code"], r["nav"], r["date"]) == ("122639", 92.1234, pd.Timestamp("2026-10-03"))
    assert r["fund_house"] == "PPFAS Mutual Fund" and "Flexi Cap" in r["category"]


def test_sec_quarters_and_derived_q4():
    from finstack.adapters.global_ import sec_periods

    def v(s, e, val):
        return {"start": s, "end": e, "val": val, "filed": "2025-01-01"}

    facts = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
        v("2024-01-01", "2024-03-31", 100e6), v("2024-04-01", "2024-06-30", 110e6),
        v("2024-07-01", "2024-09-30", 120e6), v("2024-01-01", "2024-12-31", 460e6)]}}}}}
    q = sec_periods(facts, quarterly=True)
    q4 = q[q["period_end"] == pd.Timestamp("2024-12-31")].iloc[0]
    assert q4["revenue"] == pytest.approx(130.0) and "derived" in q4["note"]
    a = sec_periods(facts, quarterly=False)
    assert a.iloc[0]["revenue"] == pytest.approx(460.0)


def test_fx_pair_and_crypto_pair_parsing():
    from finstack.adapters import crypto, macro

    assert macro.pair("USD/INR") == ("USD", "INR") == macro.pair("USDINR=X") == macro.pair("usdinr")
    assert crypto.pair("BTC/USDT") == ("BTC", "USDT")
    assert crypto.pair("bitcoin") == ("BTC", "USD")
    assert crypto.pair("ETHUSDT") == ("ETH", "USDT")
    assert macro.split_id("DGS10") == ("fred", "DGS10")
    assert macro.split_id("imf:WEO:2025-10/IND.NGDP_RPCH") == ("dbn", "IMF/WEO:2025-10/IND.NGDP_RPCH")


def test_eod_rows_accept_untraded_contracts():
    df = pd.DataFrame({"date": pd.to_datetime(["2026-10-01"] * 3), "symbol": ["NIFTY"] * 3,
                       "expiry": pd.to_datetime(["2026-10-27"] * 3), "strike": [20000, 21000, 22000],
                       "option_type": ["CE", "CE", "CE"], "open": [0, 120, 50], "high": [0, 130, 40],
                       "low": [0, 110, 45], "close": [2.5, 125, 47], "volume": [0, 10, 5]})
    good, bad, notes = validate.eod_rows(df)
    assert len(good) == 2 and len(bad) == 1          # untraded row kept; high < low row rejected
    assert bad.iloc[0]["strike"] == 22000 and any("did not trade" in n for n in notes)
