"""Real pipelines end to end, with the exchange libraries replaced by fakes that return
recorded NSE/BSE sample responses (the sandbox these tests were written in cannot reach exchanges)."""
import copy
import datetime as dt

import pandas as pd
import pytest
from conftest import load

pytest.importorskip("nse")      # the adapters below are only tried when their library is installed

import finstack as fs  # noqa: E402
from finstack.adapters import india_company, india_derivs, india_prices
from finstack.core import calendar  # noqa: E402


class FakeNSE:
    def __init__(self):
        self.calls = []

    def fetch_equity_historical_data(self, symbol, from_date, to_date, series="eq"):
        self.calls.append(("history", symbol, from_date, to_date))
        base = load("fetch_equity_historical_data.json")[0]
        rows = []
        for i, d in enumerate(calendar.trading_days(from_date, to_date)):
            r = copy.deepcopy(base)
            r.update(CH_SYMBOL=symbol, CH_TIMESTAMP=d.isoformat(), mTIMESTAMP=d.strftime("%d-%b-%Y"),
                     CH_CLOSING_PRICE=1767.85 + i, CH_TRADE_HIGH_PRICE=1809.9 + i, CH_OPENING_PRICE=1780 + i)
            rows.append(r)
        return rows

    def quote(self, symbol):
        return load("quote.json")

    def list_equity_stocks_by_index(self, index):
        return load("list_equity_stocks_by_index.json")

    def option_chain(self, symbol, expiry_date=None):
        return load("option_chain.json")

    def results_comparison(self, symbol):
        return load("results_comparison.json")

    def actions(self, segment="equities", symbol=None, from_date=None, to_date=None):
        return load("actions.json")

    def fetch_historical_index_data(self, index, from_date, to_date):
        rows = []
        base = load("fetch_historical_index_data.json")[0]
        for d in calendar.trading_days(from_date, to_date):
            r = dict(base, EOD_TIMESTAMP=d.strftime("%d-%b-%Y").upper(), EOD_INDEX_NAME=index)
            rows.append(r)
        return rows

    def equity_bhavcopy(self, date, folder):
        self.calls.append(("bhavcopy", date.date()))
        syms = [f"S{i:02d}" for i in range(40)]
        df = pd.DataFrame({"TradDt": date.strftime("%Y-%m-%d"), "TckrSymb": syms, "SctySrs": "EQ",
                           "ISIN": "INE000000000", "OpnPric": 100.0, "HghPric": 102.0, "LwPric": 99.0,
                           "ClsPric": 101.0, "PrvsClsgPric": 100.0, "TtlTradgVol": 1000, "TtlTrfVal": 101000.0})
        path = f"{folder}/BhavCopy_NSE_CM_0_0_0_{date:%Y%m%d}_F_0000.csv"
        df.to_csv(path, index=False)
        return path


class FakeBSE:
    def resultsSnapshot(self, code):
        return load("bse_resultsSnapshot.json")

    def quote(self, code):
        return load("bse_quote.json")


@pytest.fixture()
def fake(monkeypatch):
    n = FakeNSE()
    for mod in (india_prices, india_derivs, india_company):
        monkeypatch.setattr(mod, "nse", lambda: n)
    monkeypatch.setattr(india_prices, "bse", lambda: FakeBSE())
    monkeypatch.setattr(india_company, "bse", lambda: FakeBSE())
    monkeypatch.setattr(calendar, "india_final_through", lambda at=None: dt.date(2026, 9, 30))
    return n


def test_daily_prices_from_nse(fake):
    df = fs.fetch("india_daily_prices", "HDFCBANK", start="2026-09-01", end="2026-09-10", sources=["nse"])
    assert list(df.columns[:6]) == ["date", "open", "high", "low", "close", "volume"]
    assert len(df) == len(calendar.trading_days(dt.date(2026, 9, 1), dt.date(2026, 9, 10)))
    assert df["symbol"].eq("HDFCBANK").all() and df.attrs["source"] == "nse"
    again = fs.fetch("india_daily_prices", "HDFCBANK", start="2026-09-02", end="2026-09-09", sources=["nse"])
    assert len(fake.calls) == 1 and again.attrs["attempts"][-1][0] == "cache"


def test_adjusted_close_from_corporate_actions(fake):
    df = fs.fetch("india_daily_prices", "GENSOL", start="2023-10-12", end="2023-10-20", sources=["nse"],
                  adjust=True)
    before = df[df["date"] < "2023-10-17"]
    after = df[df["date"] >= "2023-10-17"]
    assert (before["adj_close"] * 3 - before["close"]).abs().max() < 1e-9      # Bonus 2:1 -> factor 3
    assert (after["adj_close"] == after["close"]).all()


def test_live_quote_stock_and_index(fake):
    q = fs.fetch("india_live_quotes", "HDFCBANK", sources=["nse"])
    r = q.iloc[0]
    assert (r["last"], r["prev_close"], r["bid"], r["ask"]) == (764.9, 778.9, 764.75, 764.9)
    i = fs.fetch("india_live_quotes", "NIFTY", sources=["nse"]).iloc[0]
    assert i["last"] == 23944.8 and i["symbol"] == "NIFTY 50"


def test_option_chain_rows(fake):
    oc = fs.fetch("india_options", "NIFTY", expiry="2023-12-28", sources=["nse"])
    assert set(oc["option_type"]) == {"CE", "PE"}
    pe = oc[(oc["strike"] == 11000) & (oc["option_type"] == "PE")].iloc[0]
    assert pe["last"] == 1.4 and pe["underlying_price"] == 19731.75 and pe["expiry"] == pd.Timestamp("2023-12-28")


def test_quarterly_results_from_nse_in_crore(fake):
    df = fs.fetch("india_company_financials", "RELIANCE", sources=["nse"])
    r = df.iloc[0]
    assert r["period_end"] == pd.Timestamp("2024-12-31")
    assert r["revenue"] == pytest.approx(128260.0) and r["net_profit"] == pytest.approx(8721.0)
    assert r["eps"] == 6.44 and r["units"].startswith("INR crore")


def test_results_from_bse_snapshot(fake, monkeypatch):
    from finstack.core import symbols

    monkeypatch.setattr(symbols, "find", lambda **kw: {"symbol": "XYZ", "bse_code": "500000", "isin": None})
    df = fs.fetch("india_company_financials", "XYZ", sources=["bse"])
    assert df.iloc[0]["revenue"] == 2883.0 and len(df) == 2       # the two quarters, not the FY column


def test_index_history(fake):
    df = fs.fetch("india_indices", "BANKNIFTY", start="2026-09-21", end="2026-09-25", sources=["nse"])
    assert len(df) == 5 and df["index"].eq("NIFTY BANK").all() and df["close"].iloc[0] == 59461.8


def test_bulk_uses_bhavcopy_when_cheaper(fake):
    syms = [f"S{i:02d}" for i in range(40)]
    df = fs.bulk("india_daily_prices", syms, start="2026-09-21", end="2026-09-25", progress=False)
    hist = [c for c in fake.calls if c[0] == "history"]
    bhav = [c for c in fake.calls if c[0] == "bhavcopy"]
    assert len(bhav) == 5 and not hist                     # 5 files instead of 40 history calls
    assert len(df) == 200 and set(df["source"]) == {"nse:bhavcopy"}


def test_forex_offline_from_bundled_ecb_file():
    pytest.importorskip("currency_converter")
    df = fs.fetch("forex", "USD/INR", start="2024-01-02", end="2024-01-05", sources=["currency_converter"])
    assert 80 < df["rate"].mean() < 86 and df["pair"].eq("USD/INR").all()
