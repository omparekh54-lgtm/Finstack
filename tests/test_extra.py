"""Phase 1 additions (beta): safety groundwork, deals, delivery, option analytics, index valuation,
ratios and analyst estimates - all offline, with the exchange libraries replaced by fakes."""
import datetime as dt
import importlib
import types

import pandas as pd
import pytest
from conftest import load

import finstack as fs
from finstack.adapters import global_extra, india_extra
from finstack.core import calendar, router


# ------------------------------------------------------------------ groundwork
def test_a_broken_adapter_module_does_not_break_the_others(monkeypatch, capsys):
    import finstack.adapters as adapters

    real = importlib.import_module

    def flaky(name, *a, **k):
        if name.endswith(".india_extra"):
            raise ImportError("simulated breakage")
        return real(name, *a, **k)

    monkeypatch.setattr(importlib, "import_module", flaky)
    importlib.reload(adapters)
    assert "india_extra" in adapters.failed
    assert "india_daily_prices" in router.PIPELINES          # stable types still registered
    assert "unavailable" in capsys.readouterr().err
    monkeypatch.setattr(importlib, "import_module", real)
    importlib.reload(adapters)
    assert adapters.failed == {}


def test_every_http_client_is_governed():
    assert {"requests", "urllib", "httpx"} <= set(fs.governor.installed())


NO_HOST_OK = {"builtin:options_analytics", "builtin:deals_all", "currency_converter", "baostock",
              "finance_datareader", "openbb"}


def test_every_source_declares_its_websites():
    router._load_pipelines()
    missing = sorted({s.key for p in router.PIPELINES.values() for q in [p, *p.variants.values()]
                      for s in q.sources if not s.hosts and s.key not in NO_HOST_OK})
    assert not missing, f"sources without hosts (needed to skip paused websites): {missing}"


def test_new_types_are_marked_beta_and_old_ones_stable():
    df = fs.pipelines()
    beta = set(df[df.status == "beta"]["type"] + ":" + df[df.status == "beta"]["what"])
    assert {"india_deals:", "india_delivery:", "india_options:analytics", "india_index_valuation:",
            "india_ratios:", "analyst_estimates:"} <= beta
    assert (df[df.type == "india_daily_prices"].status == "stable").all()


# ------------------------------------------------------------------ fakes
class FakeNSE:
    def __init__(self):
        self.calls = []

    def bulk_deals(self, option_type, from_date, to_date):
        self.calls.append(("deals", option_type))
        return [{"BD_DT_DATE": "03-Oct-2026", "BD_SYMBOL": "TCS", "BD_SCRIP_NAME": "Tata Consultancy",
                 "BD_CLIENT_NAME": "SOME FUND", "BD_BUY_SELL": "BUY", "BD_QTY_TRD": "1,20,000",
                 "BD_TP_WATP": "3,850.50", "BD_REMARKS": "-"},
                {"BD_DT_DATE": "03-Oct-2026", "BD_SYMBOL": "INFY", "BD_SCRIP_NAME": "Infosys",
                 "BD_CLIENT_NAME": "OTHER FUND", "BD_BUY_SELL": "SELL", "BD_QTY_TRD": "50000",
                 "BD_TP_WATP": "1500", "BD_REMARKS": "-"}]

    def delivery_bhavcopy(self, date, folder):
        self.calls.append(("delivery", date.date()))
        path = f"{folder}/sec_bhavdata_full_{date:%d%m%Y}.csv"
        pd.DataFrame({"SYMBOL": ["TCS", "INFY", "XYZ"], " SERIES": [" EQ", " EQ", " BE"],
                      " DATE1": [f" {date:%d-%b-%Y}"] * 3, " CLOSE_PRICE": [3850.0, 1500.0, 12.0],
                      " TTL_TRD_QNTY": [1000, 2000, 10], " DELIV_QTY": [600, 900, 10],
                      " DELIV_PER": [60.0, 45.0, 100.0]}).to_csv(path, index=False)
        return path

    def option_chain(self, symbol, expiry_date=None):
        return load("option_chain.json")


@pytest.fixture()
def fake(monkeypatch):
    from finstack.adapters import india_derivs

    n = FakeNSE()
    monkeypatch.setattr(india_extra, "nse", lambda: n)
    monkeypatch.setattr(india_derivs, "nse", lambda: n)
    monkeypatch.setattr(calendar, "india_final_through", lambda at=None: dt.date(2026, 10, 5))
    return n


# ------------------------------------------------------------------ deals
def test_bulk_deals_and_per_symbol_filter_reuse_one_request(fake):
    all_ = fs.fetch("india_deals", what="bulk", start="2026-09-01", end="2026-10-03", sources=["nse"])
    assert set(all_["symbol"]) == {"TCS", "INFY"}
    r = all_[all_.symbol == "TCS"].iloc[0]
    assert (r["side"], r["quantity"], r["price"], r["client"]) == ("BUY", 120000, 3850.5, "SOME FUND")
    tcs = fs.fetch("india_deals", "TCS", what="bulk", start="2026-09-01", end="2026-10-03")
    assert list(tcs["symbol"]) == ["TCS"]
    assert fake.calls.count(("deals", "bulk_deals")) == 1        # the second call came from the cache


# ------------------------------------------------------------------ delivery
def test_delivery_per_stock_from_one_daily_file(fake):
    df = fs.fetch("india_delivery", "TCS", start="2026-09-28", end="2026-09-30", sources=["nse"])
    assert list(df["symbol"].unique()) == ["TCS"] and len(df) == 3
    assert df["delivery_pct"].iloc[0] == 60.0 and df["delivery_qty"].iloc[0] == 600
    n_files = len([c for c in fake.calls if c[0] == "delivery"])
    fs.fetch("india_delivery", "INFY", start="2026-09-28", end="2026-09-30", sources=["nse"])
    assert len([c for c in fake.calls if c[0] == "delivery"]) == n_files      # same files, cached


# ------------------------------------------------------------------ option analytics
def test_max_pain_and_pcr_by_hand():
    chain = pd.DataFrame({"strike": [100, 100, 110, 110, 120, 120],
                          "option_type": ["CE", "PE", "CE", "PE", "CE", "PE"],
                          "oi": [10, 50, 30, 30, 60, 5], "underlying_price": 108.0})
    a = india_extra.option_analytics(chain)
    # writers' payout at 100: CE 0 + PE (10*30 + 20*5) = 400; at 110: CE 10*10 + PE 10*5 = 150;
    # at 120: CE 20*10 + 10*30 = 500 + PE 0 -> max pain 110
    assert a["max_pain"].iloc[0] == 110 and a["atm_strike"].iloc[0] == 110
    assert a["pcr_total"].iloc[0] == pytest.approx(85 / 100)
    assert a.set_index("strike").loc[100, "pcr"] == 5.0


def test_option_analytics_reuses_the_cached_chain(fake):
    df = fs.fetch("india_options", "NIFTY", what="analytics", expiry="2023-12-28")
    assert {"max_pain", "pcr_total", "ce_oi", "pe_oi"} <= set(df.columns) and len(df) > 0


# ------------------------------------------------------------------ index valuation
def test_index_valuation(monkeypatch):
    class Ay:
        @staticmethod
        def index_pe_df(symbol, a, b):
            d = pd.date_range(a, b, freq="B")
            return pd.DataFrame({"date": d, "symbol": symbol, "price_to_earnings": 22.5,
                                 "price_to_book": 3.4, "dividend_yield": 1.2})

    monkeypatch.setattr(india_extra, "get", lambda key, sub="": Ay)
    monkeypatch.setattr(calendar, "india_final_through", lambda at=None: dt.date(2026, 10, 5))
    df = fs.fetch("india_index_valuation", "NIFTY 50", start="2026-09-21", end="2026-09-25", sources=["aynse"])
    assert list(df.columns[:4]) == ["date", "pe", "pb", "div_yield"] and df["pe"].iloc[0] == 22.5
    assert df["index"].iloc[0] == "NIFTY 50"


# ------------------------------------------------------------------ ratios
@pytest.mark.parametrize("label,name", [
    ("Basic EPS (Rs.)", "eps"), ("Return on Networth / Equity (%)", "roe"),
    ("Return on Capital Employed (%)", "roce"), ("Price/BV (X)", "pb"), ("Total Debt/Equity (X)", "debt_to_equity"),
    ("Net Profit Margin (%)", "net_margin"), ("Current Ratio (X)", "current_ratio"), ("Book Value [ExclRevalReserve]/Share (Rs.)", "book_value_per_share"),
])
def test_ratio_labels(label, name):
    assert india_extra.ratio_name(label) == name


def test_ratios_from_yahoo(monkeypatch):
    class T:
        def __init__(self, t):
            self.info = {"trailingPE": 28.0, "priceToBook": 12.0, "returnOnEquity": 0.48, "debtToEquity": 8.5,
                         "profitMargins": 0.19, "dividendYield": 1.6, "trailingEps": 135.0}

    monkeypatch.setattr(india_extra, "get", lambda key, sub="": types.SimpleNamespace(Ticker=T))
    r = fs.fetch("india_ratios", "TCS", sources=["yfinance"]).iloc[0]
    assert (r["pe"], r["pb"], r["roe"], r["net_margin"], r["debt_to_equity"]) == (28.0, 12.0, 48.0, 19.0, 0.085)
    assert r["symbol"] == "TCS"


# ------------------------------------------------------------------ analyst estimates
def test_analyst_targets_and_symbol_mapping(monkeypatch):
    seen = []

    class T:
        def __init__(self, t):
            seen.append(t)
            self.analyst_price_targets = {"current": 3850.0, "low": 3200.0, "high": 4700.0, "mean": 4100.0}

    monkeypatch.setattr(global_extra, "get", lambda key, sub="": types.SimpleNamespace(Ticker=T))
    df = fs.fetch("analyst_estimates", "TCS", what="price_targets", sources=["yfinance"])
    assert df.iloc[0]["mean"] == 4100.0 and df.iloc[0]["symbol"] == "TCS.NS"
    fs.fetch("analyst_estimates", "AAPL", what="price_targets", market="US", sources=["yfinance"])
    assert seen == ["TCS.NS", "AAPL"]


# ================================================================== Phase 2
class FakeNSE2(FakeNSE):
    def priceband_report(self, date, folder):
        self.calls.append(("bands", date.date()))
        path = f"{folder}/sec_list_{date:%d%m%Y}.csv"
        pd.DataFrame({"Symbol": ["ADANIENT", "XYZ"], "Series": ["EQ", "BE"], "Security Name": ["Adani", "Xyz"],
                      "Band": ["No Band", "5"], "Remarks": ["-", "-"]}).to_csv(path, index=False)
        return path

    def fno_lots(self):
        return load("fno_lots.json")

    def get_futures_expiry(self, index="nifty"):
        return ["27-Oct-2026", "24-Nov-2026", "29-Dec-2026"]

    def fetch_historical_fno_data(self, symbol, instrument, from_date, to_date, expiry=None, option_type=None,
                                  strike_price=None):
        self.calls.append(("fno", symbol, instrument))
        base = load("fetch_historical_fno_data.json")[0]
        return [dict(base, FH_TIMESTAMP=d.strftime("%d-%b-%Y"), FH_SYMBOL=symbol)
                for d in calendar.trading_days(from_date, to_date)]

    def live_volume_gainers(self):
        return load("live_volume_gainers.json")

    def equity_meta_info(self, symbol):
        return load("equity_meta_info.json")


@pytest.fixture()
def fake2(monkeypatch):
    n = FakeNSE2()
    monkeypatch.setattr(india_extra, "nse", lambda: n)
    from finstack.adapters import india_prices

    monkeypatch.setattr(india_prices, "nse", lambda: n)
    monkeypatch.setattr(calendar, "india_final_through", lambda at=None: dt.date(2026, 10, 5))
    return n


def test_price_bands(fake2):
    df = fs.fetch("india_price_bands", "XYZ", start="2026-10-01", end="2026-10-01")
    r = df.iloc[0]
    assert (r["symbol"], r["band_pct"]) == ("XYZ", 5.0)
    allb = fs.fetch("india_price_bands", start="2026-10-01", end="2026-10-01")
    assert len(allb) == 2 and pd.isna(allb.set_index("symbol").loc["ADANIENT", "band_pct"])
    assert fake2.calls.count(("bands", dt.date(2026, 10, 1))) == 1


def test_lot_sizes_and_expiries(fake2):
    lots = fs.fetch("india_fno_reference", what="lots")
    assert lots.set_index("symbol").loc["NIFTY", "lot_size"] == 50 and len(lots) > 30
    one = fs.fetch("india_fno_reference", "AXISBANK", what="lots")
    assert list(one["lot_size"]) == [625]
    exp = fs.fetch("india_fno_reference", "NIFTY", what="expiries")
    assert exp["expiry"].iloc[0] == pd.Timestamp("2026-10-27")


def test_fno_contract_history(fake2):
    df = fs.fetch("india_fno_history", "BANKNIFTY", expiry="2026-02-24", start="2025-12-15", end="2025-12-19",
                  sources=["nse"])
    assert len(df) == 5 and {"settle", "oi", "oi_change", "underlying_price"} <= set(df.columns)
    assert df["oi"].iloc[0] == 45300 and df["symbol"].iloc[0] == "BANKNIFTY"
    assert ("fno", "BANKNIFTY", "futidx") in fake2.calls


def test_volume_gainers_added_without_touching_old_breadth(fake2):
    df = fs.fetch("india_market_breadth", what="volume_gainers", sources=["nse:volume_gainers"])
    assert df.iloc[0]["symbol"] == "NEWGEN"
    labels = [s.label for s in router.PIPELINES["india_market_breadth"].sources]
    assert labels[:2] == ["nse", "builtin:nse_fiidii"]          # original sources first, unchanged


def test_margins_daily_file(monkeypatch):
    class CM:
        @staticmethod
        def var_end_of_day(d):
            return pd.DataFrame({"Symbol": ["TCS ", "INFY"], "Series": ["EQ", "EQ"], "VaR Margin": ["12.5", "13"],
                                 "Extreme Loss Rate": [3.5, 3.5], "Applicable Margin Rate": [16.0, 16.5]})

    monkeypatch.setattr(india_extra, "get", lambda key, sub="": CM)
    monkeypatch.setattr(calendar, "india_final_through", lambda at=None: dt.date(2026, 10, 5))
    df = fs.fetch("india_margins", "TCS", start="2026-10-01", end="2026-10-01")
    assert df.iloc[0]["var_margin"] == 12.5 and df.iloc[0]["applicable_margin_rate"] == 16.0


def test_company_profile_india_and_global(monkeypatch):
    class NP:
        @staticmethod
        def nse_eq(sym):
            return {"info": {"companyName": "Tata Consultancy Services Limited", "isin": "INE467B01029"},
                    "industryInfo": {"macro": "Information Technology", "sector": "Information Technology",
                                     "industry": "IT - Services", "basicIndustry": "Computers - Software"},
                    "metadata": {"listingDate": "25-Aug-2004", "pdSectorPe": 30.1, "pdSymbolPe": 28.0}}

    class T:
        def __init__(self, t):
            self.info = {"longName": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics",
                         "fullTimeEmployees": 160000}

    def fake_get(key, sub=""):
        return NP if key == "nsepython" else types.SimpleNamespace(Ticker=T)

    monkeypatch.setattr(india_extra, "get", fake_get)
    tcs = fs.fetch("company_profile", "TCS", sources=["nsepython"]).iloc[0]
    assert tcs["basic_industry"] == "Computers - Software" and tcs["isin"] == "INE467B01029"
    aapl = fs.fetch("company_profile", "AAPL", market="US", sources=["yfinance"]).iloc[0]
    assert (aapl["symbol"], aapl["employees"]) == ("AAPL", 160000)
    assert "nsepython" not in fs.route("company_profile", "AAPL", market="US")["source"].tolist()
