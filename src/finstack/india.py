"""Indian-market helpers: NSE/BSE prices, F&O, IPOs, corporate data, fundamentals, mutual funds.

Every helper tries several libraries in order, so one broken scraper does not
break your project. Symbols are plain NSE codes ("RELIANCE", "TCS", "NIFTY").
"""
from __future__ import annotations

import datetime as _dt
import os
import tempfile
from functools import lru_cache
from typing import Optional

import pandas as pd
import requests

from .loader import get, is_installed

_TIMEOUT = 20
_CACHE_DIR = os.path.join(tempfile.gettempdir(), "finstack_nse")


def _d(x) -> _dt.date:
    if x is None or isinstance(x, _dt.date):
        return x
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return _dt.datetime.strptime(x, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unrecognised date '{x}', use YYYY-MM-DD")


def _fail(what: str, errors: list):
    raise RuntimeError(f"All sources failed for {what}:\n  " + ("\n  ".join(errors) or "no source library installed"))


# ---------------------------------------------------------------- clients
@lru_cache(maxsize=1)
def nse_client():
    """Shared NseIndiaApi client (pip name 'nse'). Use it for anything not wrapped below:

    >>> n = fs.nse_client(); n.bulk_deals(); n.holidays(); n.circulars()
    """
    get("nse")
    from nse import NSE

    os.makedirs(_CACHE_DIR, exist_ok=True)
    return NSE(download_folder=_CACHE_DIR)


@lru_cache(maxsize=1)
def tradingview():
    """Anonymous TradingView client (tvDatafeed)."""
    get("tvdatafeed")
    from tvDatafeed import TvDatafeed

    return TvDatafeed()


# ---------------------------------------------------------------- prices
_TV_INTERVALS = {"1m": "in_1_minute", "3m": "in_3_minute", "5m": "in_5_minute", "15m": "in_15_minute",
                 "30m": "in_30_minute", "1h": "in_1_hour", "2h": "in_2_hour", "4h": "in_4_hour",
                 "1d": "in_daily", "1wk": "in_weekly", "1mo": "in_monthly"}


def tv_history(symbol: str, exchange: str = "NSE", interval: str = "1d", n_bars: int = 500) -> pd.DataFrame:
    """TradingView OHLCV. Works for NSE/BSE stocks, indices (NIFTY, BANKNIFTY), MCX, futures,
    and any other exchange (NASDAQ, NYSE, BINANCE...). Intervals: 1m 3m 5m 15m 30m 1h 2h 4h 1d 1wk 1mo.
    """
    from tvDatafeed import Interval

    df = tradingview().get_hist(symbol=symbol, exchange=exchange,
                                interval=getattr(Interval, _TV_INTERVALS[interval]), n_bars=n_bars)
    if df is None or df.empty:
        raise ValueError("TradingView returned no data")
    df = df.drop(columns=["symbol"], errors="ignore").rename(columns=str.title)
    df.index.name = "Date"
    return df


def nse_history(symbol: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """Daily NSE equity history. Order: nse -> jugaad-data -> aynse -> TradingView.
    Dates as 'YYYY-MM-DD' (or 'DD-MM-YYYY').
    """
    end_d = _d(end) or _dt.date.today()
    start_d = _d(start) or end_d - _dt.timedelta(days=365)
    errors = []
    if is_installed("nse"):
        try:
            rows = nse_client().fetch_equity_historical_data(symbol.upper(), start_d, end_d)
            if rows:
                return pd.DataFrame(rows)
        except Exception as e:
            errors.append(f"nse: {e}")
    if is_installed("jugaad_data"):
        try:
            from jugaad_data.nse import stock_df
            df = stock_df(symbol=symbol.upper(), from_date=start_d, to_date=end_d, series="EQ")
            if df is not None and not df.empty:
                return df
            errors.append("jugaad-data: empty")
        except Exception as e:
            errors.append(f"jugaad-data: {e}")
    if is_installed("aynse"):
        try:
            df = get("aynse").stock_df(symbol=symbol, from_date=start_d.isoformat(), to_date=end_d.isoformat())
            if df is not None and not df.empty:
                return df
            errors.append("aynse: empty")
        except Exception as e:
            errors.append(f"aynse: {e}")
    if is_installed("tvdatafeed"):
        try:
            df = tv_history(symbol.upper(), "NSE", "1d", n_bars=(end_d - start_d).days + 10).loc[str(start_d):str(end_d)]
            if not df.empty:
                return df
            errors.append("tradingview: empty")
        except Exception as e:
            errors.append(f"tradingview: {e}")
    _fail(symbol, errors)


def nse_quote(symbol: str) -> dict:
    """Live NSE quote. Order: nse -> nsepython -> nsetools."""
    errors = []
    if is_installed("nse"):
        try:
            return nse_client().quote(symbol.upper())
        except Exception as e:
            errors.append(f"nse: {e}")
    if is_installed("nsepython"):
        try:
            return get("nsepython").nse_eq(symbol.upper())
        except Exception as e:
            errors.append(f"nsepython: {e}")
    if is_installed("nsetools"):
        try:
            return get("nsetools").Nse().get_quote(symbol.upper())
        except Exception as e:
            errors.append(f"nsetools: {e}")
    _fail(symbol, errors)


def bse_quote(scrip_code: str) -> dict:
    """Live BSE quote by scrip code (e.g. '500325' for Reliance)."""
    return get("bsedata", "bse").BSE().getQuote(str(scrip_code))


def bhavcopy(date: Optional[str] = None, segment: str = "equity") -> pd.DataFrame:
    """End-of-day bhavcopy for all NSE stocks (segment='equity') or F&O (segment='fno')."""
    day = _d(date) or _dt.date.today()
    errors = []
    if is_installed("nsefin"):
        try:
            c = get("nsefin").NSEClient()
            dt_ = _dt.datetime.combine(day, _dt.time())
            df = c.get_equity_bhav_copy(dt_) if segment == "equity" else c.get_fno_bhav_copy(dt_)
            if df is not None and not df.empty:
                return df
            errors.append("nsefin: empty (holiday or not published yet?)")
        except Exception as e:
            errors.append(f"nsefin: {e}")
    if is_installed("nse"):
        try:
            dt_ = _dt.datetime.combine(day, _dt.time())
            n = nse_client()
            path = n.equity_bhavcopy(dt_) if segment == "equity" else n.fno_bhavcopy(dt_)
            return pd.read_csv(path)
        except Exception as e:
            errors.append(f"nse: {e}")
    _fail(f"bhavcopy {day}", errors)


# ---------------------------------------------------------------- derivatives
def option_chain(symbol: str = "NIFTY") -> object:
    """Option chain for an index or F&O stock. Order: nse -> nsepython -> nsefin."""
    errors = []
    if is_installed("nse"):
        try:
            return nse_client().option_chain(symbol.lower() if symbol.upper() in
                                             ("NIFTY", "BANKNIFTY", "FINNIFTY", "NIFTYIT") else symbol.upper())
        except Exception as e:
            errors.append(f"nse: {e}")
    if is_installed("nsepython"):
        try:
            return get("nsepython").nse_optionchain_scrapper(symbol.upper())
        except Exception as e:
            errors.append(f"nsepython: {e}")
    if is_installed("nsefin"):
        try:
            return get("nsefin").NSEClient().get_option_chain(symbol.upper())
        except Exception as e:
            errors.append(f"nsefin: {e}")
    _fail(f"option chain {symbol}", errors)


def fii_dii() -> object:
    """Latest FII/DII cash-market activity."""
    errors = []
    if is_installed("nsepython"):
        try:
            return get("nsepython").nse_fiidii()
        except Exception as e:
            errors.append(f"nsepython: {e}")
    if is_installed("nsefin"):
        try:
            return get("nsefin").NSEClient().get_fii_dii_activity()
        except Exception as e:
            errors.append(f"nsefin: {e}")
    _fail("FII/DII", errors)


# ---------------------------------------------------------------- corporate data
def ipos(kind: str = "current") -> list:
    """Indian IPOs from NSE. kind = 'current' | 'upcoming' | 'past'."""
    n = nse_client()
    return {"current": n.list_current_ipo, "upcoming": n.list_upcoming_ipo, "past": n.list_past_ipo}[kind]()


def corporate_actions(symbol: Optional[str] = None) -> list:
    """Dividends, splits, bonuses etc. from NSE (all stocks if symbol is None)."""
    return nse_client().actions(symbol=symbol.upper() if symbol else None)


def shareholding(symbol: str) -> list:
    """Quarterly shareholding pattern (promoter/FII/DII/public) from NSE filings."""
    return nse_client().shareholding(symbol.upper())


def index_constituents(index: str = "NIFTY 50") -> dict:
    """Stocks in an NSE index (e.g. 'NIFTY 50', 'NIFTY BANK', 'NIFTY MIDCAP 100')."""
    return nse_client().list_equity_stocks_by_index(index.lower())


# ---------------------------------------------------------------- fundamentals
def moneycontrol(symbol: str, statement: str = "ratios", consolidated: bool = True) -> pd.DataFrame:
    """Moneycontrol statements. statement = overview | income | balance_sheet | cash_flow | ratios."""
    get("bharat_sm_data")
    from Fundamentals import MoneyControl

    mc = MoneyControl()
    sc_id, _ = mc.get_ticker(symbol)
    kind = "consolidated" if consolidated else "standalone"
    fn = {"overview": mc.get_overview_mini_statement, "income": mc.get_income_mini_statement,
          "balance_sheet": mc.get_balance_sheet_mini_statement, "cash_flow": mc.get_cash_flow_mini_statement,
          "ratios": mc.get_ratios_mini_statement}[statement]
    return fn(sc_id, statement_type=kind)


def tickertape(symbol: str, what: str = "scorecard"):
    """Tickertape data. what = scorecard | income | balance_sheet | cash_flow | peers."""
    get("bharat_sm_data")
    from Fundamentals import Tickertape

    tt = Tickertape()
    ticker, _ = tt.get_ticker(symbol)
    tid = ticker.get("sid") if isinstance(ticker, dict) else ticker
    return {"scorecard": tt.get_score_card, "income": tt.get_income_data,
            "balance_sheet": tt.get_balance_sheet_data, "cash_flow": tt.get_cash_flow_data,
            "peers": tt.peers_comparison}[what](tid)


def screener(symbol: str, table: str = "ratios", username: Optional[str] = None,
             password: Optional[str] = None, consolidated: bool = True):
    """Screener.in data.

    With a free Screener.in login (args or SCREENER_USER / SCREENER_PASS env vars) this uses
    Bharat-sm-data and returns a DataFrame for table = quarterly | profit_loss | balance_sheet |
    cash_flow | ratios | shareholding | peers. Without a login it falls back to openscreener
    (needs Playwright) and returns its Stock object.
    """
    user = username or os.environ.get("SCREENER_USER")
    pw = password or os.environ.get("SCREENER_PASS")
    if user and pw and is_installed("bharat_sm_data"):
        from Fundamentals import Screener

        s = Screener(user, pw)
        url = f"/company/{symbol.upper()}/" + ("consolidated/" if consolidated else "")
        return {"quarterly": s.get_quarterly_results, "profit_loss": s.get_profit_and_loss,
                "balance_sheet": s.get_balance_sheet, "cash_flow": s.get_cash_flow, "ratios": s.get_ratios,
                "shareholding": s.get_shareholding_pattern, "peers": s.get_peers_comparison}[table](url)
    return get("openscreener").Stock(symbol.upper(), consolidated=consolidated)


# ---------------------------------------------------------------- mutual funds
def mf_nav(scheme_code: str, history_nav: bool = False):
    """Mutual fund NAV. Order: mftool -> mfapi.in (free REST, no install)."""
    if is_installed("mftool"):
        try:
            mf = get("mftool").Mftool()
            return (mf.get_scheme_historical_nav(str(scheme_code), as_Dataframe=True)
                    if history_nav else mf.get_scheme_quote(str(scheme_code)))
        except Exception:
            pass
    url = f"https://api.mfapi.in/mf/{scheme_code}" + ("" if history_nav else "/latest")
    r = requests.get(url, timeout=_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if history_nav:
        df = pd.DataFrame(j["data"])
        df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
        df["nav"] = df["nav"].astype(float)
        return df.set_index("date").sort_index()
    return {**j.get("meta", {}), **(j.get("data") or [{}])[0]}


def mf_search(name: str) -> dict:
    """Find AMFI scheme codes by (part of) the fund name. Order: mftool -> mfapi.in."""
    n = name.lower()
    if is_installed("mftool"):
        try:
            codes = get("mftool").Mftool().get_scheme_codes()
            return {c: v for c, v in codes.items() if n in str(v).lower()}
        except Exception:
            pass
    r = requests.get("https://api.mfapi.in/mf/search", params={"q": name}, timeout=_TIMEOUT)
    r.raise_for_status()
    return {str(x["schemeCode"]): x["schemeName"] for x in r.json()}
