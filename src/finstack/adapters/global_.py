"""Global pipelines: quotes, daily prices, company financials (US SEC first), other countries' exchanges."""
from __future__ import annotations

import datetime as dt
import sys
from typing import Dict, List

import pandas as pd

from ..core import cache, calendar, config, validate
from ..core.router import Pipeline, Req, Source, register
from ..loader import get
from ._common import YAHOO_HOSTS, bars_since, chunks, concat, env, keep, need, to_records, tv_bars, yf_history, \
    yq_history
from .india_company import RESULT_COLS, period_end, periods_from_wide


def _t(req) -> str:
    return req.inst.yahoo or req.inst.query


def _us(req) -> bool:
    t = _t(req)
    return "." not in t and "=" not in t and "^" not in t and ":" not in t


def _final(req):
    return calendar.global_final_through(_t(req))


# =============================================================== daily prices
def _g_yahoo(req):
    return yf_history(_t(req), req.start, req.end)


def _g_yq(req):
    return yq_history(_t(req), req.start, req.end)


def _g_tiingo(req):
    from tiingo import TiingoClient

    c = TiingoClient({"api_key": env("TIINGO_API_KEY"), "session": True})
    return c.get_dataframe(_t(req), startDate=req.start.isoformat(), endDate=req.end.isoformat(), frequency="daily")


def _g_polygon(req):
    from polygon import RESTClient

    aggs = RESTClient(env("POLYGON_API_KEY")).get_aggs(_t(req), 1, "day", req.start.isoformat(), req.end.isoformat(),
                                                       adjusted=False, limit=50000)
    return pd.DataFrame([{"date": pd.to_datetime(a.timestamp, unit="ms", utc=True).tz_convert("America/New_York"),
                          "open": a.open, "high": a.high, "low": a.low, "close": a.close, "volume": a.volume,
                          "vwap": a.vwap} for a in aggs])


def _g_twelvedata(req):
    from twelvedata import TDClient

    ts = TDClient(apikey=env("TWELVEDATA_API_KEY")).time_series(
        symbol=_t(req), interval="1day", start_date=req.start.isoformat(), end_date=req.end.isoformat(),
        outputsize=5000)
    return ts.as_pandas()


def _g_av(req):
    from alpha_vantage.timeseries import TimeSeries

    df, _ = TimeSeries(key=env("ALPHAVANTAGE_API_KEY"), output_format="pandas").get_daily(_t(req), outputsize="full")
    df.columns = [c.split(". ", 1)[-1] for c in df.columns]
    return df


def _g_eodhd(req):
    from eodhd import APIClient

    t = _t(req) if "." in _t(req) else f"{_t(req)}.US"
    rows = APIClient(env("EODHD_API_KEY")).get_eod_historical_stock_market_data(
        symbol=t, period="d", from_date=req.start.isoformat(), to_date=req.end.isoformat(), order="a")
    return pd.DataFrame(rows).rename(columns={"adjusted_close": "adj_close"})


def _g_stooq(req):
    s = _t(req).lower()
    if _us(req):
        s += ".us"
    return get("pandas_datareader").DataReader(s, "stooq", req.start, req.end)


def _g_fdr(req):
    return get("finance_datareader").DataReader(_t(req), req.start.isoformat(), req.end.isoformat())


def _g_tv(req):
    exch, sym = req.inst.query.split(":", 1)
    return tv_bars(sym, exch, "1d", bars_since(req.start))


def _g_alpaca(req):
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    c = StockHistoricalDataClient(env("ALPACA_API_KEY"), env("ALPACA_SECRET_KEY"))
    bars = c.get_stock_bars(StockBarsRequest(symbol_or_symbols=_t(req), timeframe=TimeFrame.Day,
                                             start=dt.datetime.combine(req.start, dt.time()),
                                             end=dt.datetime.combine(req.end, dt.time(23, 59)), adjustment="raw"))
    df = bars.df.reset_index()
    return df.rename(columns={"timestamp": "date"}).drop(columns=["symbol"], errors="ignore")


def _g_openbb(req):
    from openbb import obb

    return obb.equity.price.historical(_t(req), start_date=req.start.isoformat(), end_date=req.end.isoformat(),
                                       provider="yfinance").to_df()


def _post_global(df, req):
    return keep(df, ("date", "open", "high", "low", "close", "adj_close", "volume", "vwap")).assign(symbol=_t(req))


def _bulk_global(p: Pipeline, reqs: List[Req]) -> Dict[str, str]:
    """Yahoo serves up to ~50 tickers in one request: download in groups instead of one by one."""
    from ..loader import is_installed

    if not is_installed("yfinance"):
        return {}
    yf = get("yfinance")
    todo = []
    for r in reqs:
        r.end = r.end or dt.date.today()
        r.start = r.start or r.end - dt.timedelta(days=p.default_days)
        if cache.missing_ranges(p.name, r.inst.key, r.start, r.end):
            todo.append(r)
    if len(todo) < 5:
        return {}
    lo, hi = min(r.start for r in todo), max(r.end for r in todo)
    done = 0
    for i in range(0, len(todo), 50):
        group = todo[i:i + 50]
        tickers = [_t(r) for r in group]
        data = yf.download(tickers, start=lo.isoformat(), end=(hi + dt.timedelta(days=1)).isoformat(),
                           group_by="ticker", auto_adjust=False, progress=False, threads=False)
        for r in group:
            try:
                sub = data[_t(r)] if isinstance(data.columns, pd.MultiIndex) else data
                sub = sub.dropna(how="all")
                if sub.empty:
                    continue
                from ..core.schema import normalize, order

                df = _post_global(normalize(sub, required=("date", "close")), r).assign(source="yfinance:batch")
                good, _, _ = validate.ohlcv(df)
                cache.write_series(p.name, r.inst.key, good, (lo, hi), _final(r), "date")
                done += 1
            except Exception:  # noqa: BLE001 - that ticker is fetched alone afterwards
                continue
        print(f"[finstack] bulk: Yahoo batch {min(i + 50, len(todo))}/{len(todo)}", file=sys.stderr)
    return {"route": f"Yahoo multi-ticker download covered {done} of {len(todo)} tickers"}


register(Pipeline(
    "global_daily_prices", "Global stocks, ETFs, indices: daily OHLCV (close raw, adj_close where the source has it)",
    "series", [
        Source("tiingo", _g_tiingo, ("api.tiingo.com",), when=_us),
        Source("yfinance", _g_yahoo, YAHOO_HOSTS, when=lambda r: ":" not in r.inst.query),
        Source("eodhd", _g_eodhd, ("eodhd.com",)),
        Source("polygon", _g_polygon, ("api.polygon.io",), when=_us),
        Source("alpaca", _g_alpaca, ("data.alpaca.markets",), when=_us, score=3.6),
        Source("twelvedata", _g_twelvedata, ("api.twelvedata.com",)),
        Source("alpha_vantage", _g_av, ("www.alphavantage.co",)),
        Source("yahooquery", _g_yq, YAHOO_HOSTS, when=lambda r: ":" not in r.inst.query),
        Source("pandas_datareader", _g_stooq, ("stooq.com",), when=lambda r: ":" not in r.inst.query),
        Source("finance_datareader", _g_fdr, (), when=lambda r: ":" not in r.inst.query),
        Source("tvdatafeed", _g_tv, ("data.tradingview.com",), when=lambda r: ":" in r.inst.query),
        Source("openbb", _g_openbb, YAHOO_HOSTS, score=1.0),
    ],
    market="GLOBAL", check=validate.ohlcv, final=_final, post=_post_global, bulk=_bulk_global,
    columns=("date", "open", "high", "low", "close", "adj_close", "volume", "vwap", "symbol"),
    params_doc="symbol (Yahoo style: AAPL, VOD.L, 7203.T, ^GSPC, EURUSD=X; or 'NASDAQ:AAPL' for TradingView), "
               "start, end",
    example='fs.fetch("global_daily_prices", "AAPL", start="10y")'))


# =============================================================== live quotes
def _row(**kw):
    return pd.DataFrame([kw])


def _l_yahoo(req):
    fi = get("yfinance").Ticker(_t(req)).fast_info
    last, prev = fi["last_price"], fi["previous_close"]
    return _row(symbol=_t(req), last=last, prev_close=prev, open=fi["open"], high=fi["day_high"],
                low=fi["day_low"], volume=fi["last_volume"], currency=fi.get("currency"),
                change=last - prev if last and prev else None, pct_change=(last / prev - 1) * 100 if prev else None)


def _l_yq(req):
    p = get("yahooquery").Ticker(_t(req)).price[_t(req)]
    need(isinstance(p, dict), str(p)[:200])
    return _row(symbol=_t(req), last=p.get("regularMarketPrice"), prev_close=p.get("regularMarketPreviousClose"),
                open=p.get("regularMarketOpen"), high=p.get("regularMarketDayHigh"), low=p.get("regularMarketDayLow"),
                volume=p.get("regularMarketVolume"), change=p.get("regularMarketChange"),
                pct_change=(p.get("regularMarketChangePercent") or 0) * 100, currency=p.get("currency"),
                ts=p.get("regularMarketTime"))


def _l_finnhub(req):
    q = get("finnhub").Client(api_key=env("FINNHUB_API_KEY")).quote(_t(req))
    need(q.get("c"), f"Finnhub: {q}")
    return _row(symbol=_t(req), last=q["c"], change=q.get("d"), pct_change=q.get("dp"), open=q.get("o"),
                high=q.get("h"), low=q.get("l"), prev_close=q.get("pc"), ts=q.get("t"))


def _l_twelvedata(req):
    from twelvedata import TDClient

    q = TDClient(apikey=env("TWELVEDATA_API_KEY")).quote(symbol=_t(req)).as_json()
    need(q.get("close"), f"Twelve Data: {q}")
    return _row(symbol=_t(req), last=q.get("close"), open=q.get("open"), high=q.get("high"), low=q.get("low"),
                prev_close=q.get("previous_close"), change=q.get("change"), pct_change=q.get("percent_change"),
                volume=q.get("volume"), ts=q.get("timestamp"), currency=q.get("currency"))


def _l_alpaca(req):
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockSnapshotRequest

    c = StockHistoricalDataClient(env("ALPACA_API_KEY"), env("ALPACA_SECRET_KEY"))
    s = c.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=_t(req)))[_t(req)]
    d, pdb = s.daily_bar, s.previous_daily_bar
    last = s.latest_trade.price
    return _row(symbol=_t(req), last=last, open=d.open, high=d.high, low=d.low, volume=d.volume,
                prev_close=pdb.close, change=last - pdb.close, pct_change=(last / pdb.close - 1) * 100,
                bid=s.latest_quote.bid_price, ask=s.latest_quote.ask_price, ts=s.latest_trade.timestamp)


register(Pipeline(
    "global_live_quotes", "Global stocks: latest quote", "snapshot", [
        Source("alpaca", _l_alpaca, ("data.alpaca.markets",), when=_us),
        Source("finnhub", _l_finnhub, ("finnhub.io",), when=_us),
        Source("twelvedata", _l_twelvedata, ("api.twelvedata.com",)),
        Source("yfinance", _l_yahoo, YAHOO_HOSTS),
        Source("yahooquery", _l_yq, YAHOO_HOSTS),
    ],
    market="GLOBAL", required=("last",), time_col="ts", check=validate.positive("last"), ttl=15,
    columns=("symbol", "last", "change", "pct_change", "open", "high", "low", "prev_close", "volume", "bid", "ask",
             "currency", "ts"),
    params_doc="symbol", example='fs.fetch("global_live_quotes", "MSFT")'))


# =============================================================== company financials (SEC first)
SEC_HOSTS = ("data.sec.gov", "www.sec.gov")
_CONCEPTS = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
                "RevenueFromContractWithCustomerIncludingAssessedTax"],
    "operating_profit": ["OperatingIncomeLoss"],
    "profit_before_tax": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
                          "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
    "tax": ["IncomeTaxExpenseBenefit"],
    "net_profit": ["NetIncomeLoss", "ProfitLoss"],
    "eps": ["EarningsPerShareBasic"],
}


def _sec_get(url):
    import requests

    ident = env("EDGAR_IDENTITY")
    r = requests.get(url, timeout=30, headers={"User-Agent": ident, "Accept-Encoding": "gzip, deflate"})
    r.raise_for_status()
    return r.json()


def sec_cik(ticker: str) -> str:
    table = cache.get_snapshot("reference", "sec_tickers", ttl=7 * 86400)
    if not isinstance(table, pd.DataFrame):
        j = _sec_get("https://www.sec.gov/files/company_tickers.json")
        table = pd.DataFrame(list(j.values()))
        cache.put_snapshot("reference", "sec_tickers", table)
    hit = table[table["ticker"].str.upper() == ticker.upper().replace(".", "-")]
    need(len(hit), f"{ticker} not in SEC's ticker list")
    return f"{int(hit.iloc[0]['cik_str']):010d}"


def sec_periods(facts: dict, quarterly: bool) -> pd.DataFrame:
    """Pick quarter (80-100 day) or year (350-380 day) values per concept; derive Q4 = year - Q1..Q3."""
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    out: Dict[pd.Timestamp, dict] = {}
    for field, names in _CONCEPTS.items():
        for name in names:
            units = (gaap.get(name) or {}).get("units") or {}
            vals = units.get("USD") or units.get("USD/shares") or []
            if not vals:
                continue
            q, y = {}, {}
            for v in vals:
                if "start" not in v:
                    continue
                days = (pd.Timestamp(v["end"]) - pd.Timestamp(v["start"])).days
                end = pd.Timestamp(v["end"])
                if 80 <= days <= 100:
                    q[end] = (v["val"], v["start"], v.get("filed", ""))
                elif 350 <= days <= 380:
                    y[end] = (v["val"], v["start"], v.get("filed", ""))
            if quarterly:
                for yend, (yval, ystart, _) in y.items():
                    if yend in q or field == "eps":
                        continue
                    inside = [k for k in q if pd.Timestamp(ystart) < k < yend]
                    if len(inside) == 3:
                        q[yend] = (yval - sum(q[k][0] for k in inside), (inside[-1].date() + dt.timedelta(days=1)).isoformat(),
                                   "derived")
                pick = q
            else:
                pick = y
            if not pick:
                continue
            scale = 1.0 if field == "eps" else 1e-6
            for end, (val, start, filed) in pick.items():
                rec = out.setdefault(end, {"period_end": end, "period_start": pd.Timestamp(start)})
                rec.setdefault(field, val * scale)
                if filed == "derived":
                    rec["note"] = "Q4 derived as full year minus Q1-Q3"
            break
    df = pd.DataFrame(list(out.values()))
    need(len(df), "no income-statement facts found")
    return df.sort_values("period_end", ascending=False).reset_index(drop=True)


def _st(req):
    return req.p("statement", "quarterly")


def _c_sec(req):
    need(_st(req) in ("quarterly", "annual"), "SEC company-facts route covers income statements")
    facts = _sec_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{sec_cik(_t(req))}.json")
    return sec_periods(facts, _st(req) == "quarterly").assign(basis="SEC XBRL company facts (as filed)")


def _c_edgar(req):
    ed = get("edgar")
    ed.set_identity(env("EDGAR_IDENTITY"))
    c = ed.Company(_t(req))
    st = _st(req)
    period = "quarterly" if st == "quarterly" else "annual"
    fn = {"quarterly": c.income_statement, "annual": c.income_statement, "balance_sheet": c.balance_sheet,
          "cash_flow": c.cash_flow}[st]
    wide = fn(periods=8, period=period, as_dataframe=True)
    return periods_from_wide(wide, 1e-6, map_items=st in ("quarterly", "annual"))


def _c_yahoo(req):
    t = get("yfinance").Ticker(_t(req))
    st = _st(req)
    wide = {"quarterly": t.quarterly_income_stmt, "annual": t.income_stmt, "balance_sheet": t.balance_sheet,
            "cash_flow": t.cashflow}[st]
    need(wide is not None and not wide.empty, "Yahoo returned nothing")
    return periods_from_wide(wide, 1e-6, map_items=st in ("quarterly", "annual"))


_YQ = {"asOfDate": "period_end", "TotalRevenue": "revenue", "OperatingIncome": "operating_profit",
       "PretaxIncome": "profit_before_tax", "TaxProvision": "tax", "NetIncome": "net_profit", "BasicEPS": "eps"}


def _c_yq(req):
    need(_st(req) in ("quarterly", "annual"), "yahooquery route covers income statements")
    df = get("yahooquery").Ticker(_t(req)).income_statement(frequency="q" if _st(req) == "quarterly" else "a")
    need(isinstance(df, pd.DataFrame) and len(df), str(df)[:200])
    df = df[df.get("periodType", "") != "TTM"].rename(columns=_YQ)
    for c in ("revenue", "operating_profit", "profit_before_tax", "tax", "net_profit"):
        if c in df:
            df[c] = df[c] * 1e-6
    return keep(df, RESULT_COLS)


def _c_av(req):
    from alpha_vantage.fundamentaldata import FundamentalData

    fd = FundamentalData(key=env("ALPHAVANTAGE_API_KEY"), output_format="pandas")
    df, _ = (fd.get_income_statement_quarterly if _st(req) == "quarterly" else fd.get_income_statement_annual)(_t(req))
    m = {"fiscalDateEnding": "period_end", "totalRevenue": "revenue", "operatingIncome": "operating_profit",
         "incomeBeforeTax": "profit_before_tax", "incomeTaxExpense": "tax", "netIncome": "net_profit"}
    df = df.rename(columns=m)
    for c in ("revenue", "operating_profit", "profit_before_tax", "tax", "net_profit"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce") * 1e-6
    return keep(df, RESULT_COLS)


def _post_gfin(df, req):
    df = df.copy()
    df["period_end"] = pd.to_datetime(df["period_end"])
    if _st(req) in ("quarterly", "annual"):
        need({"revenue", "net_profit"} & set(df.columns), "no revenue or profit lines recognised")
    return df.assign(units="USD million (EPS in USD)" if _us(req) else "millions, reporting currency",
                     symbol=_t(req)).sort_values("period_end", ascending=False)


register(Pipeline(
    "global_company_financials", "US and global company results and statements (millions)", "snapshot", [
        Source("builtin:sec", _c_sec, SEC_HOSTS, creds=("EDGAR_IDENTITY",), when=_us, score=4.8),
        Source("edgar", _c_edgar, SEC_HOSTS, when=_us),
        Source("alpha_vantage", _c_av, ("www.alphavantage.co",), when=_us),
        Source("yfinance", _c_yahoo, YAHOO_HOSTS),
        Source("yahooquery", _c_yq, YAHOO_HOSTS),
    ],
    market="GLOBAL", required=("period_end",), time_col="_none", normalize=False, check=validate.results,
    ttl=12 * 3600, post=_post_gfin, columns=RESULT_COLS,
    params_doc="symbol, statement=quarterly|annual|balance_sheet|cash_flow (SEC needs EDGAR_IDENTITY='Name email')",
    example='fs.fetch("global_company_financials", "AAPL", statement="quarterly")'))


# =============================================================== other countries' exchanges
_CN = {"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume",
       "成交额": "value"}
_KR = {"날짜": "date", "시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume",
       "거래대금": "value"}
_YAHOO_SUFFIX = {"HK": ".HK", "KR": ".KS", "TW": ".TW", "JP": ".T", "RU": ".ME"}


def _mkt(req) -> tuple:
    s = req.symbol.strip()
    if ":" in s:
        m, code = s.split(":", 1)
        return m.upper(), code.strip()
    return str(req.p("market", "CN")).upper(), s


def _cn_yahoo(code: str) -> str:
    return code + (".SS" if code.startswith(("6", "9", "5")) else ".SZ")


def _w_akshare(req):
    m, code = _mkt(req)
    ak = get("akshare")
    a, b = req.start.strftime("%Y%m%d"), req.end.strftime("%Y%m%d")
    if m == "CN":
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=a, end_date=b, adjust="")
    elif m == "HK":
        df = ak.stock_hk_hist(symbol=code, period="daily", start_date=a, end_date=b, adjust="")
    else:
        raise LookupError("akshare route: CN and HK")
    return df.rename(columns=_CN)


def _w_efinance(req):
    m, code = _mkt(req)
    need(m in ("CN", "HK", "US"), "efinance route: CN, HK, US")
    df = get("efinance").stock.get_quote_history(code, beg=req.start.strftime("%Y%m%d"), end=req.end.strftime("%Y%m%d"))
    return df.rename(columns=_CN)


def _w_baostock(req):
    m, code = _mkt(req)
    need(m == "CN", "baostock route: CN")
    bs = get("baostock")
    bs.login()
    try:
        rs = bs.query_history_k_data_plus(("sh." if code.startswith(("6", "9", "5")) else "sz.") + code,
                                          "date,open,high,low,close,volume,amount", start_date=req.start.isoformat(),
                                          end_date=req.end.isoformat(), frequency="d", adjustflag="3")
        df = rs.get_data()
    finally:
        bs.logout()
    return df.rename(columns={"amount": "value"})


def _w_tushare(req):
    m, code = _mkt(req)
    need(m == "CN", "tushare route: CN")
    pro = get("tushare").pro_api(env("TUSHARE_TOKEN"))
    df = pro.daily(ts_code=_cn_yahoo(code).replace(".SS", ".SH"), start_date=req.start.strftime("%Y%m%d"),
                   end_date=req.end.strftime("%Y%m%d"))
    return df.rename(columns={"trade_date": "date", "vol": "volume", "amount": "value"})


def _w_pykrx(req):
    m, code = _mkt(req)
    need(m == "KR", "pykrx route: KR")
    df = get("pykrx").stock.get_market_ohlcv(req.start.strftime("%Y%m%d"), req.end.strftime("%Y%m%d"), code)
    return df.reset_index().rename(columns=_KR)


def _w_fdr(req):
    m, code = _mkt(req)
    need(m in ("KR", "US", "JP"), "FinanceDataReader route: KR, US, JP")
    return get("finance_datareader").DataReader(code, req.start.isoformat(), req.end.isoformat())


def _w_twstock(req):
    m, code = _mkt(req)
    need(m == "TW", "twstock route: TW")
    s = get("twstock").Stock(code, initial_fetch=False)
    rows = s.fetch_from(req.start.year, req.start.month)
    return pd.DataFrame([{"date": r.date, "open": r.open, "high": r.high, "low": r.low, "close": r.close,
                          "volume": r.capacity, "value": r.turnover, "trades": r.transaction} for r in rows])


def _w_apimoex(req):
    import requests

    m, code = _mkt(req)
    need(m == "RU", "apimoex route: RU")
    with requests.Session() as s:
        rows = get("apimoex").get_board_history(s, code, start=req.start.isoformat(), end=req.end.isoformat(),
                                                columns=("TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME",
                                                         "VALUE"))
    return pd.DataFrame(rows).rename(columns={"TRADEDATE": "date"})


def _w_yahoo(req):
    m, code = _mkt(req)
    t = _cn_yahoo(code) if m == "CN" else code + _YAHOO_SUFFIX.get(m, "")
    return yf_history(t, req.start, req.end)


register(Pipeline(
    "world_markets", "Other countries' exchanges (China, Hong Kong, Korea, Taiwan, Japan, Russia): daily OHLCV",
    "series", [
        Source("akshare", _w_akshare, ("push2his.eastmoney.com",)),
        Source("tushare", _w_tushare, ("api.tushare.pro",)),
        Source("baostock", _w_baostock, ()),
        Source("efinance", _w_efinance, ("push2his.eastmoney.com",)),
        Source("pykrx", _w_pykrx, ("data.krx.co.kr",)),
        Source("apimoex", _w_apimoex, ("iss.moex.com",)),
        Source("twstock", _w_twstock, ("www.twse.com.tw",)),
        Source("finance_datareader", _w_fdr, ()),
        Source("yfinance", _w_yahoo, YAHOO_HOSTS, score=3.0),
    ],
    market="raw", check=validate.ohlcv, post=lambda df, r: keep(df, ("date", "open", "high", "low", "close",
                                                                      "volume", "value", "trades")).assign(
        symbol=r.symbol), entity=lambda r: f"W:{':'.join(_mkt(r))}",
    final=lambda r: dt.date.today() - dt.timedelta(days=1),
    columns=("date", "open", "high", "low", "close", "volume", "value", "trades", "symbol"),
    params_doc="symbol as 'MARKET:CODE' (CN:600519, HK:00700, KR:005930, TW:2330, JP:7203, RU:SBER), start, end",
    example='fs.fetch("world_markets", "CN:600519", start="3y")'))
