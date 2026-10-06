"""India price pipelines: daily prices, live quotes, intraday candles, bhavcopy files, indices, breadth."""
from __future__ import annotations

import datetime as dt
import io
import os
import sys
from typing import Dict, List

import pandas as pd

from ..core import cache, calendar, schema, validate
from ..core.router import Pipeline, Req, Source, register, snapshot_key
from ..core.symbols import INDICES, resolve
from ..loader import get
from . import brokers as B
from ._common import (BSE_HOSTS, NSE_ARCHIVE_HOSTS, NSE_HOSTS, TMP, TV_HOSTS, YAHOO_HOSTS, bars_since, bse,
                      chunks, concat, dtm, eq_series, india_final, india_has_data, india_ttl, keep, need, nse,
                      to_records, tv_bars, yf_history, yq_history)

IST = "Asia/Kolkata"


def _equity(req: Req) -> bool:
    return req.inst is not None and req.inst.kind == "equity"


def _index(req: Req) -> bool:
    return req.inst is not None and req.inst.kind == "index"


def _has_nse(req: Req) -> bool:
    return _equity(req) and bool(req.inst.nse)


def _tv_split(inst) -> tuple:
    exch, sym = inst.tv.split(":", 1)
    return sym, exch


# =============================================================== daily prices
def _d_nse(req: Req):
    return nse().fetch_equity_historical_data(req.inst.nse, req.start, req.end, series="eq")


def _d_jugaad(req: Req):
    from jugaad_data.nse import stock_df

    return stock_df(symbol=req.inst.nse, from_date=req.start, to_date=req.end, series="EQ")


def _d_aynse(req: Req):
    return get("aynse").stock_df(req.inst.nse, req.start, req.end)


def _d_nselib(req: Req):
    cm = get("nselib", "capital_market")
    return cm.price_volume_and_deliverable_position_data(
        symbol=req.inst.nse, from_date=req.start.strftime("%d-%m-%Y"), to_date=req.end.strftime("%d-%m-%Y"))


def _d_nsefin(req: Req):
    return get("nsefin").NSEClient().get_equity_historical_data(
        req.inst.nse, req.start.strftime("%d-%m-%Y"), req.end.strftime("%d-%m-%Y"))


def _d_ism(req: Req):
    return get("indian_stock_market").NSE().get_ohlc_data(req.inst.nse, timeframe="1Day", is_index=False,
                                                          start_date=dtm(req.start),
                                                          end_date=dtm(req.end + dt.timedelta(days=1)))


def _d_yahoo(req: Req):
    need(req.inst.yahoo, "no Yahoo ticker")
    return yf_history(req.inst.yahoo, req.start, req.end)


def _d_yq(req: Req):
    return yq_history(req.inst.yahoo, req.start, req.end)


def _d_tv(req: Req):
    sym, exch = _tv_split(req.inst)
    return tv_bars(sym, exch, "1d", bars_since(req.start))


def _d_upstox(req):
    return B.upstox_daily(req)


def _d_kite(req):
    return B.kite_history(req, "day", 1900)


def _d_dhan(req):
    return B.dhan_daily(req)


def _d_angel(req):
    return B.angel_candles(req, "ONE_DAY", 1900).rename(columns={"ts": "date"})


def _d_fyers(req):
    return B.fyers_history(req, "D", 365).rename(columns={"ts": "date"})


def _post_daily(df: pd.DataFrame, req: Req) -> pd.DataFrame:
    df = eq_series(df)
    df = keep(df, ("date", "open", "high", "low", "close", "volume", "value", "trades", "vwap", "prev_close",
                   "delivery_qty", "delivery_pct", "isin"))
    return df.assign(symbol=req.inst.nse or req.inst.query)


def _adjust(df: pd.DataFrame, req: Req) -> pd.DataFrame:
    """adjust=True: add split/bonus-adjusted adj_close from NSE/BSE corporate actions."""
    if not req.p("adjust"):
        return df
    from ..core.router import fetch

    try:
        acts = fetch("india_corporate_events", req.symbol, what="actions",
                     start=df["date"].min().date(), end=dt.date.today())
        out = validate.adjust_for_actions(df, acts, ex_col="ex_date", purpose_col="purpose")
        out.attrs = dict(df.attrs)
        n = int((acts["purpose"].map(validate.action_factor).notna()).sum()) if len(acts) else 0
        out.attrs.setdefault("issues", []).append(f"adj_close adjusted for {n} split/bonus action(s)")
        return out
    except Exception as e:  # noqa: BLE001
        df.attrs.setdefault("issues", []).append(f"could not adjust for corporate actions: {e}"[:200])
        return df


def _bulk_daily(p: Pipeline, reqs: List[Req]) -> Dict[str, str]:
    """Many symbols: one NSE bhavcopy per trading day can be cheaper than per-symbol history calls.
    Cost model: bhavcopy = D downloads at ~1/s; per symbol = S x ceil(D/100) calls at ~3/s."""
    from ..core.router import fetch

    final = calendar.india_final_through()
    by_ent = {}
    for r in reqs:
        if r.inst is None or r.inst.kind != "equity" or not r.inst.nse:
            continue
        r.end = min(r.end or final, final)
        r.start = r.start or (r.end - dt.timedelta(days=p.default_days))
        miss = cache.missing_ranges(p.name, r.inst.key, r.start, r.end)
        if miss:
            by_ent[r.inst.key] = (r, miss)
    if len(by_ent) < 2:
        return {}
    lo = min(a for _, m in by_ent.values() for a, _ in m)
    hi = max(b for _, m in by_ent.values() for _, b in m)
    days = calendar.trading_days(lo, hi)
    per_symbol_calls = sum(sum(-(-len(calendar.trading_days(a, b)) // 70) for a, b in m) for _, m in by_ent.values())
    if not days or len(days) / 1.0 >= per_symbol_calls / 3.0:
        return {"route": f"per-symbol history ({per_symbol_calls} calls) is cheaper than {len(days)} bhavcopies"}
    print(f"[finstack] bulk: {len(days)} daily bhavcopies for {len(by_ent)} symbols "
          f"(instead of ~{per_symbol_calls} history calls)", file=sys.stderr)
    wanted = {r.inst.nse.upper(): k for k, (r, _) in by_ent.items()}
    rows = {k: [] for k in by_ent}
    got_days = []
    for i, d in enumerate(days, 1):
        try:
            f = fetch("india_eod_files", start=d, end=d, segment="equity")
        except Exception:  # noqa: BLE001 - that day is fetched per symbol later
            continue
        got_days.append(d)
        f = eq_series(f)
        f = f[f["symbol"].astype(str).str.upper().isin(wanted)]
        for sym, g in f.groupby(f["symbol"].astype(str).str.upper()):
            rows[wanted[sym]].append(g)
        if i % 25 == 0:
            print(f"[finstack] bulk: {i}/{len(days)} bhavcopies", file=sys.stderr)
    if not got_days:
        raise RuntimeError("no bhavcopy could be downloaded")
    # only mark as covered the contiguous run of days we actually have
    first, last = got_days[0], got_days[-1]
    if len(got_days) != len(calendar.trading_days(first, last)):
        missing = sorted(set(calendar.trading_days(first, last)) - set(got_days))
        last = calendar.previous_trading_day(missing[0])
        if last < first:
            return {"route": "bhavcopy (incomplete)"}
    for k, (r, _) in by_ent.items():
        if not rows[k]:
            continue
        g = pd.concat(rows[k], ignore_index=True)
        g = g[(g["date"] >= pd.Timestamp(first)) & (g["date"] <= pd.Timestamp(last))]
        g = _post_daily(g.drop(columns=["source"], errors="ignore"), r).assign(source="nse:bhavcopy")
        good, bad, _ = validate.ohlcv(g)
        cache.write_series(p.name, k, good, (first, last), final, p.time_col, p.keys)
    return {"route": f"bhavcopy {first}..{last}"}


DAILY_SOURCES = [
    Source("upstox", _d_upstox, B.UPSTOX_HOSTS, when=_equity),
    Source("kiteconnect", _d_kite, B.KITE_HOSTS, when=_equity),
    Source("dhanhq", _d_dhan, B.DHAN_HOSTS, when=_equity),
    Source("smartapi", _d_angel, B.ANGEL_HOSTS, when=_equity, libs=("smartapi",)),
    Source("fyers", _d_fyers, B.FYERS_HOSTS, when=_equity),
    Source("nse", _d_nse, NSE_HOSTS, when=_has_nse),
    Source("jugaad_data", _d_jugaad, NSE_HOSTS, when=_has_nse),
    Source("aynse", _d_aynse, NSE_HOSTS, when=_has_nse),
    Source("nselib", _d_nselib, NSE_HOSTS, when=_has_nse),
    Source("yfinance", _d_yahoo, YAHOO_HOSTS, when=_equity),
    Source("tvdatafeed", _d_tv, TV_HOSTS, when=_equity),
    Source("nsefin", _d_nsefin, NSE_HOSTS, when=_has_nse),
    Source("indian_stock_market", _d_ism, ("charting.nseindia.com",), when=_has_nse),
    Source("yahooquery", _d_yq, YAHOO_HOSTS, when=_equity),
]

register(Pipeline(
    "india_daily_prices", "Indian stocks: daily OHLCV (official, unadjusted; adjust=True adds adj_close)",
    "series", DAILY_SOURCES, market="IN", check=validate.ohlcv, final=india_final, has_data=india_has_data,
    post=_post_daily, finish=_adjust, bulk=_bulk_daily, default_days=365,
    params_doc="symbol (NSE symbol, BSE code, ISIN), start, end, adjust=False",
    example='fs.fetch("india_daily_prices", "RELIANCE", start="2015-01-01", adjust=True)'))


# =============================================================== eod files (bhavcopy)
def _seg(req):
    return req.p("segment", "equity")


def _read(path) -> pd.DataFrame:
    return pd.read_csv(path)


def _f_nse(req):
    n, d = nse(), dtm(req.start)
    os.makedirs(TMP, exist_ok=True)
    seg = _seg(req)
    fn = getattr(n, {"equity": "equity_bhavcopy", "fno": "fno_bhavcopy", "delivery": "delivery_bhavcopy",
                     "index": "indices_bhavcopy"}[seg])
    return _read(fn(d, TMP))


def _f_jugaad(req):
    from jugaad_data.nse import bhavcopy_fo_save, bhavcopy_save

    os.makedirs(TMP, exist_ok=True)
    fn = bhavcopy_save if _seg(req) == "equity" else bhavcopy_fo_save
    return _read(fn(req.start, TMP))


def _f_nsefin(req):
    c = get("nsefin").NSEClient()
    return (c.get_equity_bhav_copy if _seg(req) == "equity" else c.get_fno_bhav_copy)(dtm(req.start))


def _f_aynse(req):
    a = get("aynse")
    return (a.bhavcopy_df if _seg(req) == "equity" else a.bhavcopy_fo_df)(req.start)


def _f_bse(req):
    os.makedirs(TMP, exist_ok=True)
    return _read(bse().bhavcopyReport(dtm(req.start), TMP))


def _post_files(df: pd.DataFrame, req: Req) -> pd.DataFrame:
    if "date" not in df.columns or df["date"].isna().all():
        df = df.assign(date=pd.Timestamp(req.start))
    for c in ("symbol", "series", "instrument", "option_type"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df.assign(exchange=req.p("exchange", "NSE"), segment=_seg(req))


def _nse_exch(req):
    return req.p("exchange", "NSE") == "NSE"


def _bse_exch(req):
    return req.p("exchange", "NSE") == "BSE" and _seg(req) == "equity"


register(Pipeline(
    "india_eod_files", "Exchange end-of-day files (bhavcopy) for every security on a day",
    "daily_files", [
        Source("nse", _f_nse, NSE_ARCHIVE_HOSTS, when=_nse_exch),
        Source("jugaad_data", _f_jugaad, NSE_ARCHIVE_HOSTS, when=lambda r: _nse_exch(r) and _seg(r) in ("equity", "fno")),
        Source("nsefin", _f_nsefin, NSE_ARCHIVE_HOSTS, when=lambda r: _nse_exch(r) and _seg(r) in ("equity", "fno")),
        Source("aynse", _f_aynse, NSE_ARCHIVE_HOSTS, when=lambda r: _nse_exch(r) and _seg(r) in ("equity", "fno")),
        Source("bse", _f_bse, ("www.bseindia.com",), when=_bse_exch),
    ],
    market="raw", needs_symbol=False, required=("symbol", "close"), check=validate.ohlcv, final=india_final,
    columns=("date", "exchange", "segment", "symbol", "series", "isin", "instrument", "expiry", "strike",
             "option_type", "open", "high", "low", "close", "last", "prev_close", "settle", "volume", "value",
             "trades", "oi", "oi_change"),
    params_doc="start, end (each trading day), segment=equity|fno|delivery|index, exchange=NSE|BSE",
    example='fs.fetch("india_eod_files", start="2026-09-01", end="2026-09-30", segment="fno")'))


# =============================================================== live quotes
def _q_row(**kw) -> pd.DataFrame:
    return pd.DataFrame([{k: v for k, v in kw.items()}])


def _q_nse(req):
    if _index(req):
        data = nse().list_equity_stocks_by_index(req.inst.nse)
        r = next(x for x in data["data"] if str(x.get("symbol")).upper() == req.inst.nse.upper())
        return _q_row(symbol=req.inst.nse, last=r.get("lastPrice"), change=r.get("change"),
                      pct_change=r.get("pChange"), open=r.get("open"), high=r.get("dayHigh"), low=r.get("dayLow"),
                      prev_close=r.get("previousClose"), volume=r.get("totalTradedVolume"),
                      value=r.get("totalTradedValue"), ts=r.get("lastUpdateTime") or data.get("timestamp"))
    q = nse().quote(req.inst.nse)
    md, ob, ti = q.get("metaData", {}), q.get("orderBook", {}), q.get("tradeInfo", {})
    return _q_row(symbol=req.inst.nse, last=ob.get("lastPrice") or ti.get("lastPrice"), change=md.get("change"),
                  pct_change=md.get("pChange"), open=md.get("open"), high=md.get("dayHigh"), low=md.get("dayLow"),
                  prev_close=md.get("previousClose"), vwap=md.get("averagePrice"),
                  volume=ti.get("totalTradedVolume"), value=ti.get("totalTradedValue"),
                  bid=ob.get("buyPrice1"), ask=ob.get("sellPrice1"), isin=md.get("isinCode"))


def _q_nsepython(req):
    q = get("nsepython").nse_eq(req.inst.nse)
    pi = q.get("priceInfo", {})
    hl = pi.get("intraDayHighLow", {})
    return _q_row(symbol=req.inst.nse, last=pi.get("lastPrice"), change=pi.get("change"),
                  pct_change=pi.get("pChange"), open=pi.get("open"), high=hl.get("max"), low=hl.get("min"),
                  prev_close=pi.get("previousClose"), vwap=pi.get("vwap"),
                  ts=(q.get("metadata") or {}).get("lastUpdateTime"))


def _q_nsetools(req):
    q = get("nsetools").Nse().get_quote(req.inst.nse)
    return _q_row(symbol=req.inst.nse, last=q.get("lastPrice"), change=q.get("change"),
                  pct_change=q.get("pChange"), open=q.get("open"), high=q.get("dayHigh") or q.get("intraDayHighLow", {}).get("max"),
                  low=q.get("dayLow"), prev_close=q.get("previousClose"))


def _q_bse(req):
    need(req.inst.bse, "BSE code unknown")
    q = bse().quote(req.inst.bse)
    return _q_row(symbol=req.inst.nse or req.inst.bse, last=q["LTP"], open=q["Open"], high=q["High"],
                  low=q["Low"], prev_close=q["PrevClose"], change=q["LTP"] - q["PrevClose"],
                  pct_change=(q["LTP"] / q["PrevClose"] - 1) * 100 if q["PrevClose"] else None, exchange="BSE")


def _q_bsedata(req):
    need(req.inst.bse, "BSE code unknown")
    q = get("bsedata", "bse").BSE().getQuote(req.inst.bse)
    return _q_row(symbol=req.inst.nse or req.inst.bse, last=q.get("currentValue"), change=q.get("change"),
                  pct_change=q.get("pChange"), open=q.get("previousOpen"), high=q.get("dayHigh"),
                  low=q.get("dayLow"), prev_close=q.get("previousClose"), exchange="BSE",
                  ts=q.get("updatedOn"))


def _q_yahoo(req):
    fi = get("yfinance").Ticker(req.inst.yahoo).fast_info
    last, prev = fi["last_price"], fi["previous_close"]
    return _q_row(symbol=req.inst.nse or req.inst.query, last=last, prev_close=prev, open=fi["open"],
                  high=fi["day_high"], low=fi["day_low"], volume=fi["last_volume"], change=last - prev,
                  pct_change=(last / prev - 1) * 100 if prev else None)


def _q_upstox(req):
    key = B.upstox_key(req.inst)
    data = B.upstox_quotes([key])
    need(data, "Upstox returned no quote")
    q = next(iter(data.values()))
    live, prev = q.get("live_ohlc") or {}, q.get("prev_ohlc") or {}
    last, pc = q.get("last_price"), prev.get("close")
    return _q_row(symbol=req.inst.nse, last=last, open=live.get("open"), high=live.get("high"), low=live.get("low"),
                  prev_close=pc, volume=live.get("volume"), change=(last - pc) if last and pc else None,
                  pct_change=(last / pc - 1) * 100 if last and pc else None)


def _q_kite(req):
    name = f"NSE:{req.inst.nse}"
    q = B.kite().quote([name])[name]
    o = q.get("ohlc", {})
    return _q_row(symbol=req.inst.nse, last=q.get("last_price"), open=o.get("open"), high=o.get("high"),
                  low=o.get("low"), prev_close=o.get("close"), volume=q.get("volume"),
                  change=q.get("net_change"), vwap=q.get("average_price"), ts=q.get("last_trade_time"))


def _q_dhan(req):
    sid, seg, _ = B.dhan_security(req.inst)
    r = B.dhan().ohlc_data({seg: [int(sid)]})
    q = (((r.get("data") or {}).get("data") or {}).get(seg) or {}).get(str(sid)) or {}
    need(q, f"Dhan: {r}")
    o = q.get("ohlc", {})
    return _q_row(symbol=req.inst.nse, last=q.get("last_price"), open=o.get("open"), high=o.get("high"),
                  low=o.get("low"), prev_close=o.get("close"))


def _q_angel(req):
    tok, tsym = B.angel_token(req.inst)
    r = B.angel().ltpData("NSE", tsym, tok)
    q = r.get("data") or {}
    need(q, f"Angel One: {r}")
    return _q_row(symbol=req.inst.nse, last=q.get("ltp"), open=q.get("open"), high=q.get("high"),
                  low=q.get("low"), prev_close=q.get("close"))


def _q_fyers(req):
    r = B.fyers().quotes({"symbols": B.fyers_symbol(req.inst)})
    v = ((r.get("d") or [{}])[0]).get("v") or {}
    need(v, f"Fyers: {r}")
    return _q_row(symbol=req.inst.nse, last=v.get("lp"), open=v.get("open_price"), high=v.get("high_price"),
                  low=v.get("low_price"), prev_close=v.get("prev_close_price"), volume=v.get("volume"),
                  change=v.get("ch"), pct_change=v.get("chp"))


def _post_quote(df, req):
    df = df.assign(fetched_at=pd.Timestamp.now(tz=IST))
    return df


def _check_quote(df):
    return validate.positive("last")(df)


def _bulk_quotes(p: Pipeline, reqs: List[Req]) -> Dict[str, str]:
    """Many quotes: one NSE index snapshot (NIFTY TOTAL MARKET, ~750 stocks) instead of one call each."""
    from ..loader import is_installed

    if not is_installed("nse"):
        return {}
    need_ = {r.inst.nse.upper(): r for r in reqs if r.inst and r.inst.kind == "equity" and r.inst.nse}
    if len(need_) < 20:
        return {}
    got = 0
    for idx in ("NIFTY TOTAL MARKET", "NIFTY 500"):
        try:
            data = nse().list_equity_stocks_by_index(idx).get("data", [])
        except Exception:  # noqa: BLE001
            continue
        now = pd.Timestamp.now(tz=IST)
        for r in data:
            s = str(r.get("symbol", "")).upper()
            if s in need_:
                req = need_.pop(s)
                row = _q_row(symbol=s, last=r.get("lastPrice"), change=r.get("change"), pct_change=r.get("pChange"),
                             open=r.get("open"), high=r.get("dayHigh"), low=r.get("dayLow"),
                             prev_close=r.get("previousClose"), volume=r.get("totalTradedVolume"),
                             value=r.get("totalTradedValue"), ts=r.get("lastUpdateTime"), fetched_at=now,
                             source=f"nse:index snapshot ({idx})")
                cache.put_snapshot(p.name, snapshot_key(req), row)
                got += 1
        if not need_:
            break
    return {"route": f"NSE index snapshot covered {got} symbols"}


register(Pipeline(
    "india_live_quotes", "Indian stocks and indices: live quote (last price, OHLC, volume)", "snapshot", [
        Source("upstox", _q_upstox, B.UPSTOX_HOSTS),
        Source("smartapi", _q_angel, B.ANGEL_HOSTS, when=_equity),
        Source("fyers", _q_fyers, B.FYERS_HOSTS),
        Source("dhanhq", _q_dhan, B.DHAN_HOSTS, when=_equity),
        Source("kiteconnect", _q_kite, B.KITE_HOSTS, when=_equity),
        Source("nse", _q_nse, NSE_HOSTS, when=lambda r: _index(r) or _has_nse(r)),
        Source("nsepython", _q_nsepython, NSE_HOSTS, when=_has_nse),
        Source("bse", _q_bse, BSE_HOSTS, when=_equity),
        Source("yfinance", _q_yahoo, YAHOO_HOSTS, when=lambda r: bool(r.inst and r.inst.yahoo)),
        Source("nsetools", _q_nsetools, NSE_HOSTS, when=_has_nse),
        Source("bsedata", _q_bsedata, ("m.bseindia.com",), when=_equity),
    ],
    market="IN", required=("last",), time_col="ts", tz=IST, check=_check_quote, ttl=india_ttl,
    post=_post_quote, bulk=_bulk_quotes,
    columns=("symbol", "last", "change", "pct_change", "open", "high", "low", "prev_close", "vwap", "volume",
             "value", "bid", "ask", "ts", "fetched_at", "exchange"),
    params_doc="symbol (stock or index)", example='fs.fetch("india_live_quotes", "SBIN")'))


# =============================================================== intraday
_MINUTES = {"1m": 1, "3m": 3, "5m": 5, "10m": 10, "15m": 15, "30m": 30, "1h": 60, "60m": 60}


def _mins(req) -> int:
    iv = str(req.p("interval", "5m"))
    if iv not in _MINUTES:
        raise ValueError(f"interval must be one of {list(_MINUTES)}")
    return _MINUTES[iv]


def _i_upstox(req):
    return B.upstox_intraday(req, _mins(req))


def _i_fyers(req):
    return B.fyers_history(req, str(_mins(req)), 100)


def _i_angel(req):
    names = {1: "ONE_MINUTE", 3: "THREE_MINUTE", 5: "FIVE_MINUTE", 10: "TEN_MINUTE", 15: "FIFTEEN_MINUTE",
             30: "THIRTY_MINUTE", 60: "ONE_HOUR"}
    return B.angel_candles(req, names[_mins(req)], 30)


def _i_dhan(req):
    m = _mins(req)
    base = m if m in (1, 5, 15, 60) else 1
    return _resample(B.dhan_intraday(req, base), m)


def _i_kite(req):
    m = _mins(req)
    return B.kite_history(req, "minute" if m == 1 else f"{m}minute", 55).rename(columns={"date": "ts"})


def _i_tv(req):
    m = _mins(req)
    iv = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1h"}.get(m, "1m")
    n = ((dt.date.today() - req.start).days + 1) * 375 // int(iv.rstrip("mh") if iv.endswith("m") else 60)
    sym, exch = _tv_split(req.inst)
    df = tv_bars(sym, exch, iv, n).reset_index().rename(columns={"Date": "ts", "datetime": "ts"})
    return _resample(df, m) if iv == "1m" and m != 1 else df


def _i_ism(req):
    m = _mins(req)
    tf = {1: "1Min", 5: "5Min", 15: "15Min", 30: "30Min", 60: "60Min"}.get(m, "1Min")
    df = get("indian_stock_market").NSE().get_ohlc_data(req.inst.nse, timeframe=tf, is_index=_index(req),
                                                        start_date=dtm(req.start),
                                                        end_date=dtm(req.end + dt.timedelta(days=1)))
    df = df.to_pandas() if hasattr(df, "to_pandas") else df
    df = df.rename(columns={"time": "ts"})
    return _resample(df, m) if tf == "1Min" and m != 1 else df


def _i_yahoo(req):
    m = _mins(req)
    age = (dt.date.today() - req.start).days
    need(age <= 59 and (m != 1 or age <= 6), "Yahoo keeps 1-minute bars for 7 days and 5-minute bars for 60")
    iv = {1: "1m", 5: "5m", 15: "15m", 30: "30m", 60: "60m"}.get(m, "1m")
    df = yf_history(req.inst.yahoo, req.start, req.end, interval=iv).reset_index()
    df = df.rename(columns={"Datetime": "ts", "Date": "ts"})
    return _resample(df, m) if iv == "1m" and m != 1 else df


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Build N-minute bars from 1-minute bars, aligned to the 09:15 open."""
    if minutes == 1 or df is None or df.empty:
        return df
    d = schema.normalize(df, required=("ts", "close"), time_col="ts", tz=IST)
    rule = f"{minutes}min"
    agg = {c: f for c, f in (("open", "first"), ("high", "max"), ("low", "min"), ("close", "last"),
                             ("volume", "sum"), ("oi", "last")) if c in d}
    out = d.set_index("ts").resample(rule, offset=f"{15 % minutes}min", label="left", closed="left").agg(agg)
    return out.dropna(subset=["close"]).reset_index()


register(Pipeline(
    "india_intraday", "Indian stocks and indices: intraday candles (1m to 1h)", "series", [
        Source("upstox", _i_upstox, B.UPSTOX_HOSTS),
        Source("fyers", _i_fyers, B.FYERS_HOSTS),
        Source("smartapi", _i_angel, B.ANGEL_HOSTS),
        Source("dhanhq", _i_dhan, B.DHAN_HOSTS),
        Source("kiteconnect", _i_kite, B.KITE_HOSTS, when=_equity),
        Source("indian_stock_market", _i_ism, ("charting.nseindia.com",), when=lambda r: bool(r.inst.nse)),
        Source("tvdatafeed", _i_tv, TV_HOSTS, when=lambda r: bool(r.inst.tv)),
        Source("yfinance", _i_yahoo, YAHOO_HOSTS, when=lambda r: bool(r.inst.yahoo)),
    ],
    market="IN", time_col="ts", tz=IST, required=("ts", "close"), check=lambda df: validate.ohlcv(df, "ts"),
    final=india_final, has_data=india_has_data, default_days=5, columns=("ts", "open", "high", "low", "close",
                                                                           "volume", "oi"),
    entity=lambda r: f"{r.inst.key}|{r.p('interval', '5m')}", keys=(), live_ttl=30,
    post=lambda df, r: keep(df, ("ts", "open", "high", "low", "close", "volume", "oi")),
    params_doc="symbol, start, end, interval=1m|3m|5m|10m|15m|30m|1h",
    example='fs.fetch("india_intraday", "BANKNIFTY", start="5d", interval="5m")'))


# =============================================================== indices
def _x_nse(req):
    if req.inst.nse == "INDIA VIX":
        return nse().fetch_historical_vix_data(req.start, req.end)
    return nse().fetch_historical_index_data(req.inst.nse, req.start, req.end)


def _x_aynse(req):
    return get("aynse").index_df(req.inst.nse, req.start, req.end)


def _x_jugaad(req):
    from jugaad_data.nse import index_df

    return index_df(symbol=req.inst.nse, from_date=req.start, to_date=req.end)


def _x_nselib(req):
    return get("nselib", "capital_market").index_data(req.inst.nse, req.start.strftime("%d-%m-%Y"),
                                                      req.end.strftime("%d-%m-%Y"))


def _x_nsefin(req):
    return get("nsefin").NSEClient().get_index_historical_data(req.inst.nse, req.start.strftime("%d-%m-%Y"),
                                                                req.end.strftime("%d-%m-%Y"))


def _x_bse(req):
    os.makedirs(TMP, exist_ok=True)
    path = bse().fetchHistoricalIndexData(req.inst.nse, req.start, req.end, folder=TMP)
    need(path, "BSE returned an empty file")
    return pd.read_csv(path)


def _x_yahoo(req):
    need(req.inst.yahoo, "no Yahoo ticker for this index")
    return yf_history(req.inst.yahoo, req.start, req.end)


def _x_tv(req):
    sym, exch = _tv_split(req.inst)
    return tv_bars(sym, exch, "1d", bars_since(req.start))


def _post_index(df, req):
    return keep(df, ("date", "open", "high", "low", "close", "volume", "value")).assign(index=req.inst.nse)


def _is_nse_index(req):
    return _index(req) and not req.inst.tv.startswith("BSE:")


def _is_bse_index(req):
    return _index(req) and req.inst.tv.startswith("BSE:")


def _c_nse(req):
    rows = nse().list_equity_stocks_by_index(req.inst.nse)["data"]
    return [r for r in rows if r.get("priority", 0) != 1 and str(r.get("symbol")).upper() != req.inst.nse]


_NIFTY_FILES = {"NIFTY 50": "ind_nifty50list", "NIFTY BANK": "ind_niftybanklist", "NIFTY 500": "ind_nifty500list",
                "NIFTY NEXT 50": "ind_niftynext50list", "NIFTY IT": "ind_niftyitlist",
                "NIFTY MIDCAP 100": "ind_niftymidcap100list", "NIFTY SMALLCAP 100": "ind_niftysmallcap100list",
                "NIFTY FINANCIAL SERVICES": "ind_niftyfinancelist", "NIFTY AUTO": "ind_niftyautolist",
                "NIFTY PHARMA": "ind_niftypharmalist", "NIFTY FMCG": "ind_niftyfmcglist",
                "NIFTY METAL": "ind_niftymetallist"}


def _c_niftyindices(req):
    import requests

    name = _NIFTY_FILES.get(req.inst.nse)
    need(name, f"no niftyindices.com list known for {req.inst.nse}")
    r = requests.get(f"https://niftyindices.com/IndexConstituent/{name}.csv", timeout=30,
                     headers={"User-Agent": "Mozilla/5.0 (finstack)"})
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def _c_aynse(req):
    return get("aynse").index_constituent_df(req.inst.nse.lower().replace(" ", ""))   # "NIFTY 50" -> "nifty50"


def _post_constituents(df, req):
    df = keep(df, ("symbol", "name", "isin", "industry", "series", "last", "pct_change", "weight"))
    return df.assign(index=req.inst.nse)


_CONSTITUENTS = Pipeline(
    "india_indices.constituents", "Stocks in an Indian index", "snapshot", [
        Source("nse", _c_nse, NSE_HOSTS, when=_is_nse_index),
        Source("builtin:niftyindices", _c_niftyindices, ("niftyindices.com",), when=_is_nse_index, score=4.0),
        Source("aynse", _c_aynse, ("niftyindices.com",), when=_is_nse_index),
    ],
    market="IN_INDEX", required=("symbol",), time_col="_none", ttl=86400, post=_post_constituents,
    rank_as="india_indices",
    extra_aliases={"industry": ["industry", "Industry", "meta_industry"], "name": ["company_name", "Company Name"]},
    columns=("symbol", "name", "isin", "industry", "series", "index"),
    params_doc="index name, what='constituents'", example='fs.fetch("india_indices", "NIFTY 50", what="constituents")')

register(Pipeline(
    "india_indices", "Indian indices: daily history (Nifty, Bank Nifty, sectoral, India VIX, Sensex)", "series", [
        Source("nse", _x_nse, NSE_HOSTS, when=_is_nse_index),
        Source("aynse", _x_aynse, NSE_HOSTS, when=_is_nse_index),
        Source("nsefin", _x_nsefin, NSE_HOSTS, when=_is_nse_index),
        Source("nselib", _x_nselib, NSE_HOSTS, when=_is_nse_index),
        Source("jugaad_data", _x_jugaad, NSE_HOSTS, when=_is_nse_index),
        Source("bse", _x_bse, ("api.bseindia.com",), when=_is_bse_index),
        Source("yfinance", _x_yahoo, YAHOO_HOSTS, when=lambda r: _index(r) and bool(r.inst.yahoo)),
        Source("tvdatafeed", _x_tv, TV_HOSTS, when=_index),
    ],
    market="IN_INDEX", check=validate.ohlcv, final=india_final, has_data=india_has_data, post=_post_index,
    columns=("date", "open", "high", "low", "close", "volume", "value", "index"),
    variants={"constituents": _CONSTITUENTS},
    params_doc="index name (NIFTY 50, BANKNIFTY, INDIA VIX, SENSEX ...), start, end; what='constituents'",
    example='fs.fetch("india_indices", "NIFTY BANK", start="10y")'))


# =============================================================== market breadth
def _what(req):
    return req.p("what", "advance_decline")


def _b_nse(req):
    w, idx = _what(req), req.p("index", "NIFTY 50")
    n = nse()
    if w == "advance_decline":
        return pd.DataFrame([{**n.advance_decline(idx.lower()), "index": idx}])
    if w in ("gainers", "losers"):
        data = n.list_equity_stocks_by_index(idx.lower())
        rows = (n.gainers if w == "gainers" else n.losers)(data, req.p("count"))
        return pd.DataFrame(rows)
    raise LookupError(f"nse source has no '{w}'")


def _b_bse(req):
    w = _what(req)
    b = bse()
    if w == "advance_decline":
        return pd.DataFrame(b.advanceDecline())
    if w == "gainers":
        return pd.DataFrame(b.gainers())
    if w == "losers":
        return pd.DataFrame(b.losers())
    if w == "52w":
        return pd.DataFrame(to_records(b.near52WeekHighLow()))
    raise LookupError(f"bse source has no '{w}'")


def _b_nsepython(req):
    w = _what(req)
    np_ = get("nsepython")
    if w == "fii_dii":
        return np_.nse_fiidii()
    if w == "pre_open":
        return np_.nse_preopen(req.p("index", "NIFTY"))
    raise LookupError(f"nsepython has no '{w}'")


def _b_nsefin(req):
    w = _what(req)
    c = get("nsefin").NSEClient()
    if w == "fii_dii":
        return c.get_fii_dii_activity()
    if w == "pre_open":
        return c.get_pre_market_info(req.p("index", "All"))
    raise LookupError(f"nsefin has no '{w}'")


def _b_nselib(req):
    if _what(req) == "fii_dii":
        return get("nselib", "capital_market.capital_market_data").fii_dii_trading_activity()
    raise LookupError("nselib only has fii_dii here")


register(Pipeline(
    "india_market_breadth", "Indian market breadth and flows: advance/decline, gainers, losers, FII/DII, pre-open",
    "snapshot", [
        Source("nse", _b_nse, NSE_HOSTS, when=lambda r: _what(r) in ("advance_decline", "gainers", "losers")),
        Source("nsepython", _b_nsepython, NSE_HOSTS, when=lambda r: _what(r) in ("fii_dii", "pre_open")),
        Source("nsefin", _b_nsefin, NSE_HOSTS, when=lambda r: _what(r) in ("fii_dii", "pre_open")),
        Source("nselib", _b_nselib, NSE_HOSTS, when=lambda r: _what(r) == "fii_dii", score=3.5),
        Source("bse", _b_bse, BSE_HOSTS, when=lambda r: _what(r) in ("advance_decline", "gainers", "losers", "52w")),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=lambda r: 60 if calendar.in_session() else 1800,
    columns=(), params_doc="what=advance_decline|gainers|losers|fii_dii|pre_open|52w, index='NIFTY 50'",
    example='fs.fetch("india_market_breadth", what="fii_dii")'))
