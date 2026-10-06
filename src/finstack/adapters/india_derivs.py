"""India derivatives pipelines: option chains and MCX commodities."""
from __future__ import annotations

import datetime as dt

import pandas as pd

from ..core import calendar, schema, validate
from ..core.router import Pipeline, Req, Source, register
from ..core.symbols import NSE_DERIV_INDEX
from ..loader import get
from . import brokers as B
from ._common import (NSE_HOSTS, TV_HOSTS, bars_since, chunks, concat, india_final, india_has_data, keep, need, nse,
                      tv_bars)

IST = "Asia/Kolkata"
CHAIN_COLS = ("underlying", "expiry", "strike", "option_type", "last", "change", "pct_change", "oi", "oi_change",
              "volume", "iv", "bid", "ask", "underlying_price", "fetched_at")


def _underlying(req: Req) -> str:
    inst = req.inst
    if inst.kind == "index":
        return NSE_DERIV_INDEX.get(inst.nse, inst.nse.replace(" ", ""))
    return inst.nse


def _expiry(req: Req):
    e = req.p("expiry")
    return pd.Timestamp(e).to_pydatetime() if e else None


def _nse_rows(raw: dict) -> pd.DataFrame:
    recs = raw.get("records") or {}
    data = recs.get("data") or (raw.get("filtered") or {}).get("data") or raw.get("data") or []
    rows = []
    for d in data:
        for side in ("CE", "PE"):
            o = d.get(side)
            if not o:
                continue
            rows.append({"underlying": o.get("underlying"), "expiry": o.get("expiryDate") or d.get("expiryDate")
                         or d.get("expiryDates"), "strike": o.get("strikePrice", d.get("strikePrice")),
                         "option_type": side, "last": o.get("lastPrice"), "change": o.get("change"),
                         "pct_change": o.get("pChange"), "oi": o.get("openInterest"),
                         "oi_change": o.get("changeinOpenInterest"), "volume": o.get("totalTradedVolume"),
                         "iv": o.get("impliedVolatility"), "bid": o.get("bidprice", o.get("buyPrice1")),
                         "ask": o.get("askPrice", o.get("sellPrice1")), "underlying_price": o.get("underlyingValue")})
    return pd.DataFrame(rows)


def _o_nse(req):
    u = _underlying(req)
    name = u.lower() if req.inst.kind == "index" else u
    return _nse_rows(nse().option_chain(name, _expiry(req)))


def _o_nsepython(req):
    return _nse_rows(get("nsepython").nse_optionchain_scrapper(_underlying(req)))


def _o_nsefin(req):
    return get("nsefin").NSEClient().get_option_chain(_underlying(req))


def _o_upstox(req):
    api = B.upstox("OptionsApi")
    key = B.upstox_key(req.inst)
    exp = _expiry(req)
    if exp is None:
        contracts = api.get_option_contracts(key).to_dict().get("data") or []
        exps = sorted({str(c.get("expiry"))[:10] for c in contracts if c.get("expiry")})
        need(exps, "Upstox returned no option contracts")
        exp = pd.Timestamp(exps[0]).to_pydatetime()
    d = api.get_put_call_option_chain(key, exp.strftime("%Y-%m-%d")).to_dict()
    rows = []
    for r in d.get("data") or []:
        for side, k in (("CE", "call_options"), ("PE", "put_options")):
            o = r.get(k) or {}
            md, gk = o.get("market_data") or {}, o.get("option_greeks") or {}
            rows.append({"underlying": _underlying(req), "expiry": r.get("expiry"), "strike": r.get("strike_price"),
                         "option_type": side, "last": md.get("ltp"), "oi": md.get("oi"),
                         "oi_change": (md.get("oi") or 0) - (md.get("prev_oi") or 0), "volume": md.get("volume"),
                         "iv": gk.get("iv"), "bid": md.get("bid_price"), "ask": md.get("ask_price"),
                         "underlying_price": r.get("underlying_spot_price"), "delta": gk.get("delta"),
                         "gamma": gk.get("gamma"), "theta": gk.get("theta"), "vega": gk.get("vega")})
    return pd.DataFrame(rows)


def _o_dhan(req):
    sid, seg, _ = B.dhan_security(req.inst)
    useg = "IDX_I" if req.inst.kind == "index" else seg
    exp = _expiry(req)
    if exp is None:
        el = B.dhan().expiry_list(int(sid), useg)
        exps = ((el.get("data") or {}).get("data") or []) if isinstance(el, dict) else []
        need(exps, f"Dhan returned no expiries: {el}")
        exp = pd.Timestamp(sorted(exps)[0]).to_pydatetime()
    r = B.dhan().option_chain(int(sid), useg, exp.strftime("%Y-%m-%d"))
    data = ((r.get("data") or {}).get("data") or {}) if isinstance(r, dict) else {}
    need(data, f"Dhan: {r}")
    spot, rows = data.get("last_price"), []
    for strike, sides in (data.get("oc") or {}).items():
        for side, k in (("CE", "ce"), ("PE", "pe")):
            o = sides.get(k) or {}
            if not o:
                continue
            gk = o.get("greeks") or {}
            rows.append({"underlying": _underlying(req), "expiry": exp, "strike": float(strike), "option_type": side,
                         "last": o.get("last_price"), "oi": o.get("oi"),
                         "oi_change": (o.get("oi") or 0) - (o.get("previous_oi") or 0), "volume": o.get("volume"),
                         "iv": o.get("implied_volatility"), "bid": o.get("top_bid_price"),
                         "ask": o.get("top_ask_price"), "underlying_price": spot, "delta": gk.get("delta"),
                         "gamma": gk.get("gamma"), "theta": gk.get("theta"), "vega": gk.get("vega")})
    return pd.DataFrame(rows)


def _post_chain(df, req):
    exp = _expiry(req)
    if exp is not None and "expiry" in df:
        df = df[df["expiry"] == pd.Timestamp(exp).normalize()]
    elif "expiry" in df and df["expiry"].notna().any():
        df = df[df["expiry"] == df["expiry"].min()]          # nearest expiry by default
    if "underlying" not in df or df["underlying"].isna().all():
        df = df.assign(underlying=_underlying(req))
    return df.assign(fetched_at=pd.Timestamp.now(tz=IST)).sort_values(["strike", "option_type"])


register(Pipeline(
    "india_options", "Indian option chain (index and stock options), nearest or chosen expiry", "snapshot", [
        Source("upstox", _o_upstox, B.UPSTOX_HOSTS),
        Source("dhanhq", _o_dhan, B.DHAN_HOSTS),
        Source("nse", _o_nse, NSE_HOSTS),
        Source("nsepython", _o_nsepython, NSE_HOSTS),
        Source("nsefin", _o_nsefin, NSE_HOSTS),
    ],
    market="IN", required=("strike", "option_type"), time_col="_none", check=validate.options,
    ttl=lambda r: 60 if calendar.in_session() else 1800, post=_post_chain, columns=CHAIN_COLS,
    params_doc="symbol (NIFTY, BANKNIFTY, FINNIFTY or an F&O stock), expiry='YYYY-MM-DD' (default nearest). "
               "Past chains: fs.fetch('india_eod_files', start=..., segment='fno')",
    example='fs.fetch("india_options", "NIFTY")'))


# =============================================================== MCX commodities
def _m_mcxlib(req):
    m = get("mcxlib", "market_data")
    frames = [m.get_historical_date_wise_data(a.strftime("%Y%m%d"), b.strftime("%Y%m%d"))
              for a, b in chunks(req.start, req.end, 360)]
    return concat(frames)


def _post_mcx(df, req):
    sym = req.symbol.upper().replace("1!", "")
    cols = [c for c in ("symbol", "commodity", "instrument_name", "name") if c in df.columns]
    if cols:
        m = pd.Series(False, index=df.index)
        for c in cols:
            m |= df[c].astype(str).str.upper().str.strip() == sym
        df = df[m]
    if "instrument" in df.columns:
        df = df[df["instrument"].astype(str).str.upper().str.startswith("FUT")]
    if "expiry" in df.columns and df["expiry"].notna().any():      # continuous front month
        df = df[df["expiry"] >= df["date"]]
        df = df.sort_values(["date", "expiry"]).groupby("date", as_index=False).first()
    return keep(df, ("date", "open", "high", "low", "close", "volume", "value", "oi", "expiry")).assign(symbol=sym)


def _m_tv(req):
    return tv_bars(f"{req.symbol.upper().replace('1!', '')}1!", "MCX", "1d", bars_since(req.start))


def _mw_mcxlib(req):
    m = get("mcxlib", "market_data")
    w = req.p("what")
    if w == "option_chain":
        need(req.p("expiry"), "expiry='DDMONYYYY' is required, e.g. '19NOV2026'")
        return m.get_option_chain(req.symbol.upper(), req.p("expiry"))
    return m.get_market_watch()


_MCX_SNAP = Pipeline(
    "india_commodities.quotes", "MCX market watch (all contracts) or a commodity option chain", "snapshot",
    [Source("mcxlib", _mw_mcxlib, ("www.mcxindia.com",))],
    market="raw", needs_symbol=False, normalize=False, rank_as="india_commodities",
    ttl=lambda r: 60 if calendar.in_session() else 1800, columns=(),
    params_doc="what='quotes' | what='option_chain' with symbol and expiry",
    example='fs.fetch("india_commodities", what="quotes")')

register(Pipeline(
    "india_commodities", "MCX commodity futures: daily history (continuous front month)", "series", [
        Source("mcxlib", _m_mcxlib, ("www.mcxindia.com",)),
        Source("tvdatafeed", _m_tv, TV_HOSTS),
    ],
    market="raw", check=validate.ohlcv, final=india_final, has_data=india_has_data, post=_post_mcx,
    entity=lambda r: f"MCX:{r.symbol.upper()}", extra_aliases={"symbol": ["symbol", "commodity"],
                                                                "volume": ["volume_lots", "volume_in_lots"]},
    variants={"quotes": _MCX_SNAP, "option_chain": _MCX_SNAP},
    columns=("date", "open", "high", "low", "close", "volume", "value", "oi", "expiry", "symbol"),
    params_doc="symbol (GOLD, SILVER, CRUDEOIL, NATURALGAS, COPPER ...), start, end; what='quotes'|'option_chain'",
    example='fs.fetch("india_commodities", "GOLD", start="2y")'))
