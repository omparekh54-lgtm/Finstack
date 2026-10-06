"""More Indian data types (beta until verified live): deals, delivery, option analytics, index valuation,
company ratios and analyst estimates.

Every source here goes through the same network governor, cache and validation as the stable types.
Request-light by design: deals and delivery come from one file/request covering every stock, option
analytics are calculated from the option chain finstack already fetches.
"""
from __future__ import annotations

import datetime as dt
import os
import re

import numpy as np
import pandas as pd

from ..core import calendar, validate
from ..core.router import PIPELINES, Pipeline, Req, Source, register
from ..core.schema import canon
from ..loader import get
from ._common import (NSE_ARCHIVE_HOSTS, NSE_HOSTS, TMP, YAHOO_HOSTS, chunks, concat, dtm, india_final,
                      india_has_data, need, nse, to_records)

BETA = "beta"


# =============================================================== bulk / block deals, short selling
_DEAL_KIND = {"bulk": "bulk_deals", "block": "block_deals", "short": "short_selling"}
_DEAL_COLS = {
    # nselib names (spaces removed by canon) and NSE JSON names
    "date": ["date", "bd_dt_date", "bd_dt_date_", "sd_dt_date"],
    "symbol": ["symbol", "bd_symbol", "sd_symbol"],
    "name": ["securityname", "security_name", "bd_scrip_name", "sd_scrip_name"],
    "client": ["clientname", "client_name", "bd_client_name"],
    "side": ["buy_sell", "buysell", "bd_buy_sell"],
    "quantity": ["quantitytraded", "quantity_traded", "quantity", "bd_qty_trd", "sd_qty_trd"],
    "price": ["tradeprice_wght_avg_price", "tradeprice_wght_avg_price_", "trade_price_wght_avg_price",
              "bd_tp_watp"],
    "remarks": ["remarks", "bd_remarks"],
}


def _deal_kind(req) -> str:
    k = str(req.p("what", "bulk")).lower()
    need(k in _DEAL_KIND, "what must be bulk, block or short")
    return k


def _deal_window(req):
    end = req.end or dt.date.today()
    start = req.start or end - dt.timedelta(days=30)
    return start, end


def _d_nse(req):
    kind = _DEAL_KIND[_deal_kind(req)]
    a, b = _deal_window(req)
    rows = []
    for s, e in chunks(a, b, 360):                     # NSE: at most one year per request
        rows += to_records(nse().bulk_deals(kind, dtm(s), dtm(e))) or []
    return pd.DataFrame(rows)


def _d_nselib(req):
    cm = get("nselib", "capital_market")
    fn = {"bulk": cm.bulk_deal_data, "block": cm.block_deals_data, "short": cm.short_selling_data}[_deal_kind(req)]
    a, b = _deal_window(req)
    return concat(fn(from_date=s.strftime("%d-%m-%Y"), to_date=e.strftime("%d-%m-%Y")) for s, e in chunks(a, b, 360))


def _d_all(req):
    """One stock's deals: filter the cached all-stocks answer instead of asking NSE again per symbol."""
    from ..core.router import fetch

    a, b = _deal_window(req)
    return fetch("india_deals", start=a, end=b, what=_deal_kind(req))


def _post_deals(df, req):
    df = df.copy().drop(columns=["source", "deal_type"], errors="ignore")
    df.columns = [canon(c) for c in df.columns]
    ren = {}
    for std, names in _DEAL_COLS.items():
        hit = next((c for c in names if c in df.columns), None)
        if hit:
            ren[hit] = std
    df = df.rename(columns=ren)
    need({"date", "symbol"} <= set(df.columns), f"unexpected columns {list(df.columns)[:10]}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True, format="mixed")
    for c in ("quantity", "price"):
        if c in df:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")
    if "side" in df:
        df["side"] = df["side"].astype(str).str.upper().str.strip()
    df = df.dropna(subset=["date"])
    if req.symbol:
        df = df[df["symbol"].astype(str).str.upper() == str(req.symbol).upper()]
    return df.assign(deal_type=_deal_kind(req)).sort_values("date", ascending=False)


def _final_ttl(req):
    """Past windows never change: cache for good. Windows that include recent days: 1 hour."""
    _, end = _deal_window(req)
    return None if end <= calendar.india_final_through() else 3600


register(Pipeline(
    "india_deals", "Bulk deals, block deals and short selling on NSE (all stocks, or one)", "snapshot", [
        Source("builtin:deals_all", _d_all, (), score=9.0, when=lambda r: bool(r.symbol)),
        Source("nse", _d_nse, NSE_HOSTS, when=lambda r: not r.symbol),
        Source("nselib", _d_nselib, NSE_HOSTS, when=lambda r: not r.symbol),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=_final_ttl, post=_post_deals, status=BETA,
    columns=("date", "symbol", "name", "client", "side", "quantity", "price", "remarks", "deal_type"),
    params_doc="what=bulk|block|short, symbol (optional), start, end (default last 30 days)",
    example='fs.fetch("india_deals", what="bulk", start="30d")'))


# =============================================================== delivery %
def _v_nse(req):
    os.makedirs(TMP, exist_ok=True)
    path = nse().delivery_bhavcopy(dtm(req.start), TMP)
    return pd.read_csv(path)


def _v_nselib(req):
    return get("nselib", "capital_market").bhav_copy_with_delivery(req.start.strftime("%d-%m-%Y"))


def _post_delivery(df, req):
    df = df.copy()
    for c in ("symbol", "series"):
        if c in df:
            df[c] = df[c].astype(str).str.strip()
    if "date" not in df or df["date"].isna().all():
        df["date"] = pd.Timestamp(req.start)
    keep = [c for c in ("date", "symbol", "series", "close", "volume", "delivery_qty", "delivery_pct", "value",
                        "trades") if c in df]
    return df[keep]


def _check_delivery(df):
    need("delivery_pct" in df, "no delivery % column")
    bad = df["delivery_pct"].notna() & ~df["delivery_pct"].between(0, 100)
    return df[~bad].reset_index(drop=True), df[bad].reset_index(drop=True), []


register(Pipeline(
    "india_delivery", "Delivery quantity and delivery % (all NSE stocks per day, or one stock over time)",
    "daily_files", [
        Source("nse", _v_nse, NSE_ARCHIVE_HOSTS),
        Source("nselib", _v_nselib, NSE_ARCHIVE_HOSTS),
    ],
    market="raw", needs_symbol=False, required=("symbol", "delivery_pct"), check=_check_delivery,
    final=india_final, post=_post_delivery, symbol_filter=True, status=BETA,
    columns=("date", "symbol", "series", "close", "volume", "delivery_qty", "delivery_pct"),
    params_doc="symbol (optional: one stock), start, end - one NSE file per trading day, cached for good",
    example='fs.fetch("india_delivery", "TCS", start="30d")'))


# =============================================================== option analytics (no extra requests)
def option_analytics(chain: pd.DataFrame) -> pd.DataFrame:
    """Per-strike open interest, put-call ratio and option-writer loss ('pain'); max pain and totals repeated
    on every row. chain: finstack option chain (strike, option_type CE/PE, oi, volume, underlying_price)."""
    need({"strike", "option_type", "oi"} <= set(chain.columns), "option chain lacks strike/option_type/oi")
    c = chain.copy()
    c["oi"] = pd.to_numeric(c["oi"], errors="coerce").fillna(0)
    piv = c.pivot_table(index="strike", columns="option_type", values="oi", aggfunc="sum").fillna(0)
    for side in ("CE", "PE"):
        if side not in piv:
            piv[side] = 0.0
    out = pd.DataFrame({"strike": piv.index.astype(float), "ce_oi": piv["CE"].values, "pe_oi": piv["PE"].values})
    if "oi_change" in c:
        ch = c.pivot_table(index="strike", columns="option_type", values="oi_change", aggfunc="sum").fillna(0)
        out["ce_oi_change"] = ch.get("CE", pd.Series(0, index=ch.index)).reindex(piv.index).fillna(0).values
        out["pe_oi_change"] = ch.get("PE", pd.Series(0, index=ch.index)).reindex(piv.index).fillna(0).values
    out["pcr"] = np.where(out["ce_oi"] > 0, out["pe_oi"] / out["ce_oi"].replace(0, np.nan), np.nan)
    k = out["strike"].values
    ce, pe = out["ce_oi"].values, out["pe_oi"].values
    # payout option writers owe if the underlying expires at each strike
    pain = [(np.maximum(0, s - k) * ce).sum() + (np.maximum(0, k - s) * pe).sum() for s in k]
    out["writers_payout"] = pain
    total_ce, total_pe = ce.sum(), pe.sum()
    out["max_pain"] = float(k[int(np.argmin(pain))]) if len(k) else np.nan
    out["pcr_total"] = total_pe / total_ce if total_ce else np.nan
    out["max_ce_oi_strike"] = float(k[int(np.argmax(ce))]) if len(k) else np.nan
    out["max_pe_oi_strike"] = float(k[int(np.argmax(pe))]) if len(k) else np.nan
    if "underlying_price" in c and c["underlying_price"].notna().any():
        spot = float(pd.to_numeric(c["underlying_price"], errors="coerce").dropna().iloc[0])
        out["underlying_price"] = spot
        out["atm_strike"] = float(k[int(np.argmin(np.abs(k - spot)))]) if len(k) else np.nan
    for col in ("underlying", "expiry"):
        if col in c:
            out[col] = c[col].dropna().iloc[0] if c[col].notna().any() else None
    return out


def _oa(req):
    from ..core.router import fetch

    kw = {"expiry": req.p("expiry")} if req.p("expiry") else {}
    chain = fetch("india_options", req.symbol, **kw)
    return option_analytics(chain)


_ANALYTICS = Pipeline(
    "india_options.analytics", "Option analytics: max pain, put-call ratio, OI by strike (calculated locally)",
    "snapshot", [Source("builtin:options_analytics", _oa, (), score=5.0)],
    market="IN", required=("strike",), time_col="_none", normalize=False, rank_as="india_options",
    ttl=lambda r: 60 if calendar.in_session() else 1800, status=BETA,
    columns=("underlying", "expiry", "strike", "ce_oi", "pe_oi", "ce_oi_change", "pe_oi_change", "pcr",
             "writers_payout", "max_pain", "pcr_total", "max_ce_oi_strike", "max_pe_oi_strike", "atm_strike",
             "underlying_price"),
    params_doc="symbol, what='analytics', expiry='YYYY-MM-DD' (default nearest)",
    example='fs.fetch("india_options", "NIFTY", what="analytics")')

if "india_options" in PIPELINES:          # additive: the existing option-chain type is untouched
    PIPELINES["india_options"].variants["analytics"] = _ANALYTICS


# =============================================================== index valuation (P/E, P/B, dividend yield)
def _iv_aynse(req):
    return get("aynse").index_pe_df(req.inst.nse, req.start, req.end)


def _iv_jugaad(req):
    from jugaad_data.nse import index_pe_df

    return index_pe_df(symbol=req.inst.nse, from_date=req.start, to_date=req.end)


def _post_iv(df, req):
    ren = {}
    for c in df.columns:
        n = c.replace("_", "")
        if n in ("pricetoearnings", "pe", "peratio"):
            ren[c] = "pe"
        elif n in ("pricetobook", "pb", "pbratio"):
            ren[c] = "pb"
        elif n in ("dividendyield", "divyield", "divyieldpct"):
            ren[c] = "div_yield"
    df = df.rename(columns=ren)
    need("pe" in df, f"no P/E column in {list(df.columns)[:10]}")
    for c in ("pe", "pb", "div_yield"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = [c for c in ("date", "pe", "pb", "div_yield") if c in df]
    return df[keep].assign(index=req.inst.nse)


def _check_iv(df):
    bad = ~df["pe"].between(1, 200)
    return df[~bad].reset_index(drop=True), df[bad].reset_index(drop=True), []


register(Pipeline(
    "india_index_valuation", "Nifty / sector index valuation history: P/E, P/B, dividend yield", "series", [
        Source("aynse", _iv_aynse, ("www.niftyindices.com", "niftyindices.com")),
        Source("jugaad_data", _iv_jugaad, ("www.niftyindices.com", "niftyindices.com")),
    ],
    market="IN_INDEX", required=("date",), check=_check_iv, final=india_final, has_data=india_has_data,
    post=_post_iv, default_days=365 * 5, status=BETA, columns=("date", "pe", "pb", "div_yield", "index"),
    params_doc="index name (NIFTY 50, NIFTY BANK, NIFTY IT ...), start, end",
    example='fs.fetch("india_index_valuation", "NIFTY 50", start="10y")'))


# =============================================================== company ratios
RATIO_FIELDS = [
    ("eps", r"^(basic )?eps"),
    ("book_value_per_share", r"^book value"),
    ("dividend_per_share", r"^dividend ?/ ?share|^dividend per share"),
    ("pe", r"^(price ?/ ?earnings|p ?/ ?e|pe)\b"),
    ("pb", r"^(price ?/ ?bv|price ?/ ?book|p ?/ ?b|pb)\b"),
    ("ev_ebitda", r"^ev ?/ ?ebitda"),
    ("roe", r"^return on (net ?worth|equity)|^roe"),
    ("roce", r"^return on capital employed|^roce"),
    ("roa", r"^return on assets|^roa"),
    ("operating_margin", r"^(pbdit|operating|ebit) margin"),
    ("net_margin", r"^net profit margin|^net margin|^pat margin"),
    ("debt_to_equity", r"^(total )?debt ?/ ?equity"),
    ("current_ratio", r"^current ratio"),
    ("interest_cover", r"^interest cover"),
    ("dividend_payout", r"^dividend payout"),
    ("dividend_yield", r"^dividend yield"),
]


def ratio_name(label: str) -> str:
    low = re.sub(r"\s+", " ", str(label).lower().replace("(%)", "").replace("(x)", "").replace("(rs.)", "")).strip()
    for name, pat in RATIO_FIELDS:
        if re.search(pat, low):
            return name
    return canon(low)


def _r_moneycontrol(req):
    from Fundamentals import MoneyControl

    from .india_company import period_end

    mc = MoneyControl()
    sc_id, _ = mc.get_ticker(req.inst.nse)
    wide = mc.get_ratios_mini_statement(sc_id, statement_type="consolidated" if req.p("consolidated", True)
                                        else "standalone")
    need(isinstance(wide, pd.DataFrame) and len(wide), "Moneycontrol returned no ratios")
    df = wide.reset_index() if not isinstance(wide.index, pd.RangeIndex) else wide
    item = df.columns[0]
    rows = {}
    for col in df.columns[1:]:
        pe = period_end(col)
        if pe is None:
            continue
        rec = rows.setdefault(pe, {"period_end": pe})
        for _, r in df.iterrows():
            v = pd.to_numeric(str(r[col]).replace(",", ""), errors="coerce")
            if pd.notna(v):
                rec.setdefault(ratio_name(r[item]), float(v))
    return pd.DataFrame(list(rows.values())).sort_values("period_end", ascending=False)


_YF_RATIOS = {"trailingPE": "pe", "forwardPE": "forward_pe", "priceToBook": "pb", "returnOnEquity": "roe",
              "returnOnAssets": "roa", "debtToEquity": "debt_to_equity", "profitMargins": "net_margin",
              "operatingMargins": "operating_margin", "currentRatio": "current_ratio",
              "dividendYield": "dividend_yield", "payoutRatio": "dividend_payout", "trailingEps": "eps",
              "bookValue": "book_value_per_share", "enterpriseToEbitda": "ev_ebitda", "beta": "beta",
              "marketCap": "market_cap", "pegRatio": "peg"}
_YF_PERCENT = {"roe", "roa", "net_margin", "operating_margin", "dividend_payout"}


def _r_yahoo(req):
    info = get("yfinance").Ticker(req.inst.yahoo).info or {}
    row = {"period_end": pd.Timestamp(dt.date.today()), "basis": "latest (Yahoo)"}
    for k, name in _YF_RATIOS.items():
        v = info.get(k)
        if isinstance(v, (int, float)):
            row[name] = float(v) * 100 if name in _YF_PERCENT else float(v)
    if "debt_to_equity" in row:
        row["debt_to_equity"] = row["debt_to_equity"] / 100       # Yahoo reports it in %
    if "dividend_yield" in row and row["dividend_yield"] < 0.5:
        row["dividend_yield"] *= 100                            # older Yahoo versions give a fraction
    need(len(row) > 3, "Yahoo returned no ratios")
    return pd.DataFrame([row])


def _post_ratios(df, req):
    return df.assign(symbol=req.inst.nse or req.inst.query,
                     units="ratios in x, margins/returns/yields in %, per-share values in INR")


register(Pipeline(
    "india_ratios", "Company ratios: P/E, P/B, ROE, ROCE, margins, debt/equity, EPS (yearly history or latest)",
    "snapshot", [
        Source("bharat_sm_data", _r_moneycontrol, ("www.moneycontrol.com", "priceapi.moneycontrol.com"),
               label="moneycontrol", when=lambda r: bool(r.inst.nse)),
        Source("yfinance", _r_yahoo, YAHOO_HOSTS, when=lambda r: bool(r.inst.yahoo)),
    ],
    market="IN", required=("period_end",), time_col="_none", normalize=False, ttl=86400, post=_post_ratios,
    status=BETA, columns=("period_end", "pe", "pb", "roe", "roce", "operating_margin", "net_margin",
                          "debt_to_equity", "eps", "book_value_per_share", "dividend_yield", "symbol"),
    params_doc="symbol, consolidated=True", example='fs.fetch("india_ratios", "TCS")'))


# =====================================================================================================
# Phase 2: market structure and company details
# =====================================================================================================

# =============================================================== price bands (circuit limits)
def _pb_nse(req):
    os.makedirs(TMP, exist_ok=True)
    return pd.read_csv(nse().priceband_report(dtm(req.start), TMP))


def _post_bands(df, req):
    df = df.copy()
    df.columns = [canon(c) for c in df.columns]
    ren = {}
    for c in df.columns:
        if c in ("symbol",):
            ren[c] = "symbol"
        elif c in ("series",):
            ren[c] = "series"
        elif c in ("security_name", "securityname", "name"):
            ren[c] = "name"
        elif c in ("band", "price_band", "priceband", "band_pct"):
            ren[c] = "band"
        elif c in ("remarks",):
            ren[c] = "remarks"
    df = df.rename(columns=ren)
    need({"symbol", "band"} <= set(df.columns), f"unexpected columns {list(df.columns)[:10]}")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    # band is a % ("2", "5", "10", "20") or "No Band"
    df["band_pct"] = pd.to_numeric(df["band"].astype(str).str.extract(r"(\d+(?:\.\d+)?)")[0], errors="coerce")
    return df.assign(date=pd.Timestamp(req.start))


register(Pipeline(
    "india_price_bands", "Price bands / circuit limits for every NSE stock (2%, 5%, 10%, 20%, no band)",
    "daily_files", [Source("nse", _pb_nse, NSE_ARCHIVE_HOSTS)],
    market="raw", needs_symbol=False, normalize=False, final=india_final, post=_post_bands, symbol_filter=True,
    status=BETA, columns=("date", "symbol", "series", "name", "band", "band_pct", "remarks"),
    params_doc="symbol (optional), start, end - one NSE file per trading day",
    example='fs.fetch("india_price_bands", "ADANIENT")'))


# =============================================================== F&O reference: lot sizes and expiries
def _fr_nse(req):
    w = req.p("what", "lots")
    n = nse()
    if w == "lots":
        lots = n.fno_lots()
        need(lots, "NSE returned no lot sizes")
        return pd.DataFrame([{"symbol": k, "lot_size": v} for k, v in lots.items()])
    if w == "expiries":
        idx = str(req.symbol or "nifty").lower()
        exps = n.get_futures_expiry(idx)
        return pd.DataFrame({"symbol": idx.upper(), "expiry": pd.to_datetime(exps, dayfirst=True)})
    raise LookupError("what must be lots or expiries")


def _post_fr(df, req):
    if req.p("what", "lots") == "lots" and req.symbol:
        df = df[df["symbol"].astype(str).str.upper() == str(req.symbol).upper()]
    return df


register(Pipeline(
    "india_fno_reference", "F&O lot sizes for every contract; futures expiry dates for an index", "snapshot",
    [Source("nse", _fr_nse, NSE_HOSTS)],
    market="raw", needs_symbol=False, normalize=False, ttl=86400, post=_post_fr, status=BETA, columns=(),
    params_doc="what=lots (symbol optional) | what=expiries with symbol nifty|banknifty|finnifty",
    example='fs.fetch("india_fno_reference", what="lots")'))


# =============================================================== F&O contract history
def _fh_args(req):
    exp = req.p("expiry")
    need(exp, "expiry='YYYY-MM-DD' is required")
    ot = req.p("option_type")
    strike = req.p("strike")
    is_index = req.inst.kind == "index"
    inst = ("opt" if ot else "fut") + ("idx" if is_index else "stk")
    from ..core.symbols import NSE_DERIV_INDEX

    sym = NSE_DERIV_INDEX.get(req.inst.nse, req.inst.nse) if is_index else req.inst.nse
    return sym, inst, pd.Timestamp(exp).date(), (str(ot).lower() if ot else None), (float(strike) if strike else None)


def _fh_nse(req):
    sym, inst, exp, ot, strike = _fh_args(req)
    return nse().fetch_historical_fno_data(sym, inst, req.start, req.end, expiry=exp, option_type=ot,
                                           strike_price=strike)


def _fh_aynse(req):
    sym, inst, exp, ot, strike = _fh_args(req)
    return get("aynse").derivatives_df(sym, req.start, req.end, exp, inst.upper(), strike_price=strike,
                                       option_type=(ot.upper() if ot else None))


def _post_fh(df, req):
    keep = [c for c in ("date", "open", "high", "low", "close", "settle", "volume", "value", "oi", "oi_change",
                        "underlying_price", "expiry", "strike", "option_type") if c in df]
    return df[keep].assign(symbol=_fh_args(req)[0])


register(Pipeline(
    "india_fno_history", "Daily history of one futures or options contract (price, settle, open interest)",
    "series", [
        Source("nse", _fh_nse, NSE_HOSTS),
        Source("aynse", _fh_aynse, NSE_HOSTS),
    ],
    market="IN", check=validate.eod_rows, final=india_final, has_data=india_has_data, post=_post_fh,
    default_days=90, status=BETA,
    extra_aliases={"oi": ["fh_open_int", "open_interest"], "oi_change": ["fh_change_in_oi", "change_in_oi"],
                   "settle": ["fh_settle_price", "settle_price"], "underlying_price": ["fh_underlying_value"],
                   "expiry": ["fh_expiry_dt"], "strike": ["fh_strike_price"], "option_type": ["fh_option_type"],
                   "open": ["fh_opening_price"], "high": ["fh_trade_high_price"], "low": ["fh_trade_low_price"],
                   "close": ["fh_closing_price"], "value": ["fh_tot_traded_val"]},
    entity=lambda r: "FNO:" + "|".join(str(x) for x in _fh_args(r)),
    columns=("date", "open", "high", "low", "close", "settle", "volume", "oi", "oi_change", "underlying_price",
             "expiry", "strike", "option_type", "symbol"),
    params_doc="symbol (NIFTY, BANKNIFTY or an F&O stock), expiry='YYYY-MM-DD', option_type=CE|PE (omit for "
               "futures), strike=24500, start, end",
    example='fs.fetch("india_fno_history", "NIFTY", expiry="2026-10-27", option_type="CE", strike=25000)'))


# =============================================================== more market breadth: volume gainers, most active
def _bx_nse(req):
    data = nse().live_volume_gainers()
    return pd.DataFrame(to_records(data))


def _bx_nselib(req):
    cm = get("nselib", "capital_market")
    if req.p("what") == "most_active":
        return cm.most_active_equities(req.p("by", "value"))
    raise LookupError("nselib route: most_active only")


if "india_market_breadth" in PIPELINES:          # additive: new `what` values only, old ones untouched
    PIPELINES["india_market_breadth"].sources += [
        Source("nse", _bx_nse, NSE_HOSTS, label="nse:volume_gainers",
               when=lambda r: r.p("what") == "volume_gainers"),
        Source("nselib", _bx_nselib, NSE_HOSTS, label="nselib:most_active",
               when=lambda r: r.p("what") == "most_active"),
    ]
    PIPELINES["india_market_breadth"].params_doc += "|volume_gainers|most_active (by=value|volume)"


# =============================================================== margins (VaR / ELM)
def _m_nselib(req):
    cm = get("nselib", "capital_market")
    return cm.var_end_of_day(req.start.strftime("%d-%m-%Y"))


def _post_margins(df, req):
    df = df.copy()
    df.columns = [canon(c) for c in df.columns]
    sym = next((c for c in df.columns if c in ("symbol", "sym")), None)
    need(sym, f"unexpected columns {list(df.columns)[:10]}")
    df = df.rename(columns={sym: "symbol"})
    df["symbol"] = df["symbol"].astype(str).str.strip()
    for c in df.columns:
        if c not in ("symbol", "series", "isin", "date", "record_type"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.assign(date=pd.Timestamp(req.start))


register(Pipeline(
    "india_margins", "Margin rates (VaR, ELM, applicable margin %) for every NSE stock, end of day",
    "daily_files", [Source("nselib", _m_nselib, NSE_ARCHIVE_HOSTS)],
    market="raw", needs_symbol=False, normalize=False, final=india_final, post=_post_margins, symbol_filter=True,
    status=BETA, columns=("date", "symbol"),
    params_doc="symbol (optional), start, end - one NSE file per trading day",
    example='fs.fetch("india_margins", "TCS")'))


# =============================================================== company profile (India + global)
def _indian(req) -> bool:
    """NSE symbol unless market= says otherwise or the symbol is a non-Indian Yahoo ticker."""
    s = str(req.symbol).upper()
    return str(req.p("market", "IN")).upper() == "IN" and ("." not in s or s.endswith((".NS", ".BO")))


def _yahoo_of(req) -> str:
    from .global_extra import yahoo_ticker

    return yahoo_ticker(req)


def _p_nsepython(req):
    q = get("nsepython").nse_eq(req.inst.nse)
    info, ind, md = q.get("info", {}), q.get("industryInfo", {}), q.get("metadata", {})
    return pd.DataFrame([{"symbol": req.inst.nse, "name": info.get("companyName"), "isin": info.get("isin"),
                          "macro": ind.get("macro"), "sector": ind.get("sector"), "industry": ind.get("industry"),
                          "basic_industry": ind.get("basicIndustry"), "listing_date": md.get("listingDate"),
                          "sector_pe": md.get("pdSectorPe"), "symbol_pe": md.get("pdSymbolPe"),
                          "index": md.get("pdSectorInd"), "fno": info.get("isFNOSec")}])


def _p_nse_meta(req):
    m = nse().equity_meta_info(req.inst.nse)
    return pd.DataFrame([{"symbol": m.get("symbol"), "name": m.get("companyName"), "isin": m.get("isin"),
                          "fno": m.get("isFNOSec"), "etf": m.get("isETFSec"), "suspended": m.get("isSuspended"),
                          "delisted": m.get("isDelisted"), "series": ",".join(m.get("activeSeries") or [])}])


_YF_PROFILE = {"longName": "name", "sector": "sector", "industry": "industry", "longBusinessSummary": "description",
               "fullTimeEmployees": "employees", "website": "website", "city": "city", "country": "country",
               "marketCap": "market_cap", "currency": "currency", "exchange": "exchange"}


def _p_yahoo(req):
    t = _yahoo_of(req)
    info = get("yfinance").Ticker(t).info or {}
    row = {v: info.get(k) for k, v in _YF_PROFILE.items() if info.get(k) is not None}
    need(row.get("name") or row.get("sector"), "Yahoo returned no profile")
    return pd.DataFrame([{"symbol": t, **row}])


def _p_fd(req):
    fd = get("financedatabase")
    t = _yahoo_of(req)
    eq = fd.Equities().select()
    need(t in eq.index, f"{t} not in FinanceDatabase")
    r = eq.loc[[t]].reset_index().iloc[0].to_dict()
    return pd.DataFrame([{"symbol": t, "name": r.get("name"), "sector": r.get("sector"),
                          "industry": r.get("industry"), "description": r.get("summary"),
                          "country": r.get("country"), "website": r.get("website"),
                          "market_cap": r.get("market_cap"), "currency": r.get("currency")}])


register(Pipeline(
    "company_profile", "Company profile: name, sector, industry, description, employees, website (India + global)",
    "snapshot", [
        Source("nsepython", _p_nsepython, NSE_HOSTS, score=4.0, when=_indian),
        Source("yfinance", _p_yahoo, YAHOO_HOSTS, score=3.8),
        Source("nse", _p_nse_meta, NSE_HOSTS, score=3.0, when=_indian),
        Source("financedatabase", _p_fd, ("raw.githubusercontent.com",), score=2.5),
    ],
    market="IN", normalize=False, ttl=7 * 86400, status=BETA,
    columns=("symbol", "name", "sector", "industry", "basic_industry", "description", "employees", "website",
             "isin", "listing_date", "market_cap"),
    params_doc="symbol (NSE symbol; for other markets a Yahoo ticker with market='US', or VOD.L, 7203.T)",
    example='fs.fetch("company_profile", "TCS")'))


# =============================================================== peers and Tickertape scorecard
def _tt_sid(symbol):
    from Fundamentals import Tickertape

    tt = Tickertape()
    t, _ = tt.get_ticker(symbol)
    return tt, (t.get("sid") if isinstance(t, dict) else t)


def _peers_tt(req):
    tt, sid = _tt_sid(req.inst.nse)
    return tt.peers_comparison(sid, comparison_type=req.p("by", "valuation"))


def _peers_screener(req):
    from Fundamentals import Screener

    from ._common import env

    s = Screener(env("SCREENER_USER"), env("SCREENER_PASS"))
    return s.get_peers_comparison(f"/company/{req.inst.nse}/consolidated/")


register(Pipeline(
    "india_peers", "Peer comparison: a company next to its competitors (valuation or technical view)", "snapshot", [
        Source("bharat_sm_data", _peers_tt, ("api.tickertape.in", "www.tickertape.in"), label="tickertape",
               score=4.0),
        Source("screener", _peers_screener, ("www.screener.in",), libs=("bharat_sm_data",), score=3.5),
    ],
    market="IN", normalize=False, ttl=86400, status=BETA, columns=(),
    params_doc="symbol, by=valuation|technical (Tickertape)", example='fs.fetch("india_peers", "TCS")'))


def _score_tt(req):
    tt, sid = _tt_sid(req.inst.nse)
    sc = tt.get_score_card(sid)
    return sc if isinstance(sc, pd.DataFrame) else pd.DataFrame(to_records(sc) or [sc])


register(Pipeline(
    "india_scorecard", "Tickertape scorecard: performance, valuation, growth, profitability, entry point, red flags",
    "snapshot", [Source("bharat_sm_data", _score_tt, ("api.tickertape.in", "www.tickertape.in"),
                        label="tickertape")],
    market="IN", normalize=False, ttl=86400, status=BETA, columns=(),
    params_doc="symbol", example='fs.fetch("india_scorecard", "TCS")'))
