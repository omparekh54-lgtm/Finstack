"""Reference data (symbol lists) and the OpenBB pass-through."""
from __future__ import annotations

import pandas as pd

from ..core import symbols
from ..core.router import Pipeline, Req, Source, register
from ..loader import get
from ._common import need


def _what(req):
    return req.p("what", "india")


def _master(req):
    need(_what(req) == "india", "symbol master covers Indian equities")
    df = symbols.master(refresh=True)
    need(len(df), "NSE and BSE lists both failed: " + "; ".join(df.attrs.get("errors", [])))
    return df


def _bse_list(req):
    need(_what(req) == "india", "BSE list covers Indian equities")
    from ..company import bse_client

    return pd.DataFrame(bse_client().listSecurities(group="", segment="Equity", status="Active")).rename(
        columns={"SCRIP_CD": "bse_code", "Scrip_Name": "name", "ISIN_NUMBER": "isin", "scrip_id": "symbol",
                 "INDUSTRY": "industry", "GROUP": "bse_group"})


def _nselib(req):
    need(_what(req) == "india", "nselib list covers Indian equities")
    df = get("nselib", "capital_market").equity_list()
    return df.rename(columns={"SYMBOL": "symbol", "NAME OF COMPANY": "name", " ISIN NUMBER": "isin",
                              "ISIN NUMBER": "isin"})


def _fd(req):
    fd = get("financedatabase")
    w = _what(req)
    country = req.p("country", "India" if w == "india" else None)
    kw = {k: req.p(k) for k in ("sector", "industry", "exchange", "market_cap") if req.p(k)}
    if country:
        kw["country"] = country
    cls = {"etfs": fd.ETFs, "funds": fd.Funds, "indices": fd.Indices, "cryptos": fd.Cryptos,
           "currencies": fd.Currencies}.get(w, fd.Equities)
    if cls in (fd.Indices, fd.Cryptos, fd.Currencies):
        kw.pop("country", None)
    return cls().select(**kw).reset_index().rename(columns={"symbol": "yahoo"})


def _sec(req):
    need(_what(req) == "us", "SEC list covers US companies")
    from ..core import cache
    from .global_ import sec_cik

    sec_cik("AAPL")                     # downloads and caches SEC's ticker list
    t = cache.get_snapshot("reference", "sec_tickers", ttl=None)
    return t.rename(columns={"cik_str": "cik", "title": "name", "ticker": "symbol"})


register(Pipeline(
    "reference", "Symbol lists: every Indian equity (NSE + BSE + ISIN), global equities/ETFs/funds, US SEC tickers",
    "snapshot", [
        Source("builtin:symbol_master", _master, ("nsearchives.nseindia.com", "api.bseindia.com"), libs=("bse",),
               score=4.7),
        Source("bse", _bse_list, ("api.bseindia.com",)),
        Source("nselib", _nselib, ("nsearchives.nseindia.com",)),
        Source("builtin:sec", _sec, ("www.sec.gov",), creds=("EDGAR_IDENTITY",), score=4.5),
        Source("financedatabase", _fd, ("raw.githubusercontent.com",)),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=7 * 86400, columns=(),
    params_doc="what=india|us|equities|etfs|funds|indices|cryptos|currencies, country=, sector=, industry=",
    example='fs.fetch("reference", what="india")'))


def _openbb(req):
    from openbb import obb

    route = req.p("route")
    need(route, "route='equity.price.historical' (any OpenBB command path)")
    fn = obb
    for part in route.split("."):
        fn = getattr(fn, part)
    kw = {k: v for k, v in req.params.items() if k not in ("route",)}
    if req.symbol:
        kw.setdefault("symbol", req.symbol)
    return fn(**kw).to_df().reset_index()


register(Pipeline(
    "all_in_one", "OpenBB pass-through: any OpenBB Platform command (last resort for anything not covered)",
    "snapshot", [Source("openbb", _openbb, ())],
    market="raw", needs_symbol=False, normalize=False, ttl=3600, columns=(),
    params_doc="route='equity.price.historical', provider='yfinance', plus that command's arguments",
    example='fs.fetch("all_in_one", "AAPL", route="equity.fundamental.income", provider="yfinance")'))
