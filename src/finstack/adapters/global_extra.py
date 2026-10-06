"""Analyst views for Indian and global stocks (beta until verified live): recommendations, price targets,
earnings estimates and earnings history. Yahoo's most rate-limited pages, so: 1 request/second through
the governor and cached for a day.
"""
from __future__ import annotations

import pandas as pd

from ..core.router import Pipeline, Req, Source, register
from ..loader import get
from ._common import YAHOO_HOSTS, need

WHAT = ("recommendations", "price_targets", "earnings_estimate", "revenue_estimate", "earnings_history",
        "upgrades_downgrades")


def yahoo_ticker(req: Req) -> str:
    """Indian NSE symbols by default (TCS -> TCS.NS); Yahoo-style tickers pass through (AAPL with
    market='US', VOD.L, 7203.T, RELIANCE.BO)."""
    s = str(req.symbol).strip().upper()
    market = str(req.p("market", "IN")).upper()
    if "." in s or "^" in s or "=" in s or market != "IN":
        return s
    return f"{s}.NS"


def _what(req) -> str:
    w = str(req.p("what", "recommendations"))
    need(w in WHAT, f"what must be one of {WHAT}")
    return w


def _frame(obj) -> pd.DataFrame:
    if obj is None:
        return pd.DataFrame()
    if isinstance(obj, dict):
        return pd.DataFrame([obj])
    if isinstance(obj, pd.DataFrame):
        return obj.reset_index() if not isinstance(obj.index, pd.RangeIndex) else obj
    return pd.DataFrame(obj)


def _a_yahoo(req):
    t = get("yfinance").Ticker(yahoo_ticker(req))
    w = _what(req)
    attr = {"recommendations": "recommendations", "price_targets": "analyst_price_targets",
            "earnings_estimate": "earnings_estimate", "revenue_estimate": "revenue_estimate",
            "earnings_history": "earnings_history", "upgrades_downgrades": "upgrades_downgrades"}[w]
    return _frame(getattr(t, attr))


def _a_yq(req):
    tk = yahoo_ticker(req)
    t = get("yahooquery").Ticker(tk)
    w = _what(req)
    if w == "recommendations":
        df = t.recommendation_trend
    elif w == "price_targets":
        fd = t.financial_data.get(tk, {})
        need(isinstance(fd, dict), str(fd)[:200])
        df = pd.DataFrame([{"current": fd.get("currentPrice"), "low": fd.get("targetLowPrice"),
                            "high": fd.get("targetHighPrice"), "mean": fd.get("targetMeanPrice"),
                            "median": fd.get("targetMedianPrice"),
                            "analysts": fd.get("numberOfAnalystOpinions"),
                            "recommendation": fd.get("recommendationKey")}])
    elif w == "earnings_history":
        df = t.earning_history
    else:
        raise LookupError(f"yahooquery route has no '{w}'")
    need(isinstance(df, pd.DataFrame), str(df)[:200])
    return _frame(df)


def _post(df, req):
    return df.assign(symbol=yahoo_ticker(req), what=_what(req))


register(Pipeline(
    "analyst_estimates", "Analyst recommendations, price targets, earnings/revenue estimates, earnings surprises",
    "snapshot", [
        Source("yfinance", _a_yahoo, YAHOO_HOSTS),
        Source("yahooquery", _a_yq, YAHOO_HOSTS),
    ],
    market="raw", normalize=False, ttl=86400, post=_post, status="beta", columns=(),
    params_doc="symbol (NSE symbol, or Yahoo ticker with market='US'), what=recommendations|price_targets|"
               "earnings_estimate|revenue_estimate|earnings_history|upgrades_downgrades",
    example='fs.fetch("analyst_estimates", "INFY", what="price_targets")'))
