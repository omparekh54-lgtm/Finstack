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


# =====================================================================================================
# Phase 3: ownership, insiders, dividends, company news (India + global)
# (ESG was dropped: Yahoo stopped serving ESG scores and no other free source exists)
# =====================================================================================================
import datetime as dt  # noqa: E402
import re  # noqa: E402

from ._common import NSE_HOSTS, env, nse, to_records  # noqa: E402
from .news import _PUBLISHER_HOSTS  # noqa: E402

SEC_HOSTS = ("data.sec.gov", "www.sec.gov")


def indian(req) -> bool:
    s = str(req.symbol).upper()
    return str(req.p("market", "IN")).upper() == "IN" and ("." not in s or s.endswith((".NS", ".BO")))


def nse_symbol(req) -> str:
    return str(req.symbol).upper().replace(".NS", "").replace(".BO", "")


def _tag(df, req, what=None):
    df = df.assign(symbol=yahoo_ticker(req) if not indian(req) else nse_symbol(req))
    return df.assign(what=what) if what else df


# =============================================================== holders
def _h_yahoo(req):
    w = str(req.p("what", "institutional"))
    attr = {"institutional": "institutional_holders", "mutual_fund": "mutualfund_holders",
            "major": "major_holders"}.get(w)
    need(attr, "what must be institutional, mutual_fund or major")
    return _frame(getattr(get("yfinance").Ticker(yahoo_ticker(req)), attr))


register(Pipeline(
    "holders", "Institutional / mutual-fund / major holders of a stock (best for US; Indian coverage is partial - "
               "for India's official pattern use india_corporate_events what='shareholding')", "snapshot",
    [Source("yfinance", _h_yahoo, YAHOO_HOSTS)],
    market="raw", normalize=False, ttl=86400, post=lambda df, r: _tag(df, r, r.p("what", "institutional")),
    status="beta", columns=(),
    params_doc="symbol, what=institutional|mutual_fund|major, market='US' for US tickers",
    example='fs.fetch("holders", "AAPL", market="US")'))


# =============================================================== insider trades
def _i_nse(req):
    n = nse()
    end = req.end or dt.date.today()
    start = req.start or end - dt.timedelta(days=365)
    sym = nse_symbol(req)
    url = f"{n.base_url}/corporates-pit"
    params = {"index": "equities", "symbol": sym, "from_date": start.strftime("%d-%m-%Y"),
              "to_date": end.strftime("%d-%m-%Y")}
    rows = _pit_rows(n._transport.request(url, params=params).json())
    if not rows:     # some date windows come back blank; NSE's own default window is the second opinion
        rows = _pit_rows(n._transport.request(url, params={"index": "equities", "symbol": sym}).json())
    need(rows, f"NSE lists no insider-trading disclosures for {sym} between {start} and {end} "
               f"(normal for companies whose insiders rarely trade)")
    df = pd.DataFrame(rows)
    if "symbol" in df:
        df = df[df["symbol"].astype(str).str.upper() == sym]
    return df


def _pit_rows(data) -> list:
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return data["data"]
    return to_records(data)


_NSE_PIT = {"acqName": "insider", "personCategory": "category", "secType": "security", "secAcq": "quantity",
            "secVal": "value", "tdpTransactionType": "transaction", "acqfromDt": "date_from", "acqtoDt": "date_to",
            "intimDt": "reported", "befAcqSharesNo": "shares_before", "afterAcqSharesNo": "shares_after",
            "acqMode": "mode", "company": "company"}


def _i_yahoo(req):
    df = _frame(get("yfinance").Ticker(yahoo_ticker(req)).insider_transactions)
    return df.rename(columns={"Insider": "insider", "Position": "category", "Transaction": "transaction",
                              "Text": "details", "Start Date": "date_from", "Shares": "quantity", "Value": "value",
                              "Ownership": "ownership"})


def _i_edgar(req):
    ed = get("edgar")
    ed.set_identity(env("EDGAR_IDENTITY"))
    filings = ed.Company(yahoo_ticker(req)).get_filings(form="4").head(int(req.p("n", 20)))
    frames = []
    for f in filings:
        try:
            frames.append(f.obj().to_dataframe())
        except Exception:  # noqa: BLE001 - skip unreadable filings
            continue
    need(frames, "no readable Form 4 filings")
    return pd.concat(frames, ignore_index=True)


def _post_insider(df, req):
    df = df.rename(columns={k: v for k, v in _NSE_PIT.items() if k in df.columns})
    for c in ("quantity", "value", "shares_before", "shares_after"):
        if c in df:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")
    return _tag(df, req)


register(Pipeline(
    "insider_trades", "Insider / promoter buying and selling (India: NSE insider-trading disclosures; US: SEC Form 4)",
    "snapshot", [
        Source("builtin:nse_insider", _i_nse, NSE_HOSTS, libs=("nse",), score=4.5, when=indian),
        Source("yfinance", _i_yahoo, YAHOO_HOSTS, score=3.5),
        Source("edgar", _i_edgar, SEC_HOSTS, score=4.0, when=lambda r: not indian(r)),
    ],
    market="raw", normalize=False, ttl=6 * 3600, post=_post_insider, status="beta", columns=(),
    params_doc="symbol (NSE symbol; US ticker with market='US'), start, end (India, default 1 year), n=20 (SEC)",
    example='fs.fetch("insider_trades", "TCS", start="1y")'))


# =============================================================== US fund holdings (13F)
def _f_edgar(req):
    ed = get("edgar")
    ed.set_identity(env("EDGAR_IDENTITY"))
    filing = ed.Company(str(req.symbol)).latest("13F-HR")
    need(filing is not None, "no 13F-HR filing found")
    table = filing.obj().infotable
    need(isinstance(table, pd.DataFrame) and len(table), "13F information table is empty")
    return table.assign(report_period=getattr(filing, "period_of_report", None),
                        filed=getattr(filing, "filing_date", None))


register(Pipeline(
    "us_fund_holdings", "What a US fund manager holds (latest SEC 13F), e.g. Berkshire Hathaway", "snapshot",
    [Source("edgar", _f_edgar, SEC_HOSTS)],
    market="raw", normalize=False, ttl=7 * 86400, status="beta", columns=(),
    params_doc="symbol = the fund's CIK or ticker (e.g. '1067983' or 'BRK-A'); needs EDGAR_IDENTITY",
    example='fs.fetch("us_fund_holdings", "1067983")'))


# =============================================================== dividends and splits
_DIV_AMOUNT = re.compile(r"(?:rs\.?|re\.?|inr|₹)\s*-?\s*(\d+(?:\.\d+)?)", re.I)


def _dv_yahoo(req):
    t = get("yfinance").Ticker(yahoo_ticker(req))
    if req.p("what", "dividends") == "splits":
        s = t.splits
        return s.rename("split_ratio").rename_axis("date").reset_index()
    s = t.dividends
    return s.rename("dividend").rename_axis("date").reset_index()


def _dv_nse(req):
    need(req.p("what", "dividends") == "dividends", "NSE route: dividends")
    from ..core.router import fetch

    acts = fetch("india_corporate_events", nse_symbol(req), what="actions", start=req.start or "20y")
    acts = acts[acts["purpose"].astype(str).str.contains("dividend", case=False)]
    amt = acts["purpose"].astype(str).map(lambda p: sum(float(x) for x in _DIV_AMOUNT.findall(p)) or None)
    return pd.DataFrame({"date": acts["ex_date"], "dividend": amt, "details": acts["purpose"]})


def _post_div(df, req):
    df = df.copy()
    d = df["date"]
    if not pd.api.types.is_datetime64_any_dtype(d):
        d = pd.to_datetime(d, errors="coerce", dayfirst=True, format="mixed")
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)          # keep the exchange's own calendar date
    df["date"] = d.dt.normalize()
    if req.start:
        df = df[df["date"] >= pd.Timestamp(req.start)]
    return _tag(df.dropna(subset=["date"]).sort_values("date"), req)


register(Pipeline(
    "dividends", "Full dividend history (amount per share, ex-date) or split history", "snapshot", [
        Source("yfinance", _dv_yahoo, YAHOO_HOSTS, score=4.0),
        Source("nse", _dv_nse, NSE_HOSTS, score=3.5, when=indian),
    ],
    market="raw", normalize=False, ttl=86400, post=_post_div, status="beta", columns=("date",),
    params_doc="symbol, what=dividends|splits, start (optional), market='US' for US tickers",
    example='fs.fetch("dividends", "ITC")'))


# =============================================================== company news
def _yahoo_news_rows(items) -> list:
    rows = []
    for it in items or []:
        c = it.get("content", it) if isinstance(it, dict) else {}
        url = (c.get("canonicalUrl") or {}).get("url") if isinstance(c.get("canonicalUrl"), dict) else c.get("link")
        rows.append({"published": c.get("pubDate") or c.get("providerPublishTime"), "title": c.get("title"),
                     "link": url, "publisher": (c.get("provider") or {}).get("displayName")
                     if isinstance(c.get("provider"), dict) else c.get("publisher"),
                     "summary": c.get("summary")})
    return rows


def company_words(req) -> list:
    """Words a headline about this company would contain: the short company name and the symbol."""
    words = []
    q = req.p("query")
    if q:
        return [str(q)]
    if indian(req):
        try:
            from ..core.symbols import resolve

            name = resolve(nse_symbol(req)).name or ""
        except Exception:  # noqa: BLE001 - symbol master unavailable: fall back to the symbol
            name = ""
        name = re.sub(r"\b(limited|ltd\.?|india|corporation|company|co\.?|the)\b", " ", name, flags=re.I)
        name = re.sub(r"[^A-Za-z0-9&' ]+", " ", name)
        name = " ".join(name.split()[:3])
        if len(name) >= 3:
            words.append(name)
    words.append(nse_symbol(req) if indian(req) else str(req.symbol).upper())
    return words


def _n_yahoo(req):
    t = get("yfinance").Ticker(yahoo_ticker(req))
    n = int(req.p("n", 30))
    getter = getattr(t, "get_news", None)
    try:
        items = getter(count=n, tab="all") if getter else t.news
    except TypeError:                     # older yfinance without count= / tab=
        items = t.news
    return pd.DataFrame(_yahoo_news_rows(items))


def _n_yahoo_search(req):
    """Yahoo's search box: finds stories by company name (better for Indian stocks than the ticker feed)."""
    yf = get("yfinance")
    rows = []
    for w in company_words(req)[:1]:
        try:
            found = yf.Search(w, max_results=1, news_count=int(req.p("n", 30)), timeout=20)
        except TypeError:                 # older yfinance without timeout=
            found = yf.Search(w, max_results=1, news_count=int(req.p("n", 30)))
        rows += _yahoo_news_rows(found.news)
    return pd.DataFrame(rows)


def _google_rss_url(req, words) -> str:
    from urllib.parse import quote_plus

    q = " OR ".join(f'"{w}"' for w in words)
    if indian(req):
        return f"https://news.google.com/rss/search?q={quote_plus(q + ' when:7d')}&hl=en-IN&gl=IN&ceid=IN:en"
    return f"https://news.google.com/rss/search?q={quote_plus(q + ' when:7d')}&hl=en-US&gl=US&ceid=US:en"


def _n_google_rss(req):
    """Google News search feed read directly (with a real timeout, unlike the gnews package)."""
    import feedparser
    import requests

    r = requests.get(_google_rss_url(req, company_words(req)), timeout=15,
                     headers={"User-Agent": "Mozilla/5.0 finstack"})
    r.raise_for_status()
    rows = []
    for e in feedparser.parse(r.content).entries[: int(req.p("n", 30))]:
        src = e.get("source")
        rows.append({"published": e.get("published"), "title": e.get("title"), "link": e.get("link"),
                     "publisher": src.get("title") if isinstance(src, dict) else None,
                     "summary": None})
    return pd.DataFrame(rows)


def _n_publisher_rss(req):
    """Indian (or US) publishers' RSS feeds, keeping headlines that mention the company."""
    from ..api import news

    feeds = ("business_standard", "economic_times", "moneycontrol", "livemint") if indian(req) else (
        "marketwatch", "cnbc", "wsj")
    frames = [news(sources=feeds, query=w) for w in company_words(req)]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return df.rename(columns={"source": "publisher"})


def _n_gnews(req):
    q = company_words(req)[0]
    items = get("gnews").GNews(language="en", country="IN" if indian(req) else "US", period=req.p("period", "7d"),
                               max_results=int(req.p("n", 30))).get_news(q)
    return pd.DataFrame([{"published": i.get("published date"), "title": i.get("title"), "link": i.get("url"),
                          "publisher": (i.get("publisher") or {}).get("title"), "summary": i.get("description")}
                         for i in items])


def _post_news(df, req):
    df = df.copy()
    p = df["published"]
    df["published"] = (pd.to_datetime(p, unit="s", utc=True, errors="coerce") if pd.api.types.is_numeric_dtype(p)
                       else pd.to_datetime(p, utc=True, errors="coerce", format="mixed"))
    df = df[df["title"].notna()].drop_duplicates("title")
    return _tag(df.sort_values("published", ascending=False), req)


register(Pipeline(
    "company_news", "Latest headlines about one company", "snapshot", [
        Source("yfinance", _n_yahoo, YAHOO_HOSTS, score=4.0),
        Source("yfinance:search", _n_yahoo_search, YAHOO_HOSTS, libs=("yfinance",), score=3.9),
        Source("builtin:google_news_rss", _n_google_rss, ("news.google.com",), libs=("feedparser",), score=3.8),
        Source("feedparser", _n_publisher_rss, _PUBLISHER_HOSTS, score=3.6),
        Source("gnews", _n_gnews, ("news.google.com",), score=3.0),
    ],
    market="raw", normalize=False, ttl=1800, post=_post_news, status="beta",
    columns=("published", "title", "publisher", "link", "summary"),
    params_doc="symbol, market='US' for US tickers, n=30, query= (override the search words; default = company name)",
    example='fs.fetch("company_news", "INFY")'))
