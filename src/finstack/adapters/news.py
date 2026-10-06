"""News and alternative-data pipelines."""
from __future__ import annotations

import datetime as dt

import pandas as pd

from ..core import validate
from ..core.router import Pipeline, Req, Source, register
from ..loader import get
from ._common import env, need

NEWS_COLS = ("published", "title", "source", "link", "summary", "provider")
_PUBLISHER_HOSTS = ("www.business-standard.com", "economictimes.indiatimes.com", "www.moneycontrol.com",
                    "www.livemint.com", "feeds.content.dowjones.io", "www.cnbc.com", "feeds.a.dj.com")


def _q(req):
    return req.p("query") or req.symbol


def _country(req):
    return str(req.p("country", "IN")).upper()


def _rss(req):
    from ..api import news

    default = ("business_standard", "economic_times", "moneycontrol", "livemint") if _country(req) == "IN" else (
        "marketwatch", "cnbc", "wsj")
    df = news(sources=req.p("feeds", default), query=_q(req))
    need(len(df) or not df.attrs.get("failed"), "; ".join(df.attrs.get("failed", []))[:200])
    return df.rename(columns={"source": "source"}).assign(provider="rss")


def _gnews(req):
    g = get("gnews").GNews(language=req.p("language", "en"), country=_country(req), period=req.p("period", "7d"),
                           max_results=int(req.p("n", 50)))
    items = g.get_news(_q(req)) if _q(req) else g.get_top_news()
    return pd.DataFrame([{"published": i.get("published date"), "title": i.get("title"), "link": i.get("url"),
                          "summary": i.get("description"), "source": (i.get("publisher") or {}).get("title")}
                         for i in items]).assign(provider="google news")


def _gdelt(req):
    need(_q(req), "GDELT needs query=")
    g = get("gdeltdoc")
    f = g.Filters(keyword=_q(req), timespan=req.p("period", "7d"), num_records=int(req.p("n", 250)),
                  country=req.p("gdelt_country"))
    df = g.GdeltDoc().article_search(f)
    need(df is not None and len(df), "GDELT returned no articles")
    return df.rename(columns={"seendate": "published", "url": "link", "domain": "source"}).assign(
        provider="gdelt", summary=None)


def _googlenews(req):
    need(_q(req), "GoogleNews needs query=")
    gn = get("GoogleNews").GoogleNews(lang=req.p("language", "en"), region=_country(req), period=req.p("period", "7d"))
    gn.search(_q(req))
    rows = gn.results()
    return pd.DataFrame([{"published": r.get("datetime") or r.get("date"), "title": r.get("title"),
                          "link": r.get("link"), "summary": r.get("desc"), "source": r.get("media")}
                         for r in rows]).assign(provider="googlenews")


def _finviz(req):
    need(req.symbol, "finviz news needs a US ticker symbol")
    from finvizfinance.quote import finvizfinance

    df = finvizfinance(req.symbol.upper()).ticker_news()
    return df.rename(columns={"Date": "published", "Title": "title", "Link": "link", "Source": "source"}).assign(
        provider="finviz", summary=None)


def _newsapi(req):
    from newsapi import NewsApiClient

    c = NewsApiClient(api_key=env("NEWSAPI_KEY"))
    j = c.get_everything(q=_q(req), language=req.p("language", "en"), sort_by="publishedAt", page_size=100) if _q(
        req) else c.get_top_headlines(country=_country(req).lower(), category="business", page_size=100)
    return pd.DataFrame([{"published": a.get("publishedAt"), "title": a.get("title"), "link": a.get("url"),
                          "summary": a.get("description"), "source": (a.get("source") or {}).get("name")}
                         for a in j.get("articles", [])]).assign(provider="newsapi")


def _post_news(df, req):
    df = df.copy()
    df["published"] = pd.to_datetime(df["published"], errors="coerce", utc=True, format="mixed")
    key = df["title"].astype(str).str.lower().str.replace(r"[^a-z0-9]+", " ", regex=True).str.strip()
    df = df[~key.duplicated() & df["title"].notna()]
    return df.sort_values("published", ascending=False)


register(Pipeline(
    "news", "Market news headlines (RSS from Indian/global publishers, Google News, GDELT ...)", "snapshot", [
        Source("feedparser", _rss, _PUBLISHER_HOSTS),
        Source("gnews", _gnews, ("news.google.com",)),
        Source("gdeltdoc", _gdelt, ("api.gdeltproject.org",)),
        Source("finvizfinance", _finviz, ("finviz.com",), when=lambda r: bool(r.symbol)),
        Source("GoogleNews", _googlenews, ("news.google.com", "www.google.com")),
        Source("newsapi", _newsapi, ("newsapi.org",)),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=600, post=_post_news, columns=NEWS_COLS,
    params_doc="query='...', country=IN|US, period='7d', n=50, feeds=(RSS names or URLs); symbol for finviz",
    example='fs.fetch("news", query="RBI policy")'))


# =============================================================== alternative data
def _alt(req):
    return req.p("what", "trends")


def _trends(req):
    kw = req.p("query") or req.symbol
    need(kw, "trends needs query=")
    kws = [kw] if isinstance(kw, str) else list(kw)
    df = get("trendspy").Trends().interest_over_time(kws, timeframe=req.p("timeframe", "today 12-m"),
                                                    geo=req.p("geo", ""))
    return df.reset_index().rename(columns={"time": "date", "index": "date"})


def _reddit(req):
    import praw

    r = praw.Reddit(client_id=env("REDDIT_CLIENT_ID"), client_secret=env("REDDIT_CLIENT_SECRET"),
                    user_agent="finstack (research)")
    sub = r.subreddit(req.p("subreddit", "IndianStreetBets"))
    posts = sub.search(req.p("query"), limit=int(req.p("n", 100))) if req.p("query") else sub.hot(
        limit=int(req.p("n", 100)))
    return pd.DataFrame([{"date": pd.to_datetime(p.created_utc, unit="s", utc=True), "title": p.title,
                          "score": p.score, "comments": p.num_comments, "link": f"https://reddit.com{p.permalink}"}
                         for p in posts])


def _gdelt_tl(req):
    kw = req.p("query") or req.symbol
    need(kw, "gdelt timeline needs query=")
    g = get("gdeltdoc")
    f = g.Filters(keyword=kw, timespan=req.p("period", "3m"), country=req.p("gdelt_country"))
    return g.GdeltDoc().timeline_search(req.p("mode", "timelinevolraw"), f)


register(Pipeline(
    "alt_data", "Alternative data: Google Trends, Reddit posts, GDELT news-volume timelines", "snapshot", [
        Source("trendspy", _trends, ("trends.google.com",), when=lambda r: _alt(r) == "trends"),
        Source("praw", _reddit, ("oauth.reddit.com", "www.reddit.com"), when=lambda r: _alt(r) == "reddit"),
        Source("gdeltdoc", _gdelt_tl, ("api.gdeltproject.org",), when=lambda r: _alt(r) == "gdelt"),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=6 * 3600, columns=(),
    params_doc="what=trends|reddit|gdelt, query='...', geo='IN', timeframe='today 12-m', subreddit=...",
    example='fs.fetch("alt_data", what="trends", query="Nifty", geo="IN")'))
