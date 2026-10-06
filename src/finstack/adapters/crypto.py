"""Crypto pipeline: daily candles from exchanges first, aggregators after."""
from __future__ import annotations

import datetime as dt

import pandas as pd

from ..core import calendar, validate
from ..core.router import Pipeline, Req, Source, register
from ..loader import get
from ._common import YAHOO_HOSTS, chunks, need, yf_history

COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple", "DOGE": "dogecoin",
                 "ADA": "cardano", "BNB": "binancecoin", "TRX": "tron", "DOT": "polkadot", "MATIC": "matic-network",
                 "POL": "polygon-ecosystem-token", "LTC": "litecoin", "AVAX": "avalanche-2", "LINK": "chainlink",
                 "SHIB": "shiba-inu", "TON": "the-open-network", "USDT": "tether", "USDC": "usd-coin"}
_SYMBOLS = {v: k for k, v in COINGECKO_IDS.items()}
_STABLE_USD = {"USDT", "USDC", "BUSD", "FDUSD"}


def pair(symbol: str) -> tuple:
    """'BTC/USDT', 'BTC-USD', 'BTCUSDT', 'bitcoin' -> (base, quote)."""
    s = symbol.strip()
    if s.lower() in _SYMBOLS:
        return _SYMBOLS[s.lower()], "USD"
    u = s.upper().replace("-", "/")
    if "/" in u:
        a, b = u.split("/", 1)
        return a, b
    for q in ("USDT", "USDC", "USD", "INR", "EUR", "BTC", "ETH"):
        if u.endswith(q) and len(u) > len(q):
            return u[: -len(q)], q
    return u, "USD"


def _ms(d: dt.date) -> int:
    return int(dt.datetime.combine(d, dt.time(), tzinfo=dt.timezone.utc).timestamp() * 1000)


def _ccxt(req):
    import ccxt

    ex = getattr(ccxt, req.p("exchange", "binance"))({"enableRateLimit": True})
    a, b = pair(req.symbol)
    if b == "USD" and ex.id == "binance":
        b = "USDT"
    sym = f"{a}/{b}"
    since, end_ms, rows = _ms(req.start), _ms(req.end + dt.timedelta(days=1)), []
    while since < end_ms:
        batch = ex.fetch_ohlcv(sym, "1d", since=since, limit=1000)
        if not batch:
            break
        rows += batch
        nxt = batch[-1][0] + 86_400_000
        if nxt <= since:
            break
        since = nxt
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    return df[df["date"] < end_ms].assign(market=f"{ex.id}:{sym}")


def _binance(req):
    from binance.client import Client

    a, b = pair(req.symbol)
    b = "USDT" if b == "USD" else b
    k = Client().get_historical_klines(f"{a}{b}", Client.KLINE_INTERVAL_1DAY, req.start.isoformat(),
                                       (req.end + dt.timedelta(days=1)).isoformat())
    df = pd.DataFrame([r[:7] for r in k], columns=["date", "open", "high", "low", "close", "volume", "close_time"])
    return df.drop(columns=["close_time"]).assign(market=f"binance:{a}/{b}")


def _coingecko(req):
    a, b = pair(req.symbol)
    cid = COINGECKO_IDS.get(a) or req.symbol.lower()
    vs = "usd" if b in _STABLE_USD else b.lower()
    cg = get("pycoingecko").CoinGeckoAPI()
    rows = []
    for s, e in chunks(req.start, req.end, 360):       # public API: one year per request
        d = cg.get_coin_market_chart_range_by_id(cid, vs, _ms(s) // 1000, _ms(e + dt.timedelta(days=1)) // 1000)
        prices = pd.DataFrame(d.get("prices") or [], columns=["date", "close"])
        vols = pd.DataFrame(d.get("total_volumes") or [], columns=["date", "volume"])
        rows.append(prices.merge(vols, on="date", how="left"))
    df = pd.concat(rows, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True).dt.normalize()
    return df.groupby("date", as_index=False).last().assign(market=f"coingecko:{cid}/{vs}")


def _cmc(req):
    from cryptocmd import CmcScraper

    a, b = pair(req.symbol)
    df = CmcScraper(a, req.start.strftime("%d-%m-%Y"), req.end.strftime("%d-%m-%Y"),
                    fiat="USD" if b in _STABLE_USD else b).get_dataframe()
    return df.rename(columns={"Market Cap": "market_cap"}).assign(market=f"coinmarketcap:{a}/USD")


def _yahoo(req):
    a, b = pair(req.symbol)
    q = "USD" if b in _STABLE_USD else b
    return yf_history(f"{a}-{q}", req.start, req.end).assign(market=f"yahoo:{a}-{q}")


register(Pipeline(
    "crypto", "Crypto daily OHLCV (UTC days): exchanges first, then CoinGecko / CoinMarketCap / Yahoo", "series", [
        Source("ccxt", _ccxt, ("api.binance.com",)),
        Source("binance", _binance, ("api.binance.com",)),
        Source("pycoingecko", _coingecko, ("api.coingecko.com",)),
        Source("cryptocmd", _cmc, ("api.coinmarketcap.com",)),
        Source("yfinance", _yahoo, YAHOO_HOSTS),
    ],
    market="raw", tz="UTC", check=validate.ohlcv, final=lambda r: calendar.utc_final_through(),
    entity=lambda r: f"CRYPTO:{'/'.join(pair(r.symbol))}:{r.p('exchange', 'binance')}",
    post=lambda df, r: df.assign(symbol="/".join(pair(r.symbol))),
    columns=("date", "open", "high", "low", "close", "volume", "market", "symbol"),
    params_doc="symbol = 'BTC/USDT', 'ETH-USD', 'bitcoin'; exchange='binance' (any ccxt exchange id)",
    example='fs.fetch("crypto", "BTC/USDT", start="3y")'))
