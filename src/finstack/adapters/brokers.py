"""Broker API adapters (used only when you set that broker's credentials).

    UPSTOX_ACCESS_TOKEN                                   Upstox (free with an account)
    KITE_API_KEY, KITE_ACCESS_TOKEN                       Zerodha Kite Connect (historical data is paid)
    DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN                     Dhan
    ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PIN, ANGEL_TOTP_SECRET   Angel One SmartAPI
    FYERS_CLIENT_ID, FYERS_ACCESS_TOKEN                   Fyers (separate extra: finstack[fyers])

Broker data is the official exchange feed via your own account; finstack never shares it.
Instrument lists (tokens / security ids) are downloaded once a day and cached.
"""
from __future__ import annotations

import datetime as dt
import os
import threading
import time
from functools import lru_cache
from typing import Optional

import pandas as pd

from ..core import cache
from ..core.router import Req
from ..core.symbols import NSE_DERIV_INDEX, Instrument
from ._common import TMP, chunks, env, need

_lock = threading.Lock()

# ------------------------------------------------------------------ Upstox
UPSTOX_HOSTS = ("api.upstox.com",)


def upstox(api: str):
    import upstox_client

    cfg = upstox_client.Configuration()
    cfg.access_token = env("UPSTOX_ACCESS_TOKEN")
    return getattr(upstox_client, api)(upstox_client.ApiClient(cfg))


def upstox_key(inst: Instrument) -> str:
    if inst.kind == "index":
        need(inst.upstox_index, f"no Upstox key known for index {inst.nse}")
        return inst.upstox_index
    if inst.isin:
        return f"NSE_EQ|{inst.isin}" if inst.nse else f"BSE_EQ|{inst.isin}"
    raise LookupError(f"ISIN unknown for {inst.query} (symbol master unavailable)")


def _candles(resp) -> pd.DataFrame:
    d = resp.to_dict() if hasattr(resp, "to_dict") else resp
    rows = (d.get("data") or {}).get("candles") or []
    df = pd.DataFrame([r[:7] for r in rows], columns=["ts", "open", "high", "low", "close", "volume", "oi"][
        :len(rows[0][:7])] if rows else ["ts"])
    return df


def upstox_daily(req: Req) -> pd.DataFrame:
    api, key = upstox("HistoryV3Api"), upstox_key(req.inst)
    frames = [_candles(api.get_historical_candle_data1(key, "days", "1", b.isoformat(), a.isoformat()))
              for a, b in chunks(req.start, req.end, 3650)]
    df = pd.concat(frames, ignore_index=True).rename(columns={"ts": "date"})
    return df


def upstox_intraday(req: Req, minutes: int) -> pd.DataFrame:
    api, key = upstox("HistoryV3Api"), upstox_key(req.inst)
    frames = []
    today = dt.date.today()
    if req.end >= today:
        frames.append(_candles(api.get_intra_day_candle_data(key, "minutes", str(minutes))))
    hist_end = min(req.end, today - dt.timedelta(days=1))
    if req.start <= hist_end:
        for a, b in chunks(req.start, hist_end, 28):          # Upstox: 1 month per minute-candle request
            frames.append(_candles(api.get_historical_candle_data1(key, "minutes", str(minutes),
                                                                    b.isoformat(), a.isoformat())))
    return pd.concat(frames, ignore_index=True)


def upstox_quotes(keys: list) -> dict:
    api = upstox("MarketQuoteV3Api")
    d = api.get_market_quote_ohlc("1d", instrument_key=",".join(keys)).to_dict()
    return d.get("data") or {}


# ------------------------------------------------------------------ Zerodha Kite
KITE_HOSTS = ("api.kite.trade",)


@lru_cache(maxsize=1)
def kite():
    from kiteconnect import KiteConnect

    k = KiteConnect(api_key=env("KITE_API_KEY"))
    k.set_access_token(env("KITE_ACCESS_TOKEN"))
    return k


def kite_token(inst: Instrument, exchange: str = "NSE") -> int:
    table = cache.get_snapshot("brokers", f"kite_instruments_{exchange}", ttl=86400)
    if not isinstance(table, pd.DataFrame):
        table = pd.DataFrame(kite().instruments(exchange))
        cache.put_snapshot("brokers", f"kite_instruments_{exchange}", table)
    name = inst.nse if inst.kind == "index" else (inst.nse or "")
    hit = table[table["tradingsymbol"].astype(str).str.upper() == name.upper()]
    need(len(hit), f"{name} not in Kite's {exchange} instrument list")
    return int(hit.iloc[0]["instrument_token"])


def kite_history(req: Req, interval: str = "day", max_days: int = 1900) -> pd.DataFrame:
    tok = kite_token(req.inst)
    rows = []
    for a, b in chunks(req.start, req.end, max_days):
        rows += kite().historical_data(tok, a, b, interval)
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ Dhan
DHAN_HOSTS = ("api.dhan.co",)
_DHAN_INDEX = {"NIFTY 50": "13", "NIFTY BANK": "25", "NIFTY FINANCIAL SERVICES": "27", "SENSEX": "51",
               "NIFTY MIDCAP SELECT": "442", "INDIA VIX": "21"}


@lru_cache(maxsize=1)
def dhan():
    from dhanhq import DhanContext, dhanhq

    return dhanhq(DhanContext(env("DHAN_CLIENT_ID"), env("DHAN_ACCESS_TOKEN")))


def dhan_security(inst: Instrument) -> tuple:
    """(security_id, exchange_segment, instrument_type)"""
    if inst.kind == "index":
        need(inst.nse in _DHAN_INDEX, f"no Dhan id known for {inst.nse}")
        return _DHAN_INDEX[inst.nse], "IDX_I", "INDEX"
    table = cache.get_snapshot("brokers", "dhan_security_list", ttl=86400)
    if not isinstance(table, pd.DataFrame):
        from dhanhq import dhanhq as D

        os.makedirs(TMP, exist_ok=True)
        table = D.fetch_security_list("compact", os.path.join(TMP, "dhan_security_list.csv"))
        need(isinstance(table, pd.DataFrame), "Dhan security list download failed")
        cache.put_snapshot("brokers", "dhan_security_list", table)
    t = table
    m = (t["SEM_EXM_EXCH_ID"].astype(str) == "NSE") & (t["SEM_TRADING_SYMBOL"].astype(str) == inst.nse) & (
        t["SEM_INSTRUMENT_NAME"].astype(str) == "EQUITY")
    need(m.any(), f"{inst.nse} not in Dhan's security list")
    return str(t[m].iloc[0]["SEM_SMST_SECURITY_ID"]), "NSE_EQ", "EQUITY"


def _dhan_frame(resp) -> pd.DataFrame:
    if isinstance(resp, dict) and resp.get("status") not in (None, "success"):
        raise RuntimeError(str(resp.get("remarks") or resp)[:200])
    data = resp.get("data", resp) if isinstance(resp, dict) else resp
    df = pd.DataFrame(data)
    if "timestamp" in df:
        df = df.rename(columns={"timestamp": "ts"})
    return df


def dhan_daily(req: Req) -> pd.DataFrame:
    sid, seg, typ = dhan_security(req.inst)
    frames = [_dhan_frame(dhan().historical_daily_data(sid, seg, typ, a.isoformat(), b.isoformat()))
              for a, b in chunks(req.start, req.end, 365 * 5)]
    return pd.concat(frames, ignore_index=True).rename(columns={"ts": "date"})


def dhan_intraday(req: Req, minutes: int) -> pd.DataFrame:
    sid, seg, typ = dhan_security(req.inst)
    frames = [_dhan_frame(dhan().intraday_minute_data(sid, seg, typ, f"{a} 09:15:00", f"{b} 15:30:00",
                                                      interval=minutes))
              for a, b in chunks(req.start, req.end, 89)]
    return pd.concat(frames, ignore_index=True)


# ------------------------------------------------------------------ Angel One
ANGEL_HOSTS = ("apiconnect.angelone.in", "apiconnect.angelbroking.com", "margincalculator.angelbroking.com")
ANGEL_MASTER = "https://margincalculator.angelbroking.com/OpenAPI_files/OpenAPIScripMaster.json"
_angel = {"obj": None, "at": 0.0}


def angel():
    with _lock:
        if _angel["obj"] is None or time.time() - _angel["at"] > 6 * 3600:
            import pyotp
            from SmartApi import SmartConnect

            obj = SmartConnect(api_key=env("ANGEL_API_KEY"))
            s = obj.generateSession(env("ANGEL_CLIENT_CODE"), env("ANGEL_PIN"),
                                    pyotp.TOTP(env("ANGEL_TOTP_SECRET")).now())
            need(s and s.get("status"), f"Angel One login failed: {s}")
            _angel.update(obj=obj, at=time.time())
        return _angel["obj"]


def angel_token(inst: Instrument) -> tuple:
    table = cache.get_snapshot("brokers", "angel_scrip_master", ttl=86400)
    if not isinstance(table, pd.DataFrame):
        import requests

        r = requests.get(ANGEL_MASTER, timeout=60)
        r.raise_for_status()
        table = pd.DataFrame(r.json())
        cache.put_snapshot("brokers", "angel_scrip_master", table)
    t = table[table["exch_seg"] == "NSE"]
    if inst.kind == "index":
        name = NSE_DERIV_INDEX.get(inst.nse, inst.nse)
        hit = t[(t["name"].astype(str).str.upper() == name.upper()) & (t["instrumenttype"] == "AMXIDX")]
    else:
        hit = t[t["symbol"] == f"{inst.nse}-EQ"]
    need(len(hit), f"{inst.query} not in Angel One's scrip master")
    return str(hit.iloc[0]["token"]), str(hit.iloc[0]["symbol"])


def angel_candles(req: Req, interval: str, max_days: int) -> pd.DataFrame:
    tok, _ = angel_token(req.inst)
    rows = []
    for a, b in chunks(req.start, req.end, max_days):
        r = angel().getCandleData({"exchange": "NSE", "symboltoken": tok, "interval": interval,
                                   "fromdate": f"{a} 09:00", "todate": f"{b} 15:30"})
        need(r and r.get("status"), f"Angel One: {r}")
        rows += r.get("data") or []
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


# ------------------------------------------------------------------ Fyers
FYERS_HOSTS = ("api-t1.fyers.in",)


@lru_cache(maxsize=1)
def fyers():
    from fyers_apiv3 import fyersModel

    os.makedirs(TMP, exist_ok=True)
    return fyersModel.FyersModel(client_id=env("FYERS_CLIENT_ID"), token=env("FYERS_ACCESS_TOKEN"),
                                 is_async=False, log_path=TMP)


def fyers_symbol(inst: Instrument) -> str:
    if inst.kind == "index":
        names = {"NIFTY 50": "NSE:NIFTY50-INDEX", "NIFTY BANK": "NSE:NIFTYBANK-INDEX",
                 "NIFTY FINANCIAL SERVICES": "NSE:FINNIFTY-INDEX", "SENSEX": "BSE:SENSEX-INDEX",
                 "INDIA VIX": "NSE:INDIAVIX-INDEX"}
        need(inst.nse in names, f"no Fyers symbol known for {inst.nse}")
        return names[inst.nse]
    return f"NSE:{inst.nse}-EQ"


def fyers_history(req: Req, resolution: str, max_days: int) -> pd.DataFrame:
    rows = []
    for a, b in chunks(req.start, req.end, max_days):
        r = fyers().history({"symbol": fyers_symbol(req.inst), "resolution": resolution, "date_format": "1",
                             "range_from": a.isoformat(), "range_to": b.isoformat(), "cont_flag": "1"})
        need(r.get("s") == "ok" or r.get("candles") is not None, f"Fyers: {r}")
        rows += r.get("candles") or []
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
