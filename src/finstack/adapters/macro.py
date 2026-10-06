"""Macro, interest-rate and currency pipelines."""
from __future__ import annotations

import datetime as dt
import io

import pandas as pd

from ..core import calendar, validate
from ..core.router import Pipeline, Req, Source, register
from ..loader import get
from ._common import YAHOO_HOSTS, bars_since, chunks, concat, env, need, tv_bars, yf_history

# =============================================================== macro series
PREFIXES = ("fred", "wb", "ecb", "estat", "dbn", "imf")


def split_id(series: str) -> tuple:
    """'fred:DGS10' -> ('fred', 'DGS10'); bare ids: with '/' -> DBnomics, else FRED."""
    s = str(series).strip()
    if ":" in s and s.split(":", 1)[0].lower() in PREFIXES:
        p, rest = s.split(":", 1)
        p, rest = p.lower(), rest.strip()
        if p == "imf":                     # IMF series are read through DBnomics' mirror
            return "dbn", rest if rest.upper().startswith("IMF/") else f"IMF/{rest}"
        return p, rest
    return ("dbn" if "/" in s else "fred"), s


def _is(*prefixes):
    return lambda req: split_id(req.symbol)[0] in prefixes


def _fred_api(req):
    _, sid = split_id(req.symbol)
    s = get("fredapi").Fred(api_key=env("FRED_API_KEY")).get_series(sid, observation_start=req.start,
                                                                     observation_end=req.end)
    return s.rename("value").rename_axis("date").reset_index()


def _fred_csv(req):
    import requests

    _, sid = split_id(req.symbol)
    r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                     params={"id": sid, "cosd": req.start.isoformat(), "coed": req.end.isoformat()}, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    need(df.shape[1] >= 2, "FRED CSV had no data column")
    return df.rename(columns={df.columns[0]: "date", df.columns[1]: "value"})


def _fred_pdr(req):
    _, sid = split_id(req.symbol)
    df = get("pandas_datareader").DataReader(sid, "fred", req.start, req.end)
    return df.rename(columns={sid: "value"}).rename_axis("date").reset_index()


def _wb_parts(req):
    _, rest = split_id(req.symbol)
    ind, _, country = rest.partition("/")
    return ind, (country or "IND")


def _wb_api(req):
    import requests

    ind, country = _wb_parts(req)
    r = requests.get(f"https://api.worldbank.org/v2/country/{country}/indicator/{ind}",
                     params={"format": "json", "per_page": 20000, "date": f"{req.start.year}:{req.end.year}"},
                     timeout=30)
    r.raise_for_status()
    j = r.json()
    need(isinstance(j, list) and len(j) > 1 and j[1], f"World Bank: {str(j)[:200]}")
    return pd.DataFrame([{"date": f"{x['date']}-12-31", "value": x["value"]} for x in j[1] if x.get("value") is not None])


def _wb_lib(req):
    ind, country = _wb_parts(req)
    df = get("wbgapi").data.DataFrame(ind, country, time=range(req.start.year, req.end.year + 1))
    s = df.iloc[0] if len(df) else pd.Series(dtype=float)
    return pd.DataFrame({"date": [f"{c[2:]}-12-31" for c in s.index], "value": s.values})


def _ecb_lib(req):
    _, key = split_id(req.symbol)
    df = get("ecbdata").ecbdata.get_series(key, start=req.start.isoformat(), end=req.end.isoformat())
    return df.rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "value"})


def _ecb_api(req):
    import requests

    _, key = split_id(req.symbol)
    flow, _, rest = key.partition(".")
    r = requests.get(f"https://data-api.ecb.europa.eu/service/data/{flow}/{rest}",
                     params={"format": "csvdata", "startPeriod": req.start.isoformat(),
                             "endPeriod": req.end.isoformat()}, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text)).rename(columns={"TIME_PERIOD": "date", "OBS_VALUE": "value"})


def _dbn_lib(req):
    _, sid = split_id(req.symbol)
    df = get("dbnomics").fetch_series(sid)
    return df.rename(columns={"period": "date"})[["date", "value"]]


def _dbn_api(req):
    import requests

    _, sid = split_id(req.symbol)
    r = requests.get(f"https://api.db.nomics.world/v22/series/{sid}", params={"observations": 1}, timeout=30)
    r.raise_for_status()
    docs = (r.json().get("series") or {}).get("docs") or []
    need(docs, "DBnomics returned no series")
    d = docs[0]
    return pd.DataFrame({"date": d.get("period_start_day") or d.get("period"), "value": d.get("value")})


def _estat_period(p: str):
    """Eurostat period labels: '2024', '2024Q1', '2024M01', '2024-01' -> period end date."""
    p = p.strip()
    try:
        if "Q" in p:
            return pd.Period(p.replace("-", ""), freq="Q").end_time.normalize()
        if "M" in p or "-" in p:
            return pd.Period(p.replace("M", "-"), freq="M").end_time.normalize()
        return pd.Period(p, freq="Y").end_time.normalize()
    except Exception:  # noqa: BLE001
        return pd.NaT


def _estat(req):
    _, code = split_id(req.symbol)
    filters = {k: v for k, v in req.params.items() if k not in ("what",)}
    df = get("eurostat").get_data_df(code, filter_pars=filters or None)
    id_cols = [c for c in df.columns if not str(c)[:4].isdigit()]
    long = df.melt(id_vars=id_cols, var_name="period", value_name="value").dropna(subset=["value"])
    long["date"] = long["period"].astype(str).map(_estat_period)
    return long


def _post_macro(df, req):
    df = df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"]).assign(series_id=req.symbol)


register(Pipeline(
    "macro", "Economic series: FRED, World Bank, ECB, Eurostat, DBnomics (IMF, OECD, BIS, RBI ... mirrored)",
    "series", [
        Source("fredapi", _fred_api, ("api.stlouisfed.org",), when=_is("fred")),
        Source("builtin:fred_csv", _fred_csv, ("fred.stlouisfed.org",), when=_is("fred"), score=4.2),
        Source("pandas_datareader", _fred_pdr, ("fred.stlouisfed.org",), when=_is("fred")),
        Source("wbgapi", _wb_lib, ("api.worldbank.org",), when=_is("wb")),
        Source("builtin:worldbank", _wb_api, ("api.worldbank.org",), when=_is("wb"), score=4.0),
        Source("ecbdata", _ecb_lib, ("data-api.ecb.europa.eu",), when=_is("ecb")),
        Source("builtin:ecb", _ecb_api, ("data-api.ecb.europa.eu",), when=_is("ecb"), score=4.0),
        Source("eurostat", _estat, ("ec.europa.eu",), when=_is("estat")),
        Source("dbnomics", _dbn_lib, ("api.db.nomics.world",), when=_is("dbn")),
        Source("builtin:dbnomics", _dbn_api, ("api.db.nomics.world",), when=_is("dbn"), score=4.0),
    ],
    market="raw", required=("date", "value"), check=validate.nonempty, post=_post_macro,
    final=lambda r: dt.date.today() - dt.timedelta(days=90), live_ttl=6 * 3600, default_days=365 * 30,
    entity=lambda r: f"{split_id(r.symbol)[0]}:{split_id(r.symbol)[1]}", columns=("date", "value", "series_id"),
    params_doc="symbol = 'fred:DGS10' | 'wb:NY.GDP.MKTP.KD.ZG/IND' | 'ecb:EXR.D.INR.EUR.SP00.A' | "
               "'estat:prc_hicp_manr' (+ filters) | 'dbn:IMF/WEO:2025-10/IND.NGDP_RPCH.pcent_change'",
    example='fs.fetch("macro", "fred:CPIAUCSL", start="1990-01-01")'))


# =============================================================== rates
_DGS = ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"]


def _r_treasury(req):
    from ..more import us_yield_curve

    frames = [us_yield_curve(y) for y in range(req.start.year, req.end.year + 1)]
    return concat(frames)


def _r_fred(req):
    import requests

    import concurrent.futures as cf

    def one(sid):
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                         params={"id": sid, "cosd": req.start.isoformat(), "coed": req.end.isoformat()}, timeout=30)
        r.raise_for_status()
        d = pd.read_csv(io.StringIO(r.text))
        return d.rename(columns={d.columns[0]: "date"}).set_index("date")

    # 11 small files: download 4 at a time (the governor still keeps FRED within its 2 requests/second)
    with cf.ThreadPoolExecutor(4) as ex:
        frames = list(ex.map(one, _DGS))
    return pd.concat(frames, axis=1).reset_index()


def _post_curve(df, req):
    df = df.copy()
    for c in df.columns:
        if c != "date":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _rbi_jugaad(req):
    from jugaad_data.rbi import RBI

    return pd.DataFrame([{"rate": k, "value": v} for k, v in RBI().current_rates().items()])


def _rbi_aynse(req):
    return pd.DataFrame([{"rate": k, "value": v} for k, v in get("aynse").RBI().current_rates().items()])


_RBI = Pipeline(
    "rates.rbi", "RBI policy and money-market rates (current)", "snapshot", [
        Source("jugaad_data", _rbi_jugaad, ("www.rbi.org.in", "rbi.org.in")),
        Source("aynse", _rbi_aynse, ("www.rbi.org.in", "rbi.org.in")),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=6 * 3600, columns=("rate", "value"), rank_as="rates",
    params_doc="what='rbi'", example='fs.fetch("rates", what="rbi")')

register(Pipeline(
    "rates", "US Treasury par yield curve (daily), RBI policy rates", "series", [
        Source("builtin:treasury", _r_treasury, ("home.treasury.gov",)),
        Source("builtin:fred_csv", _r_fred, ("fred.stlouisfed.org",), score=3.5),
    ],
    market="raw", needs_symbol=False, required=("date",), post=_post_curve, check=validate.nonempty,
    final=lambda r: dt.date.today() - dt.timedelta(days=2), entity=lambda r: "US_CURVE", default_days=365,
    variants={"rbi": _RBI}, columns=("date",),
    params_doc="start, end (US curve); what='rbi' for current RBI rates; other series via fs.fetch('macro', ...)",
    example='fs.fetch("rates", start="2020-01-01")'))


# =============================================================== forex
def pair(symbol: str) -> tuple:
    s = symbol.upper().replace("=X", "").replace("-", "/").replace(" ", "")
    if "/" in s:
        a, b = s.split("/", 1)
    else:
        need(len(s) == 6, "use a pair like 'USD/INR'")
        a, b = s[:3], s[3:]
    return a, b


def _x_cc(req):
    from currency_converter import CurrencyConverter

    a, b = pair(req.symbol)
    c = CurrencyConverter(fallback_on_missing_rate=True, fallback_on_wrong_date=False)
    rows = []
    d = req.start
    while d <= req.end:
        try:
            rows.append({"date": d, "rate": c.convert(1, a, b, d)})
        except Exception:  # noqa: BLE001 - weekends / missing days
            pass
        d += dt.timedelta(days=1)
    return pd.DataFrame(rows)


def _x_frankfurter(req):
    import requests

    a, b = pair(req.symbol)
    rows = []
    for s, e in chunks(req.start, req.end, 365 * 3):
        r = requests.get(f"https://api.frankfurter.dev/v1/{s}..{e}", params={"from": a, "to": b}, timeout=30)
        r.raise_for_status()
        rows += [{"date": d, "rate": v.get(b)} for d, v in (r.json().get("rates") or {}).items()]
    return pd.DataFrame(rows)


def _x_ecb(req):
    a, b = pair(req.symbol)
    ecb = get("ecbdata").ecbdata

    def per_eur(cur):
        if cur == "EUR":
            return None
        d = ecb.get_series(f"EXR.D.{cur}.EUR.SP00.A", start=req.start.isoformat(), end=req.end.isoformat())
        return d.set_index("TIME_PERIOD")["OBS_VALUE"].astype(float)

    pa, pb = per_eur(a), per_eur(b)
    if pa is None:
        s = pb
    elif pb is None:
        s = 1 / pa
    else:
        s = (pb / pa).dropna()
    return s.rename("rate").rename_axis("date").reset_index()


def _x_yahoo(req):
    a, b = pair(req.symbol)
    df = yf_history(f"{a}{b}=X", req.start, req.end)
    return df


def _x_tv(req):
    a, b = pair(req.symbol)
    return tv_bars(f"{a}{b}", "FX_IDC", "1d", bars_since(req.start))


def _x_twelvedata(req):
    from twelvedata import TDClient

    a, b = pair(req.symbol)
    return TDClient(apikey=env("TWELVEDATA_API_KEY")).time_series(
        symbol=f"{a}/{b}", interval="1day", start_date=req.start.isoformat(), end_date=req.end.isoformat(),
        outputsize=5000).as_pandas()


def _x_av(req):
    from alpha_vantage.foreignexchange import ForeignExchange

    a, b = pair(req.symbol)
    df, _ = ForeignExchange(key=env("ALPHAVANTAGE_API_KEY"), output_format="pandas").get_currency_exchange_daily(
        a, b, outputsize="full")
    df.columns = [c.split(". ", 1)[-1] for c in df.columns]
    return df


def _post_fx(df, req):
    df = df.copy()
    if "rate" not in df.columns and "close" in df.columns:
        df["rate"] = df["close"]
    a, b = pair(req.symbol)
    return df.assign(pair=f"{a}/{b}")


register(Pipeline(
    "forex", "Currency rates: daily history for any pair (ECB reference rates first)", "series", [
        Source("ecbdata", _x_ecb, ("data-api.ecb.europa.eu",)),
        Source("builtin:frankfurter", _x_frankfurter, ("api.frankfurter.dev",)),
        Source("currency_converter", _x_cc, ()),
        Source("yfinance", _x_yahoo, YAHOO_HOSTS),
        Source("tvdatafeed", _x_tv, ("data.tradingview.com",)),
        Source("twelvedata", _x_twelvedata, ("api.twelvedata.com",)),
        Source("alpha_vantage", _x_av, ("www.alphavantage.co",)),
    ],
    market="raw", required=("date",), check=validate.positive("rate"), post=_post_fx,
    final=lambda r: calendar.utc_final_through(), entity=lambda r: "FX:" + "/".join(pair(r.symbol)),
    columns=("date", "rate", "open", "high", "low", "close", "pair"),
    params_doc="symbol = 'USD/INR' (or 'USDINR', 'EURUSD=X'), start, end",
    example='fs.fetch("forex", "USD/INR", start="5y")'))
