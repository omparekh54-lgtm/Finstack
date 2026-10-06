"""Standard column names, so every source returns the same table.

Each standard column lists the names sources use for it, best first (NSE "next API", old and new
UDiFF bhavcopy, jugaad-data, nselib, aynse, Yahoo, brokers ...). normalize() renames them, parses
dates (DD-MM-YYYY Indian formats and ISO), turns "1,234.50" and "-" into numbers, and raises
SchemaError when a required column is missing so the router moves on to the next source.
"""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd


class SchemaError(ValueError):
    """The source answered, but not with the columns this data type needs."""


def canon(name) -> str:
    s = str(name).strip().lower().replace("%", "pct_")
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


ALIASES: Dict[str, List[str]] = {
    # time
    "date": ["date", "ch_timestamp", "date1", "fh_timestamp", "bd_dt_date", "mtimestamp", "eod_timestamp", "traddt", "trade_date", "trading_date",
             "timestamp", "datetime", "index", "time", "dt", "nav_date"],
    "ts": ["ts", "datetime", "timestamp", "time", "date"],
    # prices
    "open": ["open", "openprice", "open_rate", "ch_opening_price", "eod_open_index_val", "opnpric", "open_price", "openrate",
             "open_index_value", "o"],
    "high": ["high", "highprice", "high_rate", "ch_trade_high_price", "eod_high_index_val", "hghpric", "high_price", "dayhigh", "highrate",
             "high_index_value", "h"],
    "low": ["low", "lowprice", "low_rate", "ch_trade_low_price", "eod_low_index_val", "lwpric", "low_price", "daylow", "lowrate",
            "low_index_value", "l"],
    "close": ["close", "closeprice", "close_rate", "ch_closing_price", "eod_close_index_val", "clspric", "close_price", "closing_index_value",
              "closing_price", "c"],
    "adj_close": ["adj_close", "adjclose", "adjusted_close", "adj_close_price"],
    "last": ["last", "lastprice_", "ltp", "ch_last_traded_price", "lastpric", "last_price", "lasttradedprice",
             "last_traded_price", "ltradert", "lastprice"],
    "prev_close": ["prev_close", "prevclose_", "ch_previous_cls_price", "prvsclsgpric", "prevclose", "previous_close",
                   "previousclose", "prev_close_price", "eod_prev_close", "prevdayclose"],
    "vwap": ["vwap", "average_price", "avg_price", "averageprice", "average_traded_price"],
    "volume": ["volume", "totaltradedquantity", "ttl_trd_qnty", "fh_tot_traded_qty", "ch_tot_traded_qty", "ttltradgvol", "tottrdqty", "total_traded_quantity",
               "hit_traded_qty", "totaltradedvolume", "no_of_shares", "trd_vol", "qty", "v",
               "tot_trd_qty", "total_traded_qty"],
    "value": ["value", "turnoverinrs", "ch_tot_traded_val", "ttltrfval", "tottrdval", "turnover", "hit_turn_over",
              "totaltradedvalue", "trd_val", "total_traded_value", "turnover_lacs", "turnover_in_lacs"],
    "trades": ["trades", "no_oftrades", "ch_total_trades", "ttlnboftxsexctd", "totaltrades", "no_of_trades", "nooftrd",
               "number_of_trades"],
    "delivery_qty": ["delivery_qty", "deliv_qty", "deliverableqty_", "deliverable_qty", "dly_qt", "deliverableqty", "delivery_quantity",
                     "deliverable_quantity"],
    "delivery_pct": ["delivery_pct", "deliv_per", "pct_dlyqttotradedqty_", "pct_dly_qt_to_traded_qty", "pct_dlyqttotradedqty", "deliverable_pct",
                     "pct_deliverble", "pct_deliverable", "delivery_percentage", "dly_qt_to_traded_qty"],
    # identity
    "symbol": ["symbol", "ch_symbol", "tckrsymb", "tcksymb", "tradingsymbol", "ticker", "sc_name", "scripname"],
    "series": ["series", "ch_series", "sctysrs"],
    "isin": ["isin", "ch_isin", "isin_number", "isincode", "isin_code"],
    "name": ["name", "company_name", "companyname", "name_of_company", "scheme_name", "long_name", "fininstrmnm"],
    # derivatives
    "instrument": ["instrument", "fininstrmtp", "instrument_type", "instrumenttype"],
    "expiry": ["expiry", "xprydt", "expiry_dt", "expirydate", "expiry_date", "fininstrmactlxprydt"],
    "strike": ["strike", "strkpric", "strike_pr", "strike_price", "strikeprice"],
    "option_type": ["option_type", "optntp", "option_typ", "optiontype"],
    "settle": ["settle", "sttlmpric", "settle_pr", "settle_price", "settlement_price"],
    "oi": ["oi", "opnintrst", "open_int", "openinterest", "open_interest"],
    "oi_change": ["oi_change", "chnginopnintrst", "chg_in_oi", "changeinopeninterest", "change_in_oi"],
    "iv": ["iv", "impliedvolatility", "implied_volatility"],
    "bid": ["bid", "bidprice", "bid_price", "buyprice1"],
    "ask": ["ask", "askprice", "ask_price", "sellprice1"],
    "underlying_price": ["underlying_price", "underlyingvalue", "underlying_value", "underlying_spot_price"],
    # funds / macro / fx
    "nav": ["nav", "net_asset_value"],
    "scheme_code": ["scheme_code", "schemecode", "amfi_code", "code"],
    "rate": ["rate", "fx_rate", "exchange_rate"],
    "value_": ["obs_value", "observation", "observation_value"],
}

NUMERIC = {"open", "high", "low", "close", "adj_close", "last", "prev_close", "vwap", "volume", "value", "trades",
           "delivery_qty", "delivery_pct", "strike", "settle", "oi", "oi_change", "iv", "bid", "ask",
           "underlying_price", "nav", "rate"}

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")


def to_number(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return s
    cleaned = (s.astype(str).str.replace(",", "", regex=False).str.replace("₹", "", regex=False).str.strip()
               .replace({"-": None, "": None, "nan": None, "None": None, "NA": None, "N/A": None}))
    return pd.to_numeric(cleaned, errors="coerce")


def to_datetime(s: pd.Series, tz: Optional[str] = None) -> pd.Series:
    """Parse ISO, DD-MM-YYYY, DD-Mon-YYYY, epoch seconds/ms. Returns tz-aware in `tz` if given."""
    if pd.api.types.is_datetime64_any_dtype(s):
        out = s
    elif pd.api.types.is_numeric_dtype(s):
        unit = "ms" if s.dropna().abs().median() > 1e11 else "s"
        out = pd.to_datetime(s, unit=unit, utc=True)
    else:
        txt = s.astype(str).str.strip()
        sample = txt.dropna().head(20)
        iso = sample.map(lambda x: bool(_ISO.match(x))).mean() > 0.5 if len(sample) else True
        out = pd.to_datetime(txt, errors="coerce", format="mixed", dayfirst=not iso)
    if tz:
        if getattr(out.dt, "tz", None) is None:
            out = out.dt.tz_localize(tz)
        else:
            out = out.dt.tz_convert(tz)
    return out


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if c[0] else c[-1] for c in df.columns]
    return df


def normalize(df, required: Sequence[str] = ("date", "close"), time_col: str = "date",
              tz: Optional[str] = None, extra_aliases: Optional[Dict[str, Iterable[str]]] = None,
              keep_unmapped: bool = True) -> pd.DataFrame:
    """Rename to standard columns, parse the time column and numbers, check required columns."""
    if df is None:
        raise SchemaError("no data")
    if hasattr(df, "to_pandas") and not isinstance(df, pd.DataFrame):   # polars
        df = df.to_pandas()
    if isinstance(df, list):
        df = pd.DataFrame(df)
    if not isinstance(df, pd.DataFrame):
        raise SchemaError(f"expected a table, got {type(df).__name__}")
    if df.empty:
        raise SchemaError("empty table")
    df = _flatten_columns(df)
    if not isinstance(df.index, pd.RangeIndex):
        names = [n for n in (df.index.names or []) if n is not None]
        if isinstance(df.index, pd.DatetimeIndex) or names:
            df = df.reset_index()
            if "index" in df.columns and isinstance(df["index"].iloc[0], pd.Timestamp):
                df = df.rename(columns={"index": "date"})
    df = df.loc[:, ~pd.Index([canon(c) for c in df.columns]).duplicated()]
    df.columns = [canon(c) for c in df.columns]

    aliases = {k: list(v) for k, v in ALIASES.items()}
    for k, v in (extra_aliases or {}).items():
        aliases[k] = [canon(x) for x in v] + aliases.get(k, [])
    # only one of date/ts is wanted
    wanted = [time_col] + [k for k in aliases if k not in ("date", "ts")]
    rename, used = {}, set()
    for std in wanted:
        if std in df.columns and std not in rename.values():
            used.add(std)
            rename[std] = std
            continue
        for cand in aliases.get(std, []):
            if cand in df.columns and cand not in used:
                rename[cand] = std
                used.add(cand)
                break
    dropped_aliases = {c for std in wanted for c in aliases.get(std, []) if c in df.columns and c not in used
                       and c not in rename.values()}
    out = df.rename(columns=rename)
    out = out[[c for c in out.columns if c in rename.values() or (keep_unmapped and c not in dropped_aliases)]]
    out = out.loc[:, ~out.columns.duplicated()]
    if "value_" in out.columns and "value" not in out.columns:
        out = out.rename(columns={"value_": "value"})
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise SchemaError(f"missing column(s) {missing}; got {list(df.columns)[:15]}")
    for c in out.columns:
        if c in NUMERIC:
            out[c] = to_number(out[c])
    if time_col in out.columns:
        if time_col == "date":
            # Daily bars: keep the exchange's own calendar date. Timestamps in UTC (epochs, "...Z")
            # are moved to the market's zone first (IST by default: 2025-03-31T18:30Z is 1 April).
            t = to_datetime(out[time_col])
            if getattr(t.dt, "tz", None) is not None:
                if str(t.dt.tz) in ("UTC", "utc", "UTC+00:00"):
                    t = t.dt.tz_convert(tz or "Asia/Kolkata")
                t = t.dt.tz_localize(None)
            t = t.dt.normalize()
        else:
            t = to_datetime(out[time_col], tz=tz)
        out[time_col] = t
        out = out[out[time_col].notna()].sort_values(time_col)
    for c in ("expiry",):
        if c in out.columns:
            out[c] = to_datetime(out[c]).dt.normalize()
            if getattr(out[c].dt, "tz", None) is not None:
                out[c] = out[c].dt.tz_localize(None)
    if "option_type" in out.columns:
        out["option_type"] = out["option_type"].astype(str).str.upper().str.strip().replace(
            {"CALL": "CE", "PUT": "PE", "C": "CE", "P": "PE"})
    return out.reset_index(drop=True)


def order(df: pd.DataFrame, first: Sequence[str]) -> pd.DataFrame:
    """Put the standard columns first, in a fixed order."""
    cols = [c for c in first if c in df.columns]
    return df[cols + [c for c in df.columns if c not in cols]]


OHLCV = ("date", "open", "high", "low", "close", "adj_close", "volume", "value", "trades", "vwap", "prev_close",
         "delivery_qty", "delivery_pct", "symbol", "series", "isin")
INTRADAY = ("ts", "open", "high", "low", "close", "volume", "oi")
