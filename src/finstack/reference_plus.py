from __future__ import annotations

from io import BytesIO
import re
from datetime import date

import pandas as pd
import requests

_TIMEOUT = 30
_SHAREHOLDING_URL = (
    "https://raw.githubusercontent.com/yurukatsu/nse-historical-membership/"
    "main/shareholding_history/data/parsed/_flat.csv"
)
_NSE_SURVEILLANCE_URL = "https://nsearchives.nseindia.com/archives/equities/mkt/{kind}{ddmmyy}.csv"


def nse_shareholding_history(symbol: str | None = None) -> pd.DataFrame:
    """Point-in-time NSE shareholding history reconstructed from regulator-filed SHP XBRL.

    The upstream dataset preserves filing/broadcast timestamps and pledge metrics.  Only
    columns used by downstream PMS consumers are loaded to keep memory usage bounded.
    """
    usecols = [
        "ticker", "period", "filing_date", "submission_date", "broadcast_date",
        "record_id", "revised", "promoter_pct", "pledge_pct",
        "pledge_pct_of_promoter", "total_shares", "isin", "source_filename",
    ]
    df = pd.read_csv(_SHAREHOLDING_URL, usecols=lambda c: c in set(usecols), low_memory=False)
    if df.empty:
        return df
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if symbol:
        df = df[df["ticker"] == symbol.upper().strip()]
    for c in ("filing_date", "submission_date", "broadcast_date"):
        if c in df:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
    if "period" in df:
        df["period_end"] = pd.to_datetime(df["period"].astype(str) + "-01", errors="coerce") + pd.offsets.MonthEnd(0)
    # Exchange dissemination/broadcast timestamp is the best knowable-at timestamp.
    df["filed_at"] = df.get("broadcast_date")
    if "submission_date" in df:
        df["filed_at"] = df["filed_at"].fillna(df["submission_date"])
    if "filing_date" in df:
        df["filed_at"] = df["filed_at"].fillna(df["filing_date"])
    return df.reset_index(drop=True)


def _truthy_surveillance(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    s = str(value).strip().upper()
    return s not in {"", "0", "N", "NO", "NA", "N/A", "NONE", "FALSE", "-", "NAN"}


def _symbol_column(df: pd.DataFrame) -> str:
    normalized = {str(c).upper().replace(" ", "").replace("_", ""): str(c) for c in df.columns}
    for key in ("SYMBOL", "SMSYMBOL", "NSESYMBOL", "TRADINGSYMBOL", "SCRIPCODE"):
        if key in normalized:
            return normalized[key]
    # Fallback: first column whose name contains SYMBOL.
    for c in df.columns:
        if "SYMBOL" in str(c).upper():
            return str(c)
    raise ValueError(f"cannot identify symbol column in REG_IND columns: {list(df.columns)[:25]}")


def _extract_surveillance(df: pd.DataFrame, as_of: date, source: str) -> list[dict]:
    if df is None or df.empty:
        return []
    symbol_col = _symbol_column(df)
    rows: list[dict] = []
    for _, row in df.iterrows():
        symbol = str(row.get(symbol_col, "")).upper().strip()
        if not symbol or symbol in {"NAN", "NONE"}:
            continue
        for measure in ("ASM", "GSM"):
            stage = None
            active = False
            for col in df.columns:
                c = str(col).upper()
                val = row.get(col)
                if measure in c and _truthy_surveillance(val):
                    active = True
                    text = f"{c} {val}".upper()
                    m = re.search(r"STAGE\s*[-:]?\s*([IVX]+|\d+)", text)
                    if m:
                        stage = m.group(1)
                    elif measure == "GSM":
                        m = re.search(r"\b([1-6])\b", str(val))
                        if m:
                            stage = m.group(1)
                elif _truthy_surveillance(val) and measure in str(val).upper():
                    active = True
                    text = str(val).upper()
                    m = re.search(r"STAGE\s*[-:]?\s*([IVX]+|\d+)", text)
                    if m:
                        stage = m.group(1)
            if active:
                rows.append({
                    "symbol": symbol,
                    "surveillance_type": measure,
                    "stage": stage,
                    "effective_date": pd.Timestamp(as_of),
                    "source": source,
                })
    return rows


def nse_surveillance(as_of: str | date | None = None) -> pd.DataFrame:
    """Return ASM/GSM flags for one trading date from NSE REG_IND/REG1_IND files.

    No account or API key is required. Missing files (holiday/non-trading day) are skipped.
    """
    d = pd.Timestamp(as_of or date.today()).date()
    ddmmyy = d.strftime("%d%m%y")
    out: list[dict] = []
    headers = {"User-Agent": "Mozilla/5.0 finstack/0.8"}
    for kind in ("REG_IND", "REG1_IND"):
        url = _NSE_SURVEILLANCE_URL.format(kind=kind, ddmmyy=ddmmyy)
        try:
            r = requests.get(url, timeout=_TIMEOUT, headers=headers)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            df = pd.read_csv(BytesIO(r.content), low_memory=False)
            out.extend(_extract_surveillance(df, d, f"NSE_{kind}"))
        except requests.RequestException:
            continue
    if not out:
        return pd.DataFrame(columns=["symbol", "surveillance_type", "stage", "effective_date", "source"])
    return pd.DataFrame(out).drop_duplicates(["symbol", "surveillance_type", "stage", "effective_date", "source"]).reset_index(drop=True)
