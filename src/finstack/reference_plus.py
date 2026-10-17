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
_INDEX_HISTORY_URL = (
    "https://raw.githubusercontent.com/aditya-jha/nse-historical-membership/"
    "main/index_history/data/index_membership_history.csv"
)
_NSE_SURVEILLANCE_URL = "https://nsearchives.nseindia.com/archives/equities/mkt/{kind}{ddmmyy}.csv"
_NSE_HOME = "https://www.nseindia.com"
_NSE_SURVEILLANCE_LANDING = "https://www.nseindia.com/regulations/exchange-market-surveillance-actions"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": _NSE_HOME + "/",
}


def nse_shareholding_history(symbol: str | None = None) -> pd.DataFrame:
    """Point-in-time NSE shareholding history reconstructed from regulator-filed SHP XBRL."""
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
    df["filed_at"] = df.get("broadcast_date")
    if "submission_date" in df:
        df["filed_at"] = df["filed_at"].fillna(df["submission_date"])
    if "filing_date" in df:
        df["filed_at"] = df["filed_at"].fillna(df["filing_date"])
    return df.reset_index(drop=True)


def nse_index_membership_history(index_name: str | None = None) -> pd.DataFrame:
    """Historical NSE index membership intervals reconstructed from official index releases."""
    df = pd.read_csv(_INDEX_HISTORY_URL, low_memory=False)
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["index_name"] = df["index_name"].astype(str).str.strip()
    df["valid_from"] = pd.to_datetime(df["valid_from"], errors="coerce").dt.date
    df["valid_to"] = pd.to_datetime(df["valid_to"], errors="coerce").dt.date
    if index_name:
        df = df[df["index_name"].str.upper() == index_name.upper().strip()]
    return df.reset_index(drop=True)


def _roman_or_arabic(value: str | None) -> str | None:
    text = str(value or "").upper()
    m = re.search(r"\d+", text)
    if m:
        return m.group(0)
    roman = re.findall(r"\b[IVXLCDM]+\b", text)
    if not roman:
        return None
    digits = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    token = roman[-1]
    total = 0
    for i, ch in enumerate(token):
        current = digits[ch]
        nxt = digits[token[i + 1]] if i + 1 < len(token) else 0
        total += -current if current < nxt else current
    return str(total) if total > 0 else None


def _json_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    return []


def _parse_current_surveillance(asm, gsm, as_of: date) -> pd.DataFrame:
    rows: list[dict] = []
    if isinstance(asm, dict):
        buckets = asm.items()
    else:
        buckets = [("", asm)]
    for bucket, payload in buckets:
        for row in _json_rows(payload):
            symbol = str(row.get("symbol") or row.get("Symbol") or "").upper().strip()
            if not symbol:
                continue
            desc = f"{row.get('survDesc','')} {row.get('survCode','')}".upper()
            stage_num = _roman_or_arabic(row.get("asmSurvIndicator")) or "1"
            long_term = str(bucket).lower() == "longterm" or "LONG TERM" in desc or "LTASM" in desc
            stage = f"LT{stage_num}" if long_term else f"ST{stage_num}"
            rows.append({"symbol": symbol, "surveillance_type": "ASM", "stage": stage,
                         "effective_date": pd.Timestamp(as_of), "source": "NSE_REPORT_ASM"})
    for row in _json_rows(gsm):
        symbol = str(row.get("symbol") or row.get("Symbol") or "").upper().strip()
        if not symbol:
            continue
        desc = str(row.get("survDesc") or "")
        code = str(row.get("survCode") or "")
        # gsmStage is an internal sequence code; actual 0-6 stage is in description/code.
        m = re.search(r"GSM\s*stage\s*(\d+)", desc, re.I) or re.search(r"GSM\s*(\d+)", code, re.I)
        stage = m.group(1) if m else (_roman_or_arabic(row.get("stage") or row.get("category")) or "1")
        rows.append({"symbol": symbol, "surveillance_type": "GSM", "stage": stage,
                     "effective_date": pd.Timestamp(as_of), "source": "NSE_REPORT_GSM"})
    if not rows:
        return pd.DataFrame(columns=["symbol", "surveillance_type", "stage", "effective_date", "source"])
    # GSM and ASM are retained as distinct hard-filter facts; duplicate source rows collapse.
    return pd.DataFrame(rows).drop_duplicates(["symbol", "surveillance_type", "stage", "effective_date"]).reset_index(drop=True)


def nse_current_surveillance(as_of: str | date | None = None) -> pd.DataFrame:
    """Current official ASM/GSM lists from NSE's reportASM/reportGSM endpoints."""
    d = pd.Timestamp(as_of or date.today()).date()
    session = requests.Session()
    session.headers.update(_HEADERS)
    # Cookie warm-up. Failure is tolerated because endpoints occasionally work without it.
    try:
        session.get(_NSE_SURVEILLANCE_LANDING, timeout=_TIMEOUT)
    except requests.RequestException:
        pass
    responses = []
    for endpoint in ("reportASM", "reportGSM"):
        r = session.get(f"{_NSE_HOME}/api/{endpoint}", timeout=_TIMEOUT)
        r.raise_for_status()
        responses.append(r.json())
    return _parse_current_surveillance(responses[0], responses[1], d)


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
                    m = re.search(r"STAGE\s*[-:]?\s*([IVXLCDM]+|\d+)", text)
                    stage = _roman_or_arabic(m.group(1)) if m else stage
                elif _truthy_surveillance(val) and measure in str(val).upper():
                    active = True
                    text = str(val).upper()
                    m = re.search(r"STAGE\s*[-:]?\s*([IVXLCDM]+|\d+)", text)
                    stage = _roman_or_arabic(m.group(1)) if m else stage
            if active:
                rows.append({"symbol": symbol, "surveillance_type": measure, "stage": stage,
                             "effective_date": pd.Timestamp(as_of), "source": source})
    return rows


def nse_surveillance(as_of: str | date | None = None) -> pd.DataFrame:
    """Historical consolidated surveillance flags from NSE REG_IND/REG1_IND daily archives.

    For the current date, the JSON ASM/GSM endpoints are attempted first because they preserve
    tenure/stage fields more cleanly. Archive files remain the point-in-time backfill source.
    """
    d = pd.Timestamp(as_of or date.today()).date()
    if d == date.today():
        try:
            current = nse_current_surveillance(d)
            if not current.empty:
                return current
        except requests.RequestException:
            pass
    ddmmyy = d.strftime("%d%m%y")
    out: list[dict] = []
    headers = {"User-Agent": _HEADERS["User-Agent"]}
    for kind in ("REG_IND", "REG1_IND"):
        url = _NSE_SURVEILLANCE_URL.format(kind=kind, ddmmyy=ddmmyy)
        try:
            r = requests.get(url, timeout=_TIMEOUT, headers=headers)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            df = pd.read_csv(BytesIO(r.content), low_memory=False)
            out.extend(_extract_surveillance(df, d, f"NSE_{kind}"))
        except (requests.RequestException, ValueError, pd.errors.ParserError):
            continue
    if not out:
        return pd.DataFrame(columns=["symbol", "surveillance_type", "stage", "effective_date", "source"])
    return pd.DataFrame(out).drop_duplicates(["symbol", "surveillance_type", "stage", "effective_date", "source"]).reset_index(drop=True)
