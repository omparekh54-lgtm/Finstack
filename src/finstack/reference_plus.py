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
# Official consolidated daily surveillance indicator file introduced in 2024.
_NSE_SURVEILLANCE_URL = "https://nsearchives.nseindia.com/content/cm/{kind}{ddmmyy}.csv"
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
    buckets = asm.items() if isinstance(asm, dict) else [("", asm)]
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
        m = re.search(r"GSM\s*stage\s*(\d+)", desc, re.I) or re.search(r"GSM\s*(\d+)", code, re.I)
        stage = m.group(1) if m else (_roman_or_arabic(row.get("stage") or row.get("category")) or "1")
        rows.append({"symbol": symbol, "surveillance_type": "GSM", "stage": f"GSM{stage}",
                     "effective_date": pd.Timestamp(as_of), "source": "NSE_REPORT_GSM"})
    if not rows:
        return pd.DataFrame(columns=["symbol", "surveillance_type", "stage", "effective_date", "source"])
    return pd.DataFrame(rows).drop_duplicates(["symbol", "surveillance_type", "stage", "effective_date"]).reset_index(drop=True)


def nse_current_surveillance(as_of: str | date | None = None) -> pd.DataFrame:
    """Current official ASM/GSM lists from NSE's reportASM/reportGSM endpoints."""
    d = pd.Timestamp(as_of or date.today()).date()
    session = requests.Session()
    session.headers.update(_HEADERS)
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


def _indicator_active(value) -> bool:
    """NSE REG_IND semantics: 100 means not applicable; GSM 0 is active."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    text = str(value).strip().upper()
    return text not in {"", "100", "NA", "N/A", "NONE", "NAN", "-"}


def _modern_reg_ind_rows(df: pd.DataFrame, as_of: date, source: str) -> list[dict]:
    normalized = {re.sub(r"[^A-Z0-9]", "", str(c).upper()): c for c in df.columns}
    symbol_col = normalized.get("SYMBOL")
    gsm_col = normalized.get("GSM")
    lt_col = next((c for k, c in normalized.items() if "LONGTERM" in k and "SURVEILLANCE" in k), None)
    st_col = next((c for k, c in normalized.items() if "SHORTTERM" in k and "SURVEILLANCE" in k), None)
    if not symbol_col or not gsm_col or (not lt_col and not st_col):
        return []
    rows: list[dict] = []
    for _, row in df.iterrows():
        symbol = str(row.get(symbol_col) or "").upper().strip()
        if not symbol or symbol in {"NAN", "NONE"}:
            continue
        gsm = row.get(gsm_col)
        lt = row.get(lt_col) if lt_col else None
        st = row.get(st_col) if st_col else None
        if _indicator_active(gsm):
            rows.append({"symbol": symbol, "surveillance_type": "GSM", "stage": f"GSM{str(gsm).strip()}",
                         "effective_date": pd.Timestamp(as_of), "source": source})
        asm_parts: list[str] = []
        if _indicator_active(lt):
            asm_parts.append(f"LT{str(lt).strip()}")
        if _indicator_active(st):
            asm_parts.append(f"ST{str(st).strip()}")
        if asm_parts:
            rows.append({"symbol": symbol, "surveillance_type": "ASM", "stage": "+".join(asm_parts),
                         "effective_date": pd.Timestamp(as_of), "source": source})
    return rows


def nse_surveillance(as_of: str | date | None = None) -> pd.DataFrame:
    """Official NSE ASM/GSM state for a date.

    Current dates use the official reportASM/reportGSM JSON endpoints when possible.
    Historical standardized data use NSE's REG_IND daily archive. The archive uses 100
    for 'not applicable'; importantly GSM value 0 is still an active GSM state.
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
            parsed = _modern_reg_ind_rows(df, d, f"NSE_{kind}")
            out.extend(parsed)
        except (requests.RequestException, ValueError, pd.errors.ParserError):
            continue
    if not out:
        return pd.DataFrame(columns=["symbol", "surveillance_type", "stage", "effective_date", "source"])
    return pd.DataFrame(out).drop_duplicates(["symbol", "surveillance_type", "stage", "effective_date", "source"]).reset_index(drop=True)
