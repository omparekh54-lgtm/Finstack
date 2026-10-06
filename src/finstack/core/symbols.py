"""Symbol master: one instrument, every source's name for it.

    fs.resolve("RELIANCE")      -> NSE RELIANCE, BSE 500325, ISIN INE002A01018, Yahoo RELIANCE.NS ...
    fs.resolve("500325")        -> same company from its BSE scrip code
    fs.resolve("INE002A01018")  -> same company from its ISIN
    fs.resolve("BANKNIFTY")     -> NIFTY BANK index: ^NSEBANK on Yahoo, NSE:BANKNIFTY on TradingView

The master list is built from NSE's EQUITY_L.csv and BSE's security list, joined on ISIN, and cached
for a week. Lookups that only need the NSE symbol never touch the network.
"""
from __future__ import annotations

import io
import re
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import pandas as pd

from . import cache

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def isin_valid(isin: str) -> bool:
    """ISIN check digit (ISO 6166): letters become numbers (A=10 ... Z=35), then the Luhn check."""
    s = str(isin or "").strip().upper()
    if not _ISIN_RE.match(s):
        return False
    digits = "".join(str(int(c, 36)) for c in s)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


# name: (yahoo, tradingview "EXCH:SYM", upstox instrument key, aliases)
INDICES: Dict[str, Tuple[Optional[str], Optional[str], Optional[str], Tuple[str, ...]]] = {
    "NIFTY 50": ("^NSEI", "NSE:NIFTY", "NSE_INDEX|Nifty 50", ("NIFTY", "NIFTY50", "^NSEI")),
    "NIFTY BANK": ("^NSEBANK", "NSE:BANKNIFTY", "NSE_INDEX|Nifty Bank", ("BANKNIFTY", "NIFTYBANK", "^NSEBANK")),
    "NIFTY FINANCIAL SERVICES": ("NIFTY_FIN_SERVICE.NS", "NSE:CNXFINANCE", "NSE_INDEX|Nifty Fin Service",
                                 ("FINNIFTY", "NIFTY FIN SERVICE")),
    "NIFTY MIDCAP SELECT": (None, "NSE:MIDCPNIFTY", "NSE_INDEX|NIFTY MID SELECT", ("MIDCPNIFTY",)),
    "NIFTY NEXT 50": ("^NSMIDCP", "NSE:NIFTYJR", "NSE_INDEX|Nifty Next 50", ("NIFTYNXT50", "NIFTY JUNIOR")),
    "NIFTY 500": ("^CRSLDX", "NSE:CNX500", "NSE_INDEX|Nifty 500", ("NIFTY500",)),
    "NIFTY IT": ("^CNXIT", "NSE:CNXIT", "NSE_INDEX|Nifty IT", ("NIFTYIT",)),
    "NIFTY AUTO": ("^CNXAUTO", "NSE:CNXAUTO", "NSE_INDEX|Nifty Auto", ()),
    "NIFTY PHARMA": ("^CNXPHARMA", "NSE:CNXPHARMA", "NSE_INDEX|Nifty Pharma", ()),
    "NIFTY FMCG": ("^CNXFMCG", "NSE:CNXFMCG", "NSE_INDEX|Nifty FMCG", ()),
    "NIFTY METAL": ("^CNXMETAL", "NSE:CNXMETAL", "NSE_INDEX|Nifty Metal", ()),
    "NIFTY REALTY": ("^CNXREALTY", "NSE:CNXREALTY", "NSE_INDEX|Nifty Realty", ()),
    "NIFTY ENERGY": ("^CNXENERGY", "NSE:CNXENERGY", "NSE_INDEX|Nifty Energy", ()),
    "NIFTY PSU BANK": ("^CNXPSUBANK", "NSE:CNXPSUBANK", "NSE_INDEX|Nifty PSU Bank", ()),
    "NIFTY MIDCAP 100": ("NIFTY_MIDCAP_100.NS", "NSE:CNXMIDCAP", "NSE_INDEX|NIFTY MIDCAP 100", ()),
    "NIFTY SMALLCAP 100": ("^CNXSC", "NSE:CNXSMALLCAP", "NSE_INDEX|NIFTY SMLCAP 100", ()),
    "INDIA VIX": ("^INDIAVIX", "NSE:INDIAVIX", "NSE_INDEX|India VIX", ("VIX", "INDIAVIX")),
    "SENSEX": ("^BSESN", "BSE:SENSEX", "BSE_INDEX|SENSEX", ("BSE SENSEX", "^BSESN")),
    "BANKEX": (None, "BSE:BANKEX", "BSE_INDEX|BANKEX", ()),
}
_INDEX_ALIASES = {a: name for name, v in INDICES.items() for a in (name, name.replace(" ", ""), *v[3])}

# option-chain names NSE's API expects for index derivatives
NSE_DERIV_INDEX = {"NIFTY 50": "NIFTY", "NIFTY BANK": "BANKNIFTY", "NIFTY FINANCIAL SERVICES": "FINNIFTY",
                   "NIFTY MIDCAP SELECT": "MIDCPNIFTY", "NIFTY NEXT 50": "NIFTYNXT50"}


@dataclass
class Instrument:
    query: str
    kind: str = "equity"            # equity | index | global
    market: str = "IN"              # IN | GLOBAL
    nse: Optional[str] = None       # NSE trading symbol, or NSE index name
    yahoo: Optional[str] = None
    tv: Optional[str] = None        # "EXCHANGE:SYMBOL"
    upstox_index: Optional[str] = None
    _isin: Optional[str] = None
    _bse: Optional[str] = None
    _name: Optional[str] = None
    _looked_up: bool = field(default=False, repr=False)

    def _lookup(self):
        if self._looked_up or self.kind != "equity" or self.market != "IN":
            return
        self._looked_up = True
        row = find(nse=self.nse, isin=self._isin, bse=self._bse)
        if row is not None:
            self.nse = self.nse or (row.get("symbol") or None)
            self._isin = self._isin or (row.get("isin") or None)
            self._bse = self._bse or (row.get("bse_code") or None)
            self._name = self._name or (row.get("name") or None)
            if not self.yahoo:
                self.yahoo = f"{self.nse}.NS" if self.nse else (f"{self._bse}.BO" if self._bse else None)
            if not self.tv:
                self.tv = f"NSE:{self.nse}" if self.nse else (f"BSE:{self._bse}" if self._bse else None)

    @property
    def isin(self) -> Optional[str]:
        if not self._isin:
            self._lookup()
        return self._isin

    @property
    def bse(self) -> Optional[str]:
        if not self._bse:
            self._lookup()
        return self._bse

    @property
    def name(self) -> Optional[str]:
        if not self._name:
            self._lookup()
        return self._name

    @property
    def key(self) -> str:
        """Stable cache key: ISIN when known offline, else exchange symbol."""
        if self.kind == "index":
            return f"INDEX:{self.nse}"
        if self.market == "IN":
            return f"NSE:{self.nse}" if self.nse else f"BSE:{self._bse}"
        return f"G:{self.yahoo or self.query}"

    def as_dict(self) -> dict:
        return {"query": self.query, "kind": self.kind, "market": self.market, "nse": self.nse, "bse": self.bse,
                "isin": self.isin, "name": self.name, "yahoo": self.yahoo, "tradingview": self.tv}


# ------------------------------------------------------------------ master list
_master: Optional[pd.DataFrame] = None
_master_failed = False
_mlock = threading.Lock()

NSE_EQUITY_LIST = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"


def _nse_list() -> pd.DataFrame:
    import requests

    r = requests.get(NSE_EQUITY_LIST, timeout=30,
                     headers={"User-Agent": "Mozilla/5.0 (finstack; +https://pypi.org/project/finstack)"})
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().upper() for c in df.columns]
    return pd.DataFrame({"symbol": df["SYMBOL"].str.strip(), "name": df["NAME OF COMPANY"].str.strip(),
                         "series": df.get("SERIES", pd.Series(dtype=str)).astype(str).str.strip(),
                         "isin": df["ISIN NUMBER"].str.strip(),
                         "face_value": pd.to_numeric(df.get("FACE VALUE"), errors="coerce")})


def _bse_list() -> pd.DataFrame:
    from ..company import bse_client

    rows = bse_client().listSecurities(group="", segment="Equity", status="Active")
    df = pd.DataFrame(rows)
    return pd.DataFrame({"bse_code": df["SCRIP_CD"].astype(str), "bse_symbol": df.get("scrip_id"),
                         "bse_name": df.get("Scrip_Name"), "isin": df["ISIN_NUMBER"].astype(str).str.strip(),
                         "industry": df.get("INDUSTRY"), "bse_group": df.get("GROUP")})


def master(refresh: bool = False) -> pd.DataFrame:
    """Every listed Indian equity: NSE symbol, BSE code, ISIN, name, industry (cached 7 days)."""
    global _master, _master_failed
    with _mlock:
        if _master is not None and not refresh:
            return _master
        if not refresh:
            cached = cache.get_snapshot("reference", "symbol_master", ttl=7 * 86400)
            if isinstance(cached, pd.DataFrame) and not cached.empty:
                _master = cached
                return _master
        if _master_failed and not refresh:
            return pd.DataFrame(columns=["symbol", "bse_code", "isin", "name"])
        parts, errors = [], []
        for fn in (_nse_list, _bse_list):
            try:
                parts.append(fn())
            except Exception as e:
                errors.append(f"{fn.__name__}: {e}")
        if not parts:
            _master_failed = True
            stale = cache.get_snapshot("reference", "symbol_master", ttl=None)
            if isinstance(stale, pd.DataFrame):
                _master = stale
                return _master
            return pd.DataFrame(columns=["symbol", "bse_code", "isin", "name"])
        df = parts[0]
        for p in parts[1:]:
            df = df.merge(p, on="isin", how="outer")
        if "name" not in df:
            df["name"] = df.get("bse_name")
        elif "bse_name" in df:
            df["name"] = df["name"].fillna(df["bse_name"])
        for c in ("symbol", "bse_code"):
            if c not in df:
                df[c] = None
        df["isin_valid"] = df["isin"].map(isin_valid)
        df.attrs["errors"] = errors
        _master = df.reset_index(drop=True)
        cache.put_snapshot("reference", "symbol_master", _master)
        return _master


def find(nse: Optional[str] = None, isin: Optional[str] = None, bse: Optional[str] = None) -> Optional[dict]:
    df = master()
    if df.empty:
        return None
    for col, val in (("isin", isin), ("symbol", nse), ("bse_code", bse)):
        if val and col in df:
            hit = df[df[col].astype(str).str.upper() == str(val).upper()]
            if not hit.empty:
                r = hit.iloc[0].to_dict()
                return {k: (None if pd.isna(v) else v) for k, v in r.items()}
    return None


def search(text: str, limit: int = 20) -> pd.DataFrame:
    """Find companies by part of the name or symbol."""
    df = master()
    if df.empty:
        return df
    t = text.upper()
    m = (df["symbol"].astype(str).str.upper().str.contains(t, regex=False)
         | df["name"].astype(str).str.upper().str.contains(t, regex=False))
    return df[m].head(limit).reset_index(drop=True)


def resolve(text: str, market: Optional[str] = None) -> Instrument:
    """Turn what the user typed into an Instrument. market: 'IN', 'GLOBAL' or None (guess)."""
    if isinstance(text, Instrument):
        return text
    t = str(text).strip()
    u = t.upper()
    idx = _INDEX_ALIASES.get(u) or _INDEX_ALIASES.get(u.replace(" ", ""))
    if idx and market != "GLOBAL":
        y, tv, upx, _ = INDICES[idx]
        return Instrument(t, kind="index", market="IN", nse=idx, yahoo=y, tv=tv, upstox_index=upx)
    if market == "IN_INDEX":       # any other NSE index name, e.g. "NIFTY TOTAL MARKET"
        return Instrument(t, kind="index", market="IN", nse=u, tv=f"NSE:{u.replace(' ', '')}")
    if u.endswith(".NS"):
        s = u[:-3]
        return Instrument(t, nse=s, yahoo=u, tv=f"NSE:{s}")
    if u.endswith(".BO"):
        s = u[:-3]
        if s.isdigit():
            return Instrument(t, _bse=s, yahoo=u, tv=f"BSE:{s}")
        return Instrument(t, nse=s, yahoo=u, tv=f"BSE:{s}")
    if isin_valid(u) and market != "GLOBAL":
        inst = Instrument(t, _isin=u)
        inst._lookup()
        if not inst.nse and not inst._bse and not u.startswith("IN"):
            return Instrument(t, kind="global", market="GLOBAL", yahoo=None, _isin=u)
        return inst
    if u.isdigit() and len(u) == 6 and market != "GLOBAL":
        inst = Instrument(t, _bse=u, yahoo=f"{u}.BO", tv=f"BSE:{u}")
        inst._lookup()
        return inst
    if market == "IN":
        return Instrument(t, nse=u, yahoo=f"{u}.NS", tv=f"NSE:{u}")
    if market == "GLOBAL" or "." in u or "=" in u or "^" in u:
        return Instrument(t, kind="global", market="GLOBAL", yahoo=t)
    if find(nse=u):
        return Instrument(t, nse=u, yahoo=f"{u}.NS", tv=f"NSE:{u}")
    return Instrument(t, kind="global", market="GLOBAL", yahoo=t)
