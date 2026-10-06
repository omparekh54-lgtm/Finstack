"""Settings and credentials.

Everything can be set three ways (first wins):
  1. fs.configure(...) in code
  2. environment variables (FINSTACK_HOME, UPSTOX_ACCESS_TOKEN, ...)
  3. ~/.finstack/config.toml, e.g.

        [keys]
        UPSTOX_ACCESS_TOKEN = "..."
        FRED_API_KEY = "..."

        [budgets]            # override a website's requests/second
        nse = 2.0
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

_OVERRIDES: Dict[str, Any] = {}
_FILE_CACHE: Optional[dict] = None


def home() -> Path:
    p = Path(_OVERRIDES.get("FINSTACK_HOME") or os.environ.get("FINSTACK_HOME") or Path.home() / ".finstack")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _file() -> dict:
    global _FILE_CACHE
    if _FILE_CACHE is None:
        path = home() / "config.toml"
        _FILE_CACHE = {}
        if path.exists():
            try:
                import tomllib
            except ModuleNotFoundError:  # Python 3.10
                try:
                    import tomli as tomllib  # type: ignore
                except ModuleNotFoundError:
                    return _FILE_CACHE
            with open(path, "rb") as fh:
                _FILE_CACHE = tomllib.load(fh)
    return _FILE_CACHE


def get(name: str, default: Any = None) -> Any:
    """A credential or setting: code override > environment > config.toml [keys] > default."""
    if name in _OVERRIDES:
        return _OVERRIDES[name]
    if os.environ.get(name):
        return os.environ[name]
    return _file().get("keys", {}).get(name, default)


def has(*names: str) -> bool:
    return all(get(n) for n in names)


def budget_override(group: str) -> Optional[float]:
    v = _OVERRIDES.get(f"budget:{group}")
    if v is None:
        v = _file().get("budgets", {}).get(group)
    return float(v) if v is not None else None


def configure(**settings: Any) -> None:
    """Set credentials or settings in code, e.g. fs.configure(FRED_API_KEY="...", FINSTACK_HOME="/data/fs").
    budgets={"nse": 2.0} overrides a website's requests per second.
    """
    budgets = settings.pop("budgets", None) or {}
    for k, v in budgets.items():
        _OVERRIDES[f"budget:{k}"] = v
    _OVERRIDES.update(settings)
    if budgets:
        from . import net

        net.reset_buckets()


# Credentials each source needs before the router will try it.
CREDENTIALS: Dict[str, tuple] = {
    "upstox": ("UPSTOX_ACCESS_TOKEN",),
    "kiteconnect": ("KITE_API_KEY", "KITE_ACCESS_TOKEN"),
    "dhanhq": ("DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN"),
    "smartapi": ("ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PIN", "ANGEL_TOTP_SECRET"),
    "fyers": ("FYERS_CLIENT_ID", "FYERS_ACCESS_TOKEN"),
    "alpaca": ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
    "finnhub": ("FINNHUB_API_KEY",),
    "twelvedata": ("TWELVEDATA_API_KEY",),
    "tiingo": ("TIINGO_API_KEY",),
    "polygon": ("POLYGON_API_KEY",),
    "alpha_vantage": ("ALPHAVANTAGE_API_KEY",),
    "fredapi": ("FRED_API_KEY",),
    "financetoolkit": ("FMP_API_KEY",),
    "fmpsdk": ("FMP_API_KEY",),
    "datagovindia": ("DATAGOVINDIA_API_KEY",),
    "edgar": ("EDGAR_IDENTITY",),
    "sec_edgar_downloader": ("EDGAR_IDENTITY",),
    "screener": ("SCREENER_USER", "SCREENER_PASS"),
    "pykrx": ("KRX_ID", "KRX_PW"),
    "tushare": ("TUSHARE_TOKEN",),
    "jquants": ("JQUANTS_REFRESH_TOKEN",),
    "praw": ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"),
    "newsapi": ("NEWSAPI_KEY",),
    "dune_client": ("DUNE_API_KEY",),
    "eodhd": ("EODHD_API_KEY",),
}


def missing_credentials(source: str) -> tuple:
    return tuple(n for n in CREDENTIALS.get(source, ()) if not get(n))
