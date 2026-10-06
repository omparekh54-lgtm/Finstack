"""Lazy importing with friendly install hints."""
from __future__ import annotations

import importlib
from typing import Optional

from .registry import CATALOG, lookup


def get(key: str, sub: str = ""):
    """Import and return a library module by its finstack key, optionally one of its sub-modules.

    >>> yf = finstack.get("yfinance")
    >>> cm = finstack.get("nselib", "capital_market")   # same as: import nselib.capital_market
    """
    lib = lookup(key)
    try:
        return importlib.import_module(f"{lib.module}.{sub}" if sub else lib.module)
    except ImportError as e:
        how = lib.install or f"pip install {lib.pip}\nor the whole group:\n    pip install 'finstack[{lib.extra}]'"
        raise ImportError(f"'{lib.pip}' is not installed. Install it with:\n    {how}") from e


def is_installed(key: str) -> bool:
    """True if the library is installed (checked without importing it, so it is fast)."""
    import importlib.util

    try:
        module = lookup(key).module
    except Exception:          # not in the catalog: treat the key as a module name
        module = key
    try:
        return importlib.util.find_spec(module) is not None
    except Exception:
        return False


def catalog(category: Optional[str] = None, region: Optional[str] = None, installed_only: bool = False):
    """Return the library catalog as a pandas DataFrame."""
    import pandas as pd

    from .registry import as_dicts

    rows = as_dicts(category, region)
    for r in rows:
        r["installed"] = is_installed(r["key"])
    df = pd.DataFrame(rows)
    if installed_only and not df.empty:
        df = df[df["installed"]]
    return df.reset_index(drop=True)


def doctor() -> dict:
    """Which libraries are importable right now?"""
    return {l.key: is_installed(l.key) for l in CATALOG}
