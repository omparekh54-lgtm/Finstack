"""Correctness checks run on every result before it is cached or returned.

Each check returns (good_rows, bad_rows, notes). Bad rows go to quarantine; notes go to
df.attrs["issues"]. If more than 20% of a source's rows are bad, the router treats the source as
failed and tries the next one.

Also here: corporate-action parsing (bonus / split ratios) used to build split-adjusted prices.
"""
from __future__ import annotations

import re
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

Result = Tuple[pd.DataFrame, pd.DataFrame, List[str]]
_EMPTY_NOTES: List[str] = []


def _split(df: pd.DataFrame, bad: pd.Series, notes: List[str]) -> Result:
    bad = bad.fillna(False).astype(bool)
    return df[~bad].reset_index(drop=True), df[bad].reset_index(drop=True), notes


def ohlcv(df: pd.DataFrame, time_col: str = "date", max_move: float = 0.20) -> Result:
    """Prices > 0; low <= open, close <= high (0.5% tolerance for rounding); one row per date."""
    notes: List[str] = []
    bad = pd.Series(False, index=df.index)
    if "close" in df:
        bad |= ~(df["close"] > 0)
    have = [c for c in ("open", "high", "low", "close") if c in df]
    if {"high", "low"} <= set(have):
        tol = 0.005 * df["high"].abs()
        bad |= df["low"] > df["high"] + tol
        for c in ("open", "close"):
            if c in df:
                ok = df[c].isna() | df["high"].isna() | df["low"].isna() | (
                    (df[c] <= df["high"] + tol) & (df[c] >= df["low"] - tol))
                bad |= ~ok
    if "volume" in df:
        bad |= df["volume"] < 0
    if time_col in df:
        dup = df.duplicated(subset=[time_col] + [c for c in ("symbol", "series", "instrument", "expiry", "strike",
                                                             "option_type")
                                                 if c in df], keep="last")
        if dup.any():
            notes.append(f"{int(dup.sum())} duplicate {time_col} rows dropped (kept the latest)")
        df = df[~dup]
        bad = bad[~dup]
    good, badrows, notes = _split(df, bad, notes)
    if "close" in good and time_col in good and len(good) > 2 and "symbol" not in good:
        ret = good["close"].pct_change().abs()
        jumps = good.loc[ret > max_move, time_col]
        if len(jumps):
            notes.append(f"{len(jumps)} day(s) moved more than {max_move:.0%} (check splits/bonus): "
                         + ", ".join(str(x)[:10] for x in jumps.head(5)))
    return good, badrows, notes


def missing_days(df: pd.DataFrame, expected_days, time_col: str = "date") -> List[str]:
    if time_col not in df or df.empty:
        return []
    have = set(pd.to_datetime(df[time_col]).dt.date)
    lo, hi = min(have), max(have)
    miss = [d for d in expected_days if lo <= d <= hi and d not in have]
    if miss:
        return [f"{len(miss)} trading day(s) missing inside the range, e.g. " + ", ".join(map(str, miss[:5]))]
    return []


def options(df: pd.DataFrame) -> Result:
    notes: List[str] = []
    bad = pd.Series(False, index=df.index)
    if "strike" in df:
        bad |= ~(df["strike"] > 0)
    if "option_type" in df:
        bad |= ~df["option_type"].isin(["CE", "PE"])
    for c in ("oi", "volume", "last", "iv"):
        if c in df:
            bad |= df[c] < 0
    if {"bid", "ask"} <= set(df.columns):
        crossed = (df["bid"] > 0) & (df["ask"] > 0) & (df["bid"] > df["ask"] * 1.05)
        if crossed.any():
            notes.append(f"{int(crossed.sum())} rows with bid above ask (stale quotes)")
    return _split(df, bad, notes)


def positive(col: str):
    def check(df: pd.DataFrame) -> Result:
        if col not in df:
            return df, df.iloc[0:0], _EMPTY_NOTES
        return _split(df, ~(df[col] > 0), [])
    return check


def results(df: pd.DataFrame) -> Result:
    """Profit and loss identities: PBT - tax ~= net profit (5% tolerance, flagged not dropped)."""
    notes: List[str] = []
    cols = set(df.columns)
    if {"profit_before_tax", "tax", "net_profit"} <= cols:
        calc = df["profit_before_tax"] - df["tax"]
        off = (calc - df["net_profit"]).abs() > 0.05 * df["net_profit"].abs().clip(lower=1)
        if off.any():
            notes.append(f"{int(off.sum())} period(s) where PBT - tax differs from net profit by >5% "
                         "(exceptional items, minority interest or a source error)")
    return df, df.iloc[0:0], notes


def nonempty(df: pd.DataFrame) -> Result:
    return df, df.iloc[0:0], _EMPTY_NOTES


def fx_triangulate(a_b: float, a_c: float, c_b: float, tol: float = 0.01) -> bool:
    """A/B should equal A/C * C/B within tol (1%)."""
    return abs(a_b - a_c * c_b) <= tol * abs(a_b)


# ------------------------------------------------------------------ corporate actions
_BONUS = re.compile(r"bonus[^0-9]*(\d+)\s*[:]\s*(\d+)", re.I)
_SPLIT = re.compile(r"(?:split|sub[- ]?division)[^0-9]*?(?:rs\.?|re\.?|inr|₹)?\s*(\d+(?:\.\d+)?)\s*/?-?\s*"
                    r"(?:each\s*)?(?:per share\s*)?to\s*(?:rs\.?|re\.?|inr|₹)?\s*(\d+(?:\.\d+)?)", re.I)


def action_factor(purpose: str) -> Optional[float]:
    """How many shares one old share becomes. 'Bonus 1:1' -> 2.0, 'Bonus 2:1' -> 3.0
    (a:b = a new shares for every b held, so factor (a+b)/b); 'Face Value Split From Rs 10 To Rs 2' -> 5.0.
    Dividends and other actions return None."""
    p = str(purpose or "")
    m = _BONUS.search(p)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return (a + b) / b if b else None
    m = _SPLIT.search(p)
    if m:
        old, new = float(m.group(1)), float(m.group(2))
        return old / new if new else None
    return None


def adjust_for_actions(df: pd.DataFrame, actions: pd.DataFrame, date_col: str = "date",
                       ex_col: str = "ex_date", purpose_col: str = "purpose") -> pd.DataFrame:
    """Add adj_close (and adj_volume) adjusted for splits and bonuses only, not dividends.
    Prices before each ex-date are divided by the factor; volumes multiplied."""
    out = df.copy()
    factor = pd.Series(1.0, index=out.index)
    if actions is not None and not actions.empty:
        for _, a in actions.iterrows():
            f = action_factor(a.get(purpose_col, ""))
            ex = pd.to_datetime(a.get(ex_col), errors="coerce", dayfirst=True)
            if f and f != 1 and pd.notna(ex):
                factor[pd.to_datetime(out[date_col]) < ex] *= f
    out["adj_close"] = out["close"] / factor
    if "volume" in out:
        out["adj_volume"] = out["volume"] * factor
    out["adj_factor"] = factor
    return out


def compare_close(a: pd.DataFrame, b: pd.DataFrame, time_col: str = "date") -> Optional[float]:
    """Median absolute % difference of closes on common dates (None if nothing overlaps)."""
    if a is None or b is None or "close" not in a or "close" not in b:
        return None
    m = a[[time_col, "close"]].merge(b[[time_col, "close"]], on=time_col, suffixes=("_a", "_b")).dropna()
    if m.empty:
        return None
    return float(np.median((m["close_a"] - m["close_b"]).abs() / m["close_b"].abs()) * 100)
