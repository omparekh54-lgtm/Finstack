"""Company results and reports: quarterly profits, annual results, annual-report PDFs,
results filings, announcements and results dates.

Indian companies use plain NSE symbols ("TCS", "RELIANCE"). US companies use their ticker
("AAPL"). Every function tries several sources and tells you which one answered.
"""
from __future__ import annotations

import datetime as _dt
import os
from typing import Optional

import pandas as pd

from .loader import get, is_installed


def _fail(what: str, errors: list):
    raise RuntimeError(f"All sources failed for {what}:\n  " + ("\n  ".join(errors) or "no source library installed"))


def _tag(obj, source: str):
    """Attach the data source to a DataFrame (df.attrs['source']) or dict."""
    if isinstance(obj, pd.DataFrame):
        obj.attrs["source"] = source
    elif isinstance(obj, dict):
        obj = {"source": source, **obj}
    return obj


def _is_india(symbol: str, market: str) -> bool:
    """India unless market='US' (a .NS / .BO suffix always means India)."""
    return symbol.upper().endswith((".NS", ".BO")) or market.upper() == "IN"


def _to_frame(data) -> Optional[pd.DataFrame]:
    """Turn an exchange JSON response into a table: first list of row-dicts found, else flatten."""
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return pd.DataFrame(v)
        return pd.json_normalize(data)
    return None


def _nse():
    from .india import nse_client

    return nse_client()


def _yahoo_symbol(symbol: str, india: bool) -> str:
    if not india or symbol.upper().endswith((".NS", ".BO")):
        return symbol
    return symbol.upper() + ".NS"


def _base(symbol: str) -> str:
    return symbol.upper().removesuffix(".NS").removesuffix(".BO")


# ---------------------------------------------------------------- results tables
def quarterly_results(symbol: str, market: str = "IN", consolidated: bool = True,
                      sources: Optional[tuple] = None) -> pd.DataFrame:
    """Last several quarters of revenue, expenses, operating profit, net profit, EPS.

    market = 'IN' (default) | 'US'.  The DataFrame's .attrs['source'] says where it came from.
    India order: NSE -> BSE -> Moneycontrol -> Tickertape -> Screener.in (needs login) -> Yahoo
    (BSE also covers companies listed only on BSE: pass the BSE scrip code, e.g. '532540').
    US order:    Yahoo -> SEC EDGAR (XBRL)
    """
    india = _is_india(symbol, market)
    sym = _base(symbol)
    errors = []
    order = sources or (("nse", "bse", "moneycontrol", "tickertape", "screener", "yahoo") if india
                        else ("yahoo", "edgar"))
    for src in order:
        try:
            if src == "nse" and is_installed("nse"):
                data = _nse().results_comparison(sym)
                rows = data
                if isinstance(data, dict):   # take the first list of row-dicts in the response
                    rows = next((v for v in data.values() if isinstance(v, list) and v and isinstance(v[0], dict)), None)
                if rows:
                    return _tag(pd.DataFrame(rows), "NSE results comparison")
                errors.append("nse: empty response")
            elif src == "bse" and is_installed("bse"):
                snap = bse_client().resultsSnapshot(_bse_code(symbol))
                df = _to_frame(snap)
                if df is not None and not df.empty:
                    return _tag(df, "BSE results snapshot")
                errors.append("bse: empty")
            elif src == "moneycontrol" and is_installed("bharat_sm_data"):
                from Fundamentals import MoneyControl

                mc = MoneyControl()
                sc_id, _ = mc.get_ticker(sym)
                df = mc.get_income_mini_statement(sc_id, statement_type="consolidated" if consolidated else "standalone",
                                                  statement_frequency=3)
                if df is not None and not df.empty:
                    return _tag(df, "Moneycontrol")
                errors.append("moneycontrol: empty")
            elif src == "tickertape" and is_installed("bharat_sm_data"):
                from Fundamentals import Tickertape

                tt = Tickertape()
                sid, _ = tt.get_ticker(sym)
                df = tt.get_income_data(sid, time_horizon="interim")
                if df is not None and not df.empty:
                    return _tag(df, "Tickertape")
                errors.append("tickertape: empty")
            elif src == "screener" and os.environ.get("SCREENER_USER") and is_installed("bharat_sm_data"):
                from .india import screener

                return _tag(screener(sym, "quarterly", consolidated=consolidated), "Screener.in")
            elif src == "yahoo" and is_installed("yfinance"):
                df = get("yfinance").Ticker(_yahoo_symbol(symbol, india)).quarterly_income_stmt
                if df is not None and not df.empty:
                    return _tag(df, "Yahoo Finance")
                errors.append("yahoo: empty")
            elif src == "edgar" and is_installed("edgar"):
                ed = get("edgar")
                if os.environ.get("EDGAR_IDENTITY"):
                    ed.set_identity(os.environ["EDGAR_IDENTITY"])
                df = ed.Company(sym).income_statement(periods=8, period="quarterly", as_dataframe=True)
                if df is not None and not df.empty:
                    return _tag(df, "SEC EDGAR")
                errors.append("edgar: empty")
        except Exception as e:
            errors.append(f"{src}: {str(e)[:150]}")
    _fail(f"quarterly results of {symbol}", errors)


def annual_results(symbol: str, market: str = "IN", consolidated: bool = True) -> pd.DataFrame:
    """Yearly profit & loss (revenue, profit, EPS) for the last several years.
    India: Moneycontrol -> Tickertape -> Yahoo.  US: Yahoo -> SEC EDGAR.
    """
    india = _is_india(symbol, market)
    sym = _base(symbol)
    errors = []
    if india and is_installed("bharat_sm_data"):
        try:
            from Fundamentals import MoneyControl

            mc = MoneyControl()
            sc_id, _ = mc.get_ticker(sym)
            df = mc.get_income_mini_statement(sc_id, statement_type="consolidated" if consolidated else "standalone",
                                              statement_frequency=12)
            if df is not None and not df.empty:
                return _tag(df, "Moneycontrol")
        except Exception as e:
            errors.append(f"moneycontrol: {str(e)[:150]}")
        try:
            from Fundamentals import Tickertape

            tt = Tickertape()
            sid, _ = tt.get_ticker(sym)
            df = tt.get_income_data(sid, time_horizon="annual")
            if df is not None and not df.empty:
                return _tag(df, "Tickertape")
        except Exception as e:
            errors.append(f"tickertape: {str(e)[:150]}")
    if is_installed("yfinance"):
        try:
            df = get("yfinance").Ticker(_yahoo_symbol(symbol, india)).income_stmt
            if df is not None and not df.empty:
                return _tag(df, "Yahoo Finance")
            errors.append("yahoo: empty")
        except Exception as e:
            errors.append(f"yahoo: {str(e)[:150]}")
    if not india and is_installed("edgar"):
        try:
            ed = get("edgar")
            if os.environ.get("EDGAR_IDENTITY"):
                ed.set_identity(os.environ["EDGAR_IDENTITY"])
            return _tag(ed.Company(sym).income_statement(periods=5, period="annual", as_dataframe=True), "SEC EDGAR")
        except Exception as e:
            errors.append(f"edgar: {str(e)[:150]}")
    _fail(f"annual results of {symbol}", errors)


def balance_sheet(symbol: str, market: str = "IN", quarterly: bool = False) -> pd.DataFrame:
    """Balance sheet. India: Moneycontrol -> Yahoo.  US: Yahoo."""
    india = _is_india(symbol, market)
    errors = []
    if india and not quarterly and is_installed("bharat_sm_data"):
        try:
            from Fundamentals import MoneyControl

            mc = MoneyControl()
            sc_id, _ = mc.get_ticker(_base(symbol))
            df = mc.get_balance_sheet_mini_statement(sc_id)
            if df is not None and not df.empty:
                return _tag(df, "Moneycontrol")
            errors.append("moneycontrol: empty")
        except Exception as e:
            errors.append(f"moneycontrol: {str(e)[:150]}")
    try:
        t = get("yfinance").Ticker(_yahoo_symbol(symbol, india))
        df = t.quarterly_balance_sheet if quarterly else t.balance_sheet
        if df is not None and not df.empty:
            return _tag(df, "Yahoo Finance")
        errors.append("yahoo: empty")
    except Exception as e:
        errors.append(f"yahoo: {str(e)[:150]}")
    _fail(f"balance sheet of {symbol}", errors)


def cash_flow(symbol: str, market: str = "IN", quarterly: bool = False) -> pd.DataFrame:
    """Cash flow statement. India: Moneycontrol -> Yahoo.  US: Yahoo."""
    india = _is_india(symbol, market)
    errors = []
    if india and not quarterly and is_installed("bharat_sm_data"):
        try:
            from Fundamentals import MoneyControl

            mc = MoneyControl()
            sc_id, _ = mc.get_ticker(_base(symbol))
            df = mc.get_cash_flow_mini_statement(sc_id)
            if df is not None and not df.empty:
                return _tag(df, "Moneycontrol")
            errors.append("moneycontrol: empty")
        except Exception as e:
            errors.append(f"moneycontrol: {str(e)[:150]}")
    try:
        t = get("yfinance").Ticker(_yahoo_symbol(symbol, india))
        df = t.quarterly_cashflow if quarterly else t.cashflow
        if df is not None and not df.empty:
            return _tag(df, "Yahoo Finance")
        errors.append("yahoo: empty")
    except Exception as e:
        errors.append(f"yahoo: {str(e)[:150]}")
    _fail(f"cash flow of {symbol}", errors)


# ---------------------------------------------------------------- reports & filings
def annual_reports(symbol: str, market: str = "IN"):
    """Annual-report documents.
    India: list of {fromYr, toYr, fileName (PDF/ZIP link)} from NSE.
    US: 10-K filings from SEC EDGAR (set EDGAR_IDENTITY="Your Name you@email.com").
    """
    if _is_india(symbol, market):
        data = _nse().annual_reports(_base(symbol))
        return data.get("data", data) if isinstance(data, dict) else data
    ed = get("edgar")
    if os.environ.get("EDGAR_IDENTITY"):
        ed.set_identity(os.environ["EDGAR_IDENTITY"])
    return ed.Company(symbol.upper()).get_filings(form="10-K")


def result_filings(symbol: Optional[str] = None, period: str = "quarterly",
                   start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """NSE financial-results filings with links to the result PDF and XBRL file.
    symbol=None returns every company's filings in the date range (default: last 30 days).
    period = quarterly | annual | half-yearly.
    """
    to_d = _dt.datetime.fromisoformat(end) if end else _dt.datetime.now()
    from_d = _dt.datetime.fromisoformat(start) if start else to_d - _dt.timedelta(days=30)
    rows = _nse().financial_results(period=period, symbol=_base(symbol) if symbol else None,
                                    from_date=from_d, to_date=to_d)
    return _tag(pd.DataFrame(rows), "NSE")


def announcements(symbol: Optional[str] = None, days: int = 30) -> pd.DataFrame:
    """Corporate announcements filed on NSE (results, dividends, orders, mergers ...) with PDF links."""
    to_d = _dt.datetime.now()
    rows = _nse().announcements(symbol=_base(symbol) if symbol else None,
                                from_date=to_d - _dt.timedelta(days=days), to_date=to_d)
    return _tag(pd.DataFrame(rows), "NSE")


def board_meetings(symbol: Optional[str] = None) -> pd.DataFrame:
    """Forthcoming board meetings on NSE (this is where quarterly-results dates are announced)."""
    return _tag(pd.DataFrame(_nse().board_meetings(symbol=_base(symbol) if symbol else None)), "NSE")


def upcoming_results() -> pd.DataFrame:
    """Companies announcing results soon (NSE event calendar). Falls back to NSE board meetings."""
    errors = []
    if is_installed("nsefin"):
        try:
            return _tag(get("nsefin").NSEClient().get_upcoming_results(), "NSE via nsefin")
        except Exception as e:
            errors.append(f"nsefin: {str(e)[:150]}")
    try:
        return board_meetings()
    except Exception as e:
        errors.append(f"nse: {str(e)[:150]}")
    _fail("upcoming results", errors)


def earnings_dates(symbol: str, market: str = "IN") -> pd.DataFrame:
    """Past and upcoming earnings dates with EPS estimate vs actual and surprise % (Yahoo)."""
    return _tag(get("yfinance").Ticker(_yahoo_symbol(symbol, _is_india(symbol, market))).get_earnings_dates(limit=12),
                "Yahoo Finance")


# ================================================================ BSE (covers ~5,000 BSE-only companies)
_BSE = None


def bse_client():
    """Shared BseIndiaApi client (pip name 'bse'). Use it for anything not wrapped below:

    >>> b = fs.bse_client(); b.near52WeekHighLow(); b.listSecurities(industry="Banks")
    """
    global _BSE
    if _BSE is None:
        import tempfile

        get("bse")
        from bse import BSE

        folder = os.path.join(tempfile.gettempdir(), "finstack_bse")
        os.makedirs(folder, exist_ok=True)
        _BSE = BSE(download_folder=folder)
    return _BSE


def _bse_code(symbol_or_code: str) -> str:
    """Accept a BSE scrip code ('500325'), NSE-style symbol ('RELIANCE'), name or ISIN."""
    s = str(symbol_or_code).strip()
    if s.isdigit():
        return s
    b = bse_client()
    try:
        return str(b.getScripCode(_base(s)))
    except Exception:
        hit = b.lookup(s)
        if hit and hit.get("bse_code"):
            return str(hit["bse_code"])
        raise ValueError(f"No BSE scrip code found for '{symbol_or_code}'")


def company_lookup(text: str) -> dict:
    """Find a company by name, NSE symbol, ISIN or BSE code -> {company_name, symbol, isin, bse_code}."""
    return bse_client().lookup(text)


def bse_results(symbol_or_code: str) -> dict:
    """BSE results snapshot: latest quarters' revenue, net profit, EPS etc. Works for BSE-only stocks too."""
    return _tag(bse_client().resultsSnapshot(_bse_code(symbol_or_code)), "BSE")


def bse_result_calendar(days: int = 30, symbol_or_code: Optional[str] = None) -> pd.DataFrame:
    """Companies scheduled to declare results on BSE in the next `days` days."""
    now = _dt.datetime.now()
    rows = bse_client().resultCalendar(from_date=now, to_date=now + _dt.timedelta(days=days),
                                       scripcode=_bse_code(symbol_or_code) if symbol_or_code else None)
    return _tag(pd.DataFrame(rows), "BSE")


def bse_announcements(symbol_or_code: Optional[str] = None, days: int = 30, page: int = 1) -> pd.DataFrame:
    """Corporate announcements filed on BSE (results, board meetings, orders ...) with PDF links."""
    to_d = _dt.datetime.now()
    data = bse_client().announcements(page_no=page, from_date=to_d - _dt.timedelta(days=days), to_date=to_d,
                                      scripcode=_bse_code(symbol_or_code) if symbol_or_code else None)
    rows = data.get("Table", data) if isinstance(data, dict) else data
    return _tag(pd.DataFrame(rows), "BSE")


def bse_actions(symbol_or_code: Optional[str] = None, days: int = 30) -> pd.DataFrame:
    """Forthcoming dividends, splits, bonuses, buybacks on BSE."""
    now = _dt.datetime.now()
    rows = bse_client().actions(from_date=now, to_date=now + _dt.timedelta(days=days),
                                scripcode=_bse_code(symbol_or_code) if symbol_or_code else None)
    return _tag(pd.DataFrame(rows), "BSE")


def list_companies(industry: str = "", group: str = "A", status: str = "Active") -> pd.DataFrame:
    """BSE-listed companies with industry, group, ISIN. group = A, B, T, X, ... ('' for all)."""
    return _tag(pd.DataFrame(bse_client().listSecurities(industry=industry, group=group, status=status)), "BSE")


# ================================================================ official numbers from XBRL filings
# Best-effort patterns for headline items in SEBI/NSE/BSE Ind AS, banking and NBFC results taxonomies.
_KEY_ITEMS = {
    "revenue_from_operations": r"^RevenueFromOperations$",
    "other_income": r"^OtherIncome$",
    "total_income": r"^(?:Income|TotalIncome)$",
    "total_expenses": r"^(?:Expenses|TotalExpenses)$",
    "profit_before_tax": r"^ProfitLossBeforeTax|^ProfitBeforeTax",
    "tax_expense": r"^(?:TaxExpense|IncomeTaxExpense)",
    "net_profit": r"^ProfitLossForPeriod$|^ProfitLossForThePeriod$|^NetProfitLossForThePeriod",
    "net_profit_owners": r"ProfitOrLossAttributableToOwnersOfParent$",
    "eps_basic": r"^BasicEarningsLossPerShare",
    "eps_diluted": r"^DilutedEarningsLossPerShare",
}


def xbrl_facts(source: str) -> pd.DataFrame:
    """Every number in an XBRL results filing as a table: element, value, unit, period, context.

    source = URL of the .xml file (from fs.result_filings) or a local file path. NSE/BSE may block
    direct downloads; if so, download the file in a browser and pass the path.
    """
    import xml.etree.ElementTree as ET

    if os.path.exists(source):
        with open(source, "rb") as fh:
            raw = fh.read()
    else:
        import requests

        r = requests.get(source, timeout=30, headers={"User-Agent": "Mozilla/5.0 finstack",
                                                      "Referer": "https://www.nseindia.com/"})
        r.raise_for_status()
        raw = r.content
    root = ET.fromstring(raw)
    local = lambda tag: tag.rsplit("}", 1)[-1]  # noqa: E731

    periods = {}
    for ctx in root.iter():
        if local(ctx.tag) != "context":
            continue
        p = {local(e.tag): (e.text or "").strip() for e in ctx.iter() if local(e.tag) in ("startDate", "endDate", "instant")}
        dims = [f"{m.get('dimension')}={(m.text or '').strip()}" for m in ctx.iter() if local(m.tag) == "explicitMember"]
        periods[ctx.get("id")] = (p.get("startDate"), p.get("endDate") or p.get("instant"), ";".join(dims))

    rows = []
    for el in root:
        ref = el.get("contextRef")
        if ref is None or el.text is None:
            continue
        start, end, dims = periods.get(ref, (None, None, ""))
        text = el.text.strip()
        try:
            value = float(text)
        except ValueError:
            value = text
        rows.append({"element": local(el.tag), "value": value, "unit": el.get("unitRef"),
                     "start": start, "end": end, "dimensions": dims, "context": ref})
    df = pd.DataFrame(rows)
    df.attrs["source"] = source
    return df


def xbrl_results(source: str) -> pd.DataFrame:
    """Headline figures (revenue, expenses, profit before tax, tax, net profit, EPS) from an XBRL
    results filing, one column per reporting period. Uses only facts without dimensions
    (i.e. the company's main figures, not segment breakdowns). Inspect fs.xbrl_facts() for everything.
    """
    import re

    facts = xbrl_facts(source)
    main = facts[(facts["dimensions"] == "") & facts["value"].map(lambda v: isinstance(v, float))]
    out = {}
    for label, pattern in _KEY_ITEMS.items():
        hit = main[main["element"].str.contains(pattern, regex=True, flags=re.I)]
        if hit.empty:
            continue
        hit = hit.assign(period=hit["start"].fillna("") + " to " + hit["end"].fillna(""))
        out[label] = hit.groupby("period")["value"].first()
    df = pd.DataFrame(out).T
    df = df[sorted(df.columns, key=lambda c: c.split(" to ")[-1], reverse=True)] if not df.empty else df
    df.attrs["source"] = source
    return df


def _xml_links(df: pd.DataFrame) -> list:
    links = []
    for row in df.to_dict("records"):
        for v in row.values():
            if isinstance(v, str) and v.lower().split("?")[0].endswith(".xml"):
                links.append(v)
    return links


def latest_results(symbol: str, period: str = "quarterly") -> pd.DataFrame:
    """Official headline figures from the company's most recent NSE results filing (XBRL).
    Searches the last 120 days of filings; df.attrs['source'] is the XBRL file used.
    """
    start = (_dt.date.today() - _dt.timedelta(days=120)).isoformat()
    links = _xml_links(result_filings(symbol, period=period, start=start))
    if not links:
        raise RuntimeError(f"No XBRL results filing found for {symbol} in the last 120 days")
    return xbrl_results(links[0])
