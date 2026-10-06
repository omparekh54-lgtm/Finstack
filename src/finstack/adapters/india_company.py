"""India company pipelines: results and statements, corporate events, IPOs, mutual funds."""
from __future__ import annotations

import calendar as _cal
import datetime as dt
import io
import re
from typing import Optional

import pandas as pd

from ..core import cache, validate
from ..core.router import Pipeline, Req, Source, register, snapshot_key
from ..core.schema import canon
from ..loader import get
from ._common import BSE_HOSTS, NSE_HOSTS, YAHOO_HOSTS, bse, dtm, need, nse, to_records

IST = "Asia/Kolkata"

# =============================================================== results and statements
# Every source's line items are mapped to these names; amounts are converted to INR crore.
RESULT_FIELDS = [
    ("revenue", r"^(revenue from operations|net sales|total revenue|revenue|sales|net revenue|operating revenue|"
                r"total income from operations|net sales income from operations|interest earned)\b"),
    ("other_income", r"^other income"),
    ("total_income", r"^(total income|income)$"),
    ("expenses", r"^(total expenses|total expenditure|expenses|total expense|expenditure)\b"),
    ("operating_profit", r"^(operating profit|ebitda|operating income|pbdit)\b"),
    ("depreciation", r"^(depreciation|depreciation and amortisation|depreciation amortization)"),
    ("interest", r"^(interest|finance costs?|interest expense)\b"),
    ("profit_before_tax", r"^(profit before tax|pbt|pretax income|profit loss before tax|profit before taxes)\b"),
    ("tax", r"^(tax|tax expense|total tax|tax provision|income tax|tax expenses)\b"),
    ("net_profit", r"^(net profit|net income$|net income common stockholders|profit after tax|pat\b|"
                   r"profit loss for period|net profit loss for the period|profit for the period|reported net profit)"),
    ("eps", r"^(eps|basic eps|diluted eps|eps basic|earnings per share|basic earnings per share|eps in rs)"),
]
PER_SHARE = {"eps"}
RESULT_COLS = ("period_end", "period_start", "revenue", "other_income", "total_income", "expenses",
               "operating_profit", "depreciation", "interest", "profit_before_tax", "tax", "net_profit", "eps",
               "units", "basis", "symbol")
_MONTHS = {m.lower(): i for i, m in enumerate(_cal.month_abbr) if m}


def period_end(label) -> Optional[pd.Timestamp]:
    """'Dec-25', "Dec '25", 'Dec 2025', 'FY24-25', '2025-03-31', '2024-04-01 to 2024-06-30' -> month end."""
    if isinstance(label, (pd.Timestamp, dt.date)):
        return pd.Timestamp(label) + pd.offsets.MonthEnd(0)
    s = str(label).strip()
    if " to " in s:
        s = s.split(" to ")[-1]
    m = re.match(r"^FY\s*'?(\d{2,4})\s*[-/]\s*'?(\d{2,4})$", s, re.I)
    if m:
        y = int(m.group(2))
        return pd.Timestamp(year=y + 2000 if y < 100 else y, month=3, day=31)
    m = re.match(r"^FY\s*'?(\d{2,4})$", s, re.I)
    if m:
        y = int(m.group(1))
        return pd.Timestamp(year=y + 2000 if y < 100 else y, month=3, day=31)
    m = re.match(r"^([A-Za-z]{3})[a-z]*[\s\-']+'?(\d{2,4})$", s)
    if m and m.group(1).lower() in _MONTHS:
        y = int(m.group(2))
        return pd.Timestamp(year=y + 2000 if y < 100 else y, month=_MONTHS[m.group(1).lower()], day=1) + \
            pd.offsets.MonthEnd(0)
    try:
        t = pd.Timestamp(s)
        return t.normalize() + pd.offsets.MonthEnd(0) if t.day != t.days_in_month else t.normalize()
    except Exception:
        return None


def _num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").replace("₹", "").strip()
    if s in ("", "-", "--", "NA", "nan", "None"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    try:
        x = float(s.strip("()"))
        return -x if neg else x
    except ValueError:
        return None


def periods_from_wide(wide: pd.DataFrame, scale: float, map_items: bool = True) -> pd.DataFrame:
    """Items as rows and periods as columns (or the other way round) -> one row per period.
    scale converts amounts to INR crore (per-share and % lines are left alone)."""
    df = wide.copy()
    if not isinstance(df.index, pd.RangeIndex) or df.index.name:
        df = df.reset_index()
    # orientation: periods are the columns whose labels parse as dates
    col_periods = {c: period_end(c) for c in df.columns[1:]}
    if sum(v is not None for v in col_periods.values()) < max(1, (len(df.columns) - 1) // 2):
        first = df.columns[0]
        df = df.set_index(first).T.reset_index()
        col_periods = {c: period_end(c) for c in df.columns[1:]}
    item_col = df.columns[0]
    rows = {}
    for c, pe in col_periods.items():
        if pe is None:
            continue
        rec = rows.setdefault(pe, {"period_end": pe})
        for _, r in df.iterrows():
            label = re.sub(r"[^a-z0-9%]+", " ", str(r[item_col]).lower().replace("_", " ")).strip()
            if not label or "%" in label or label.endswith(" pct") or "margin" in label:
                continue
            val = _num(r[c])
            if val is None:
                continue
            if map_items:
                for field, pat in RESULT_FIELDS:
                    if field not in rec and re.search(pat, label):
                        rec[field] = val if field in PER_SHARE else val * scale
                        break
            else:
                key = canon(label)
                per_share = "per share" in label or label.startswith("eps")
                rec.setdefault(key, val if per_share else val * scale)
    out = pd.DataFrame(list(rows.values()))
    if out.empty:
        return out
    return out.sort_values("period_end", ascending=False).reset_index(drop=True)


def _statement(req) -> str:
    return req.p("statement", "quarterly")


def _consolidated(req) -> bool:
    return bool(req.p("consolidated", True))


def _results_only(req):
    return _statement(req) in ("quarterly", "annual")


def _f_xbrl(req):
    from ..company import latest_results

    need(_statement(req) == "quarterly", "XBRL route gives the latest quarter only")
    wide = latest_results(req.inst.nse)
    df = periods_from_wide(wide, 1e-7)
    starts = {period_end(c): str(c).split(" to ")[0] for c in wide.columns if " to " in str(c)}
    df["period_start"] = df["period_end"].map(lambda p: pd.Timestamp(starts[p]) if p in starts else None)
    df["xbrl"] = wide.attrs.get("source")
    # keep quarter-length periods (filings also carry year-to-date columns)
    if df["period_start"].notna().any():
        days = (df["period_end"] - pd.to_datetime(df["period_start"])).dt.days
        df = df[days.isna() | (days < 100)]
    return df.assign(basis="as filed (XBRL)")


def _f_nse(req):
    need(_statement(req) == "quarterly", "NSE results comparison is quarterly")
    rows = to_records(nse().results_comparison(req.inst.nse))
    need(rows, "NSE returned no results")
    out = []
    for r in rows:
        g = lambda *k: next((_num(r.get(x)) for x in k if _num(r.get(x)) is not None), None)  # noqa: E731
        lakh = lambda *k: (g(*k) / 100 if g(*k) is not None else None)  # noqa: E731  (INR lakh -> crore)
        out.append({"period_end": period_end(pd.to_datetime(r.get("re_to_dt"), dayfirst=True)),
                    "period_start": pd.to_datetime(r.get("re_from_dt"), dayfirst=True),
                    "revenue": lakh("re_net_sale", "re_int_earned", "re_rev_frm_opr"),
                    "other_income": lakh("re_oth_inc_new", "re_oth_inc"),
                    "total_income": lakh("re_total_inc"),
                    "expenses": lakh("re_oth_tot_exp", "re_tot_exp"),
                    "depreciation": lakh("re_depr_und_exp"), "interest": lakh("re_int_new", "re_int_expd"),
                    "profit_before_tax": lakh("re_pro_loss_bef_tax"), "tax": lakh("re_tax"),
                    "net_profit": lakh("re_net_profit", "re_con_pro_loss", "re_proloss_ord_act"),
                    "eps": g("re_basic_eps_for_cont_dic_opr", "re_basic_eps", "re_bsc_eps_bfr_exi")})
    return pd.DataFrame(out).assign(basis="as filed on NSE (standalone unless the filing is consolidated)")


def _f_bse(req):
    need(_results_only(req), "BSE snapshot has P&L headlines only")
    need(req.inst.bse, "BSE code unknown")
    snap = bse().resultsSnapshot(req.inst.bse)
    block = snap.get("results_in_crores") or {}
    need(block.get("data"), "BSE returned no results")
    wide = pd.DataFrame(block["data"], columns=block["fields"])
    df = periods_from_wide(wide, 1.0)
    annual = df["period_end"].dt.month == 3
    labels = [c for c in block["fields"][1:]]
    fy = {period_end(c) for c in labels if str(c).upper().startswith("FY")}
    is_fy = df["period_end"].isin(fy)
    df = df[is_fy] if _statement(req) == "annual" else df[~is_fy]
    return df.assign(basis="BSE results snapshot")


def _mc(req):
    from Fundamentals import MoneyControl

    mc = MoneyControl()
    sc_id, _ = mc.get_ticker(req.inst.nse)
    kind = "consolidated" if _consolidated(req) else "standalone"
    st = _statement(req)
    if st == "quarterly":
        return periods_from_wide(mc.get_income_mini_statement(sc_id, statement_type=kind, statement_frequency=3), 1.0)
    if st == "annual":
        return periods_from_wide(mc.get_income_mini_statement(sc_id, statement_type=kind, statement_frequency=12), 1.0)
    fn = mc.get_balance_sheet_mini_statement if st == "balance_sheet" else mc.get_cash_flow_mini_statement
    return periods_from_wide(fn(sc_id, statement_type=kind), 1.0, map_items=False)


def _tt(req):
    from Fundamentals import Tickertape

    need(_results_only(req), "Tickertape route covers income statements")
    tt = Tickertape()
    sid, _ = tt.get_ticker(req.inst.nse)
    sid = sid.get("sid") if isinstance(sid, dict) else sid
    raw = tt.get_income_data(sid, time_horizon="interim" if _statement(req) == "quarterly" else "annual")
    df = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(to_records(raw) or raw)
    return periods_from_wide(df, 1.0)


def _scr(req):
    from ..india import screener

    table = {"quarterly": "quarterly", "annual": "profit_loss", "balance_sheet": "balance_sheet",
             "cash_flow": "cash_flow"}[_statement(req)]
    raw = screener(req.inst.nse, table, consolidated=_consolidated(req))
    return periods_from_wide(raw, 1.0, map_items=_results_only(req))


def _yf(req):
    t = get("yfinance").Ticker(req.inst.yahoo)
    st = _statement(req)
    wide = {"quarterly": t.quarterly_income_stmt, "annual": t.income_stmt, "balance_sheet": t.balance_sheet,
            "cash_flow": t.cashflow}[st]
    need(wide is not None and not wide.empty, "Yahoo returned nothing")
    return periods_from_wide(wide, 1e-7, map_items=_results_only(req)).assign(basis="Yahoo (consolidated)")


def _post_fin(df, req):
    df = df.assign(units="INR crore (EPS in INR)", symbol=req.inst.nse or req.inst.query)
    if "basis" not in df:
        df["basis"] = "consolidated" if _consolidated(req) else "standalone"
    if _results_only(req):
        need({"revenue", "net_profit"} & set(df.columns), "no revenue or profit lines recognised")
    return df.sort_values("period_end", ascending=False)


register(Pipeline(
    "india_company_financials", "Indian company results and statements (INR crore)", "snapshot", [
        Source("builtin:xbrl", _f_xbrl, ("www.nseindia.com", "nsearchives.nseindia.com"), libs=("nse",),
               when=lambda r: bool(r.inst.nse)),
        Source("nse", _f_nse, NSE_HOSTS, when=lambda r: bool(r.inst.nse)),
        Source("bse", _f_bse, BSE_HOSTS),
        Source("bharat_sm_data", _mc, ("www.moneycontrol.com", "priceapi.moneycontrol.com"), label="moneycontrol",
               when=lambda r: bool(r.inst.nse)),
        Source("screener", _scr, ("www.screener.in",), libs=("bharat_sm_data",), score=3.9,
               when=lambda r: bool(r.inst.nse)),
        Source("bharat_sm_data", _tt, ("api.tickertape.in", "www.tickertape.in"), label="tickertape",
               when=lambda r: bool(r.inst.nse)),
        Source("yfinance", _yf, YAHOO_HOSTS, when=lambda r: bool(r.inst.yahoo)),
    ],
    market="IN", required=("period_end",), time_col="_none", normalize=False, check=validate.results,
    ttl=12 * 3600, post=_post_fin, columns=RESULT_COLS,
    params_doc="symbol, statement=quarterly|annual|balance_sheet|cash_flow, consolidated=True",
    example='fs.fetch("india_company_financials", "TCS", statement="quarterly")'))


# =============================================================== corporate events
def _ev(req):
    return req.p("what", "actions")


def _window(req, back=365, fwd=90):
    today = dt.date.today()
    return dtm(req.start or today - dt.timedelta(days=back)), dtm(req.end or today + dt.timedelta(days=fwd))


def _sym(req):
    return req.inst.nse if req.inst else None


def _since(df: pd.DataFrame, start) -> pd.DataFrame:
    """Keep rows dated on/after start, using the first date-like column NSE returned."""
    if df.empty:
        return df
    for c in ("broadCastDate", "an_dt", "sort_date", "filingDate", "exchdisstime", "date"):
        if c in df.columns:
            d = pd.to_datetime(df[c], errors="coerce", dayfirst=True, format="mixed")
            if d.notna().any():
                return df[d >= pd.Timestamp(start)]
    return df


def _e_nse(req):
    n, w = nse(), _ev(req)
    if w == "actions":
        a, b = _window(req)
        return pd.DataFrame(n.actions(segment="equities", symbol=_sym(req), from_date=a, to_date=b))
    if w == "announcements":
        a, b = _window(req, 30, 0)
        df = pd.DataFrame(n.announcements(symbol=_sym(req), from_date=a, to_date=b))
        if df.empty and _sym(req):        # NSE sometimes ignores symbol+date filters together
            df = _since(pd.DataFrame(n.announcements(symbol=_sym(req))), a)
        return df
    if w in ("board_meetings", "results_calendar"):
        a, b = _window(req, 30, 90)
        df = pd.DataFrame(n.board_meetings(symbol=_sym(req), from_date=a, to_date=b))
        if w == "results_calendar" and len(df):
            purpose = next((c for c in df.columns if "purpose" in c.lower()), None)
            if purpose:
                df = df[df[purpose].astype(str).str.contains("result", case=False)]
        return df
    if w == "result_filings":
        a, b = _window(req, 120, 0)
        period = req.p("period", "quarterly")
        df = pd.DataFrame(n.financial_results(period=period, symbol=_sym(req), from_date=a, to_date=b))
        if df.empty and _sym(req):        # with a symbol, NSE often returns nothing for a date window
            df = _since(pd.DataFrame(n.financial_results(period=period, symbol=_sym(req))), a)
        if df.empty and _sym(req):        # last resort: every company's filings in the window, then filter
            alln = pd.DataFrame(n.financial_results(period=period, from_date=a, to_date=b))
            if "symbol" in alln:
                df = alln[alln["symbol"].astype(str).str.upper() == _sym(req).upper()]
        return df
    if w == "shareholding":
        need(_sym(req), "shareholding needs a symbol")
        return pd.DataFrame(to_records(n.shareholding(_sym(req))) or n.shareholding(_sym(req)))
    if w == "annual_reports":
        need(_sym(req), "annual_reports needs a symbol")
        return pd.DataFrame(to_records(n.annual_reports(_sym(req))))
    raise LookupError(f"nse source has no '{w}'")


def _e_bse(req):
    b, w = bse(), _ev(req)
    code = req.inst.bse if req.inst else None
    if w == "actions":
        a, e = _window(req)
        return pd.DataFrame(b.actions(from_date=a, to_date=e, scripcode=code))
    if w == "announcements":
        a, e = _window(req, 30, 0)
        return pd.DataFrame(to_records(b.announcements(from_date=a, to_date=e, scripcode=code)))
    if w in ("results_calendar", "board_meetings"):
        a, e = _window(req, 0, 30)
        return pd.DataFrame(b.resultCalendar(from_date=a, to_date=e, scripcode=code))
    raise LookupError(f"bse source has no '{w}'")


_EV_RENAME = {"exDate": "ex_date", "Ex_date": "ex_date", "subject": "purpose", "Purpose": "purpose",
              "recDate": "record_date", "RD_Date": "record_date", "comp": "name", "long_name": "name",
              "Long_Name": "name", "short_name": "symbol_bse", "scrip_code": "bse_code", "scrip_Code": "bse_code",
              "faceVal": "face_value", "meeting_date": "date", "bm_date": "date", "bm_purpose": "purpose",
              "attchmntFile": "attachment", "desc": "subject", "an_dt": "date", "sm_name": "name"}


def _post_ev(df, req):
    df = df.rename(columns={k: v for k, v in _EV_RENAME.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()]
    for c in ("ex_date", "record_date", "date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c].replace({"-": None, "": None}), errors="coerce", dayfirst=True, format="mixed")
    if _ev(req) == "actions" and "purpose" in df.columns:
        df["split_bonus_factor"] = df["purpose"].map(validate.action_factor)
    return df


register(Pipeline(
    "india_corporate_events", "Indian corporate events: actions, announcements, board meetings, results dates, "
    "result filings (XBRL links), shareholding, annual reports", "snapshot", [
        Source("nse", _e_nse, NSE_HOSTS),
        Source("bse", _e_bse, BSE_HOSTS, when=lambda r: _ev(r) in ("actions", "announcements", "results_calendar",
                                                                      "board_meetings")),
    ],
    market="IN", needs_symbol=False, normalize=False, ttl=6 * 3600, post=_post_ev, columns=(),
    params_doc="symbol (optional: all companies), what=actions|announcements|board_meetings|results_calendar|"
               "result_filings|shareholding|annual_reports, start, end",
    example='fs.fetch("india_corporate_events", "INFY", what="actions", start="10y")'))


# =============================================================== IPOs
def _i_nse(req):
    n, w = nse(), req.p("what", "current")
    if w == "past":
        a, b = _window(req, 365, 0)
        return pd.DataFrame(n.list_past_ipo(from_date=a, to_date=b))
    return pd.DataFrame({"current": n.list_current_ipo, "upcoming": n.list_upcoming_ipo}[w]())


def _i_aynse(req):
    a, w = get("aynse"), req.p("what", "current")
    if w == "past":
        s, e = _window(req, 365, 0)
        return pd.DataFrame(a.ipo_past_issues(s.date(), e.date()))
    return pd.DataFrame({"current": a.ipo_current_issues, "upcoming": a.ipo_upcoming_issues}[w]())


register(Pipeline(
    "india_ipos", "Indian IPOs: open now, upcoming, past", "snapshot", [
        Source("nse", _i_nse, NSE_HOSTS), Source("aynse", _i_aynse, NSE_HOSTS)],
    market="raw", needs_symbol=False, normalize=False, ttl=6 * 3600, columns=(),
    params_doc="what=current|upcoming|past, start, end (past only)", example='fs.fetch("india_ipos", what="upcoming")'))


# =============================================================== mutual funds
AMFI_NAV_ALL = "https://www.amfiindia.com/spages/NAVAll.txt"


def _mf_code(req) -> str:
    code = str(req.symbol).strip()
    need(code.isdigit(), "use the AMFI scheme code (find it with fs.fetch('india_mutual_funds', what='search', "
                         "query='parag parikh'))")
    return code


def _n_mfapi(req):
    import requests

    r = requests.get(f"https://api.mfapi.in/mf/{_mf_code(req)}", timeout=30)
    r.raise_for_status()
    j = r.json()
    df = pd.DataFrame(j.get("data") or [])
    need(len(df), "mfapi returned no NAVs")
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    return df.assign(scheme_code=_mf_code(req), name=(j.get("meta") or {}).get("scheme_name"))


def _n_mftool(req):
    df = get("mftool").Mftool().get_scheme_historical_nav(_mf_code(req), as_Dataframe=True)
    df = df.reset_index().rename(columns={"index": "date"})
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")
    return df


def _n_aynse(req):
    return get("aynse").mutual_fund_history_df(_mf_code(req), req.start, req.end)


def parse_navall(text: str) -> pd.DataFrame:
    """AMFI NAVAll.txt -> scheme_code, isin_growth, isin_reinvest, name, nav, date, fund_house, category."""
    rows, house, category = [], None, None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("Scheme Code"):
            continue
        parts = line.split(";")
        if len(parts) >= 6 and parts[0].strip().isdigit():
            rows.append({"scheme_code": parts[0].strip(), "isin_growth": parts[1].strip() or None,
                         "isin_reinvest": parts[2].strip() or None, "name": parts[3].strip(),
                         "nav": _num(parts[4]), "date": pd.to_datetime(parts[5].strip(), format="%d-%b-%Y",
                                                                       errors="coerce"),
                         "fund_house": house, "category": category})
        elif "(" in line and ")" in line and len(parts) == 1:
            category = line
        elif len(parts) == 1:
            house = line
    return pd.DataFrame(rows)


def _l_amfi(req):
    import requests

    r = requests.get(AMFI_NAV_ALL, timeout=60)
    r.raise_for_status()
    df = parse_navall(r.text)
    need(len(df), "AMFI file could not be parsed")
    if req.symbol:
        df = df[df["scheme_code"] == str(req.symbol)]
    q = req.p("query")
    if q:
        df = df[df["name"].str.contains(q, case=False, regex=False)]
    return df


def _l_mfapi(req):
    import requests

    if req.p("query") and not req.symbol:
        r = requests.get("https://api.mfapi.in/mf/search", params={"q": req.p("query")}, timeout=30)
        r.raise_for_status()
        return pd.DataFrame(r.json()).rename(columns={"schemeCode": "scheme_code", "schemeName": "name"})
    r = requests.get(f"https://api.mfapi.in/mf/{_mf_code(req)}/latest", timeout=30)
    r.raise_for_status()
    j = r.json()
    d = (j.get("data") or [{}])[0]
    return pd.DataFrame([{"scheme_code": _mf_code(req), "name": (j.get("meta") or {}).get("scheme_name"),
                          "nav": _num(d.get("nav")), "date": pd.to_datetime(d.get("date"), format="%d-%m-%Y")}])


_MF_LATEST = Pipeline(
    "india_mutual_funds.latest", "Latest NAV of one, some or all Indian mutual fund schemes (AMFI)", "snapshot", [
        Source("builtin:amfi", _l_amfi, ("www.amfiindia.com",), score=4.6),
        Source("builtin:mfapi", _l_mfapi, ("api.mfapi.in",)),
    ],
    market="raw", needs_symbol=False, normalize=False, ttl=3 * 3600, rank_as="india_mutual_funds",
    columns=("scheme_code", "name", "nav", "date", "fund_house", "category", "isin_growth", "isin_reinvest"),
    params_doc="what='latest' (all schemes) or symbol=scheme code; what='search' with query='...'",
    example='fs.fetch("india_mutual_funds", what="latest")')


def _bulk_mf(p: Pipeline, reqs):
    """Latest NAVs only need one AMFI file for every scheme."""
    return {}


register(Pipeline(
    "india_mutual_funds", "Indian mutual funds: NAV history per scheme", "series", [
        Source("mftool", _n_mftool, ("api.mfapi.in",)),
        Source("builtin:mfapi", _n_mfapi, ("api.mfapi.in",)),
        Source("aynse", _n_aynse, ("www.amfiindia.com",)),
    ],
    market="raw", required=("date", "nav"), check=validate.positive("nav"),
    final=lambda r: dt.date.today() - dt.timedelta(days=2), entity=lambda r: f"AMFI:{r.symbol}",
    columns=("date", "nav", "scheme_code", "name"), default_days=365 * 5,
    post=lambda df, r: df.assign(scheme_code=str(r.symbol)).drop(columns=[c for c in ("close",) if c in df]),
    variants={"latest": _MF_LATEST, "search": _MF_LATEST},
    params_doc="symbol = AMFI scheme code, start, end; what='latest' | what='search', query='...'",
    example='fs.fetch("india_mutual_funds", "122639", start="5y")'))
