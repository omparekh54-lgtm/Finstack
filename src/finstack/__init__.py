"""finstack: one import for free financial data, with a focus on Indian markets.

    import finstack as fs

    # one call per data type; ranked sources, fallbacks, cache, validation, polite pacing
    fs.fetch("india_daily_prices", "RELIANCE", start="2015-01-01")
    fs.fetch("india_options", "NIFTY")
    fs.fetch("india_company_financials", "TCS", statement="quarterly")
    fs.bulk("india_daily_prices", ["TCS", "INFY", "HDFCBANK"], start="5y")
    fs.pipelines()
    fs.route("india_daily_prices", "TCS")
"""
import os as _os

from .check import check
from .api import crypto_price, filings, fundamentals, fx, history, macro, news, quote, symbols
from .company import (annual_reports, annual_results, announcements, balance_sheet, board_meetings,
                      bse_actions, bse_announcements, bse_client, bse_result_calendar, bse_results,
                      cash_flow, company_lookup, earnings_dates, latest_results, list_companies,
                      quarterly_results, result_filings, upcoming_results, xbrl_facts, xbrl_results)
from .india import (bhavcopy, bse_quote, corporate_actions, fii_dii, index_constituents, ipos,
                    mf_nav, mf_search, moneycontrol, nse_client, nse_history, nse_quote,
                    option_chain, screener, shareholding, tickertape, tradingview, tv_history)
from .loader import catalog, doctor, get, is_installed
from .more import (dbnomics, ecb, eurostat, gdelt, google_news, imf_weo, mcx, sdmx_client,
                   trends, us_yield_curve, world_history)
from .rankings import data_types, sources
from .reference_plus import (nse_current_surveillance, nse_index_membership_history,
                             nse_shareholding_history, nse_surveillance)
from .registry import CATALOG, CATEGORIES, lookup

from .core import cache as _cache
from .core import health as _health
from .core import net as governor
from .core.config import configure
from .core.policy import terms
from .core.router import NoData, bulk, fetch, pipelines, route
from .core.symbols import isin_valid, resolve
from .core.symbols import search as search_symbols


def health(days: float = 7):
    return _health.report(days)


def cache_info():
    return _cache.info()


def clear_cache(data_type=None, symbol=None) -> int:
    entity = resolve(symbol, "IN").key if symbol and data_type and data_type.startswith("india") else symbol
    return _cache.clear(data_type, entity)


def net_stats():
    return governor.stats()


if _os.environ.get("FINSTACK_GOVERNOR", "1") != "0":
    governor.install()

__version__ = "0.8.0"

__all__ = [
    "fetch", "bulk", "route", "pipelines", "resolve", "search_symbols", "isin_valid", "configure",
    "governor", "health", "net_stats", "cache_info", "clear_cache", "NoData", "terms",
    "get", "catalog", "doctor", "is_installed", "lookup", "CATALOG", "CATEGORIES", "check",
    "data_types", "sources",
    "quarterly_results", "annual_results", "balance_sheet", "cash_flow", "annual_reports",
    "result_filings", "announcements", "board_meetings", "upcoming_results", "earnings_dates",
    "latest_results", "xbrl_results", "xbrl_facts", "company_lookup", "list_companies",
    "bse_client", "bse_results", "bse_result_calendar", "bse_announcements", "bse_actions",
    "fundamentals", "moneycontrol", "tickertape", "screener", "shareholding", "corporate_actions",
    "nse_client", "nse_history", "nse_quote", "bse_quote", "bhavcopy", "tv_history", "tradingview",
    "option_chain", "fii_dii", "ipos", "index_constituents", "mcx", "mf_nav", "mf_search",
    "nse_shareholding_history", "nse_surveillance", "nse_current_surveillance",
    "nse_index_membership_history",
    "history", "quote", "symbols", "world_history", "crypto_price", "filings",
    "macro", "dbnomics", "sdmx_client", "imf_weo", "ecb", "eurostat", "us_yield_curve", "fx",
    "news", "google_news", "gdelt", "trends",
]
