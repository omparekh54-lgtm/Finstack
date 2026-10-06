"""Network governor: one shared speed limit per WEBSITE, across every library.

Ten finstack libraries call nseindia.com. If each paced itself separately they would still flood
NSE together, so finstack paces at the transport level instead. It wraps the send step of
requests, httpx, curl_cffi, urllib and aiohttp. Every request any library makes waits for its
website's token bucket, and every response is observed:

  * 429 / 503  -> wait for Retry-After (or an exponential pause) and halve that website's rate
  * 403        -> pause the website (30 s, doubling up to 15 min) and halve its rate
  * successes  -> rate slowly recovers to the configured budget

Nothing here hides who you are: no proxy rotation, no fake identities. It only slows down.
Disable with FINSTACK_GOVERNOR=0 or fs.governor.uninstall().
"""
from __future__ import annotations

import contextvars
import email.utils
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from . import config

# (group, host suffixes, requests per second, burst, one bucket per host?)
GROUPS: List[Tuple[str, Tuple[str, ...], float, int, bool]] = [
    ("nse-archives", ("nsearchives.nseindia.com", "archives.nseindia.com"), 1.0, 1, False),
    ("nse", ("nseindia.com",), 3.0, 3, False),
    ("niftyindices", ("niftyindices.com",), 1.0, 1, False),
    ("bse", ("bseindia.com",), 2.0, 2, False),
    ("mcx", ("mcxindia.com",), 1.0, 1, False),
    ("amfi", ("amfiindia.com",), 1.0, 1, False),
    ("mfapi", ("mfapi.in",), 2.0, 2, False),
    ("yahoo", ("yahoo.com",), 1.0, 2, False),
    ("tradingview", ("tradingview.com",), 0.5, 1, False),
    ("moneycontrol", ("moneycontrol.com",), 0.5, 1, False),
    ("tickertape", ("tickertape.in",), 0.5, 1, False),
    ("screener", ("screener.in",), 0.5, 1, False),
    ("sensibull", ("sensibull.com",), 0.5, 1, False),
    ("eastmoney", ("eastmoney.com",), 2.0, 2, False),
    ("sec", ("sec.gov",), 8.0, 8, False),            # SEC fair access: max 10/s
    ("coingecko", ("coingecko.com",), 0.1, 1, False),
    ("google-trends", ("trends.google.com",), 0.15, 1, False),
    ("google-news", ("news.google.com",), 0.5, 1, False),
    ("gdelt", ("gdeltproject.org",), 0.2, 1, False),
    ("reddit", ("reddit.com",), 1.5, 5, False),
    ("stats", ("treasury.gov", "worldbank.org", "imf.org", "ecb.europa.eu", "europa.eu", "bis.org",
               "oecd.org", "db.nomics.world", "stlouisfed.org", "frankfurter.dev", "frankfurter.app",
               "rbi.org.in", "data.gov.in"), 2.0, 2, True),
    ("publishers", ("business-standard.com", "indiatimes.com", "livemint.com", "dowjones.io",
                    "cnbc.com", "dj.com"), 1.0, 1, True),
]
DEFAULT = ("default", 0.0, 1)     # other websites (incl. your own APIs): not paced, but still backed off on 429/403

_in_governed = contextvars.ContextVar("finstack_governed", default=False)


class WebsitePaused(RuntimeError):
    """Raised instead of sleeping when a website is paused for longer than FINSTACK_MAX_WAIT seconds
    (default 30). The router catches it and moves on to the next source."""


def _max_wait() -> float:
    try:
        return float(config.get("FINSTACK_MAX_WAIT", 30))
    except (TypeError, ValueError):
        return 30.0


@dataclass
class Bucket:
    name: str
    base_rate: float
    burst: int
    factor: float = 1.0
    tokens: float = 0.0
    last: float = field(default_factory=time.monotonic)
    blocked_until: float = 0.0
    penalties: int = 0
    ok_streak: int = 0
    requests: int = 0
    waited: float = 0.0
    n429: int = 0
    n403: int = 0
    n5xx: int = 0
    errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self):
        self.tokens = float(self.burst)

    @property
    def rate(self) -> float:
        if self.base_rate <= 0:
            return float("inf")
        return max(self.base_rate * self.factor, 0.01)

    def reserve(self) -> float:
        """Take a token; return how many seconds the caller must wait before sending."""
        with self.lock:
            now = time.monotonic()
            if self.base_rate <= 0:                     # unpaced website: only honour pauses
                self.requests += 1
                wait = max(0.0, self.blocked_until - now)
                self.waited += wait
                return wait
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
            self.last = now
            self.requests += 1
            wait = max(0.0, self.blocked_until - now)
            self.tokens -= 1
            if self.tokens < 0:
                wait = max(wait, -self.tokens / self.rate)
            if wait > 0:
                wait += random.uniform(0, min(1.0, 0.15 / self.rate))   # small jitter, never synchronised bursts
            self.waited += wait
            return wait

    def observe(self, status: Optional[int], headers=None) -> None:
        with self.lock:
            now = time.monotonic()
            if status is None:
                self.errors += 1
                return
            if status in (429, 503):
                if status == 429:
                    self.n429 += 1
                else:
                    self.n5xx += 1
                pause = _retry_after(headers) or min(60.0, 2.0 ** min(self.penalties, 6))
                self.blocked_until = max(self.blocked_until, now + pause)
                self.factor = max(0.1, self.factor * 0.5)
                self.penalties += 1
                self.ok_streak = 0
            elif status == 403:
                self.n403 += 1
                pause = min(900.0, 30.0 * 2 ** min(self.penalties, 5))
                self.blocked_until = max(self.blocked_until, now + pause)
                self.factor = max(0.1, self.factor * 0.5)
                self.penalties += 1
                self.ok_streak = 0
            elif status >= 500:
                self.n5xx += 1
            elif status < 400:
                self.ok_streak += 1
                if self.ok_streak >= 20:
                    self.factor = min(1.0, self.factor * 1.25)
                    self.penalties = max(0, self.penalties - 1)
                    self.ok_streak = 0

    def blocked_for(self) -> float:
        return max(0.0, self.blocked_until - time.monotonic())


def _retry_after(headers) -> Optional[float]:
    if not headers:
        return None
    try:
        v = headers.get("Retry-After") or headers.get("retry-after")
    except Exception:
        return None
    if not v:
        return None
    try:
        return min(900.0, float(v))
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(v).timestamp()
            return max(0.0, min(900.0, when - time.time()))
        except Exception:
            return None


_buckets: Dict[str, Bucket] = {}
_blk = threading.Lock()


def group_for(host: str) -> Tuple[str, float, int]:
    host = (host or "").lower().rstrip(".")
    for name, suffixes, rate, burst, per_host in GROUPS:
        for s in suffixes:
            if host == s or host.endswith("." + s):
                key = f"{name}:{s}" if per_host else name
                override = config.budget_override(name)
                return key, (override or rate), max(burst, 1)
    override = config.budget_override("default")
    return f"{DEFAULT[0]}:{host}", (override or DEFAULT[1]), DEFAULT[2]   # one bucket per unknown host


def bucket(host: str) -> Bucket:
    key, rate, burst = group_for(host)
    with _blk:
        b = _buckets.get(key)
        if b is None:
            b = _buckets[key] = Bucket(key, rate, burst)
        return b


def reset_buckets() -> None:
    with _blk:
        _buckets.clear()


def register(name: str, suffixes, rate: float, burst: int = 1, per_host: bool = False) -> None:
    """Add or replace a website group (most specific groups should be registered first)."""
    global GROUPS
    GROUPS = [g for g in GROUPS if g[0] != name]
    GROUPS.insert(0, (name, tuple(suffixes), float(rate), int(burst), per_host))
    reset_buckets()


def is_blocked(host: str, more_than: float = 60.0) -> bool:
    """True if this website is paused for longer than `more_than` seconds (router skips it)."""
    return bucket(host).blocked_for() > more_than


def stats():
    """Per-website pacing statistics as a DataFrame."""
    import pandas as pd

    rows = []
    with _blk:
        for b in _buckets.values():
            rows.append({"website": b.name, "rate_per_s": round(b.rate, 3) if b.base_rate > 0 else None,
                         "budget_per_s": b.base_rate or None,
                         "requests": b.requests, "waited_s": round(b.waited, 1), "http_429": b.n429,
                         "http_403": b.n403, "http_5xx": b.n5xx, "errors": b.errors,
                         "paused_for_s": round(b.blocked_for(), 1)})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------- the hooks
def _before(host: str) -> Optional[Bucket]:
    if _in_governed.get() or not host:
        return None
    b = bucket(host)
    if b.blocked_for() > _max_wait():
        raise WebsitePaused(f"{b.name} is paused for {b.blocked_for():.0f}s after rate-limit/block responses")
    w = b.reserve()
    if w > 0:
        time.sleep(w)
    return b


async def _before_async(host: str) -> Optional[Bucket]:
    import asyncio

    if _in_governed.get() or not host:
        return None
    b = bucket(host)
    if b.blocked_for() > _max_wait():
        raise WebsitePaused(f"{b.name} is paused for {b.blocked_for():.0f}s after rate-limit/block responses")
    w = b.reserve()
    if w > 0:
        await asyncio.sleep(w)
    return b


def _host(url) -> str:
    try:
        return urlparse(str(url)).hostname or ""
    except Exception:
        return ""


_originals: Dict[str, tuple] = {}


def _patch(owner, attr, wrapper_factory, key):
    orig = getattr(owner, attr)
    if getattr(orig, "_finstack_wrapped", False):
        return
    w = wrapper_factory(orig)
    w._finstack_wrapped = True
    _originals[key] = (owner, attr, orig)
    setattr(owner, attr, w)


def install() -> List[str]:
    """Wrap every supported HTTP client. Safe to call more than once."""
    done = []
    try:
        import requests.adapters as ra

        def f(orig):
            def send(self, request, **kw):
                b = _before(_host(request.url))
                token = _in_governed.set(True)
                try:
                    r = orig(self, request, **kw)
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(r.status_code, r.headers)
                return r
            return send
        _patch(ra.HTTPAdapter, "send", f, "requests")
        done.append("requests")
    except ImportError:
        pass
    try:
        import httpx

        def fs(orig):
            def handle_request(self, request):
                b = _before(request.url.host)
                token = _in_governed.set(True)
                try:
                    r = orig(self, request)
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(r.status_code, r.headers)
                return r
            return handle_request

        def fa(orig):
            async def handle_async_request(self, request):
                b = await _before_async(request.url.host)
                token = _in_governed.set(True)
                try:
                    r = await orig(self, request)
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(r.status_code, r.headers)
                return r
            return handle_async_request
        _patch(httpx.HTTPTransport, "handle_request", fs, "httpx")
        _patch(httpx.AsyncHTTPTransport, "handle_async_request", fa, "httpx-async")
        done.append("httpx")
    except ImportError:
        pass
    try:
        import curl_cffi.requests as cr

        def cs(orig):
            def request(self, method, url, *a, **kw):
                b = _before(_host(url))
                token = _in_governed.set(True)
                try:
                    r = orig(self, method, url, *a, **kw)
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(getattr(r, "status_code", None), getattr(r, "headers", None))
                return r
            return request

        def ca(orig):
            async def request(self, method, url, *a, **kw):
                b = await _before_async(_host(url))
                token = _in_governed.set(True)
                try:
                    r = await orig(self, method, url, *a, **kw)
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(getattr(r, "status_code", None), getattr(r, "headers", None))
                return r
            return request
        _patch(cr.Session, "request", cs, "curl_cffi")
        _patch(cr.AsyncSession, "request", ca, "curl_cffi-async")
        done.append("curl_cffi")
    except ImportError:
        pass
    try:
        import urllib.error
        import urllib.request as ur

        def uo(orig):
            def open(self, fullurl, *a, **kw):
                url = fullurl.get_full_url() if hasattr(fullurl, "get_full_url") else fullurl
                b = _before(_host(url))
                token = _in_governed.set(True)
                try:
                    r = orig(self, fullurl, *a, **kw)
                except urllib.error.HTTPError as e:
                    if b:
                        b.observe(e.code, e.headers)
                    raise
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(getattr(r, "status", 200), getattr(r, "headers", None))
                return r
            return open
        _patch(ur.OpenerDirector, "open", uo, "urllib")
        done.append("urllib")
    except ImportError:
        pass
    try:
        import aiohttp

        def ah(orig):
            async def _request(self, method, str_or_url, *a, **kw):
                b = await _before_async(_host(str_or_url))
                token = _in_governed.set(True)
                try:
                    r = await orig(self, method, str_or_url, *a, **kw)
                except Exception:
                    if b:
                        b.observe(None)
                    raise
                finally:
                    _in_governed.reset(token)
                if b:
                    b.observe(r.status, r.headers)
                return r
            return _request
        _patch(aiohttp.ClientSession, "_request", ah, "aiohttp")
        done.append("aiohttp")
    except ImportError:
        pass
    return done


def uninstall() -> None:
    for owner, attr, orig in _originals.values():
        setattr(owner, attr, orig)
    _originals.clear()


def installed() -> List[str]:
    return sorted(_originals)
