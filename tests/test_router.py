"""The router with fake sources: fallback order, breaker, incremental cache, snapshots, bulk, verify."""
import datetime as dt

import pandas as pd
import pytest

from finstack.core import cache, health, router, validate
from finstack.core.router import NoData, Pipeline, Source, fetch, register

D0 = dt.date(2025, 1, 1)


def bars(start, end, price=100.0):
    days = pd.date_range(start, end, freq="D")
    return pd.DataFrame({"Date": days.strftime("%d-%m-%Y"), "OPEN": price, "HIGH": price + 1, "LOW": price - 1,
                         "CLOSE": price + 0.5, "VOLUME": 1000})


class Calls:
    def __init__(self):
        self.log = []


def make(name, sources, **kw):
    kw.setdefault("market", "raw")
    kw.setdefault("check", validate.ohlcv)
    kw.setdefault("final", lambda r: dt.date(2025, 12, 31))
    kw.setdefault("entity", lambda r: f"T:{r.symbol}")
    return register(Pipeline(name, name, kw.pop("kind", "series"), sources, **kw))


def test_fallback_order_and_provenance():
    calls = Calls()

    def broken(req):
        calls.log.append("broken")
        raise ConnectionError("boom")

    def wrong(req):
        calls.log.append("wrong")
        return pd.DataFrame({"foo": [1]})

    def good(req):
        calls.log.append("good")
        return bars(req.start, req.end)

    make("t_fallback", [Source("builtin:good", good, score=1), Source("builtin:broken", broken, score=3),
                        Source("builtin:wrong", wrong, score=2)])
    df = fetch("t_fallback", "X", start="2025-01-01", end="2025-01-10")
    assert calls.log == ["broken", "wrong", "good"]              # ranked order, failures skipped
    assert len(df) == 10 and set(df["source"]) == {"builtin:good"}
    att = dict(df.attrs["attempts"])
    assert att["builtin:broken"].startswith("failed") and att["builtin:wrong"].startswith("wrong format")


def test_incremental_cache_only_downloads_missing_ranges():
    ranges = []

    def src(req):
        ranges.append((req.start, req.end))
        return bars(req.start, req.end)

    make("t_cache", [Source("builtin:s", src)])
    fetch("t_cache", "X", start="2025-01-10", end="2025-01-20")
    fetch("t_cache", "X", start="2025-01-12", end="2025-01-18")     # fully cached
    df = fetch("t_cache", "X", start="2025-01-05", end="2025-01-25")
    assert ranges == [(dt.date(2025, 1, 10), dt.date(2025, 1, 20)), (dt.date(2025, 1, 5), dt.date(2025, 1, 9)),
                      (dt.date(2025, 1, 21), dt.date(2025, 1, 25))]
    assert len(df) == 21 and df["date"].is_monotonic_increasing and not df["date"].duplicated().any()


def test_non_final_tail_is_refetched():
    ranges = []

    def src(req):
        ranges.append((req.start, req.end))
        return bars(req.start, req.end)

    make("t_tail", [Source("builtin:s", src)], final=lambda r: dt.date(2025, 1, 15), live_ttl=0)
    fetch("t_tail", "X", start="2025-01-10", end="2025-01-20")
    fetch("t_tail", "X", start="2025-01-10", end="2025-01-20")
    assert ranges[1] == (dt.date(2025, 1, 16), dt.date(2025, 1, 20))    # only the not-yet-final days again


def test_breaker_opens_after_three_failures_and_skips_source():
    n = {"bad": 0}

    def bad(req):
        n["bad"] += 1
        raise RuntimeError("down")

    make("t_breaker", [Source("builtin:bad", bad, score=5), Source("builtin:ok", lambda r: bars(r.start, r.end))])
    for i in range(4):
        fetch("t_breaker", f"S{i}", start="2025-01-01", end="2025-01-03")
    assert n["bad"] == 3
    assert health.is_open("builtin:bad", "t_breaker") > 0
    route = router.route("t_breaker", "S9")
    assert route.iloc[0]["source"] == "builtin:ok" and "paused" in route.iloc[1]["status"]


def test_block_error_opens_breaker_immediately():
    make("t_block", [Source("builtin:blocked", lambda r: (_ for _ in ()).throw(RuntimeError("HTTP 403 Forbidden")),
                            score=5), Source("builtin:ok", lambda r: bars(r.start, r.end))])
    fetch("t_block", "A", start="2025-01-01", end="2025-01-02")
    assert health.is_open("builtin:blocked", "t_block") > 0


def test_invalid_rows_quarantined_and_too_many_means_fallback(tmp_path):
    def mostly_bad(req):
        df = bars(req.start, req.end)
        df["HIGH"] = df["LOW"] - 5            # every row inconsistent
        return df

    def one_bad(req):
        df = bars(req.start, req.end)
        df.loc[0, "HIGH"] = df.loc[0, "LOW"] - 5
        return df

    make("t_valid", [Source("builtin:bad", mostly_bad, score=5), Source("builtin:mostly_ok", one_bad)])
    df = fetch("t_valid", "Q", start="2025-01-01", end="2025-01-10")
    assert set(df["source"]) == {"builtin:mostly_ok"} and len(df) == 9
    assert any("quarantined" in i for i in df.attrs["issues"])
    assert list((tmp_path / "quarantine" / "t_valid").glob("*.csv"))


def test_all_fail_raises_nodata_with_attempts():
    make("t_fail", [Source("builtin:a", lambda r: (_ for _ in ()).throw(ValueError("nope")))])
    with pytest.raises(NoData) as e:
        fetch("t_fail", "Z", start="2025-01-01", end="2025-01-02")
    assert "builtin:a: failed - ValueError: nope" in str(e.value)


def test_skip_reasons_for_missing_library_and_credentials(monkeypatch):
    from finstack.core import config

    make("t_skip", [Source("builtin:keyed", lambda r: bars(r.start, r.end), creds=("MY_TEST_TOKEN",)),
                    Source("builtin:x", lambda r: bars(r.start, r.end), libs=("no_such_library_xyz",))])
    r = router.route("t_skip", "A").set_index("source")
    assert "needs MY_TEST_TOKEN" in r.loc["builtin:keyed", "status"]
    assert "not installed" in r.loc["builtin:x", "status"]
    config.configure(MY_TEST_TOKEN="abc")
    assert router.route("t_skip", "A").set_index("source").loc["builtin:keyed", "status"] == "ready"


def test_snapshot_ttl_and_stale_fallback():
    n = {"calls": 0, "fail": False}

    def quote(req):
        n["calls"] += 1
        if n["fail"]:
            raise ConnectionError("offline")
        return pd.DataFrame([{"symbol": req.symbol, "last": 10.0 + n["calls"]}])

    make("t_snap", [Source("builtin:q", quote)], kind="snapshot", required=("last",), time_col="ts",
         check=validate.positive("last"), ttl=60)
    a = fetch("t_snap", "A")
    b = fetch("t_snap", "A")
    assert n["calls"] == 1 and a["last"].iloc[0] == b["last"].iloc[0] == 11.0
    n["fail"] = True
    c = fetch("t_snap", "A", refresh=True)
    assert c.attrs.get("stale") and c["last"].iloc[0] == 11.0


def test_bulk_parallel_with_failures_reported():
    def src(req):
        if req.symbol == "BAD":
            raise RuntimeError("no such symbol")
        return bars(req.start, req.end, price=50.0)

    make("t_bulk", [Source("builtin:s", src)])
    df = router.bulk("t_bulk", ["A", "B", "BAD", "C"], start="2025-01-01", end="2025-01-05", workers=3,
                     progress=False)
    assert sorted(df["query"].unique()) == ["A", "B", "C"] and len(df) == 15
    assert "BAD" in df.attrs["failed"]


def test_bulk_route_is_used_then_cache_serves_symbols():
    per_symbol = {"n": 0}

    def src(req):
        per_symbol["n"] += 1
        return bars(req.start, req.end)

    def bulk_route(p, reqs):
        for r in reqs:
            cache.write_series(p.name, f"T:{r.symbol}", router._shape(p, bars(r.start, r.end, 7.0), r).assign(
                source="bulk"), (r.start, r.end), dt.date(2025, 12, 31))
        return {"route": "test"}

    make("t_bulk2", [Source("builtin:s", src)], bulk=bulk_route)
    df = router.bulk("t_bulk2", ["A", "B"], start="2025-01-01", end="2025-01-03", progress=False)
    assert per_symbol["n"] == 0 and set(df["source"]) == {"bulk"}


def test_verify_compares_two_independent_sources():
    make("t_verify", [Source("builtin:a", lambda r: bars(r.start, r.end, 100.0), score=2),
                      Source("builtin:b", lambda r: bars(r.start, r.end, 100.2), score=1)])
    df = fetch("t_verify", "V", start="2025-01-01", end="2025-01-05", verify=True)
    v = df.attrs["verified"]
    assert v["with"] == "builtin:b" and v["agree"] and 0.1 < v["median_close_diff_pct"] < 0.3


def test_daily_files_cache_final_days(monkeypatch):
    from finstack.core import calendar

    n = {"calls": 0}

    def eod(req):
        n["calls"] += 1
        return pd.DataFrame({"SYMBOL": ["A", "B"], "CLOSE": [1.0, 2.0], "OPEN": [1, 2], "HIGH": [1, 2], "LOW": [1, 2],
                             "TIMESTAMP": [req.start.strftime("%d-%b-%Y")] * 2})

    make("t_files", [Source("builtin:e", eod)], kind="daily_files", needs_symbol=False,
         required=("symbol", "close"), final=lambda r: dt.date(2026, 9, 30))
    a = fetch("t_files", start="2026-09-28", end="2026-09-30")
    b = fetch("t_files", start="2026-09-28", end="2026-09-30")
    assert n["calls"] == 3 and len(a) == len(b) == 6
    assert calendar.is_trading_day(dt.date(2026, 9, 28))
