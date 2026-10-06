"""Source policy: permitted-only mode and the one-time notice."""
import datetime as dt

import pandas as pd

import finstack as fs
from finstack.core import config, policy, router
from finstack.core.router import Pipeline, Source, register


def _bars(req):
    d = pd.date_range(req.start, req.end, freq="D")
    return pd.DataFrame({"date": d, "close": 10.0, "open": 10.0, "high": 11.0, "low": 9.0})


def _pipe():
    return register(Pipeline("t_policy", "t", "series", [
        Source("nse", _bars, libs=(), score=5),                       # restricted website
        Source("builtin:frankfurter", _bars, score=1)],               # permitted open API
        market="raw", final=lambda r: dt.date(2025, 12, 31), entity=lambda r: f"T:{r.symbol}"))


def test_permitted_policy_skips_restricted_sources():
    _pipe()
    df = fs.fetch("t_policy", "A", start="2025-01-01", end="2025-01-03")
    assert set(df["source"]) == {"nse"}
    config.configure(policy="permitted")
    df = fs.fetch("t_policy", "B", start="2025-01-01", end="2025-01-03")
    assert set(df["source"]) == {"builtin:frankfurter"}
    assert "policy='permitted'" in router.route("t_policy", "B").set_index("source").loc["nse", "status"]


def test_notice_is_shown_once_per_computer(capsys, tmp_path):
    policy._shown = False
    policy.notice_once("nse")
    policy.notice_once("nse")
    first = capsys.readouterr().err
    assert first.count("Notice (shown once)") == 1
    policy._shown = False                       # new Python process on the same computer
    policy.notice_once("nse")
    assert capsys.readouterr().err == ""
    assert (config.home() / "terms_notice_shown").exists()


def test_every_source_is_classified():
    router._load_pipelines()
    keys = {s.key for p in router.PIPELINES.values() for q in [p, *p.variants.values()] for s in q.sources}
    unclassified = [k for k in keys if k not in policy.PERMITTED and k not in policy.RESTRICTED_SITES]
    assert not unclassified, unclassified
    assert set(fs.terms()["status"]) == {"permitted", "restricted"}
