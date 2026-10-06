import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
FIX = Path(__file__).parent / "fixtures"

os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ["no_proxy"] = os.environ["NO_PROXY"]


def pytest_configure(config):
    """Keep pytest's scratch folders inside the project. On some Windows PCs the shared
    TEMP/pytest-of-<user> folder is locked ("Access is denied"), which breaks every test."""
    if not config.option.basetemp:
        config.option.basetemp = str(ROOT / ".pytest_tmp")


def load(name):
    return json.loads((FIX / name).read_text())


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    """Every test gets its own cache, health log and settings."""
    from finstack.core import cache, config, health, net

    monkeypatch.setenv("FINSTACK_HOME", str(tmp_path))
    config._OVERRIDES.clear()
    config._FILE_CACHE = None
    cache._db = None
    health._db = None
    health._breakers.clear()
    net.reset_buckets()
    yield
    cache._db = None
    health._db = None
