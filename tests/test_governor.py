"""The network governor against a real local HTTP server: pacing, Retry-After, 403 pauses, all clients."""
import http.server
import threading
import time

import pytest

from finstack.core import config, net


class Handler(http.server.BaseHTTPRequestHandler):
    script = []          # list of (status, headers) to return in order; then 200
    hits = []

    def do_GET(self):
        Handler.hits.append(time.monotonic())
        status, headers = Handler.script.pop(0) if Handler.script else (200, {})
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


@pytest.fixture()
def server():
    Handler.script, Handler.hits = [], []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    net.install()
    yield f"http://127.0.0.1:{srv.server_address[1]}/"
    srv.shutdown()
    net.GROUPS[:] = [g for g in net.GROUPS if g[0] != "local"]
    net.reset_buckets()


def test_requests_are_paced_per_website(server):
    import requests

    net.register("local", ("127.0.0.1",), rate=5.0, burst=1)
    t0 = time.monotonic()
    for _ in range(6):
        assert requests.get(server, timeout=5).status_code == 200
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.9, elapsed                 # 6 requests at 5/s with burst 1 need >= 1 s
    gaps = [b - a for a, b in zip(Handler.hits, Handler.hits[1:])]
    assert min(gaps) >= 0.15
    st = net.stats().set_index("website").loc["local"]
    assert st["requests"] == 6


def test_threads_share_one_budget(server):
    import concurrent.futures as cf

    import requests

    net.register("local", ("127.0.0.1",), rate=10.0, burst=1)
    t0 = time.monotonic()
    with cf.ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda _: requests.get(server, timeout=5).status_code, range(12)))
    assert time.monotonic() - t0 >= 1.0           # 12 requests at 10/s even from 8 threads


def test_retry_after_and_rate_halving(server):
    import requests

    net.register("local", ("127.0.0.1",), rate=20.0, burst=5)
    Handler.script = [(429, {"Retry-After": "1"})]
    assert requests.get(server, timeout=5).status_code == 429
    b = net.bucket("127.0.0.1")
    assert b.factor == 0.5 and b.n429 == 1 and 0 < b.blocked_for() <= 1.0
    t0 = time.monotonic()
    assert requests.get(server, timeout=5).status_code == 200     # waits out Retry-After
    assert time.monotonic() - t0 >= 0.8


def test_403_pauses_website_and_raises_instead_of_sleeping(server):
    import requests

    config.configure(FINSTACK_MAX_WAIT=5)
    net.register("local", ("127.0.0.1",), rate=20.0, burst=5)
    Handler.script = [(403, {})]
    assert requests.get(server, timeout=5).status_code == 403
    assert net.is_blocked("127.0.0.1", more_than=10)
    with pytest.raises(net.WebsitePaused):
        requests.get(server, timeout=5)
    assert len(Handler.hits) == 1                 # the second request never reached the website


def test_urllib_and_httpx_are_governed(server):
    import urllib.request

    net.register("local", ("127.0.0.1",), rate=5.0, burst=1)
    t0 = time.monotonic()
    for _ in range(3):
        urllib.request.urlopen(server, timeout=5).read()
    try:
        import httpx

        for _ in range(3):
            httpx.get(server, timeout=5)
        n = 6
    except ImportError:
        n = 3
    assert time.monotonic() - t0 >= (n - 1) / 5.0 * 0.9
    assert net.bucket("127.0.0.1").requests == n


def test_budget_override_from_configure(server):
    net.register("local", ("127.0.0.1",), rate=5.0, burst=1)
    config.configure(budgets={"local": 50.0})
    assert net.bucket("127.0.0.1").base_rate == 50.0
