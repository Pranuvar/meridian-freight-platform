import pytest
import requests

from src.ingestion.api_client import ApiError, fetch_all, get_json


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers, self.text = status, body, headers or {}, str(body)

    def json(self):
        return self._body


class FakeSession:
    """Plays back a scripted list of responses/exceptions and counts calls. No network needed."""
    def __init__(self, script):
        self.script, self.calls = list(script), 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_retries_transient_failures_then_succeeds():
    sleeps = []
    s = FakeSession([Resp(500), requests.ConnectionError(), Resp(200, {"ok": 1})])
    assert get_json("http://x", session=s, sleep=sleeps.append) == {"ok": 1}
    assert s.calls == 3 and len(sleeps) == 2


def test_gives_up_after_max_attempts():
    s = FakeSession([Resp(503)] * 3)
    with pytest.raises(ApiError, match="gave up after 3 attempts"):
        get_json("http://x", session=s, max_attempts=3, sleep=lambda _: None)
    assert s.calls == 3


def test_client_errors_are_not_retried():
    s = FakeSession([Resp(404)])
    with pytest.raises(ApiError, match="non-retryable"):
        get_json("http://x", session=s, sleep=lambda _: None)
    assert s.calls == 1            # exactly one call: no pointless retrying


def test_retry_after_header_is_obeyed():
    sleeps = []
    s = FakeSession([Resp(429, headers={"Retry-After": "2"}), Resp(200, {"ok": 1})])
    get_json("http://x", session=s, sleep=sleeps.append)
    assert sleeps == [2.0]


def test_pagination_collects_every_page_and_fails_whole_if_a_page_fails():
    good = FakeSession([Resp(200, {"data": [1, 2], "page": 1, "total_pages": 2}),
                        Resp(200, {"data": [3], "page": 2, "total_pages": 2})])
    assert fetch_all("http://x", {}, session=good) == [1, 2, 3]
    bad = FakeSession([Resp(200, {"data": [1, 2], "page": 1, "total_pages": 2}), Resp(500), Resp(500)])
    with pytest.raises(ApiError):
        fetch_all("http://x", {}, session=bad, max_attempts=2, sleep=lambda _: None)
