"""HTTP client with retry, backoff, Retry-After handling and pagination."""
import random
import time

import requests

RETRYABLE = {429, 500, 502, 503, 504}


class ApiError(Exception):
    """Raised when the API cannot deliver: retries exhausted, or a non-retryable error."""


def get_json(url, params=None, session=None, max_attempts=5, base_delay=0.5, timeout=10, sleep=time.sleep):
    """GET with retries. `session` and `sleep` are injectable so tests need no network and no waiting."""
    http = session or requests
    reason = "unknown"
    for attempt in range(1, max_attempts + 1):
        retry_after = None
        try:
            r = http.get(url, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as e:    # server down / too slow
            reason = f"{type(e).__name__}"
        else:
            if r.status_code == 200:
                return r.json()
            if r.status_code not in RETRYABLE:      # 4xx: OUR request is wrong. Retrying won't fix it.
                raise ApiError(f"non-retryable HTTP {r.status_code} for {url}: {r.text[:200]}")
            reason = f"HTTP {r.status_code}"
            retry_after = r.headers.get("Retry-After")
        if attempt < max_attempts:
            # Obey the server if it tells us how long to wait, else exponential backoff + jitter.
            delay = float(retry_after) if retry_after else base_delay * 2 ** (attempt - 1) + random.uniform(0, 0.25)
            print(f"  attempt {attempt}/{max_attempts} failed ({reason}); retrying in {delay:.1f}s")
            sleep(delay)
    raise ApiError(f"gave up after {max_attempts} attempts on {url}: {reason}")


def fetch_all(url, params, **kw):
    """Follow pagination until the last page. If ANY page fails for good, the whole call fails,
    so we never return a silently incomplete dataset."""
    records, page = [], 1
    while True:
        body = get_json(url, {**params, "page": page}, **kw)
        records += body["data"]
        if page >= body["total_pages"]:
            return records
        page += 1
