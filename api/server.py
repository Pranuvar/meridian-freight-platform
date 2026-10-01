"""Mock reference-data API: FX rates and weekly diesel prices, with deliberate failures.

Run:  API_FAIL_RATE=0.25 uvicorn api.server:app --port 8000
  API_FAIL_RATE = share of requests answered with a random 500/502/503   (default 0.25)
  API_RATE_LIMIT = share of requests answered with 429 + Retry-After     (default 0.10)
"""
import math
import os
import random
from datetime import date, timedelta

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Response

FAIL_RATE = float(os.getenv("API_FAIL_RATE", "0.25"))
RATE_LIMIT = float(os.getenv("API_RATE_LIMIT", "0.10"))
PAGE_SIZE = 200

app = FastAPI(title="Meridian reference API")
chaos = random.Random()          # deliberately NOT seeded: failures should differ run to run


def _build():
    """Deterministic data (fixed seed), so the same rates come back every time the server starts."""
    rng = np.random.default_rng(7)
    start, end = date(2025, 1, 1), date(2026, 6, 30)     # covers the initial load AND the later increment
    fx, x = {}, 84.0
    for i in range((end - start).days + 1):
        d = start + timedelta(days=i)
        x += 0.15 * (84.0 - x) + rng.normal(0, 0.25)     # mean-reverting drift around 84
        x = min(max(x, 83.0), 85.5)
        if d.weekday() < 5:                              # NO weekend rates: a real FX quirk Gold must handle
            fx[d] = round(x, 4)
    fuel, price = {}, 90.0
    d = start - timedelta(days=start.weekday())          # weekly, on Mondays
    while d <= end:
        price += rng.normal(0, 0.6)
        fuel[d] = round(min(max(price, 82.0), 100.0), 2)
        d += timedelta(days=7)
    return fx, fuel


FX, FUEL = _build()


def _maybe_fail(response: Response):
    r = chaos.random()
    if r < RATE_LIMIT:
        raise HTTPException(429, "rate limit exceeded", headers={"Retry-After": "1"})
    if r < RATE_LIMIT + FAIL_RATE:
        raise HTTPException(chaos.choice([500, 502, 503]), "simulated upstream failure")


def _page(records: list, page: int):
    total = max(1, math.ceil(len(records) / PAGE_SIZE))
    if page > total:
        raise HTTPException(404, "page out of range")     # a NON-retryable error
    chunk = records[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
    return {"data": chunk, "page": page, "total_pages": total}


@app.get("/health")
def health():
    return {"status": "ok"}          # never fails, so we can tell "server up" from "endpoint flaky"


@app.get("/fx-rates")
def fx_rates(response: Response, start: date, end: date, page: int = Query(1, ge=1)):
    _maybe_fail(response)
    rows = [{"date": d.isoformat(), "base": "USD", "quote": "INR", "rate": r}
            for d, r in sorted(FX.items()) if start <= d <= end]
    return _page(rows, page)


@app.get("/fuel-index")
def fuel_index(response: Response, start: date, end: date, page: int = Query(1, ge=1)):
    _maybe_fail(response)
    rows = [{"week_start": d.isoformat(), "diesel_inr_per_litre": p}
            for d, p in sorted(FUEL.items()) if start <= d <= end]
    return _page(rows, page)
