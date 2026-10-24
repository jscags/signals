#!/usr/bin/env python3
"""What the 9-step put-selling process can and cannot be measured with.

PHASE 0. Writes nothing, stores nothing, concludes nothing about whether the
strategy works. It answers the two questions that decide whether any of the
rest is worth building, because both have been assumed in this repo before and
both turned out to be wrong when finally measured:

  1. HOW FAR BACK can we price these tickers? The entire risk in selling puts
     is the sustained drawdown, so a window that contains no drawdown cannot
     test the strategy -- it can only flatter it. market_data says the free
     tier serves two years. Two years from today starts after the 2022 bear
     market ended, which is precisely the period that would decide this.

  2. IS THERE AN OPTIONS CHAIN at all? Steps 4 through 7 are entirely about
     strikes and premium, and this repo has never fetched an option. A screener
     that silently skipped those steps would be answering a different question
     than the one asked.

The second question has a live half and a historical half, and they are not
the same question. A screener needs today's chain. A BACKTEST needs the chain
as it stood on every past entry date, which is a far rarer and more expensive
thing -- so this reports them separately rather than letting a working live
endpoint imply a testable history.

Run it with POLYGON_API_KEY set. It makes a handful of requests, paced.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, timedelta

import market_data

# The canonical vehicles for this strategy: 3x equity index and semis, plus a
# 2x as a control. Which ones to actually trade is the user's step 1 and his
# call; this is about what the DATA supports, and that is the same answer for
# any liquid leveraged ETF.
TICKERS = ("TQQQ", "SOXL", "SPXL", "QLD")

# Deliberately far past any free tier's limit. Asking for ten years and seeing
# what comes back measures the real ceiling instead of trusting a comment.
ASK_YEARS = 10
PACE = 12.5             # the free tier's published 5/min, with margin


def _get(url, timeout=30):
    """One GET. Returns (status, body). An HTTP error is an ANSWER, not a
    failure -- a 403 on the options endpoint is the finding we are after."""
    request = urllib.request.Request(url, headers={
        "User-Agent": market_data.USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _brief(body, n=240):
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return repr(body[:n])
    for key in ("message", "error"):
        if data.get(key):
            return f"{data.get('status', '?')}: {data[key]}"
    return repr(json.dumps(data)[:n])


def probe_history(key):
    """How deep the daily bars actually go, per ticker."""
    print("=" * 72)
    print("1. PRICE HISTORY DEPTH")
    print("   The strategy's whole risk is the drawdown. Ask for 10 years and")
    print("   report what comes back -- the gap is what cannot be tested.")
    print("=" * 72)
    start = (date.today() - timedelta(days=365 * ASK_YEARS)).isoformat()
    end = date.today().isoformat()
    depths = {}
    for i, ticker in enumerate(TICKERS):
        if i:
            time.sleep(PACE)
        url = market_data.POLYGON_URL.format(
            symbol=ticker, start=start, end=end, key=key)
        status, body = _get(url)
        shown = url.replace(key, "***")
        print(f"\n{ticker}  asked {start} .. {end}")
        try:
            bars = market_data.parse_polygon(body)
        except market_data.PriceError as exc:
            print(f"  HTTP {status}  FAILED: {exc}")
            print(f"  {_brief(body)}")
            continue
        first, last = bars[0][0], bars[-1][0]
        years = (date.fromisoformat(last) - date.fromisoformat(first)).days / 365.25
        depths[ticker] = (first, last, len(bars))
        print(f"  HTTP {status}  {len(bars):,} bars   {first} .. {last}"
              f"   ({years:.1f} years)")
        # The one comparison that matters for this strategy.
        if first > "2022-01-01":
            print(f"  !! starts AFTER the 2022 drawdown -- the regime that")
            print(f"     decides this strategy is not in the window")
    return depths


def probe_options(key):
    """Whether an options chain is reachable at all, live or historical."""
    print("\n" + "=" * 72)
    print("2. OPTIONS CHAIN")
    print("   Steps 4-7 are strikes and premium. This repo has never fetched")
    print("   an option. Live and historical are separate questions.")
    print("=" * 72)
    expiry = (date.today() + timedelta(days=30)).isoformat()
    endpoints = (
        ("contracts (reference)",
         "https://api.polygon.io/v3/reference/options/contracts"
         f"?underlying_ticker=TQQQ&contract_type=put"
         f"&expiration_date.gte={date.today().isoformat()}"
         f"&limit=5&apiKey={key}",
         "which strikes exist -- reference data, no prices"),
        ("chain snapshot (LIVE prices)",
         "https://api.polygon.io/v3/snapshot/options/TQQQ"
         f"?contract_type=put&limit=5&apiKey={key}",
         "today's premium and greeks -- what a live screener needs"),
        ("aggregates for one contract (HISTORY)",
         "https://api.polygon.io/v2/aggs/ticker/O:TQQQ270115P00040000"
         f"/range/1/day/2026-01-01/{date.today().isoformat()}"
         f"?adjusted=true&limit=10&apiKey={key}",
         "what a premium WAS on a past date -- what a backtest needs"),
    )
    verdict = {}
    for i, (name, url, why) in enumerate(endpoints):
        if i:
            time.sleep(PACE)
        status, body = _get(url)
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            data = {}
        results = data.get("results")
        n = len(results) if isinstance(results, list) else (1 if results else 0)
        state = (data.get("status") or "").upper()
        usable = status == 200 and n > 0 and state not in ("ERROR", "NOT_AUTHORIZED")
        verdict[name] = usable
        print(f"\n{name}")
        print(f"  for: {why}")
        print(f"  HTTP {status}  results {n}  -> {'USABLE' if usable else 'NO'}")
        print(f"  {_brief(body)}")
    return verdict


def main():
    key = os.environ.get("POLYGON_API_KEY", "")
    if not key:
        print("POLYGON_API_KEY is not set.")
        print("Stooq is not a fallback here: it answers a datacenter IP with a")
        print("JavaScript challenge, which is the host declining automated")
        print("access from this address, not an error to route around.")
        return 2

    print("WHEEL DATA PROBE -- writes nothing, decides nothing\n")
    depths = probe_history(key)
    verdict = probe_options(key)

    print("\n" + "=" * 72)
    print("WHAT THIS MEANS")
    print("=" * 72)
    if depths:
        earliest = min(v[0] for v in depths.values())
        print(f"Deepest history available: {earliest}")
        if earliest > "2022-06-01":
            print("A backtest over this window would cover a period with no")
            print("sustained bear market. For a strategy whose losses are")
            print("ALL in the drawdown, that is not a weak test, it is a")
            print("test that cannot fail. Longer history is required before")
            print("any result here means anything.")
    else:
        print("No price history at all -- nothing downstream is possible.")
    live = verdict.get("chain snapshot (LIVE prices)")
    hist = verdict.get("aggregates for one contract (HISTORY)")
    print(f"\nLive chain for a screener: {'yes' if live else 'NO'}")
    print(f"Historical premium for a backtest: {'yes' if hist else 'NO'}")
    if not hist:
        print("\nWithout historical premium, steps 4-7 cannot be backtested")
        print("directly -- but they do not have to be. A short put's outcome")
        print("is the underlying's path plus the premium received, and only")
        print("the premium is unknown. Sweeping it as a parameter answers a")
        print("better question than sourcing it would: not 'what did the")
        print("option pay' but 'what would it have needed to pay to break")
        print("even'. That is computable from the price history alone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
