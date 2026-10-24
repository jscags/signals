#!/usr/bin/env python3
"""Steps 1-3 and 8-9 of the put-selling process: is a ticker set up today?

This module is deliberately only the PRICE half of the process. Steps 4-7 are
strikes and premium, and --probe-brent's sibling settled what is available
there: the free tier answers a live options chain with 403 NOT_AUTHORIZED, so
nothing here can tell you which strike pays 5%. It can tell you whether a
ticker is in a state where you should go and look, which is what steps 1-3
actually are.

That division is honest rather than reluctant. A screener that guessed at a
premium it could not see would be inventing the most important number in the
trade.

WHAT "SET UP" MEANS HERE
------------------------
    step 1   the ticker is one of a small fixed watchlist (your choice, not
             the screener's -- it is a list, not a ranking)
    step 2   both EMA clouds are green, meaning the fast EMA of each pair sits
             above the slow one
    step 3   the most recent session closed down by at least the drop
             threshold

All three must hold on the SAME day. A ticker that had a 5% red day a
fortnight ago is not set up now, and a ticker whose clouds turned green
yesterday after a red day the week before never had a qualifying day at all.

POINT IN TIME
-------------
Every EMA on day D is computed from closes through D and no further. This is
the same discipline setup_pit.py enforces for filings, for the same reason: a
backtest that lets day D see day D+1 produces a rule that cannot be traded.
The signal day is the red close; the earliest honest entry is the next open,
which is what the backtest uses.
"""

import sys

# The two clouds. These are the common daily-chart pair -- a fast cloud for
# the near-term trend and a slower one for the prevailing one -- but they are
# a parameter, not a law, and the backtest sweeps them. If your charts use a
# different pairing, change these and the numbers follow.
CLOUDS = ((5, 12), (34, 50))

# "Roughly 5%" from step 3. Also swept: the difference between 4% and 6% on a
# 3x fund is the difference between a common event and a rare one, and that
# changes how often capital is deployed more than any other single choice.
DROP_PCT = 5.0


def ema(closes, span):
    """Standard EMA, seeded with the SMA of the first `span` closes.

    Returns a list the same length as `closes`, with None for every position
    before the seed is complete. The Nones matter: a chart library would
    happily start the line at bar 0 and the first few values would be mostly
    an artifact of the seed, which on a 50-period EMA is two and a half months
    of made-up trend.
    """
    if span <= 0:
        raise ValueError("span must be positive")
    out = [None] * len(closes)
    if len(closes) < span:
        return out
    seed = sum(closes[:span]) / span
    out[span - 1] = seed
    k = 2.0 / (span + 1.0)
    prev = seed
    for i in range(span, len(closes)):
        prev = closes[i] * k + prev * (1.0 - k)
        out[i] = prev
    return out


def cloud_greens(closes, clouds=CLOUDS):
    """Per-day: is every cloud green? None until all EMAs are defined.

    None is not False. A day before the slowest EMA has enough history is a
    day we cannot judge, and calling it "not green" would silently turn the
    first fifty sessions of any series into a run of negative answers.
    """
    pairs = [(ema(closes, fast), ema(closes, slow)) for fast, slow in clouds]
    out = []
    for i in range(len(closes)):
        values = [(f[i], s[i]) for f, s in pairs]
        if any(f is None or s is None for f, s in values):
            out.append(None)
        else:
            out.append(all(f > s for f, s in values))
    return out


def daily_moves(closes):
    """Close-to-close percentage change, None for the first bar."""
    out = [None]
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        out.append(None if not prev else (closes[i] - prev) / prev * 100.0)
    return out


def evaluate(bars, clouds=CLOUDS, drop_pct=DROP_PCT):
    """Per-day verdicts for one ticker. `bars` is [(date, open, ..., close)].

    Returns a list of dicts, one per bar, each carrying every input to the
    decision rather than just the verdict. A screener that says "no setup"
    without saying which of the three conditions failed is unauditable, and
    the whole point of this repo is that an absence has to be explicable.
    """
    if not bars:
        return []
    days = [b[0] for b in bars]
    closes = [float(b[4]) for b in bars]
    greens = cloud_greens(closes, clouds)
    moves = daily_moves(closes)
    out = []
    for i, day in enumerate(days):
        green, move = greens[i], moves[i]
        if green is None:
            why, qualifies = "not enough history for the clouds", False
        elif move is None:
            why, qualifies = "no previous close to measure against", False
        elif not green:
            why, qualifies = "clouds not both green", False
        elif move > -drop_pct:
            why = f"no {drop_pct:g}% red day ({move:+.2f}%)"
            qualifies = False
        else:
            why, qualifies = f"qualifies ({move:+.2f}%)", True
        out.append({"day": day, "close": closes[i], "move": move,
                    "clouds_green": green, "qualifies": qualifies,
                    "why": why})
    return out


def latest_state(bars, clouds=CLOUDS, drop_pct=DROP_PCT):
    """What the page shows: today's verdict plus how long the clouds have held.

    The streak is included because step 8 is "live your life until the next red
    day" -- the useful thing to see at a glance is not only whether today
    qualifies but whether the ticker is in the waiting state at all.
    """
    rows = evaluate(bars, clouds, drop_pct)
    if not rows:
        return None
    last = dict(rows[-1])
    streak = 0
    for row in reversed(rows):
        if row["clouds_green"] is True:
            streak += 1
        else:
            break
    last["green_streak"] = streak
    recent = [r for r in rows if r["qualifies"]]
    last["last_qualified"] = recent[-1]["day"] if recent else None
    last["qualified_count"] = len(recent)
    return last


def load_bars(conn, ticker):
    """Bars for one ticker in the shape evaluate() wants, oldest first."""
    return conn.execute(
        "SELECT day, open, high, low, close, volume FROM prices"
        " WHERE ticker = ? AND close > 0 ORDER BY day", (ticker,)).fetchall()


def report(tickers, clouds=CLOUDS, drop_pct=DROP_PCT):
    """Today's state and how often this fired, for the whole watchlist.

    The frequency is the number that decides whether the process is tradeable
    at all. A rule that qualified twice in two years is not a system, and one
    that qualifies weekly means capital is always committed -- which is a very
    different trade from the one the steps describe.
    """
    import market_data
    conn = market_data.connect()
    fetched, cached, failed = market_data.refresh(conn, list(tickers))
    print(f"\nprices: {fetched} fetched, {cached} cached, {failed} failed\n")

    print(f"clouds {clouds}   drop {drop_pct:g}%")
    print("=" * 72)
    any_rows = False
    for ticker in tickers:
        bars = load_bars(conn, ticker)
        if not bars:
            # Never silently. An unpriceable ticker is a known unknown.
            print(f"\n{ticker}: NO PRICE DATA -- not 'no setup', no data")
            continue
        any_rows = True
        rows = evaluate(bars, clouds, drop_pct)
        state = latest_state(bars, clouds, drop_pct)
        judged = [r for r in rows if r["clouds_green"] is not None]
        hits = [r for r in rows if r["qualifies"]]
        span = f"{bars[0][0]} .. {bars[-1][0]}"
        print(f"\n{ticker}  {len(bars)} sessions  {span}")
        print(f"  judgeable days: {len(judged)} "
              f"(the first {len(rows) - len(judged)} lack EMA history)")
        green = sum(1 for r in judged if r["clouds_green"])
        print(f"  clouds green on {green}/{len(judged)} "
              f"({green / len(judged) * 100:.0f}%)")
        print(f"  qualifying days: {len(hits)}", end="")
        if judged:
            print(f"  (one every {len(judged) / len(hits):.0f} sessions)"
                  if hits else "  -- the rule never fired")
        else:
            print()
        if hits:
            print(f"  most recent: {', '.join(r['day'] for r in hits[-5:])}")
        print(f"  TODAY ({state['day']}): "
              f"{'SET UP' if state['qualifies'] else 'no'} -- {state['why']}")
        print(f"    clouds green {state['green_streak']} session(s) running")
    if not any_rows:
        return 1
    print("\n" + "=" * 72)
    print("Steps 4-7 are not answered here and cannot be: the free tier")
    print("refuses a live options chain (403). For a ticker marked SET UP,")
    print("the strike and premium are a question for your broker.")
    return 0


def selftest():
    """Arithmetic and edge cases, offline. No network, no database."""
    checks = 0

    def ok(label, cond):
        nonlocal checks
        assert cond, "FAILED: " + label
        checks += 1

    # EMA against a hand-computed case.
    closes = [1.0, 2.0, 3.0, 4.0, 5.0]
    got = ema(closes, 3)
    ok("EMA is None before its seed", got[0] is None and got[1] is None)
    ok("EMA seeds on the SMA", abs(got[2] - 2.0) < 1e-12)
    # k = 2/4 = .5 -> 4*.5 + 2*.5 = 3.0 -> 5*.5 + 3*.5 = 4.0
    ok("EMA steps correctly", abs(got[3] - 3.0) < 1e-12
       and abs(got[4] - 4.0) < 1e-12)
    ok("EMA of a too-short series is all None", ema([1.0, 2.0], 5) == [None] * 2)

    # A flat series cannot be green: fast == slow is not fast > slow.
    flat = [10.0] * 80
    ok("a flat series is not green", cloud_greens(flat)[-1] is False)
    # A monotonically rising series must be.
    rising = [10.0 + i for i in range(80)]
    ok("a rising series is green", cloud_greens(rising)[-1] is True)
    # Falling must not be.
    falling = [100.0 - i for i in range(80)]
    ok("a falling series is not green", cloud_greens(falling)[-1] is False)
    # Before the slowest EMA is defined, the answer is None, not False.
    ok("unjudgeable days are None, not False",
       cloud_greens(rising)[40] is None)

    # The three conditions must coincide on one day.
    bars = [(f"2026-01-{i + 1:02d}", 0, 0, 0, 10.0 + i) for i in range(80)]
    rows = evaluate(bars)
    ok("a steadily rising series never qualifies",
       not any(r["qualifies"] for r in rows))
    ok("and says why", "no 5% red day" in rows[-1]["why"])

    # Now drop the last close 6% from a green uptrend -> qualifies.
    drop = list(bars)
    prev = drop[-2][4]          # the close the move is measured AGAINST
    drop[-1] = ("2026-03-22", 0, 0, 0, prev * 0.94)
    rows = evaluate(drop)
    ok("a 6% red day in a green trend qualifies", rows[-1]["qualifies"])

    # Same drop with the clouds red must NOT qualify -- this is the condition
    # most likely to be quietly dropped, since a big red day is the eye-catching
    # part and the trend filter is the boring part that does the work.
    down = [(f"2026-01-{i + 1:02d}", 0, 0, 0, 100.0 - i) for i in range(80)]
    prev = down[-2][4]
    down[-1] = ("2026-03-22", 0, 0, 0, prev * 0.94)
    rows = evaluate(down)
    ok("a red day in a downtrend does not qualify", not rows[-1]["qualifies"])
    ok("and blames the clouds", rows[-1]["why"] == "clouds not both green")

    # A drop of exactly the threshold counts; a hair less does not.
    for pct, expect in ((-5.0, True), (-4.99, False)):
        edge = list(bars)
        prev = edge[-2][4]
        edge[-1] = ("2026-03-22", 0, 0, 0, prev * (1 + pct / 100.0))
        ok(f"a {pct}% day qualifies={expect}",
           evaluate(edge)[-1]["qualifies"] is expect)

    ok("no bars gives no rows", evaluate([]) == [])
    ok("no bars gives no state", latest_state([]) is None)

    state = latest_state(drop)
    ok("state reports the green streak", state["green_streak"] > 0)
    ok("state reports the last qualifying day",
       state["last_qualified"] == "2026-03-22")

    print(f"wheel selftest: {checks} checks passed")
    return 0


def main():
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--report", action="store_true",
                    help="fetch prices and report today's state plus how "
                         "often the rule has fired")
    ap.add_argument("--tickers", default="TQQQ,SOXL,SPXL",
                    help="comma-separated watchlist (your step 1)")
    ap.add_argument("--drop", type=float, default=DROP_PCT,
                    help="the red-day threshold in percent")
    ap.add_argument("--clouds", default="5/12,34/50",
                    help="EMA cloud pairs, e.g. 5/12,34/50")
    args = ap.parse_args()
    if not args.report:
        return selftest()
    clouds = tuple(tuple(int(n) for n in pair.split("/"))
                   for pair in args.clouds.split(","))
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    return report(tickers, clouds, args.drop)


if __name__ == "__main__":
    sys.exit(main())
