#!/usr/bin/env python3
"""How deep is the dip, and what happened the last time it was this deep?

A tracker that says "SOXL is down 8%" tells you nothing you cannot read off a
chart in two seconds. The useful statement is conditional: dips THIS deep, in
THIS name, were followed by THIS distribution of outcomes, over THIS many
episodes. That is what this module computes.

THE TRAP THIS MODULE IS BUILT AROUND
------------------------------------
Consecutive days inside one selloff are not independent observations. A
fourteen-day slide that stays 8% below its high produces fourteen rows that
all describe the same event, and counting them as fourteen samples inflates n
by an order of magnitude and makes a single episode look like a body of
evidence. Every day in a drawdown is also highly correlated with the next, so
the forward returns are nearly the same number repeated.

So days are grouped into EPISODES: one entry per distinct selloff, taken at
its first qualifying day, with a cooldown before the same name can count
again. n then means what a reader assumes it means.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It does not rank, score, or recommend. It reports a distribution and the
sample size behind it, and where the sample is too small to mean anything it
says so rather than printing a median of three numbers in the same typeface
as a median of thirty.

A LEVERAGED ETF IS NOT A STOCK
------------------------------
These funds reset their leverage daily, so they decay in choppy markets and
can fail to recover even when the index they track does. A dip in SOXL is not
a discount on semiconductors; it is a 3x-levered path that may never return.
Nothing here models that, which is precisely why the forward-return
distribution is quoted with its worst case next to its median.
"""

import sys

DEPTHS = (3.0, 5.0, 8.0, 12.0, 20.0)   # percent below the rolling high
HORIZONS = (5, 10, 21, 63)             # sessions forward
HIGH_LOOKBACK = 20                     # sessions in the rolling high
COOLDOWN = 10                          # sessions before a new episode counts
MIN_EPISODES = 8                       # below this, report but do not imply


# A 3x fund moves hard, but not this hard. Anything past this in one
# session is treated as suspect until proven otherwise.
GAP_SUSPECT_PCT = 25.0
# Ratios a split produces. A "crash" that lands on one of these to within a
# couple of percent is almost certainly a corporate action that the price
# series has not been adjusted for.
SPLIT_RATIOS = (0.25, 1 / 3.0, 0.5, 2.0, 3.0, 4.0)
SPLIT_TOLERANCE = 0.03


def suspect_gaps(days, closes, threshold=GAP_SUSPECT_PCT):
    """Single sessions too large to be a market move. Returns a list.

    This exists because of what a split does to a price series. If the
    history is not adjusted, a 2-for-1 shows up as a 50% overnight collapse,
    and every statistic downstream -- drawdown, dip depth, forward return,
    the alert threshold -- reads a corporate action as a crash. It would not
    error. It would produce confident, wrong numbers, which is worse.

    A move is flagged when it exceeds the threshold, and separately marked
    when the ratio lands on a common split factor, because that is the
    difference between "unusual day" and "not a price move at all".
    """
    out = []
    for i in range(1, len(closes)):
        prev, close = closes[i - 1], closes[i]
        if not prev or not close:
            continue
        move = (close - prev) / prev * 100.0
        if abs(move) < threshold:
            continue
        ratio = close / prev
        near = next((r for r in SPLIT_RATIOS
                     if abs(ratio - r) <= SPLIT_TOLERANCE), None)
        out.append({"day": days[i], "prev_day": days[i - 1],
                    "move": move, "ratio": ratio,
                    "split_like": near, "prev": prev, "close": close})
    return out


def rolling_high(closes, lookback=HIGH_LOOKBACK):
    """Highest close of the preceding `lookback` sessions, inclusive.

    None until there is a full window. A partial window would compare today
    against a handful of days and call a normal wobble a drawdown.
    """
    out = []
    for i in range(len(closes)):
        if i + 1 < lookback:
            out.append(None)
        else:
            out.append(max(closes[i + 1 - lookback:i + 1]))
    return out


def drawdowns(closes, lookback=HIGH_LOOKBACK):
    """Percent below the rolling high, per day. Positive means below."""
    highs = rolling_high(closes, lookback)
    out = []
    for close, high in zip(closes, highs):
        if high is None or not high:
            out.append(None)
        else:
            out.append((high - close) / high * 100.0)
    return out


def episodes(days, closes, depth, lookback=HIGH_LOOKBACK, cooldown=COOLDOWN):
    """Indices of the first day of each distinct dip at least `depth` deep.

    The cooldown is what makes an episode an episode. Without it a long
    slide returns one index per day and every statistic downstream is
    computed on the same selloff over and over.
    """
    dd = drawdowns(closes, lookback)
    out, last = [], None
    for i, value in enumerate(dd):
        if value is None or value < depth:
            continue
        if last is not None and i - last < cooldown:
            continue
        out.append(i)
        last = i
    return out


def forward(closes, i, horizon):
    """Return from close[i] to close[i + horizon], or None past the data.

    None, not a truncated window. An episode whose horizon runs off the end
    of the series has not produced an outcome yet, and letting it quietly
    report a shorter hold would mix two different measurements.
    """
    j = i + horizon
    if j >= len(closes) or not closes[i]:
        return None
    return (closes[j] - closes[i]) / closes[i] * 100.0


def profile(days, closes, depths=DEPTHS, horizons=HORIZONS,
            lookback=HIGH_LOOKBACK, cooldown=COOLDOWN):
    """For each depth: how often, and what followed. Returns a list of dicts."""
    rows = []
    for depth in depths:
        idx = episodes(days, closes, depth, lookback, cooldown)
        row = {"depth": depth, "n_episodes": len(idx),
               "last": days[idx[-1]] if idx else None, "horizons": {}}
        for horizon in horizons:
            vals = [forward(closes, i, horizon) for i in idx]
            got = sorted(v for v in vals if v is not None)
            pending = sum(1 for v in vals if v is None)
            if got:
                row["horizons"][horizon] = {
                    "n": len(got), "pending": pending,
                    "median": got[len(got) // 2],
                    "worst": got[0], "best": got[-1],
                    "positive": sum(1 for v in got if v > 0),
                }
            else:
                row["horizons"][horizon] = {"n": 0, "pending": pending}
        rows.append(row)
    return rows


def current(days, closes, lookback=HIGH_LOOKBACK):
    """Where the name sits right now."""
    dd = drawdowns(closes, lookback)
    if not dd or dd[-1] is None:
        return None
    highs = rolling_high(closes, lookback)
    streak = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] < closes[i - 1]:
            streak += 1
        else:
            break
    return {"day": days[-1], "close": closes[-1],
            "high": highs[-1], "drawdown": dd[-1],
            "down_streak": streak,
            "deepest_matched": max((d for d in DEPTHS if dd[-1] >= d),
                                   default=None)}


def selftest():
    checks = 0

    def ok(label, cond):
        nonlocal checks
        assert cond, "FAILED: " + label
        checks += 1

    # Rolling high needs a full window before it answers.
    closes = [10.0] * 30
    rh = rolling_high(closes, 20)
    ok("rolling high is None before a full window",
       rh[18] is None and rh[19] == 10.0)

    # A flat series has no drawdown; a fall produces one.
    ok("flat series has zero drawdown", drawdowns(closes, 20)[-1] == 0.0)
    falling = [100.0] * 20 + [90.0]
    ok("a 10% fall reads as 10%",
       abs(drawdowns(falling, 20)[-1] - 10.0) < 1e-9)

    # THE IMPORTANT ONE. A long slide must be ONE episode, not many.
    slide = [100.0] * 20 + [88.0] * 15
    many = episodes(list(range(len(slide))), slide, 8.0, 20, cooldown=10)
    ok("a 15-day slide is not 15 episodes", len(many) == 2)
    none_cd = episodes(list(range(len(slide))), slide, 8.0, 20, cooldown=999)
    ok("and with a long cooldown it is exactly one", len(none_cd) == 1)
    ok("without a cooldown it would have been fifteen",
       len(episodes(list(range(len(slide))), slide, 8.0, 20, cooldown=1)) == 15)

    # Forward returns stop at the end of the data rather than shortening.
    ok("forward past the end is None", forward([1.0, 2.0], 0, 5) is None)
    ok("forward inside the data is a percentage",
       abs(forward([100.0, 110.0], 0, 1) - 10.0) < 1e-9)

    # A depth nothing reaches reports zero episodes, not an empty median.
    days = [f"d{i:03d}" for i in range(40)]
    flat = [100.0] * 40
    rows = profile(days, flat, depths=(5.0,), horizons=(5,))
    ok("an unreached depth reports n=0", rows[0]["n_episodes"] == 0)
    ok("and carries no median", "median" not in rows[0]["horizons"][5])

    # Pending outcomes are counted, not dropped.
    path = [100.0] * 20 + [85.0] * 3
    rows = profile(list(range(len(path))), path, depths=(8.0,), horizons=(63,))
    ok("an episode with no outcome yet is pending",
       rows[0]["horizons"][63]["pending"] == rows[0]["n_episodes"])

    # A split must be caught, and told apart from a violent but real day.
    split = [100.0] * 10 + [50.0] * 10
    found = suspect_gaps([f"d{i}" for i in range(20)], split)
    ok("a 2-for-1 is flagged", len(found) == 1)
    ok("and identified as split-like", found[0]["split_like"] == 0.5)
    rough = [100.0] * 10 + [72.0] * 10          # -28%, violent but not a split
    r2 = suspect_gaps([f"d{i}" for i in range(20)], rough)
    ok("a violent real day is flagged too", len(r2) == 1)
    ok("but not called a split", r2[0]["split_like"] is None)
    ok("an ordinary 9% day is not flagged",
       suspect_gaps(["a", "b"], [100.0, 91.0]) == [])

    cur = current(days, [100.0] * 39 + [92.0])
    ok("current reports the drawdown", abs(cur["drawdown"] - 8.0) < 1e-9)
    ok("current reports the down streak", cur["down_streak"] == 1)
    # 8.0 clears the 8% band exactly, so that is the deepest it matches.
    ok("current matches the deepest band", cur["deepest_matched"] == 8.0)
    shallow = current(days, [100.0] * 39 + [94.0])
    ok("a 6% dip matches the 5% band, not the 8%",
       shallow["deepest_matched"] == 5.0)

    print(f"dips selftest: {checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(selftest())
