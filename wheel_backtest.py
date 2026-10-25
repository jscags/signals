#!/usr/bin/env python3
"""Did the 9-step process pay, and did the screener's filter earn its place?

Two questions, and only the second one this window can actually answer.

The probe settled the boundary: option prices are available from roughly
October 2024, and no earlier at any price the free tier will sell. That
window contains no sustained bear market. A strategy whose losses live
entirely in the drawdown therefore cannot FAIL this test, so a good result
here is not evidence that it is safe, and this module says so in its own
output rather than leaving the reader to remember it.

What the window CAN settle is relative, and it is the question the screener
exists for: do entries filtered by steps 1-3 beat entries taken without the
filter, over the identical period, on the identical tickers, with the
identical strike rule? If they do not, the EMA clouds and the 5% red day are
decoration, and the honest thing is to say so -- the same verdict this repo
reached about Lane A.

HOW A TRADE IS MODELLED
-----------------------
    signal      a qualifying close on day D (steps 1-3)
    entry       the NEXT session, filled at that session's close. A full
                session after the signal, so nothing can see its own future.
                The option's close is used rather than its open because an
                option's opening print is frequently a stale or crossed
                quote on a contract that has not traded yet.
    strike      the one whose premium is nearest the target yield, measured
                as premium / strike -- which is the cash actually secured,
                and is step 5's own comparison
    expiry      the listed expiry nearest 30 days from entry (step 4)
    exit        the first session whose LOW touches half the entry premium
                (step 7's GTC order), otherwise held to expiry
    assignment  at expiry, underlying below strike -> assigned, and the loss
                is (strike - underlying close) net of the premium kept

WHERE THIS IS OPTIMISTIC, STATED PLAINLY
----------------------------------------
  * A GTC limit is treated as filled when the session's low touches it. A
    real limit needs someone to trade there, so this is the friendly end of
    what would have happened.
  * Commission and slippage are not modelled. On a strategy that collects
    small premiums many times, they are not negligible.
  * The option's close is a last trade, not a fill. Wide spreads on a 3x
    ETF's far strikes make that an approximation.
  * Early assignment is not modelled; American puts deep in the money can be
    assigned before expiry, which would end the trade worse than here.

None of those favour the conclusion that the filter helps, because they
apply equally to the filtered arm and the control arm. They do favour the
strategy's absolute numbers, which is why the absolute numbers are the ones
to distrust.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

import market_data
import wheel

POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")
PACE = 12.5
TARGET_YIELD = 5.0          # step 5: premium / strike, in percent
DTE_TARGET = 30             # step 4
PROFIT_TAKE = 0.50          # step 7
N_CANDIDATES = 6            # strikes priced per entry, to find the yield

SCHEMA = """
-- Daily bars for one option contract. Cached because each one costs a
-- request against a five-per-minute limit, and a run that had to re-fetch
-- everything after a failure would take hours it does not have.
CREATE TABLE IF NOT EXISTS option_bars (
    contract TEXT NOT NULL,
    day      TEXT NOT NULL,
    open     REAL, high REAL, low REAL, close REAL,
    volume   REAL, vwap REAL,
    PRIMARY KEY (contract, day)
);

-- One row per contract ASKED FOR, including the ones that returned nothing,
-- for the same reason price_meta exists: a contract that never traded and a
-- contract we failed to fetch must not look alike.
CREATE TABLE IF NOT EXISTS option_meta (
    contract   TEXT PRIMARY KEY,
    underlying TEXT, expiry TEXT, strike REAL,
    status     TEXT, n_days INTEGER, fetched_at TEXT, note TEXT
);

-- The listed chain for one underlying and expiry, cached the same way.
CREATE TABLE IF NOT EXISTS option_chain (
    underlying TEXT NOT NULL,
    expiry     TEXT NOT NULL,
    strike     REAL NOT NULL,
    contract   TEXT NOT NULL,
    PRIMARY KEY (underlying, expiry, strike)
);
CREATE TABLE IF NOT EXISTS chain_meta (
    underlying TEXT NOT NULL,
    expiry     TEXT NOT NULL,
    n          INTEGER,
    fetched_at TEXT,
    PRIMARY KEY (underlying, expiry)
);
"""


class Budget:
    """A hard ceiling on requests, because the pace makes runaway loops
    expensive in wall-clock time rather than in money, and a job that is
    killed at four hours leaves nothing behind."""

    def __init__(self, limit):
        self.limit, self.used, self.last = limit, 0, 0.0

    def spend(self):
        if self.used >= self.limit:
            raise RuntimeError(f"request budget of {self.limit} exhausted")
        gap = time.time() - self.last
        if self.last and gap < PACE:
            time.sleep(PACE - gap)
        self.last = time.time()
        self.used += 1


def connect(path=None):
    conn = market_data.connect(path)
    conn.executescript(SCHEMA)
    return conn


def _once(url, budget):
    budget.spend()
    request = urllib.request.Request(url, headers={
        "User-Agent": market_data.USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode())
        except (json.JSONDecodeError, ValueError):
            return exc.code, {}
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        return None, {"error": f"{type(exc).__name__}: {exc}"}


# Retried rather than cached. A 429 means "slow down", a 5xx means "not now",
# and a dropped connection means nothing at all -- but the first version of
# this module wrote every one of them into the cache as a contract with no
# bars, which is this repo's oldest failure mode wearing new clothes: a
# transport error recorded as an absence, permanently, and never retried.
RETRY_STATUSES = (429, 500, 502, 503, 504, None)


def _get(url, budget, tries=4):
    status, data = _once(url, budget)
    for attempt in range(1, tries):
        if status not in RETRY_STATUSES:
            break
        back = PACE * (attempt + 1)
        print(f"    http {status} -- backing off {back:.0f}s and retrying "
              f"({attempt}/{tries - 1})")
        time.sleep(back)
        status, data = _once(url, budget)
    return status, data


def load_chain(conn, underlying, expiry, budget, key):
    """Strikes listed for one underlying and expiry. Cached."""
    row = conn.execute(
        "SELECT n FROM chain_meta WHERE underlying = ? AND expiry = ?",
        (underlying, expiry)).fetchone()
    if row is None:
        url = ("https://api.polygon.io/v3/reference/options/contracts"
               f"?underlying_ticker={underlying}&contract_type=put"
               f"&expiration_date={expiry}&expired=true&limit=1000"
               f"&apiKey={key}")
        status, data = _get(url, budget)
        if status != 200:
            # NOT cached. A refused request is not an empty chain, and
            # writing it as one would make every later run agree.
            print(f"    chain {underlying} {expiry}: http {status} "
                  f"-- not cached, will retry")
            return []
        results = data.get("results") or []
        conn.executemany(
            "INSERT OR REPLACE INTO option_chain"
            " (underlying, expiry, strike, contract) VALUES (?,?,?,?)",
            [(underlying, expiry, float(c["strike_price"]), c["ticker"])
             for c in results
             if c.get("strike_price") is not None and c.get("ticker")])
        conn.execute(
            "INSERT OR REPLACE INTO chain_meta (underlying, expiry, n,"
            " fetched_at) VALUES (?,?,?,?)",
            (underlying, expiry, len(results),
             datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    return conn.execute(
        "SELECT strike, contract FROM option_chain"
        " WHERE underlying = ? AND expiry = ? ORDER BY strike",
        (underlying, expiry)).fetchall()


def load_contract(conn, contract, start, end, budget, key,
                  underlying=None, strike=None, expiry=None):
    """Daily bars for one contract over a range. Cached, including misses."""
    seen = conn.execute(
        "SELECT status FROM option_meta WHERE contract = ?"
        # FAILED is not a cache hit. Only an actual answer from the server --
        # bars, or a 200 carrying none -- settles a contract.
        " AND status IN ('OK','NONE')", (contract,)).fetchone()
    if seen is None:
        url = (f"https://api.polygon.io/v2/aggs/ticker/{contract}"
               f"/range/1/day/{start}/{end}?adjusted=true&sort=asc"
               f"&limit=5000&apiKey={key}")
        status, data = _get(url, budget)
        results = data.get("results") or []
        rows = []
        for bar in results:
            try:
                # UTC, matching market_data.parse_polygon. A daily bar is
                # stamped at midnight ET, so a local-time conversion on a
                # machine west of UTC would file it under the previous day.
                day = datetime.fromtimestamp(
                    bar["t"] / 1000, tz=timezone.utc).date().isoformat()
                rows.append((contract, day, float(bar.get("o") or 0),
                             float(bar.get("h") or 0), float(bar.get("l") or 0),
                             float(bar["c"]), float(bar.get("v") or 0),
                             float(bar.get("vw") or 0)))
            except (KeyError, TypeError, ValueError, OSError):
                continue
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO option_bars (contract, day, open,"
                " high, low, close, volume, vwap) VALUES (?,?,?,?,?,?,?,?)",
                rows)
        if status == 200:
            state = "OK" if rows else "NONE"
            note = "" if rows else "200 with no results -- never traded"
        else:
            state = "FAILED"
            note = (data.get("message") or f"http {status}")[:120]
        conn.execute(
            "INSERT OR REPLACE INTO option_meta (contract, underlying, expiry,"
            " strike, status, n_days, fetched_at, note) VALUES (?,?,?,?,?,?,?,?)",
            (contract, underlying, expiry, strike, state, len(rows),
             datetime.now().isoformat(timespec="seconds"), note))
        conn.commit()
        if state == "FAILED":
            return []
    return conn.execute(
        "SELECT day, open, high, low, close, volume, vwap FROM option_bars"
        " WHERE contract = ? AND day >= ? AND day <= ? ORDER BY day",
        (contract, start, end)).fetchall()


def pick_expiry(conn, underlying, entry_day, budget, key):
    """The listed expiry nearest DTE_TARGET days after entry.

    Expiries are probed rather than listed, because the contracts endpoint
    filtered by a single date is one cheap request and a full listing of
    every expiry is not. Fridays are tried outward from the target.
    """
    target = date.fromisoformat(entry_day) + timedelta(days=DTE_TARGET)
    # Walk to the nearest Friday, then alternate outward a week at a time.
    friday = target + timedelta(days=(4 - target.weekday()) % 7)
    for shift in (0, -7, 7, -14, 14):
        candidate = (friday + timedelta(days=shift)).isoformat()
        chain = load_chain(conn, underlying, candidate, budget, key)
        if chain:
            return candidate, chain
    return None, []


def simulate(conn, ticker, signal_day, bars_by_day, days, budget, key,
             target_yield=TARGET_YIELD, on_assignment="realize"):
    """One trade from one signal. Returns a dict, or a dict saying why not."""
    i = days.index(signal_day)
    if i + 1 >= len(days):
        return {"signal": signal_day, "skipped": "no session after the signal"}
    entry_day = days[i + 1]
    spot = bars_by_day[entry_day]["close"]

    expiry, chain = pick_expiry(conn, ticker, entry_day, budget, key)
    if not chain:
        return {"signal": signal_day, "entry": entry_day,
                "skipped": "no listed expiry near 30 days"}
    if expiry <= entry_day:
        return {"signal": signal_day, "entry": entry_day,
                "skipped": "expiry not after entry"}

    # Candidates: at and below the money, where a put with real premium and
    # a tolerable assignment price lives. Spanning 0-25% out covers the 5%
    # yield on anything from a calm tape to a panicked one.
    below = [r for r in chain if 0.70 * spot <= r["strike"] <= 1.02 * spot]
    if not below:
        return {"signal": signal_day, "entry": entry_day, "expiry": expiry,
                "skipped": f"no strikes within 30% of spot {spot:.2f}"}
    step = max(1, len(below) // N_CANDIDATES)
    candidates = below[::step][-N_CANDIDATES:]

    best, tried, empty = None, 0, 0
    for row in candidates:
        tried += 1
        obars = load_contract(conn, row["contract"], entry_day, expiry,
                              budget, key, ticker, row["strike"], expiry)
        if not obars or obars[0]["day"] != entry_day:
            empty += 1
            continue
        premium = obars[0]["close"]
        if premium <= 0:
            empty += 1
            continue
        got = premium / row["strike"] * 100.0
        gap = abs(got - target_yield)
        if best is None or gap < best["gap"]:
            best = {"gap": gap, "yield": got, "premium": premium,
                    "strike": row["strike"], "contract": row["contract"],
                    "bars": obars}
    if best is None:
        return {"signal": signal_day, "entry": entry_day, "expiry": expiry,
                "skipped": f"no candidate priced on entry day "
                           f"({tried} tried, {empty} empty)"}

    # Step 7: GTC to buy back at half the premium.
    target_price = best["premium"] * (1.0 - PROFIT_TAKE)
    exit_day, exit_price, outcome = None, None, None
    for bar in best["bars"][1:]:
        if bar["low"] and bar["low"] <= target_price:
            exit_day, exit_price, outcome = bar["day"], target_price, "profit target"
            break
    if exit_day is None:
        # Held to expiry. Assignment is decided by the underlying, not the
        # contract's own last print, which may be stale on expiry day.
        last = max((d for d in days if d <= expiry), default=None)
        under = bars_by_day[last]["close"] if last else None
        if under is None:
            return {"signal": signal_day, "entry": entry_day,
                    "skipped": "no underlying close at expiry"}
        exit_day = last
        if under >= best["strike"]:
            exit_price, outcome = 0.0, "expired worthless"
        else:
            exit_price = best["strike"] - under
            outcome = "assigned"
    pnl = best["premium"] - exit_price          # per share
    held = (date.fromisoformat(exit_day) - date.fromisoformat(entry_day)).days
    still_open = False

    # STEP 6's promise, tested rather than taken on trust: "if you get
    # assigned, you're buying an ETF you already chose at the strike price you
    # agreed to". Under on_assignment="hold" an assignment is not booked as a
    # loss -- the shares are kept at a cost basis of strike minus premium and
    # followed until they come back to it.
    #
    # This is the generous reading of the strategy and it is also the one that
    # is easiest to fool yourself with, because a position that is never
    # closed never shows a loss. So a holding that has not recovered by the
    # end of the data is marked to market and flagged open, never counted as
    # though it had been closed.
    if outcome == "assigned" and on_assignment == "hold":
        basis = best["strike"] - best["premium"]
        after = [d for d in days if d > exit_day]
        recovered = next(
            (d for d in after if bars_by_day[d]["close"] >= basis), None)
        if recovered:
            close = bars_by_day[recovered]["close"]
            exit_day, outcome = recovered, "assigned, recovered"
        else:
            close = bars_by_day[after[-1]]["close"] if after else under
            exit_day = after[-1] if after else exit_day
            outcome, still_open = "assigned, STILL HELD", True
        pnl = close - basis
        held = (date.fromisoformat(exit_day)
                - date.fromisoformat(entry_day)).days

    # What the two alternatives did over the IDENTICAL days. Selling a put
    # caps the upside at the premium while keeping nearly all the downside,
    # so the honest competitor is not cash -- it is owning the thing. On a 3x
    # fund in a rising market that comparison is the whole argument, and it
    # is the one the nine steps never make.
    hold = None
    exit_close = bars_by_day.get(exit_day, {}).get("close")
    if exit_close and spot:
        hold = (exit_close - spot) / spot * 100.0
    bench = benchmark_return(conn, entry_day, exit_day)
    return {
        "signal": signal_day, "entry": entry_day, "exit": exit_day,
        "expiry": expiry, "strike": best["strike"], "spot": spot,
        "premium": best["premium"], "yield_pct": best["yield"],
        "otm_pct": (spot - best["strike"]) / spot * 100.0,
        "exit_price": exit_price, "outcome": outcome,
        "pnl": pnl, "return_pct": pnl / best["strike"] * 100.0,
        "held_days": held, "contract": best["contract"],
        "hold_pct": hold, "bench_pct": bench, "still_open": still_open,
    }


def benchmark_return(conn, start_day, end_day):
    """SPY over the same days, or None if it cannot be priced.

    None rather than zero. A benchmark that could not be read is not a
    benchmark that went nowhere, and averaging the second into a column of
    real numbers is how a comparison quietly becomes flattering.
    """
    rows = conn.execute(
        "SELECT day, close FROM prices WHERE ticker = ? AND day >= ?"
        " AND day <= ? AND close > 0 ORDER BY day",
        (market_data.BENCHMARK, start_day, end_day)).fetchall()
    if len(rows) < 2:
        return None
    first, last = rows[0]["close"], rows[-1]["close"]
    return (last - first) / first * 100.0 if first else None


def control_days(days, rows, n_wanted, seed=11):
    """Unfiltered entry days: the same count, spread evenly through the same
    span, ignoring the clouds and the red day entirely.

    Evenly spaced rather than random so the control is reproducible and
    cannot be reshuffled until it loses. It is the honest comparison for the
    question 'did the filter matter', because it holds everything else --
    ticker, period, strike rule, exit rule -- identical.
    """
    judgeable = [r["day"] for r in rows if r["clouds_green"] is not None]
    if not judgeable or n_wanted <= 0:
        return []
    step = max(1, len(judgeable) // n_wanted)
    return judgeable[::step][:n_wanted]


def shifted_days(days, signals, offset=5):
    """Entries a fixed number of sessions AFTER each signal, unfiltered.

    The second control, and the stricter one. The evenly-spaced control tests
    the filter against the whole window, but the filtered entries cluster --
    four of SOXL's twenty-five land inside three weeks of June -- so a flat
    control can penalise the rule for WHEN it traded rather than for how well
    it chose. Entering a week after each signal holds the calendar nearly
    constant and removes only the red day itself, which isolates the thing
    the filter actually claims.

    Days that are themselves signals are dropped, or the control would be
    partly made of the arm it is meant to be compared against.
    """
    pos = {d: i for i, d in enumerate(days)}
    taken = set(signals)
    out = []
    for day in signals:
        j = pos.get(day, -1) + offset
        if 0 <= j < len(days) and days[j] not in taken:
            out.append(days[j])
            taken.add(days[j])
    return out


def summarise(label, trades):
    """Totals that cannot hide a tail behind a hit rate."""
    done = [t for t in trades if "return_pct" in t]
    skipped = [t for t in trades if "skipped" in t]
    print(f"\n{label}")
    print(f"  entries attempted : {len(trades)}")
    print(f"  priced            : {len(done)}")
    if skipped:
        reasons = {}
        for t in skipped:
            reasons[t["skipped"]] = reasons.get(t["skipped"], 0) + 1
        print(f"  not priced        : {len(skipped)}")
        for why, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"      {n:3}  {why}")
    if not done:
        print("  -- nothing priced, so nothing is claimed")
        return None
    # An open position is not a result. Folding a holding that has not come
    # back into the same total as closed trades is the exact self-deception
    # the hold-through-assignment rule invites, so they are split.
    open_ = [t for t in done if t.get("still_open")]
    closed = [t for t in done if not t.get("still_open")]
    if not closed:
        print(f"  all {len(open_)} position(s) still open -- nothing realised")
        if open_:
            marks = sorted(t["return_pct"] for t in open_)
            print(f"  marked to market  : median {marks[len(marks) // 2]:+.2f}%"
                  f"  worst {marks[0]:+.2f}%")
        return None
    rets = sorted(t["return_pct"] for t in closed)
    wins = [r for r in rets if r > 0]
    total = sum(rets)
    held = sum(t["held_days"] for t in closed)
    outcomes = {}
    for t in done:
        outcomes[t["outcome"]] = outcomes.get(t["outcome"], 0) + 1
    print(f"  closed            : {len(closed)}"
          + (f"   STILL OPEN: {len(open_)}" if open_ else ""))
    print(f"  hit rate          : {len(wins)}/{len(rets)}"
          f" ({len(wins) / len(rets) * 100:.0f}%)   <- the misleading number")
    print(f"  median return     : {rets[len(rets) // 2]:+.2f}%")
    print(f"  mean return       : {total / len(rets):+.2f}%")
    print(f"  WORST             : {rets[0]:+.2f}%"
          f"   best {rets[-1]:+.2f}%")
    print(f"  sum of returns    : {total:+.2f}%  (on capital secured per trade)")
    print(f"  capital-days used : {held}")
    # The alternatives, over exactly the days each trade was open.
    holds = [t["hold_pct"] for t in closed if t.get("hold_pct") is not None]
    benches = [t["bench_pct"] for t in closed if t.get("bench_pct") is not None]
    if holds:
        print(f"  -- owning it instead, same days: sum {sum(holds):+.2f}%,"
              f" median {sorted(holds)[len(holds) // 2]:+.2f}%"
              f"  ({len(holds)}/{len(done)} priced)")
    if benches:
        print(f"  -- SPY instead, same days:       sum {sum(benches):+.2f}%,"
              f" median {sorted(benches)[len(benches) // 2]:+.2f}%"
              f"  ({len(benches)}/{len(done)} priced)")
    for name, n in sorted(outcomes.items(), key=lambda kv: -kv[1]):
        print(f"      {n:3}  {name}")
    if open_:
        marks = sorted(t["return_pct"] for t in open_)
        unreal = sum(marks)
        print(f"  -- {len(open_)} position(s) STILL OPEN, marked to market:"
              f" {unreal:+.2f}% unrealised, worst {marks[0]:+.2f}%")
        print(f"     realised {total:+.2f}% and unrealised {unreal:+.2f}%"
              f" together: {total + unreal:+.2f}%")
        print(f"     capital still tied up in them:"
              f" {sum(t['held_days'] for t in open_)} day(s)")
    losses = [r for r in rets if r <= 0]
    if losses:
        print(f"  losses total      : {sum(losses):+.2f}% across {len(losses)}")
        print(f"  wins total        : {sum(wins):+.2f}% across {len(wins)}")
    return {"n": len(rets), "median": rets[len(rets) // 2],
            "mean": total / len(rets), "worst": rets[0], "sum": total,
            "hit": len(wins) / len(rets) * 100.0, "held": held,
            "open": len(open_),
            "unrealised": sum(t["return_pct"] for t in open_),
            "hold_sum": sum(holds) if holds else None}


def run(tickers, clouds, drop_pct, budget_limit, control="spread",
        target_yield=TARGET_YIELD, price_start=None,
        on_assignment="realize"):
    if not POLYGON_KEY:
        print("POLYGON_API_KEY is not set.")
        return 2
    budget = Budget(budget_limit)
    conn = connect()
    # An earlier build of this module wrote transport errors into the cache
    # as contracts with no bars. Those rows are indistinguishable from real
    # answers by status alone, so any that carry an http note are cleared and
    # will be asked again.
    stale = conn.execute(
        "DELETE FROM option_meta WHERE status = 'FAILED'"
        " OR (status = 'EMPTY' AND note LIKE 'http %')").rowcount
    conn.commit()
    if stale:
        print(f"cleared {stale} contract(s) cached from a failed request")
    # refresh() reads the window from the module, not from an argument, so
    # widening it is done here. The default asks for 700 days; the free tier
    # actually serves about 730, and on a two-year study those extra weeks
    # are a month of signals rather than a rounding error.
    if price_start:
        market_data.HISTORY_START = price_start
    # The benchmark is fetched with the rest: a comparison that depends on a
    # later request is a comparison that goes missing when the budget runs out.
    fetched, cached, failed = market_data.refresh(
        conn, list(tickers) + [market_data.BENCHMARK])
    print(f"underlying prices: {fetched} fetched, {cached} cached,"
          f" {failed} failed  (from {market_data.HISTORY_START})")
    print(f"on assignment: {on_assignment}")

    results = {}
    for ticker in tickers:
        bars = wheel.load_bars(conn, ticker)
        if not bars:
            print(f"\n{ticker}: NO PRICE DATA -- skipped, not 'no trades'")
            continue
        rows = wheel.evaluate(bars, clouds, drop_pct)
        days = [r["day"] for r in rows]
        by_day = {b[0]: {"close": float(b[4])} for b in bars}
        signals = [r["day"] for r in rows if r["qualifies"]]
        print(f"\n{'=' * 72}\n{ticker}: {len(signals)} qualifying signal(s)"
              f" over {len(days)} sessions")
        print(f"{'=' * 72}")

        trades = []
        for day in signals:
            try:
                trades.append(simulate(conn, ticker, day, by_day, days,
                                       budget, POLYGON_KEY, target_yield,
                                       on_assignment))
            except RuntimeError as exc:
                print(f"  stopped early: {exc}")
                break
        filt = summarise(f"{ticker} -- FILTERED (steps 1-3 applied)", trades)

        arms = {}
        if control and signals:
            plans = []
            if control in ("spread", "both"):
                plans.append(("spread", control_days(days, rows, len(signals)),
                              "no filter, same count, spread across the span"))
            if control in ("shifted", "both"):
                plans.append(("shifted", shifted_days(days, signals),
                              "no filter, 5 sessions after each signal,"
                              " same calendar"))
            for name, picks, why in plans:
                trades_c = []
                for day in picks:
                    try:
                        trades_c.append(simulate(conn, ticker, day, by_day,
                                                 days, budget, POLYGON_KEY,
                                                 target_yield, on_assignment))
                    except RuntimeError as exc:
                        print(f"  control {name} stopped early: {exc}")
                        break
                arms[name] = summarise(
                    f"{ticker} -- CONTROL [{name}] ({why})", trades_c)
        results[ticker] = (filt, arms)
        print(f"\n  requests used so far: {budget.used}/{budget.limit}")

    print(f"\n{'=' * 72}\nDID THE FILTER EARN ITS PLACE?\n{'=' * 72}")
    for ticker, (filt, arms) in results.items():
        if not filt:
            print(f"\n{ticker}: nothing priced on the filtered arm")
            continue
        print(f"\n{ticker}  filtered n={filt['n']}")
        for name, ctrl in arms.items():
            if not ctrl:
                print(f"  vs {name}: nothing priced")
                continue
            edge = filt["median"] - ctrl["median"]
            print(f"  vs {name:8} n={ctrl['n']:3}  "
                  f"median {filt['median']:+.2f}% vs {ctrl['median']:+.2f}%"
                  f"  -> {edge:+.2f}% to the filter")
            print(f"  {'':11}      worst  {filt['worst']:+.2f}% vs"
                  f" {ctrl['worst']:+.2f}%   "
                  f"sum {filt['sum']:+.2f}% vs {ctrl['sum']:+.2f}%")
            if filt["n"] < 10 or ctrl["n"] < 10:
                print(f"  {'':11}      !! fewer than ten trades on one side."
                      f" This repo has already been burned by a cell"
                      f" resting on n=2.")

    print(f"\n{'=' * 72}")
    print("SCOPE. Option history begins around October 2024, so every number")
    print("above comes from a period with no sustained bear market. The")
    print("losses this strategy is capable of are not in this window. A good")
    print("result here is not evidence of safety; it is evidence about a")
    print("rising market, which is the only market that was available.")
    print(f"requests used: {budget.used}/{budget.limit}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--tickers", default="SOXL,TQQQ,SPXL")
    ap.add_argument("--clouds", default="5/12,34/50")
    ap.add_argument("--drop", type=float, default=wheel.DROP_PCT)
    ap.add_argument("--yield", dest="target_yield", type=float,
                    default=TARGET_YIELD, help="step 5's premium/strike %%")
    ap.add_argument("--budget", type=int, default=900,
                    help="hard ceiling on API requests")
    ap.add_argument("--control", default="spread",
                    choices=("spread", "shifted", "both", "none"),
                    help="which unfiltered comparison to run")
    ap.add_argument("--price-start", default=None)
    ap.add_argument("--on-assignment", default="realize",
                    choices=("realize", "hold"),
                    help="realize books the loss at expiry; hold keeps the "
                         "shares at strike minus premium and follows them "
                         "back to that basis -- step 6's own promise")
    args = ap.parse_args()
    clouds = tuple(tuple(int(n) for n in p.split("/"))
                   for p in args.clouds.split(","))
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    return run(tickers, clouds, args.drop, args.budget,
               control=None if args.control == "none" else args.control,
               target_yield=args.target_yield,
               price_start=args.price_start,
               on_assignment=args.on_assignment)


if __name__ == "__main__":
    sys.exit(main())
