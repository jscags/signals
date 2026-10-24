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
             target_yield=TARGET_YIELD):
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
    return {
        "signal": signal_day, "entry": entry_day, "exit": exit_day,
        "expiry": expiry, "strike": best["strike"], "spot": spot,
        "premium": best["premium"], "yield_pct": best["yield"],
        "otm_pct": (spot - best["strike"]) / spot * 100.0,
        "exit_price": exit_price, "outcome": outcome,
        "pnl": pnl, "return_pct": pnl / best["strike"] * 100.0,
        "held_days": held, "contract": best["contract"],
    }


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
    rets = sorted(t["return_pct"] for t in done)
    wins = [r for r in rets if r > 0]
    total = sum(rets)
    held = sum(t["held_days"] for t in done)
    outcomes = {}
    for t in done:
        outcomes[t["outcome"]] = outcomes.get(t["outcome"], 0) + 1
    print(f"  hit rate          : {len(wins)}/{len(rets)}"
          f" ({len(wins) / len(rets) * 100:.0f}%)   <- the misleading number")
    print(f"  median return     : {rets[len(rets) // 2]:+.2f}%")
    print(f"  mean return       : {total / len(rets):+.2f}%")
    print(f"  WORST             : {rets[0]:+.2f}%"
          f"   best {rets[-1]:+.2f}%")
    print(f"  sum of returns    : {total:+.2f}%  (on capital secured per trade)")
    print(f"  capital-days used : {held}")
    for name, n in sorted(outcomes.items(), key=lambda kv: -kv[1]):
        print(f"      {n:3}  {name}")
    losses = [r for r in rets if r <= 0]
    if losses:
        print(f"  losses total      : {sum(losses):+.2f}% across {len(losses)}")
        print(f"  wins total        : {sum(wins):+.2f}% across {len(wins)}")
    return {"n": len(rets), "median": rets[len(rets) // 2],
            "mean": total / len(rets), "worst": rets[0], "sum": total,
            "hit": len(wins) / len(rets) * 100.0, "held": held}


def run(tickers, clouds, drop_pct, budget_limit, control=True,
        target_yield=TARGET_YIELD, price_start=None):
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
    fetched, cached, failed = market_data.refresh(conn, list(tickers))
    print(f"underlying prices: {fetched} fetched, {cached} cached,"
          f" {failed} failed  (from {market_data.HISTORY_START})")

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
                                       budget, POLYGON_KEY, target_yield))
            except RuntimeError as exc:
                print(f"  stopped early: {exc}")
                break
        filt = summarise(f"{ticker} -- FILTERED (steps 1-3 applied)", trades)

        ctrl = None
        if control and signals:
            picks = control_days(days, rows, len(signals))
            ctrl_trades = []
            for day in picks:
                try:
                    ctrl_trades.append(simulate(conn, ticker, day, by_day,
                                                days, budget, POLYGON_KEY,
                                                target_yield))
                except RuntimeError as exc:
                    print(f"  control stopped early: {exc}")
                    break
            ctrl = summarise(f"{ticker} -- CONTROL (no filter, same count,"
                             f" same span)", ctrl_trades)
        results[ticker] = (filt, ctrl)
        print(f"\n  requests used so far: {budget.used}/{budget.limit}")

    print(f"\n{'=' * 72}\nDID THE FILTER EARN ITS PLACE?\n{'=' * 72}")
    for ticker, (filt, ctrl) in results.items():
        if not filt or not ctrl:
            print(f"{ticker}: not comparable "
                  f"(filtered n={filt['n'] if filt else 0},"
                  f" control n={ctrl['n'] if ctrl else 0})")
            continue
        edge = filt["median"] - ctrl["median"]
        print(f"\n{ticker}  filtered n={filt['n']}  control n={ctrl['n']}")
        print(f"  median    {filt['median']:+.2f}%  vs  {ctrl['median']:+.2f}%"
              f"   -> {edge:+.2f}% to the filter")
        print(f"  worst     {filt['worst']:+.2f}%  vs  {ctrl['worst']:+.2f}%")
        print(f"  sum       {filt['sum']:+.2f}%  vs  {ctrl['sum']:+.2f}%")
        if filt["n"] < 10 or ctrl["n"] < 10:
            print(f"  !! fewer than ten trades on one side. This repo has"
                  f" already been burned once by a cell resting on n=2.")

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
    ap.add_argument("--no-control", action="store_true")
    ap.add_argument("--price-start", default=None)
    args = ap.parse_args()
    clouds = tuple(tuple(int(n) for n in p.split("/"))
                   for p in args.clouds.split(","))
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    return run(tickers, clouds, args.drop, args.budget,
               control=not args.no_control, target_yield=args.target_yield,
               price_start=args.price_start)


if __name__ == "__main__":
    sys.exit(main())
