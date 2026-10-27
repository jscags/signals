#!/usr/bin/env python3
"""Evaluate the pre-registered triggers in theses.json. Reads, never writes it.

Three states, and keeping them apart is the whole job:

    FIRED    the condition was evaluated and held
    DORMANT  the condition was evaluated and did not hold
    STALE    the condition could NOT be evaluated

STALE is not a worse DORMANT. It is a different sentence. "We looked and the
answer is no" and "we could not look" lead to opposite actions, and a tracker
that renders them alike is worse than no tracker, because it converts every
outage into reassurance. Every path that cannot produce an answer here
returns STALE with the reason attached, and the reason is stored.

The bar for FIRED is deliberately high: the metric must be computed, from
data that exists, against a threshold written down before the fact. Anything
short of that is STALE.
"""

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta

FIRED, DORMANT, STALE = "FIRED", "DORMANT", "STALE"

SCHEMA = """
-- One row per trigger per evaluation. Append-only history: a trigger that
-- fired in March and is dormant today must leave the March row behind, or
-- the record silently becomes "what is true now" rather than "what we saw".
CREATE TABLE IF NOT EXISTS thesis_state (
    thesis_id  TEXT NOT NULL,
    trigger_id TEXT NOT NULL,
    as_of      TEXT NOT NULL,
    state      TEXT NOT NULL,
    value      REAL,
    detail     TEXT,
    reason     TEXT,
    PRIMARY KEY (trigger_id, as_of)
);
CREATE INDEX IF NOT EXISTS idx_thesis_state_day ON thesis_state(as_of);
"""


def load(path="theses.json"):
    with open(path) as fh:
        return json.load(fh)


def eval_price_drawdown(trigger, prices, today):
    """Drawdown from a rolling high, for the named tickers."""
    import dips
    import wheel
    tickers = trigger.get("tickers") or []
    if not tickers:
        return STALE, None, {}, "no tickers named"
    worst, detail, missing = None, {}, []
    for ticker in tickers:
        bars = wheel.load_bars(prices, ticker)
        if len(bars) < dips.HIGH_LOOKBACK + 1:
            missing.append(ticker)
            continue
        days = [b[0] for b in bars]
        closes = [float(b[4]) for b in bars]
        # A corporate action is not a drawdown.
        gaps = [g for g in dips.suspect_gaps(
            days[-(dips.HIGH_LOOKBACK + 1):],
            closes[-(dips.HIGH_LOOKBACK + 1):]) if g["split_like"]]
        if gaps:
            missing.append(f"{ticker} (split-like gap {gaps[0]['day']})")
            continue
        state = dips.current(days, closes)
        if state is None:
            missing.append(ticker)
            continue
        detail[ticker] = round(state["drawdown"], 2)
        worst = state["drawdown"] if worst is None else max(
            worst, state["drawdown"])
    if worst is None:
        return STALE, None, detail, f"no usable prices for {tickers}"
    threshold = _threshold(trigger["fires_when"])
    if threshold is None:
        return STALE, worst, detail, "could not parse fires_when"
    note = f" ({len(missing)} unpriced: {', '.join(missing)})" if missing else ""
    if worst > threshold:
        return FIRED, worst, detail, f"worst drawdown {worst:.1f}%{note}"
    return DORMANT, worst, detail, f"worst drawdown {worst:.1f}%{note}"


def eval_curve(trigger, conn, today):
    """The 2s10s spread, straight from the stored gauge."""
    row = conn.execute(
        "SELECT value, as_of, stale FROM market_gauges WHERE name = ?",
        ("yield_spread",)).fetchone()
    if row is None:
        return STALE, None, {}, "yield_spread gauge has never been fetched"
    value, as_of, is_stale = row[0], row[1], row[2]
    if is_stale:
        return STALE, value, {"as_of": as_of}, (
            f"gauge last refresh failed; {as_of} value is not today's")
    threshold = _threshold(trigger["fires_when"])
    if threshold is None:
        return STALE, value, {}, "could not parse fires_when"
    state = FIRED if value < threshold else DORMANT
    return state, value, {"as_of": as_of}, f"{value:+.0f} bp as of {as_of}"


def eval_ratio_drawdown(trigger, prices, today):
    """Drawdown of a two-ticker ratio from its own rolling high."""
    import wheel
    pair = trigger.get("pair") or ["HYG", "TLT"]
    lookback = int(trigger.get("lookback", 60))
    series = {}
    for ticker in pair:
        bars = wheel.load_bars(prices, ticker)
        if not bars:
            return STALE, None, {}, f"no prices for {ticker}"
        series[ticker] = {b[0]: float(b[4]) for b in bars}
    days = sorted(set(series[pair[0]]) & set(series[pair[1]]))
    if len(days) < lookback + 1:
        return STALE, None, {}, (
            f"only {len(days)} overlapping sessions, need {lookback + 1}")
    ratio = [series[pair[0]][d] / series[pair[1]][d] for d in days
             if series[pair[1]][d]]
    window = ratio[-(lookback + 1):]
    high = max(window)
    draw = (high - ratio[-1]) / high * 100.0 if high else None
    if draw is None:
        return STALE, None, {}, "ratio high was zero"
    threshold = _threshold(trigger["fires_when"])
    if threshold is None:
        return STALE, draw, {}, "could not parse fires_when"
    detail = {"pair": "/".join(pair), "ratio": round(ratio[-1], 4),
              "high": round(high, 4), "as_of": days[-1]}
    state = FIRED if draw > threshold else DORMANT
    return state, draw, detail, f"{draw:.1f}% below its {lookback}d high"


def eval_insider_cluster(trigger, conn, today):
    """Distinct issuers with large sales in a rolling window.

    Reports STALE while the table is too shallow to have calibrated the
    threshold against. The number in fires_when would otherwise be a figure
    with nothing behind it, which is the failure pre-registration exists to
    prevent.
    """
    tickers = trigger.get("tickers") or []
    if not tickers:
        return STALE, None, {}, "no tickers named"
    need_days = int(trigger.get("requires_history_days", 0))
    row = conn.execute(
        f"SELECT MIN(txn_date), MAX(txn_date), COUNT(*) FROM insider_sales"
        f" WHERE ticker IN ({','.join('?' * len(tickers))})", tickers
    ).fetchone()
    if not row or not row[2]:
        return STALE, None, {}, "no Form 4 rows for any named issuer"
    lo, hi, n = row
    depth = (date.fromisoformat(hi) - date.fromisoformat(lo)).days
    if need_days and depth < need_days:
        return STALE, None, {"depth_days": depth, "rows": n}, (
            f"only {depth} days of history for these issuers ({lo}..{hi}); "
            f"the threshold requires {need_days} to have been calibrated")
    # Deduped: co-filer double counting is a known defect in this table.
    floor, need = _cluster_terms(trigger["fires_when"])
    if floor is None:
        return STALE, None, {}, "could not parse fires_when"
    since = (date.fromisoformat(today) - timedelta(days=90)).isoformat()
    rows = conn.execute(
        f"SELECT DISTINCT ticker, owner, txn_date, shares, price, value"
        f" FROM insider_sales WHERE COALESCE(suspect,0)=0 AND value >= ?"
        f" AND txn_date >= ? AND ticker IN ({','.join('?' * len(tickers))})",
        [floor, since] + tickers).fetchall()
    issuers = sorted({r[0] for r in rows})
    state = FIRED if len(issuers) >= need else DORMANT
    return state, float(len(issuers)), {"issuers": issuers}, (
        f"{len(issuers)} issuer(s) with sales >= ${floor/1e6:.0f}m in 90d")


def _threshold(expr):
    import re
    m = re.search(r"[<>]=?\s*(-?\d+(?:\.\d+)?)", expr or "")
    return float(m.group(1)) if m else None


def _cluster_terms(expr):
    import re
    need = re.search(r">=\s*(\d+)", expr or "")
    floor = re.search(r"value\s*>=\s*(\d+)", expr or "")
    return (float(floor.group(1)) if floor else None,
            int(need.group(1)) if need else None)


# Which evaluator handles which trigger. Unknown ids are STALE rather than
# silently skipped: a trigger nobody implemented must not read as DORMANT.
HANDLERS = {
    "ai-complex-drawdown": ("price", eval_price_drawdown),
    "curve-inversion": ("gauge", eval_curve),
    "credit-stress-proxy": ("price", eval_ratio_drawdown),
    "insider-sale-cluster": ("db", eval_insider_cluster),
}


def run(conn, theses, prices=None, today=None):
    today = today or date.today().isoformat()
    conn.executescript(SCHEMA)
    out = []
    for thesis in theses["theses"]:
        for trigger in thesis["triggers"]:
            tid = trigger["id"]
            handler = HANDLERS.get(tid)
            if handler is None:
                state, value, detail, reason = (
                    STALE, None, {},
                    "no evaluator implemented for this trigger yet")
            else:
                kind, fn = handler
                try:
                    if kind == "price":
                        if prices is None:
                            raise RuntimeError("no price database")
                        state, value, detail, reason = fn(
                            trigger, prices, today)
                    elif kind == "gauge" or kind == "db":
                        state, value, detail, reason = fn(
                            trigger, conn, today)
                except Exception as exc:
                    # An exception is STALE, never DORMANT. This is the line
                    # that stops an outage reading as reassurance.
                    state, value, detail, reason = (
                        STALE, None, {},
                        f"{type(exc).__name__}: {exc}")
            conn.execute(
                "INSERT OR REPLACE INTO thesis_state (thesis_id, trigger_id,"
                " as_of, state, value, detail, reason) VALUES (?,?,?,?,?,?,?)",
                (thesis["id"], tid, today, state, value,
                 json.dumps(detail), reason))
            out.append({"thesis": thesis["id"], "trigger": tid,
                        "state": state, "value": value, "reason": reason})
            print(f"  {state:8} {tid:24} {reason}")
    conn.commit()
    counts = {}
    for r in out:
        counts[r["state"]] = counts.get(r["state"], 0) + 1
    print(f"\n{counts.get(FIRED,0)} fired, {counts.get(DORMANT,0)} dormant, "
          f"{counts.get(STALE,0)} STALE (could not be evaluated)")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default="discovery.db")
    ap.add_argument("--price-db", default=None)
    ap.add_argument("--theses", default="theses.json")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    conn = sqlite3.connect(args.db)
    prices = None
    try:
        import market_data
        prices = market_data.connect(args.price_db)
    except Exception as exc:
        print(f"no price database ({exc}); price triggers will be STALE")
    print(f"evaluating {args.theses}\n")
    run(conn, load(args.theses), prices)
    return 0


def selftest():
    checks = 0

    def ok(label, cond):
        nonlocal checks
        assert cond, "FAILED: " + label
        checks += 1

    ok("threshold parses a bare number", _threshold("spread_bp < 0") == 0.0)
    ok("threshold parses a decimal", _threshold("x > 8.5") == 8.5)
    ok("threshold parses a negative", _threshold("yoy < -3") == -3.0)
    ok("an unparseable condition is None", _threshold("capex slows") is None)
    floor, need = _cluster_terms(
        "distinct_issuers >= 4 within 90 days with value >= 5000000")
    ok("cluster terms parse", floor == 5000000.0 and need == 4)

    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA)
    conn.executescript(
        "CREATE TABLE market_gauges (name TEXT PRIMARY KEY, value REAL,"
        " label TEXT, as_of TEXT, detail TEXT, fetched_at TEXT, stale INT);"
        "CREATE TABLE insider_sales (accession TEXT, issuer_cik INT,"
        " ticker TEXT, issuer TEXT, owner TEXT, owner_title TEXT,"
        " txn_date TEXT, shares REAL, price REAL, value REAL, suspect INT);")

    # A gauge that has never been fetched is STALE, not DORMANT.
    t = {"id": "curve-inversion", "fires_when": "spread_bp < 0"}
    state, _, _, why = eval_curve(t, conn, "2026-10-27")
    ok("an absent gauge is STALE", state == STALE)
    ok("and says it was never fetched", "never been fetched" in why)

    # A STALE gauge stays STALE even though it carries a number.
    conn.execute("INSERT INTO market_gauges VALUES"
                 " ('yield_spread', 38.0, 'normal', '2026-10-20', '{}',"
                 " '2026-10-27', 1)")
    state, value, _, why = eval_curve(t, conn, "2026-10-27")
    ok("a failed refresh is STALE despite having a value",
       state == STALE and value == 38.0)

    conn.execute("UPDATE market_gauges SET stale = 0")
    state, _, _, _ = eval_curve(t, conn, "2026-10-27")
    ok("a fresh positive spread is DORMANT", state == DORMANT)
    conn.execute("UPDATE market_gauges SET value = -12.0")
    state, _, _, _ = eval_curve(t, conn, "2026-10-27")
    ok("an inverted curve FIRES", state == FIRED)

    # Shallow history must be STALE, not a dormant verdict.
    t2 = {"id": "insider-sale-cluster", "tickers": ["AAA", "BBB"],
          "requires_history_days": 730,
          "fires_when": "distinct_issuers >= 4 within 90 days"
                        " with value >= 5000000"}
    conn.executemany(
        "INSERT INTO insider_sales (ticker, txn_date, value, suspect)"
        " VALUES (?,?,?,0)",
        [("AAA", "2026-09-01", 9e6), ("BBB", "2026-09-02", 9e6)])
    state, _, detail, why = eval_insider_cluster(t2, conn, "2026-10-27")
    ok("shallow history is STALE", state == STALE)
    ok("and names the depth", "only 1 days of history" in why)

    # With the requirement lifted it evaluates and is honestly DORMANT.
    t3 = dict(t2); t3["requires_history_days"] = 0
    state, value, detail, why = eval_insider_cluster(t3, conn, "2026-10-27")
    ok("with depth waived it evaluates", state == DORMANT and value == 2.0)
    ok("and lists the issuers", detail["issuers"] == ["AAA", "BBB"])

    # An unimplemented trigger must be STALE, never DORMANT.
    theses = {"theses": [{"id": "t", "triggers": [
        {"id": "not-implemented-yet", "type": "metric",
         "fires_when": "x > 1"}]}]}
    res = run(conn, theses, None, "2026-10-27")
    ok("an unimplemented trigger is STALE", res[0]["state"] == STALE)
    ok("and says so", "no evaluator" in res[0]["reason"])

    # A handler that raises must be STALE, never DORMANT.
    boom = {"theses": [{"id": "t2", "triggers": [
        {"id": "ai-complex-drawdown", "type": "price",
         "tickers": ["X"], "fires_when": "drawdown_pct > 20"}]}]}
    res = run(conn, boom, None, "2026-10-27")
    ok("a missing price database is STALE", res[0]["state"] == STALE)

    print(f"thesis_track selftest: {checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
