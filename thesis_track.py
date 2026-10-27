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


# The XBRL tags each metric trigger reads. Several alternates per concept
# because filers do not all use the same tag, and a concept that silently
# resolved to nothing would report DORMANT when it means ABSENT.
CONCEPTS = {
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment",
              "PaymentsToAcquireProductiveAssets"],
    "revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax",
                "Revenues", "RevenueFromContractWithCustomerIncludingAssessedTax"],
    "inventory": ["InventoryNet"],
    "receivables": ["AccountsReceivableNetCurrent",
                    "AccountsReceivableNet"],
}

# A FLOW is measured over a span (capex, revenue); a STOCK is measured at an
# instant (inventory, receivables). They are not interchangeable and the
# distinction is not cosmetic: four quarters of a flow SUM to a year, while
# four quarters of a stock do not sum to anything at all.
#
# It matters for a second reason. A filer tags revenue for the quarter AND
# for the full year, and both rows carry the same period end. Reading them
# alike -- which this module did -- lets an annual row stand in for a
# quarter, so a trailing-four-quarter sum can quietly carry closer to seven
# quarters of revenue. The growth rate computed from it is then wrong in a
# way that still looks like a plausible percentage.
KIND = {"capex": "flow", "revenue": "flow",
        "inventory": "stock", "receivables": "stock"}
QUARTER_DAYS = (60, 120)       # a reported quarter, generously bounded


def _facts(cik, cache):
    """companyfacts for one issuer, fetched once per run."""
    import setup_pit
    if cik not in cache:
        cache[cik] = setup_pit.fetch_facts(cik)
    return cache[cik]


def _span_days(start, end):
    """Length of a reported period, or None for an instant."""
    if not start:
        return None
    try:
        return (date.fromisoformat(end) - date.fromisoformat(start)).days
    except ValueError:
        return None


def _series(payload, names, today, kind="flow"):
    """Dated values for the first tag that has any, point in time.

    Point in time means a fact counts from the date it was FILED, never the
    date its period ended. A quarter ending in June and filed in August was
    not knowable in July, and letting it be is the look-ahead that makes a
    backtest lie. Same discipline setup_pit enforces for the setup rule.

    A flow keeps only QUARTERLY rows and a stock keeps only instants, so an
    annual row can never stand in for a quarter. Without that filter the
    annual and quarterly facts for the same period end are indistinguishable
    here, the later-filed one wins, and a four-quarter sum silently carries
    more than four quarters.
    """
    facts = ((payload or {}).get("facts") or {}).get("us-gaap") or {}
    for name in names:
        unit_map = (facts.get(name) or {}).get("units") or {}
        rows = unit_map.get("USD") or []
        out = []
        for fact in rows:
            end, filed, val = (fact.get("end"), fact.get("filed"),
                               fact.get("val"))
            if not end or not filed or val is None:
                continue
            if filed > today:
                continue                      # not knowable yet
            span = _span_days(fact.get("start"), end)
            if kind == "flow":
                if span is None or not (
                        QUARTER_DAYS[0] <= span <= QUARTER_DAYS[1]):
                    continue                  # annual, or an instant
            elif span is not None:
                continue                      # a stock has no span
            out.append((end, filed, float(val), fact.get("fp"),
                        fact.get("form")))
        if out:
            # Latest filing wins for a given period end, so a restatement
            # supersedes rather than duplicating.
            best = {}
            for end, filed, val, fp, form in sorted(out, key=lambda r: r[1]):
                best[end] = (filed, val, fp, form)
            return name, sorted((e, v[1]) for e, v in best.items())
    return None, []


def _ttm_yoy(series, kind="flow"):
    """Year-over-year growth, computed the way the quantity allows.

    A flow is summed over four quarters and compared with the four before
    it. A stock is NOT summed -- four quarter-end inventory balances do not
    add up to a year of inventory -- so the latest balance is compared with
    the balance four quarters earlier.
    """
    need = 8 if kind == "flow" else 5
    if len(series) < need:
        return None, None, f"only {len(series)} periods, need {need}"
    vals = [v for _e, v in series]
    if kind == "flow":
        now, prior = sum(vals[-4:]), sum(vals[-8:-4])
    else:
        now, prior = vals[-1], vals[-5]
    if not prior:
        return now, None, "prior-year base is zero"
    return now, (now - prior) / abs(prior) * 100.0, None


def eval_capex(trigger, today, cache):
    """Combined trailing-four-quarter capex growth across the named CIKs."""
    ciks = trigger.get("ciks") or {}
    if not ciks:
        return STALE, None, {}, "no ciks named"
    import setup_pit
    detail, totals_now, totals_prior, failed = {}, 0.0, 0.0, []
    for name, cik in ciks.items():
        payload, outcome = _facts(int(cik), cache)
        if outcome != setup_pit.OK:
            failed.append(f"{name}:{outcome}")
            continue
        tag, series = _series(payload, CONCEPTS["capex"], today, "flow")
        if len(series) < 8:
            failed.append(f"{name}:thin({len(series)})")
            continue
        vals = [v for _e, v in series]
        totals_now += sum(vals[-4:])
        totals_prior += sum(vals[-8:-4])
        detail[name] = {"tag": tag, "ttm": round(sum(vals[-4:]) / 1e9, 2)}
    # Any issuer missing makes the COMBINED figure wrong, not merely smaller.
    if failed:
        return STALE, None, detail, (
            f"combined figure needs all {len(ciks)} issuers; missing "
            f"{', '.join(failed)}")
    if not totals_prior:
        return STALE, None, detail, "prior-year combined base is zero"
    growth = (totals_now - totals_prior) / abs(totals_prior) * 100.0
    threshold = _threshold(trigger["fires_when"])
    if threshold is None:
        return STALE, growth, detail, "could not parse fires_when"
    state = FIRED if growth < threshold else DORMANT
    detail["combined_ttm_bn"] = round(totals_now / 1e9, 1)
    return state, growth, detail, (
        f"combined TTM capex ${totals_now/1e9:.0f}bn, {growth:+.1f}% YoY")


def eval_divergence(trigger, today, cache, left, right, unit="pp"):
    """Growth of one concept minus growth of another, per issuer."""
    ciks = trigger.get("ciks") or {}
    if not ciks:
        return STALE, None, {}, "no ciks named"
    import setup_pit
    detail, worst, failed = {}, None, []
    for name, cik in ciks.items():
        payload, outcome = _facts(int(cik), cache)
        if outcome != setup_pit.OK:
            failed.append(f"{name}:{outcome}")
            continue
        _lt, ls = _series(payload, CONCEPTS[left], today, KIND[left])
        _rt, rs = _series(payload, CONCEPTS[right], today, KIND[right])
        _n1, lg, e1 = _ttm_yoy(ls, KIND[left])
        _n2, rg, e2 = _ttm_yoy(rs, KIND[right])
        if lg is None or rg is None:
            failed.append(f"{name}:{e1 or e2}")
            continue
        gap = lg - rg
        detail[name] = round(gap, 1)
        worst = gap if worst is None else max(worst, gap)
    if worst is None:
        return STALE, None, detail, (
            f"no issuer evaluable ({'; '.join(failed)})")
    threshold = _threshold(trigger["fires_when"])
    if threshold is None:
        return STALE, worst, detail, "could not parse fires_when"
    note = f"; could not read {', '.join(failed)}" if failed else ""
    state = FIRED if worst > threshold else DORMANT
    return state, worst, detail, (
        f"widest gap {worst:+.1f}{unit} "
        f"({max(detail, key=detail.get)}){note}")


def eval_dso(trigger, today, cache):
    """Days sales outstanding, and its year-over-year change IN DAYS.

    This trigger was pre-registered as 'dso_change_days > 15'. It was being
    evaluated as receivables growth minus revenue growth, in percentage
    points, compared against that same 15 -- two different quantities
    sharing a number. It fired on its first live run at '+48.8pp', which is
    not a count of days and never was.

    The threshold is not moved to match what the code computed. The
    pre-registration said days, so the code computes days: receivables
    divided by revenue per day, where revenue per day is the trailing four
    quarters over 365.
    """
    ciks = trigger.get("ciks") or {}
    if not ciks:
        return STALE, None, {}, "no ciks named"
    import setup_pit
    detail, worst, failed = {}, None, []
    for name, cik in ciks.items():
        payload, outcome = _facts(int(cik), cache)
        if outcome != setup_pit.OK:
            failed.append(f"{name}:{outcome}")
            continue
        _rt, ar = _series(payload, CONCEPTS["receivables"], today, "stock")
        _vt, rev = _series(payload, CONCEPTS["revenue"], today, "flow")
        if len(ar) < 5 or len(rev) < 8:
            failed.append(f"{name}:thin(ar={len(ar)},rev={len(rev)})")
            continue
        rv = [v for _e, v in rev]
        now_rev, prior_rev = sum(rv[-4:]), sum(rv[-8:-4])
        if not now_rev or not prior_rev:
            failed.append(f"{name}:zero-revenue-base")
            continue
        av = [v for _e, v in ar]
        dso_now = av[-1] / (now_rev / 365.0)
        dso_prior = av[-5] / (prior_rev / 365.0)
        change = dso_now - dso_prior
        detail[name] = {"dso": round(dso_now, 1),
                        "dso_prior": round(dso_prior, 1),
                        "change_days": round(change, 1)}
        worst = change if worst is None else max(worst, change)
    if worst is None:
        return STALE, None, detail, (
            f"no issuer evaluable ({'; '.join(failed)})")
    threshold = _threshold(trigger["fires_when"])
    if threshold is None:
        return STALE, worst, detail, "could not parse fires_when"
    note = f"; could not read {', '.join(failed)}" if failed else ""
    widest = max(detail, key=lambda k: detail[k]["change_days"])
    return (FIRED if worst > threshold else DORMANT), worst, detail, (
        f"widest DSO change {worst:+.1f} days ({widest}: "
        f"{detail[widest]['dso_prior']:.0f} to "
        f"{detail[widest]['dso']:.0f}){note}")


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
    "capex-growth-negative": ("facts", eval_capex),
    "capex-growth-decelerating": ("facts", eval_capex),
    "inventory-outgrows-revenue": (
        "facts", lambda t, today, cache: eval_divergence(
            t, today, cache, "inventory", "revenue")),
    "receivables-stretching": ("facts", eval_dso),
    "ai-complex-drawdown": ("price", eval_price_drawdown),
    "curve-inversion": ("gauge", eval_curve),
    "credit-stress-proxy": ("price", eval_ratio_drawdown),
    "insider-sale-cluster": ("db", eval_insider_cluster),
}


def run(conn, theses, prices=None, today=None):
    today = today or date.today().isoformat()
    conn.executescript(SCHEMA)
    facts_cache = {}          # one companyfacts fetch per issuer per run
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
                    elif kind == "facts":
                        state, value, detail, reason = fn(
                            trigger, today, facts_cache)
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

    # -- flows, stocks, and the annual row that pretends to be a quarter --
    import setup_pit

    def usd(rows):
        return {"units": {"USD": rows}}

    def q(end, val, filed, start):
        return {"start": start, "end": end, "val": val, "filed": filed,
                "form": "10-Q"}

    # Eight quarters ending each March/June/September/December, 100 each,
    # plus the ANNUAL row a 10-K files for the same December period end.
    ends = [("2024-03-31", "2024-01-01"), ("2024-06-30", "2024-04-01"),
            ("2024-09-30", "2024-07-01"), ("2024-12-31", "2024-10-01"),
            ("2025-03-31", "2025-01-01"), ("2025-06-30", "2025-04-01"),
            ("2025-09-30", "2025-07-01"), ("2025-12-31", "2025-10-01")]
    rev_rows = [q(e, 100.0, e, s) for e, s in ends]
    # Filed LATER than the quarter, so under the old "latest filing wins"
    # rule the annual row displaced the quarter it shares an end with.
    rev_rows += [{"start": "2024-01-01", "end": "2024-12-31", "val": 400.0,
                  "filed": "2025-02-01", "form": "10-K"},
                 {"start": "2025-01-01", "end": "2025-12-31", "val": 400.0,
                  "filed": "2026-02-01", "form": "10-K"}]
    # Receivables: a balance sheet instant, no start.
    ar_rows = [{"end": e, "val": 50.0 + 10 * i, "filed": e, "form": "10-Q"}
               for i, (e, _s) in enumerate(ends)]
    pay = {"facts": {"us-gaap": {
        "RevenueFromContractWithCustomerExcludingAssessedTax": usd(rev_rows),
        "AccountsReceivableNetCurrent": usd(ar_rows)}}}

    _t, flow = _series(pay, CONCEPTS["revenue"], "2026-10-27", "flow")
    ok("an annual row never stands in for a quarter",
       len(flow) == 8 and all(v == 100.0 for _e, v in flow))
    ttm, growth, _e = _ttm_yoy(flow, "flow")
    ok("four quarters of a flow sum to four quarters, not seven",
       ttm == 400.0 and growth == 0.0)

    _t, stock = _series(pay, CONCEPTS["receivables"], "2026-10-27", "stock")
    ok("a stock keeps its instants", len(stock) == 8)
    ok("a flow filter would have discarded them",
       _series(pay, CONCEPTS["receivables"], "2026-10-27", "flow")[1] == [])
    bal, sgrowth, _e = _ttm_yoy(stock, "stock")
    # 120 against 80 four quarters back: +50%, not a sum of balances.
    ok("a stock is compared, not summed",
       bal == 120.0 and round(sgrowth, 1) == 50.0)

    # -- DSO is measured in days, because days is what was registered -----
    cache = {7: (pay, setup_pit.OK)}
    t4 = {"id": "receivables-stretching", "ciks": {"ZZZ": 7},
          "fires_when": "dso_change_days > 15"}
    state, value, detail, why = eval_dso(t4, "2026-10-27", cache)
    # AR 120 on TTM revenue 400 is 109.5 days; 80 on 400 is 73.0. +36.5.
    ok("DSO is reported in days", round(value, 1) == 36.5)
    ok("and the reason says days, not points", "days" in why)
    ok("the detail carries both ends of the comparison",
       detail["ZZZ"]["dso_prior"] == 73.0 and detail["ZZZ"]["dso"] == 109.5)
    ok("a 36-day stretch clears a 15-day threshold", state == FIRED)

    t5 = dict(t4); t5["fires_when"] = "dso_change_days > 60"
    ok("and the same 36 days is DORMANT against 60",
       eval_dso(t5, "2026-10-27", cache)[0] == DORMANT)

    # An issuer that cannot be read is STALE, never a smaller number.
    bad = {11: ({}, setup_pit.OK)}
    t6 = dict(t4); t6["ciks"] = {"ZZZ": 11}
    state, _v, _d, why = eval_dso(t6, "2026-10-27", bad)
    ok("an unreadable issuer is STALE, not DORMANT", state == STALE)
    ok("and says which one and why", "ZZZ" in why and "thin" in why)

    # Point in time still holds: nothing filed after the as-of date counts.
    _t, early = _series(pay, CONCEPTS["revenue"], "2024-06-30", "flow")
    ok("a fact filed later is not knowable earlier",
       [e for e, _v in early] == ["2024-03-31", "2024-06-30"])

    print(f"thesis_track selftest: {checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
