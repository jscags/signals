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

KNOWN DEVIATION FROM THE WORK ORDER
-----------------------------------
The work order says Lane D gets its own module, its own SQLite database and
its own workflow file. It has its own module and its own tables; it does NOT
have its own database. This is deliberate and it is written here rather than
left as a silent gap.

Two of the evaluators read tables this lane does not own. eval_insider_cluster
reads insider_sales, collected from Form 4 by the EDGAR lane, and eval_curve
reads market_gauges, collected with the dashboard's gauges. Splitting the
database means either ATTACHing across two files on every run -- which is a
shared database with extra steps and a second way to get the path wrong -- or
copying those tables into a second one, which creates two copies of the same
facts that can disagree. Neither is worth the separation it buys here.

What the separation was FOR is still honoured: Lane D is its own module with
its own schema, its own tables, and its own step in the workflow that cannot
take the collection run down with it. If the lane ever stops reading the
other lanes' tables, this becomes a cheap change and should be made.
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
    crossed = sum(1 for v in detail.values() if v > threshold)
    reason = (f"{_breadth(crossed, len(detail))}; "
              f"worst drawdown {worst:.1f}%{note}")
    detail = {"tickers": detail, "crossed": crossed,
              "evaluated": len(detail), "threshold": threshold}
    return (FIRED if worst > threshold else DORMANT), worst, detail, reason


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
# Nobody runs a year of receivables. Past this, the denominator is wrong.
DSO_SANE_DAYS = 365.0


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


def _quarters(rows):
    """Discrete quarters from rows that may be reported cumulatively.

    Cash-flow concepts are filed YEAR TO DATE: NVDA's capital expenditure
    arrives as spans of 90, 181, 272 and 363 days, all starting on the same
    fiscal-year day. Keeping only the ~90-day rows leaves one quarter per
    year, and four of those summed is a figure covering four years.

    So the cumulative rows are differenced back into quarters -- Q2 is the
    half year minus the first quarter, and so on -- and used where no
    discrete row exists. A derived quarter is kept only if the period it
    implies is really a quarter long; anything else is a gap in the filings
    and is left as one.

    `rows` are (start, end, filed, value), already filtered to what was
    knowable on the as-of date.
    """
    best = {}
    for start, end, filed, val in rows:
        if _span_days(start, end) is None:
            continue                          # an instant, not a flow
        key = (start, end)
        # Latest filing wins for a period, so a restatement supersedes.
        if key not in best or filed >= best[key][0]:
            best[key] = (filed, float(val))

    # Rows sharing a start are one year-to-date ladder; a row whose own
    # span is already a quarter is a rung that needs no arithmetic.
    by_start = {}
    for (start, end), (_filed, val) in best.items():
        by_start.setdefault(start, []).append((end, val))

    direct, derived = {}, {}
    for start, items in by_start.items():
        items.sort()
        prev_end, prev_val = start, 0.0
        for end, val in items:
            own = _span_days(start, end)
            step = _span_days(prev_end, end)
            if own is not None and QUARTER_DAYS[0] <= own <= QUARTER_DAYS[1]:
                direct[end] = val
            elif step is not None and QUARTER_DAYS[0] <= step <= QUARTER_DAYS[1]:
                derived[end] = val - prev_val
            prev_end, prev_val = end, val

    out = dict(derived)
    out.update(direct)                        # a filed quarter is believed
    return sorted(out.items())


def _series(payload, names, today, kind="flow"):
    """Dated values for the best tag among `names`, point in time.

    Point in time means a fact counts from the date it was FILED, never the
    date its period ended. A quarter ending in June and filed in August was
    not knowable in July, and letting it be is the look-ahead that makes a
    backtest lie. Same discipline setup_pit enforces for the setup rule.

    BEST, not first. The rule used to be "the first tag that has any rows",
    which is wrong in a way that is invisible from the code: NVDA stopped
    tagging RevenueFromContractWithCustomerExcludingAssessedTax after 2020
    but the old rows are still in companyfacts, so that tag always had
    some, the alternates were never reached, and 2026 receivables were
    divided by 2019 revenue. That produced a days-sales-outstanding of
    2,108 and published it as a fired trigger. A tag a filer abandoned
    still answers; it just answers about the wrong decade. So every tag is
    read and the one reaching furthest forward wins.
    """
    facts = ((payload or {}).get("facts") or {}).get("us-gaap") or {}
    best_name, best = None, []
    for name in names:
        rows = ((facts.get(name) or {}).get("units") or {}).get("USD") or []
        keep = []
        for fact in rows:
            end, filed, val = (fact.get("end"), fact.get("filed"),
                               fact.get("val"))
            if not end or not filed or val is None:
                continue
            if filed > today:
                continue                      # not knowable yet
            keep.append((fact.get("start"), end, filed, val))
        if kind == "flow":
            series = _quarters(keep)
        else:
            # A stock is an instant and has no span. Latest filing wins.
            best_by_end = {}
            for start, end, filed, val in keep:
                if start is not None:
                    continue
                if end not in best_by_end or filed >= best_by_end[end][0]:
                    best_by_end[end] = (filed, float(val))
            series = sorted((e, v) for e, (_f, v) in best_by_end.items())
        if not series:
            continue
        # Furthest-forward wins; length breaks a tie.
        if not best or (series[-1][0], len(series)) > (best[-1][0], len(best)):
            best_name, best = name, series
    return best_name, best


def _breadth(crossed, evaluated):
    """How many of the named issuers crossed, said plainly.

    These triggers fire when the WIDEST gap clears the threshold, which is
    how they were pre-registered and is not changed here. But the reason
    string used to read only "widest gap +58.8pp (AVGO)", which sounds like
    a statement about the four named semiconductor issuers when it is a
    statement about one of them. One of four and four of four are different
    findings and the page should not render them identically.
    """
    return f"{crossed} of {evaluated} issuer(s) crossed"


def _not_consecutive(series, n=8):
    """Why the last n periods are not adjacent quarters, or None.

    Eight rows that survived the duration filter are not necessarily
    adjacent. A filer who reports year-to-date spans leaves only its first
    quarters behind, and summing four of those -- one per year -- produces
    a confident number covering four separate years. The first run after
    the duration filter went in halved combined capex and doubled its
    growth rate, which is exactly what that looks like from outside.
    """
    ends = [e for e, _v in series[-n:]]
    gaps = [(date.fromisoformat(b) - date.fromisoformat(a)).days
            for a, b in zip(ends, ends[1:])]
    if any(not (QUARTER_DAYS[0] <= g <= QUARTER_DAYS[1]) for g in gaps):
        return (f"periods are not consecutive quarters "
                f"({ends[0]}..{ends[-1]}, gaps {sorted(set(gaps))})")
    return None


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
        # The eight periods must be CONSECUTIVE quarters. Eight rows that
        # survived the duration filter are not necessarily adjacent: a
        # filer who reports year-to-date spans leaves only its first
        # quarters behind, and summing four of those -- one per year --
        # produces a confident number covering four separate years. The
        # first run after the filter went in halved combined capex and
        # doubled its growth rate, which is what that looks like.
        gap = _not_consecutive(series)
        if gap:
            return None, None, gap
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
        gap = _not_consecutive(series)
        if gap:
            failed.append(f"{name}:{gap}")
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
    widest = max(detail, key=detail.get)
    crossed = sum(1 for v in detail.values() if v > threshold)
    detail = {"issuers": detail, "crossed": crossed,
              "evaluated": len(detail), "threshold": threshold}
    return state, worst, detail, (
        f"{_breadth(crossed, detail['evaluated'])}; "
        f"widest gap {worst:+.1f}{unit} ({widest}){note}")


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
        gap = _not_consecutive(rev)
        if gap:
            failed.append(f"{name}:revenue {gap}")
            continue
        rv = [v for _e, v in rev]
        now_rev, prior_rev = sum(rv[-4:]), sum(rv[-8:-4])
        if not now_rev or not prior_rev:
            failed.append(f"{name}:zero-revenue-base")
            continue
        av = [v for _e, v in ar]
        dso_now = av[-1] / (now_rev / 365.0)
        dso_prior = av[-5] / (prior_rev / 365.0)
        # A company does not wait two years to collect a bill. A DSO
        # outside any plausible range does not mean the issuer is in
        # trouble, it means the revenue denominator is wrong -- and the
        # first live run of this evaluator produced 2,108 days for NVDA,
        # which it reported as a FIRED trigger. A number this module
        # cannot stand behind is STALE, not a finding.
        if not (0 < dso_now <= DSO_SANE_DAYS
                and 0 < dso_prior <= DSO_SANE_DAYS):
            failed.append(
                f"{name}:implausible-dso({dso_prior:.0f}->{dso_now:.0f}d)")
            continue
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
    crossed = sum(1 for d in detail.values()
                  if d["change_days"] > threshold)
    reason = (f"{_breadth(crossed, len(detail))}; "
              f"widest DSO change {worst:+.1f} days ({widest}: "
              f"{detail[widest]['dso_prior']:.0f} to "
              f"{detail[widest]['dso']:.0f}){note}")
    detail = {"issuers": detail, "crossed": crossed,
              "evaluated": len(detail), "threshold": threshold}
    return (FIRED if worst > threshold else DORMANT), worst, detail, reason


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


def asof_months(n, today=None):
    """n monthly as-of dates, oldest first, ending today."""
    end = date.fromisoformat(today) if today else date.today()
    out = []
    y, m = end.year, end.month
    for _ in range(n):
        out.append(date(y, m, min(end.day, 28)).isoformat())
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return sorted(out)


def episodes(flags):
    """Runs of consecutive True, counted once each.

    A threshold that is over the line for nine months running is ONE
    episode, not nine. Counting months would make every threshold look
    better the longer the condition persisted, which is the inflation this
    repo already walked into once with dip depths.
    """
    n, prev = 0, False
    for f in flags:
        if f and not prev:
            n += 1
        prev = f
    return n


# What each metric trigger is measured in, for the calibration table.
SWEEPS = {
    "inventory-outgrows-revenue": ([0, 5, 10, 15, 20, 25, 30, 40, 50, 60],
                                   "pp"),
    "receivables-stretching": ([0, 5, 10, 15, 20, 25, 30, 40], "days"),
    "capex-growth-negative": ([-20, -10, -5, 0, 5, 10, 15, 25], "%"),
    "capex-growth-decelerating": ([-20, -10, -5, 0, 5, 10, 15, 25], "%"),
}


def calibrate(path="theses.json", months=36, today=None):
    """What each threshold WOULD have done, month by month, point in time.

    Reads nothing forward: every as-of date sees only facts FILED by then,
    the same discipline the live evaluator uses. Writes nothing -- not the
    thesis file, not the database. This exists so a threshold can be moved
    deliberately and the move recorded, rather than nudged until it fires.

    What to look for is the sample collapsing out from under the number,
    exactly as on the dip slider: a threshold with one episode behind it is
    a number with nothing behind it, however neat the table looks.
    """
    spec = load(path)
    cache, dates = {}, asof_months(months, today)
    print(f"calibrating over {len(dates)} monthly as-of dates, "
          f"{dates[0]} .. {dates[-1]}")
    print("point in time: each date sees only facts filed by then.\n")

    for thesis in spec.get("theses", []):
        for trigger in thesis.get("triggers", []):
            tid = trigger["id"]
            sweep = SWEEPS.get(tid)
            handler = HANDLERS.get(tid)
            if not sweep or not handler or handler[0] != "facts":
                continue
            grid, unit = sweep
            fn = handler[1]
            committed = _threshold(trigger.get("fires_when", ""))
            vals, stale = [], 0
            for as_of in dates:
                try:
                    _s, value, _d, _r = fn(trigger, as_of, cache)
                except Exception:
                    value = None
                if value is None:
                    stale += 1
                vals.append(value)

            seen = [v for v in vals if v is not None]
            print(f"== {tid}  (committed: {trigger.get('fires_when')})")
            if not seen:
                print("   nothing evaluable over this window -- "
                      "no basis for any threshold\n")
                continue
            print(f"   {len(seen)} of {len(dates)} dates evaluable"
                  f"{f', {stale} not' if stale else ''}; "
                  f"range {min(seen):+.1f} .. {max(seen):+.1f}{unit}")
            print(f"   {'threshold':>12}  {'months':>6}  {'episodes':>8}   "
                  f"first fire")
            for t in grid:
                # capex fires BELOW its threshold; the others above.
                below = tid.startswith("capex")
                flags = [v is not None and (v < t if below else v > t)
                         for v in vals]
                first = next((d for d, f in zip(dates, flags) if f), "never")
                mark = "  <-- committed" if (
                    committed is not None and abs(t - committed) < 1e-9) else ""
                print(f"   {t:>9}{unit:>3}  {sum(flags):>6}  "
                      f"{episodes(flags):>8}   {first}{mark}")
            print()
    return 0


def price_tickers(spec):
    """Every ticker a price trigger needs, so they can be fetched.

    Read from the thesis file rather than hard-coded, so adding a price
    trigger does not silently add a trigger whose data nobody collects.
    """
    out = set()
    for thesis in (spec or {}).get("theses", []):
        for trigger in thesis.get("triggers", []):
            if trigger.get("type") != "price":
                continue
            out.update(trigger.get("tickers") or [])
            out.update(trigger.get("pair") or [])
    return out


def explain(cik, today=None):
    """Dump every USD row for one issuer's concepts, durations included.

    A diagnostic, not part of the pipeline. The evaluators disagreed with
    reality by two orders of magnitude and no amount of reading the code
    settles why: the only way to find out which rows a filer actually
    publishes is to look at them. data.sec.gov is not reachable from every
    environment, so this exists to be run where it is.
    """
    import setup_pit
    today = today or date.today().isoformat()
    payload, outcome = setup_pit.fetch_facts(int(cik))
    print(f"CIK {cik}: fetch {outcome}")
    if outcome != setup_pit.OK:
        return 1
    facts = ((payload or {}).get("facts") or {}).get("us-gaap") or {}
    for concept, tags in CONCEPTS.items():
        print(f"\n== {concept} ({KIND[concept]})")
        for tag in tags:
            rows = (facts.get(tag) or {}).get("units", {}).get("USD") or []
            if not rows:
                print(f"  {tag}: absent")
                continue
            spans = {}
            for r in rows:
                spans[_span_days(r.get("start"), r.get("end"))] = \
                    spans.get(_span_days(r.get("start"), r.get("end")), 0) + 1
            print(f"  {tag}: {len(rows)} row(s); spans "
                  f"{sorted(spans.items(), key=lambda kv: (kv[0] is not None, kv[0]))}")
            for r in sorted(rows, key=lambda r: (r.get("end") or ""))[-6:]:
                print(f"     start={r.get('start')} end={r.get('end')} "
                      f"span={_span_days(r.get('start'), r.get('end'))} "
                      f"filed={r.get('filed')} form={r.get('form')} "
                      f"fp={r.get('fp')} val={r.get('val'):,.0f}")
            _t, kept = _series(payload, [tag], today, KIND[concept])
            print(f"     -> _series keeps {len(kept)}: {kept[-5:]}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default="discovery.db")
    ap.add_argument("--price-db", default=None)
    ap.add_argument("--theses", default="theses.json")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--explain", type=int, default=None, metavar="CIK",
                    help="dump one issuer's XBRL rows and stop")
    ap.add_argument("--calibrate", type=int, default=None, metavar="MONTHS",
                    help="what each threshold would have done; writes nothing")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    if args.explain:
        return explain(args.explain)
    if args.calibrate:
        return calibrate(args.theses, args.calibrate)
    conn = sqlite3.connect(args.db)
    spec = load(args.theses)
    prices = None
    try:
        import market_data
        prices = market_data.connect(args.price_db)
        # The price triggers name tickers nothing else in this pipeline
        # fetches -- the setups page only collects the leveraged ETFs. For
        # three runs that made credit-stress-proxy and ai-complex-drawdown
        # report STALE, which was true but for a plumbing reason rather
        # than anything about the market. A trigger whose data was never
        # requested is not evidence of anything.
        wanted = price_tickers(spec)
        if wanted:
            market_data.refresh(prices, sorted(wanted))
    except Exception as exc:
        print(f"no price database ({exc}); price triggers will be STALE")
    print(f"evaluating {args.theses}\n")
    run(conn, spec, prices)
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
       detail["issuers"]["ZZZ"]["dso_prior"] == 73.0
       and detail["issuers"]["ZZZ"]["dso"] == 109.5)
    ok("a 36-day stretch clears a 15-day threshold", state == FIRED)
    ok("and the reason says how many issuers crossed, not just the widest",
       "1 of 1 issuer(s) crossed" in why)

    # The case this exists for: one name out of four carrying the trigger.
    # It still FIRES -- that is how it was registered and the rule is not
    # changed -- but the page must not read as a statement about four.
    quiet = {"facts": {"us-gaap": {
        "RevenueFromContractWithCustomerExcludingAssessedTax": usd(rev_rows),
        "AccountsReceivableNetCurrent": usd(
            [{"end": e, "val": 80.0, "filed": e, "form": "10-Q"}
             for e, _s in ends])}}}          # flat AR: no stretch at all
    wide = {"id": "receivables-stretching",
            "ciks": {"LOUD": 7, "A": 21, "B": 22, "C": 23},
            "fires_when": "dso_change_days > 15"}
    state, _v, detail, why = eval_dso(wide, "2026-10-27", {
        7: (pay, setup_pit.OK), 21: (quiet, setup_pit.OK),
        22: (quiet, setup_pit.OK), 23: (quiet, setup_pit.OK)})
    ok("one issuer out of four still fires, as registered", state == FIRED)
    ok("but the reason says one of four, not just the widest",
       "1 of 4 issuer(s) crossed" in why)
    ok("and the count is stored, not only printed",
       detail["crossed"] == 1 and detail["evaluated"] == 4)

    t5 = dict(t4); t5["fires_when"] = "dso_change_days > 60"
    ok("and the same 36 days is DORMANT against 60",
       eval_dso(t5, "2026-10-27", cache)[0] == DORMANT)

    # An issuer that cannot be read is STALE, never a smaller number.
    bad = {11: ({}, setup_pit.OK)}
    t6 = dict(t4); t6["ciks"] = {"ZZZ": 11}
    state, _v, _d, why = eval_dso(t6, "2026-10-27", bad)
    ok("an unreadable issuer is STALE, not DORMANT", state == STALE)
    ok("and says which one and why", "ZZZ" in why and "thin" in why)

    # -- four first-quarters are not a year ------------------------------
    # What a year-to-date filer leaves behind once the duration filter has
    # run: one quarter per year, eight years apart-ish, summing to a
    # perfectly plausible-looking number that covers four separate years.
    yearly = [(f"{y}-03-31", 50.0) for y in range(2018, 2026)]
    ok("non-consecutive quarters are refused",
       _not_consecutive(yearly) is not None)
    ok("and the refusal shows the gaps", "gaps" in _not_consecutive(yearly))
    _n, g, err = _ttm_yoy(yearly, "flow")
    ok("so no growth rate is produced from them", g is None and err)
    ok("consecutive quarters pass", _not_consecutive(flow) is None)

    sparse = {"facts": {"us-gaap": {
        "RevenueFromContractWithCustomerExcludingAssessedTax": usd(
            [q(e, 50.0, e, f"{e[:4]}-01-01") for e, _v in yearly]),
        "AccountsReceivableNetCurrent": usd(ar_rows)}}}
    t7 = {"id": "receivables-stretching", "ciks": {"YTD": 9},
          "fires_when": "dso_change_days > 15"}
    state, _v, _d, why = eval_dso(t7, "2026-10-27", {9: (sparse, setup_pit.OK)})
    ok("a year-to-date filer is STALE, not a finding", state == STALE)
    ok("and the reason names the gap", "consecutive" in why)

    # -- an impossible DSO is a broken denominator, not a discovery ------
    # NVDA came back at 2,108 days on the first live run and was published
    # as FIRED. Nobody waits five years to collect a bill.
    huge_ar = [{"end": e, "val": 1e12, "filed": e, "form": "10-Q"}
               for e, _s in ends]
    absurd = {"facts": {"us-gaap": {
        "RevenueFromContractWithCustomerExcludingAssessedTax": usd(rev_rows),
        "AccountsReceivableNetCurrent": usd(huge_ar)}}}
    t8 = {"id": "receivables-stretching", "ciks": {"BAD": 13},
          "fires_when": "dso_change_days > 15"}
    state, _v, _d, why = eval_dso(
        t8, "2026-10-27", {13: (absurd, setup_pit.OK)})
    ok("an implausible DSO is STALE, never FIRED", state == STALE)
    ok("and says the denominator is what is wrong",
       "implausible-dso" in why)

    # -- year-to-date cash flow, differenced back into quarters ----------
    # NVDA's capital expenditure, exactly as filed: one fiscal year's rows
    # all start on the same day and run 90, 181, 272 and 363 days.
    ytd = [("2025-01-27", "2025-04-27", "2025-05-28", 1227e6),
           ("2025-01-27", "2025-07-27", "2025-08-27", 3122e6),
           ("2025-01-27", "2025-10-26", "2025-11-19", 4758e6),
           ("2025-01-27", "2026-01-25", "2026-02-25", 6042e6)]
    qs = dict(_quarters(ytd))
    ok("a year-to-date first quarter is itself",
       round(qs["2025-04-27"] / 1e6) == 1227)
    ok("the second quarter is the half year minus the first",
       round(qs["2025-07-27"] / 1e6) == 1895)
    ok("the third is the difference again",
       round(qs["2025-10-26"] / 1e6) == 1636)
    ok("and the fourth comes out of the full year",
       round(qs["2026-01-25"] / 1e6) == 1284)
    ok("four derived quarters sum back to the filed year",
       round(sum(qs.values()) / 1e6) == 6042)
    ok("and they are consecutive",
       _not_consecutive(sorted(qs.items()), n=4) is None)

    # A filed quarter is believed over a derived one.
    mixed = ytd + [("2025-04-28", "2025-07-27", "2025-08-27", 9e9)]
    ok("a discrete filing beats a derived quarter",
       dict(_quarters(mixed))["2025-07-27"] == 9e9)

    # A year with a missing interim leaves a gap rather than inventing one.
    holey = [ytd[0], ytd[3]]
    ok("a jump from Q1 to the full year derives nothing",
       "2026-01-25" not in dict(_quarters(holey)))

    # -- a tag the filer abandoned must not win ---------------------------
    # This is what produced the 2,108-day DSO: the first tag still had
    # rows, they just stopped in 2020.
    abandoned = {"facts": {"us-gaap": {
        "RevenueFromContractWithCustomerExcludingAssessedTax": usd(
            [q(e, 3e9, e, s) for e, s in
             [("2019-10-27", "2019-07-29"), ("2020-01-26", "2019-10-28")]]),
        "Revenues": usd(rev_rows)}}}
    tag, picked = _series(abandoned, CONCEPTS["revenue"], "2026-10-27",
                          "flow")
    ok("the tag reaching furthest forward wins, not the first listed",
       tag == "Revenues")
    ok("so the series is current, not a decade old",
       picked[-1][0] == "2025-12-31")

    # -- calibration counts episodes, not months --------------------------
    ok("a run of consecutive months is one episode",
       episodes([False, True, True, True, False]) == 1)
    ok("two separated runs are two", episodes([True, False, True]) == 2)
    ok("nothing over the line is no episodes",
       episodes([False, False]) == 0)
    ok("a run that is still open at the end still counts",
       episodes([False, True, True]) == 1)
    d = asof_months(14, "2026-03-31")
    ok("as-of dates run backwards a month at a time, oldest first",
       len(d) == 14 and d[0] == "2025-02-28" and d[-1] == "2026-03-28")
    ok("and cross the year boundary correctly", "2025-12-28" in d)

    # -- the price triggers must declare what to fetch --------------------
    spec = load("theses.json")
    want = price_tickers(spec)
    ok("the price triggers name the tickers they need",
       {"HYG", "TLT", "NVDA", "SMH", "SOXX"} <= want)
    ok("and a gauge trigger contributes none",
       price_tickers({"theses": [{"triggers": [
           {"type": "price", "id": "curve-inversion"}]}]}) == set())
    ok("a metric trigger's issuers are not fetched as prices",
       price_tickers({"theses": [{"triggers": [
           {"type": "metric", "tickers": ["XXX"]}]}]}) == set())

    # Point in time still holds: nothing filed after the as-of date counts.
    _t, early = _series(pay, CONCEPTS["revenue"], "2024-06-30", "flow")
    ok("a fact filed later is not knowable earlier",
       [e for e, _v in early] == ["2024-03-31", "2024-06-30"])

    print(f"thesis_track selftest: {checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
