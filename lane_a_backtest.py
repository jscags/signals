"""Lane A, Phase 1: does the metric distinguish anything, or does it fire on everyone?

The thesis under test is that a company whose contract liabilities outgrow its
recognised revenue for several quarters running is shifting to recurring
contracts, and that the income statement understates it while that happens.
Axon 2013-2016 is the reference case. The thesis is UNVALIDATED, and this file
exists to test it rather than to act on it.

Two measurements, and the second is the one that matters:

  1.1  Axon alone. Establishes the shape of the spread and whether the series
       survives the ASC 606 tag change intact.
  1.2  Forty companies drawn from the market as it stood at the end of 2015,
       at Axon's scale, with no survivorship filter. A rule that fires on
       everything fires on Axon too, so firing on Axon proves nothing without
       this.

No boolean rule is implemented. The threshold is an empirical question and
deciding it before seeing the distribution is how a rule gets fitted to the one
answer already known.

Standalone by construction: nothing here imports the collector, writes to its
database, or touches its dashboard.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date

# ---------------------------------------------------------------- constants

# The SEC rejects requests without a declared contact. Same string the existing
# workflow uses; overridable so a different operator can identify themselves.
USER_AGENT = (os.environ.get("EDGAR_USER_AGENT")
              or "edgar-discovery 52y9fp5njf@privaterelay.appleid.com")

# The published ceiling is 10 requests a second. Eight leaves headroom for the
# fact that the limit is enforced on their clock, not ours.
MIN_REQUEST_INTERVAL = 1.0 / 8

AXON_CIK = 1069183

# Post-ASC 606 first, so a period reported under both spellings is read under
# the newer one. "First with data FOR THE PERIOD" rather than first with data
# at all -- that is what splices a series across the 2018 rename instead of
# truncating it at the switch.
CL_TAGS = (
    "ContractWithCustomerLiabilityCurrent",
    "DeferredRevenueCurrent",
    "DeferredRevenueCurrentAndNoncurrent",
    "ContractWithCustomerLiability",
)
REV_TAGS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
)

# A quarter is about 91 days. Anything outside this is year-to-date or annual,
# and differencing cumulative figures introduces errors that look like signal.
QUARTER_MIN_DAYS = 80
QUARTER_MAX_DAYS = 100

WINDOW_START = date(2012, 1, 1)
WINDOW_END = date(2018, 12, 31)

# The control set: Axon's scale at the end of 2015, when its revenue was ~$198m.
CONTROL_MIN_REVENUE = 50_000_000
CONTROL_MAX_REVENUE = 1_000_000_000
CONTROL_SAMPLE = 40
CONTROL_PERIOD_INSTANT = "CY2015Q4I"
CONTROL_PERIOD_ANNUAL = "CY2015"

_last_request = 0.0
STATS = {"ok": 0, "missing": 0, "failed": 0}
FAILURES = []


# ---------------------------------------------------------------- http


class FetchFailed(RuntimeError):
    """The request did not come back. Distinct from "this tag has no data"."""


def fetch_json(url, label=""):
    """One rate-limited GET. Returns None when the SEC says 404.

    A 404 is an answer -- an issuer that never used a tag has no document at
    that address -- and is counted separately from a failure. Everything else
    raises with its status code visible, because the failure this project has
    twice paid for is a zero-result run that looked like a zero-signal run.
    """
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request = time.time()

    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            STATS["missing"] += 1
            return None
        STATS["failed"] += 1
        FAILURES.append(f"HTTP {exc.code}  {label or url}")
        raise FetchFailed(f"HTTP {exc.code} for {url}") from exc
    except OSError as exc:
        STATS["failed"] += 1
        FAILURES.append(f"transport  {label or url}  ({exc})")
        raise FetchFailed(f"could not reach {url}: {exc}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        STATS["failed"] += 1
        FAILURES.append(f"unreadable JSON  {label or url}")
        raise FetchFailed(f"unreadable JSON from {url}") from exc
    STATS["ok"] += 1
    return payload


def concept(cik, tag):
    """Every USD fact an issuer has filed for one concept, or None if never."""
    url = ("https://data.sec.gov/api/xbrl/companyconcept/"
           "CIK{0:010d}/us-gaap/{1}.json".format(cik, tag))
    try:
        payload = fetch_json(url, label="CIK {0} {1}".format(cik, tag))
    except FetchFailed as exc:
        print("    ! {0}".format(exc), file=sys.stderr)
        return None
    if not payload:
        return None
    facts = []
    for unit, rows in (payload.get("units") or {}).items():
        if unit == "USD":
            facts.extend(rows)
    return facts


def frame(tag, period):
    """One concept across every filer for one period."""
    url = ("https://data.sec.gov/api/xbrl/frames/us-gaap/"
           "{0}/USD/{1}.json".format(tag, period))
    payload = fetch_json(url, label="frame {0} {1}".format(tag, period))
    return (payload or {}).get("data") or []


# ---------------------------------------------------------------- parsing


def as_date(text):
    try:
        return date.fromisoformat(str(text)[:10])
    except (TypeError, ValueError):
        return None


def dedupe(facts):
    """One record per (start, end), keeping the one filed latest.

    The same quarter is filed repeatedly: in its own 10-Q, again as a
    comparative in the next year's, and again in any amendment. They are one
    fact, and the newest filing carries any restatement.
    """
    best = {}
    for fact in facts or ():
        if fact.get("val") is None:
            continue
        key = (fact.get("start"), fact.get("end"))
        prior = best.get(key)
        if prior is None or (fact.get("filed") or "") > (prior.get("filed") or ""):
            best[key] = fact
    return list(best.values())


def quarter_of(when):
    """The calendar quarter a period end falls in, as a sortable key.

    Calendar rather than fiscal, deliberately. An off-calendar filer's Q1
    ending 31 January lands in CY Q1 and is compared against the same filer's
    previous 31 January, so the year-over-year pairing stays within one
    company's own calendar even when that calendar is unusual.
    """
    return (when.year, (when.month - 1) // 3 + 1)


def prior_year(key):
    return (key[0] - 1, key[1])


def instant_series(facts):
    """Dated balances: an `end` and no `start`. Latest end wins its quarter."""
    out = {}
    for fact in dedupe(facts):
        if fact.get("start") or not fact.get("end"):
            continue
        when = as_date(fact["end"])
        if not when:
            continue
        key = quarter_of(when)
        if key not in out or when > out[key][0]:
            out[key] = (when, float(fact["val"]))
    return out


def quarterly_series(facts):
    """Dated quarterly flows, with the year-to-date periods thrown away.

    Returns (series, dropped). The count is reported rather than swallowed:
    a filer that reports only cumulatively drops out of this metric entirely,
    and that is a coverage limitation worth stating, not a silent zero.
    """
    out = {}
    dropped = 0
    for fact in dedupe(facts):
        start, end = as_date(fact.get("start")), as_date(fact.get("end"))
        if not start or not end:
            continue
        span = (end - start).days
        if not QUARTER_MIN_DAYS <= span <= QUARTER_MAX_DAYS:
            dropped += 1
            continue
        key = quarter_of(end)
        if key not in out or end > out[key][0]:
            out[key] = (end, float(fact["val"]))
    return out, dropped


def splice(cik, tags, instant):
    """Read every tag, then take each quarter from the first tag that has it.

    Returns (series, provenance, per_tag, dropped). `provenance` names the tag
    each quarter came from, which is the only way to see the ASC 606 switch
    rather than infer it from a step in the level.
    """
    per_tag = {}
    dropped = 0
    for tag in tags:
        facts = concept(cik, tag)
        if not facts:
            continue
        if instant:
            per_tag[tag] = instant_series(facts)
        else:
            series, lost = quarterly_series(facts)
            per_tag[tag] = series
            dropped += lost

    series, provenance = {}, {}
    for tag in tags:                      # preference order, newest spelling first
        for key, value in (per_tag.get(tag) or {}).items():
            if key not in series:
                series[key] = value
                provenance[key] = tag
    return series, provenance, per_tag, dropped


def tag_switches(provenance, per_tag):
    """Every quarter where the source tag changed, with both sides shown.

    A switch is reported, never smoothed. Where the outgoing tag also carries
    a value at the switch quarter the two are printed side by side, because a
    step in the level at the ASC 606 boundary is a real discontinuity in what
    was measured and interpolating across it would invent a trend.
    """
    switches = []
    keys = sorted(provenance)
    for earlier, later in zip(keys, keys[1:]):
        before, after = provenance[earlier], provenance[later]
        if before == after:
            continue
        old_at_new = (per_tag.get(before) or {}).get(later)
        new_value = (per_tag.get(after) or {}).get(later)
        overlap = None
        if old_at_new and new_value and old_at_new[1]:
            step = (new_value[1] - old_at_new[1]) / abs(old_at_new[1]) * 100.0
            overlap = (old_at_new[1], new_value[1], step)
        switches.append({
            "quarter": later, "from_tag": before, "to_tag": after,
            "overlap": overlap,
        })
    return switches


# ---------------------------------------------------------------- the metric


def spread_series(cl, rev):
    """Year-over-year growth of each, and the gap between them, per quarter.

    Each side is compared against its own value four quarters earlier -- a
    balance against a balance, a flow against a flow. Comparing the two to each
    other directly would be comparing a stock to a rate.
    """
    rows = []
    for key in sorted(set(cl) & set(rev)):
        back = prior_year(key)
        if back not in cl or back not in rev:
            continue
        cl_now, cl_then = cl[key][1], cl[back][1]
        rev_now, rev_then = rev[key][1], rev[back][1]
        if cl_then <= 0 or rev_then <= 0:
            continue
        cl_yoy = (cl_now - cl_then) / cl_then * 100.0
        rev_yoy = (rev_now - rev_then) / rev_then * 100.0
        rows.append({
            "quarter": key,
            "cl": cl_now,
            "rev": rev_now,
            "cl_yoy": cl_yoy,
            "rev_yoy": rev_yoy,
            "spread": cl_yoy - rev_yoy,
            # Carried so a threshold sweep can apply the SHIPPED rule, which
            # floors on both of these and which Phase 1 deliberately did not
            # implement. Without them the control test measures the bare
            # inequality -- a different thing from the rule in production.
            "ratio": cl_now / rev_now if rev_now else None,
        })
    return rows


def longest_run(rows, minimum=0.0):
    """Longest stretch of consecutive quarters above a spread, and where."""
    best, best_end, run = 0, None, 0
    previous = None
    for row in rows:
        contiguous = previous is None or row["quarter"] == next_quarter(previous)
        if row["spread"] > minimum and contiguous:
            run += 1
        elif row["spread"] > minimum:
            run = 1
        else:
            run = 0
        if run > best:
            best, best_end = run, row["quarter"]
        previous = row["quarter"]
    return best, best_end


def next_quarter(key):
    year, quarter = key
    return (year + 1, 1) if quarter == 4 else (year, quarter + 1)


def first_run_of(rows, length, minimum=0.0):
    """The quarter at which a run of `length` consecutive quarters completes."""
    run, previous = 0, None
    for row in rows:
        contiguous = previous is None or row["quarter"] == next_quarter(previous)
        run = (run + 1) if (row["spread"] > minimum and contiguous) else (
            1 if row["spread"] > minimum else 0)
        previous = row["quarter"]
        if run >= length:
            return row["quarter"]
    return None


def fires_under(rows, quarters, gap, min_revenue, min_ratio):
    """Would the SHIPPED rule have fired anywhere in this window?

    The live condition is a conjunction held over consecutive quarters: the
    liability material against revenue, revenue above a floor, and the growth
    gap above a threshold, all in the same quarter, for N quarters running.

    Contiguity is by calendar quarter, so a gap in the series breaks the run
    rather than bridging it -- the same rule the raw backtest uses.
    """
    best = run = 0
    previous = None
    for row in rows:
        ok = (row["spread"] >= gap
              and row["rev"] >= min_revenue
              and row["ratio"] is not None and row["ratio"] >= min_ratio)
        contiguous = previous is None or row["quarter"] == next_quarter(previous)
        run = (run + 1) if (ok and contiguous) else (1 if ok else 0)
        previous = row["quarter"]
        best = max(best, run)
    return best >= quarters, best


# The shipped thresholds, and the axes to move them along. Ratio first because
# it is the one doing the work in production: it is what separates a business
# whose customers pay ahead from one where deferred revenue is a rounding item.
SHIPPED = {"quarters": 3, "gap": 5.0, "min_revenue": 10_000_000, "min_ratio": 0.15}
SWEEP_RATIO = (0.0, 0.15, 0.25, 0.40, 0.60)
SWEEP_QUARTERS = (3, 4, 6, 8)


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * pct / 100.0
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def percentile_rank(value, values):
    """What share of the distribution this value is at or above."""
    if not values:
        return None
    return sum(1 for v in values if v <= value) / len(values) * 100.0


def in_window(rows):
    return [r for r in rows
            if WINDOW_START.year <= r["quarter"][0] <= WINDOW_END.year]


def measure(cik):
    """Everything Phase 1 needs about one company, from up to eight requests."""
    cl, cl_prov, cl_per_tag, _ = splice(cik, CL_TAGS, instant=True)
    rev, rev_prov, rev_per_tag, dropped = splice(cik, REV_TAGS, instant=False)
    rows = in_window(spread_series(cl, rev))
    return {
        "cik": cik,
        "rows": rows,
        "cl_provenance": cl_prov,
        "rev_provenance": rev_prov,
        "cl_switches": tag_switches(cl_prov, cl_per_tag),
        "rev_switches": tag_switches(rev_prov, rev_per_tag),
        "ytd_dropped": dropped,
        "has_cl": bool(cl),
        "has_rev": bool(rev),
    }


# ---------------------------------------------------------------- 1.1 Axon


def quarter_label(key):
    return "{0}Q{1}".format(key[0], key[1])


def run_axon(cik=AXON_CIK):
    print("=" * 78)
    print("1.1  SINGLE-COMPANY BACKTEST -- CIK {0}".format(cik))
    print("=" * 78)
    result = measure(cik)
    rows = result["rows"]

    if not rows:
        print("NO USABLE QUARTERS. This is a data failure, not a finding.")
        return result

    print("\n{0:>8} {1:>16} {2:>16} {3:>9} {4:>9} {5:>9}".format(
        "quarter", "contract liab", "revenue", "CL yoy%", "rev yoy%", "spread"))
    for row in rows:
        print("{0:>8} {1:>16,.0f} {2:>16,.0f} {3:>9.1f} {4:>9.1f} {5:>9.1f}".format(
            quarter_label(row["quarter"]), row["cl"], row["rev"],
            row["cl_yoy"], row["rev_yoy"], row["spread"]))

    print("\ntag provenance (quarter: contract-liability tag | revenue tag)")
    for row in rows:
        key = row["quarter"]
        print("  {0}: {1} | {2}".format(
            quarter_label(key),
            result["cl_provenance"].get(key, "?"),
            result["rev_provenance"].get(key, "?")))

    for name, switches in (("contract liability", result["cl_switches"]),
                           ("revenue", result["rev_switches"])):
        if not switches:
            print("\n{0}: no tag switch in window".format(name))
            continue
        print("\n{0}: {1} tag switch(es)".format(name, len(switches)))
        for switch in switches:
            line = "  {0}: {1} -> {2}".format(
                quarter_label(switch["quarter"]), switch["from_tag"],
                switch["to_tag"])
            if switch["overlap"]:
                old, new, step = switch["overlap"]
                line += ("   both reported: {0:,.0f} vs {1:,.0f} "
                         "({2:+.1f}% step -- DISCONTINUITY, not smoothed)"
                         .format(old, new, step))
            else:
                line += "   no overlapping quarter to compare levels"
            print(line)

    print("\nYTD/annual revenue periods discarded: {0}".format(
        result["ytd_dropped"]))
    for length in (2, 3, 4):
        found = first_run_of(rows, length)
        print("first run of {0} consecutive positive-spread quarters: {1}".format(
            length, quarter_label(found) if found else "never in window"))
    run, ended = longest_run(rows)
    print("longest positive run: {0} quarters, through {1}".format(
        run, quarter_label(ended) if ended else "-"))
    print("max spread: {0:.1f}pp".format(max(r["spread"] for r in rows)))
    return result


# ---------------------------------------------------------------- 1.2 control


def build_control_set(sample=CONTROL_SAMPLE, exclude=(AXON_CIK,)):
    """Everyone at Axon's scale as at end-2015, sampled deterministically.

    Drawn from the market as it stood then rather than from a list chosen now.
    Nothing is filtered for having survived: a company that later went bankrupt
    or was acquired is exactly the case the metric needs to be wrong about, and
    dropping it would flatter the result.
    """
    print("=" * 78)
    print("1.2  CONTROL SET")
    print("=" * 78)
    liabilities = frame("DeferredRevenueCurrent", CONTROL_PERIOD_INSTANT)
    revenues = frame("Revenues", CONTROL_PERIOD_ANNUAL)
    print("frame DeferredRevenueCurrent {0}: {1:,} filers".format(
        CONTROL_PERIOD_INSTANT, len(liabilities)))
    print("frame Revenues {0}: {1:,} filers".format(
        CONTROL_PERIOD_ANNUAL, len(revenues)))

    by_cik = {}
    for entry in revenues:
        if entry.get("cik") and entry.get("val") is not None:
            by_cik[int(entry["cik"])] = entry

    universe = []
    for entry in liabilities:
        cik = int(entry.get("cik") or 0)
        annual = by_cik.get(cik)
        if not cik or cik in exclude or not annual:
            continue
        revenue = float(annual["val"])
        if CONTROL_MIN_REVENUE <= revenue <= CONTROL_MAX_REVENUE:
            universe.append((cik, entry.get("entityName") or annual.get("entityName")
                             or "", revenue))

    universe.sort(key=lambda row: row[0])
    print("both tags, revenue ${0:,.0f}-${1:,.0f}: {2:,} companies".format(
        float(CONTROL_MIN_REVENUE), float(CONTROL_MAX_REVENUE), len(universe)))
    if not universe:
        return []
    stride = max(1, len(universe) // sample)
    picked = universe[::stride][:sample]
    print("deterministic stride {0} -> {1} sampled\n".format(stride, len(picked)))
    return picked


def run_control(sample=CONTROL_SAMPLE, out_path="lane_a_control.csv"):
    picked = build_control_set(sample)
    if not picked:
        print("CONTROL SET EMPTY -- the frames call returned nothing usable.")
        return [], []

    results, unusable = [], []
    for n, (cik, name, revenue) in enumerate(picked, 1):
        result = measure(cik)
        rows = result["rows"]
        if not rows:
            unusable.append((cik, name,
                             "no contract liability" if not result["has_cl"]
                             else "no quarterly revenue" if not result["has_rev"]
                             else "no overlapping year-over-year quarters"))
            print("  {0:>2}/{1} CIK {2:<8} {3:<34} UNUSABLE".format(
                n, len(picked), cik, name[:34]))
            continue
        run, _ = longest_run(rows)
        positive = sum(1 for r in rows if r["spread"] > 0)
        peak = max(r["spread"] for r in rows)
        results.append({
            "cik": cik, "entity_name": name, "cy2015_revenue": revenue,
            "quarters_measured": len(rows),
            "quarters_with_positive_spread": positive,
            "max_spread": peak, "longest_consecutive_run": run,
            "ytd_dropped": result["ytd_dropped"],
            "tag_switches": len(result["cl_switches"]) + len(result["rev_switches"]),
        })
        print("  {0:>2}/{1} CIK {2:<8} {3:<34} q={4:<3} pos={5:<3} run={6:<2} "
              "max={7:>7.1f}pp".format(n, len(picked), cik, name[:34], len(rows),
                                       positive, run, peak))

    if results:
        with open(out_path, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(results[0]))
            writer.writeheader()
            writer.writerows(results)
        print("\nwrote {0} ({1} rows)".format(out_path, len(results)))

    print("\nunusable: {0} of {1}".format(len(unusable), len(picked)))
    for cik, name, why in unusable:
        print("  CIK {0:<8} {1:<40} {2}".format(cik, name[:40], why))
    return results, unusable


# ---------------------------------------------------------------- report


def run_sweep(sample=CONTROL_SAMPLE, cik=AXON_CIK):
    """How selective is the SHIPPED rule, and where does Axon sit under it?

    Phase 1 measured the bare inequality and found Axon at the median with 68%
    of the control firing. That is a true statement about the thesis and a
    false one about the rule, which floors on revenue and on the liability's
    materiality and fires on 2% of the live universe. This measures the rule.

    Prints a grid rather than a verdict. A threshold picked off a curve with
    its selectivity stated is a decision; one picked because it happens to
    isolate Axon is the failure this whole exercise exists to avoid, and the
    grid makes the difference visible -- if Axon only fires where the control
    also collapses to nothing, that is not discrimination, it is a filter
    tightened until one name survives.
    """
    print("=" * 78)
    print("THRESHOLD SWEEP -- the shipped rule, not the bare inequality")
    print("=" * 78)
    axon_rows = measure(cik)["rows"]
    if not axon_rows:
        print("no Axon history; nothing to sweep")
        return

    picked = build_control_set(sample)
    control = []
    for n, (control_cik, name, _revenue) in enumerate(picked, 1):
        rows = measure(control_cik)["rows"]
        if rows:
            control.append((name, rows))
        if n % 10 == 0:
            print(f"  ... {n}/{len(picked)}", flush=True)
    print(f"\ncontrol companies with usable history: {len(control)}")
    print(f"shipped thresholds: {SHIPPED}\n")

    print(f"{'quarters':>8} {'ratio':>7} {'control fires':>14} {'rate':>7}  Axon")
    for quarters in SWEEP_QUARTERS:
        for ratio in SWEEP_RATIO:
            hits = sum(1 for _name, rows in control
                       if fires_under(rows, quarters, SHIPPED["gap"],
                                      SHIPPED["min_revenue"], ratio)[0])
            axon_fires, axon_run = fires_under(
                axon_rows, quarters, SHIPPED["gap"],
                SHIPPED["min_revenue"], ratio)
            rate = hits / len(control) * 100 if control else 0.0
            print(f"{quarters:>8} {ratio:>7.2f} {hits:>10}/{len(control):<3} "
                  f"{rate:>6.1f}%  {'YES' if axon_fires else 'no':>3}"
                  f"  (run {axon_run})")
        print()

    # The names that survive at the tightest setting where Axon still fires --
    # the check the rate alone cannot make. If they read as one industry, the
    # rule is a sector screen wearing a metric's clothes.
    for quarters in SWEEP_QUARTERS:
        for ratio in reversed(SWEEP_RATIO):
            if fires_under(axon_rows, quarters, SHIPPED["gap"],
                           SHIPPED["min_revenue"], ratio)[0]:
                survivors = [name for name, rows in control
                             if fires_under(rows, quarters, SHIPPED["gap"],
                                            SHIPPED["min_revenue"], ratio)[0]]
                print(f"at {quarters} quarters / ratio {ratio:.2f}, Axon fires "
                      f"alongside {len(survivors)} of {len(control)}:")
                for name in sorted(survivors)[:20]:
                    print(f"    {name[:60]}")
                return


def report(axon, control, unusable):
    print("\n" + "=" * 78)
    print("1.3  WHERE AXON SITS IN THE DISTRIBUTION")
    print("=" * 78)
    if not control:
        print("No control results. Nothing can be concluded.")
        return
    if not axon or not axon["rows"]:
        print("No Axon result. Nothing can be concluded.")
        return

    axon_rows = axon["rows"]
    axon_max = max(r["spread"] for r in axon_rows)
    axon_run, _ = longest_run(axon_rows)

    spreads = [r["max_spread"] for r in control]
    runs = [r["longest_consecutive_run"] for r in control]
    fired = [r for r in control if r["longest_consecutive_run"] >= 3]

    print("control companies measured : {0}".format(len(control)))
    print("unusable                   : {0}".format(len(unusable)))
    print("hit 3+ consecutive quarters: {0} of {1}  ({2:.0f}%)".format(
        len(fired), len(control), len(fired) / len(control) * 100.0))
    print("\nmax spread across the control set")
    for pct in (50, 75, 90, 95):
        print("  p{0:<3} {1:>9.1f}pp".format(pct, percentile(spreads, pct)))
    print("  max  {0:>9.1f}pp".format(max(spreads)))
    print("\nlongest consecutive run across the control set")
    for pct in (50, 75, 90):
        print("  p{0:<3} {1:>9.1f} quarters".format(pct, percentile(runs, pct)))

    print("\nAXON  max spread {0:.1f}pp   longest run {1} quarters".format(
        axon_max, axon_run))
    print("AXON  max spread sits at the {0:.0f}th percentile of the control set"
          .format(percentile_rank(axon_max, spreads)))
    print("AXON  longest run sits at the {0:.0f}th percentile"
          .format(percentile_rank(axon_run, runs)))
    print("\nRead this before concluding anything: a rule that fires on most of")
    print("the control set is describing an industry, not an opportunity. If")
    print("Axon is near the middle of this distribution the metric is noise,")
    print("and the answer is to report that -- not to raise the threshold until")
    print("Axon stands out, which is fitting the rule to the one known answer.")


# ---------------------------------------------------------------- self-test


def selftest():
    """Exercise the parsing rules offline, where no network is needed.

    Every one of these is a defect this file would otherwise ship: a quarter
    counted twice from an amendment, a year-to-date period differenced as if it
    were a quarter, a series truncated at the ASC 606 rename.
    """
    failures = []

    def check(label, got, want):
        if got == want:
            print("PASS  {0}".format(label))
        else:
            print("FAIL  {0}\n      got={1!r}\n     want={2!r}".format(
                label, got, want))
            failures.append(label)

    # Amended filings restate the same quarter; the latest filing wins.
    facts = [
        {"start": "2015-01-01", "end": "2015-03-31", "val": 100, "filed": "2015-05-01"},
        {"start": "2015-01-01", "end": "2015-03-31", "val": 110, "filed": "2016-05-01"},
    ]
    series, _ = quarterly_series(facts)
    check("an amended quarter is one fact, not two", len(series), 1)
    check("...and the later filing wins", series[(2015, 1)][1], 110.0)

    # Year-to-date revenue must be discarded, not differenced.
    facts = [
        {"start": "2015-01-01", "end": "2015-03-31", "val": 10, "filed": "2015-05-01"},
        {"start": "2015-01-01", "end": "2015-06-30", "val": 21, "filed": "2015-08-01"},
        {"start": "2015-01-01", "end": "2015-12-31", "val": 44, "filed": "2016-02-01"},
    ]
    series, dropped = quarterly_series(facts)
    check("year-to-date periods are dropped", sorted(series), [(2015, 1)])
    check("...and counted", dropped, 2)

    # A balance has an end and no start; a flow has both.
    facts = [
        {"end": "2015-03-31", "val": 500, "filed": "2015-05-01"},
        {"start": "2015-01-01", "end": "2015-03-31", "val": 10, "filed": "2015-05-01"},
    ]
    check("a balance is read as instantaneous",
          instant_series(facts)[(2015, 1)][1], 500.0)
    check("...and the flow beside it is not", len(instant_series(facts)), 1)

    # Year over year, each side against itself.
    cl = {(2015, 1): (date(2015, 3, 31), 150.0), (2014, 1): (date(2014, 3, 31), 100.0)}
    rev = {(2015, 1): (date(2015, 3, 31), 110.0), (2014, 1): (date(2014, 3, 31), 100.0)}
    rows = spread_series(cl, rev)
    check("growth is measured against four quarters back", len(rows), 1)
    check("...contract liability +50%", round(rows[0]["cl_yoy"], 1), 50.0)
    check("...revenue +10%", round(rows[0]["rev_yoy"], 1), 10.0)
    check("...spread is the gap in points", round(rows[0]["spread"], 1), 40.0)

    # A quarter with no year-ago comparison is skipped, not zero-filled.
    check("an unpaired quarter yields nothing",
          spread_series({(2015, 1): (date(2015, 3, 31), 1.0)},
                        {(2015, 1): (date(2015, 3, 31), 1.0)}), [])

    # Runs must be consecutive; a gap in the calendar breaks them.
    rows = [{"quarter": (2015, 1), "spread": 9.0},
            {"quarter": (2015, 2), "spread": 9.0},
            {"quarter": (2015, 4), "spread": 9.0}]
    check("a missing quarter breaks the run", longest_run(rows)[0], 2)
    check("...and a 3-run is not claimed", first_run_of(rows, 3), None)

    rows = [{"quarter": (2015, q), "spread": 9.0} for q in (1, 2, 3, 4)]
    check("four consecutive quarters run four", longest_run(rows)[0], 4)
    check("...the 3-run completes at Q3", first_run_of(rows, 3), (2015, 3))
    check("a year boundary is consecutive",
          longest_run([{"quarter": (2015, 4), "spread": 1.0},
                       {"quarter": (2016, 1), "spread": 1.0}])[0], 2)

    # The splice: each quarter from the first tag that has it.
    per_tag = {
        "ContractWithCustomerLiabilityCurrent": {(2018, 2): (date(2018, 6, 30), 70.0)},
        "DeferredRevenueCurrent": {(2017, 2): (date(2017, 6, 30), 50.0),
                                   (2018, 2): (date(2018, 6, 30), 68.0)},
    }
    series, provenance = {}, {}
    for tag in CL_TAGS:
        for key, value in (per_tag.get(tag) or {}).items():
            if key not in series:
                series[key] = value
                provenance[key] = tag
    check("the newer spelling wins where both report",
          provenance[(2018, 2)], "ContractWithCustomerLiabilityCurrent")
    check("...and the older one still supplies its own history",
          provenance[(2017, 2)], "DeferredRevenueCurrent")
    switches = tag_switches(provenance, per_tag)
    check("the switch is reported", len(switches), 1)
    check("...with both sides measured",
          round(switches[0]["overlap"][2], 1), round((70.0 - 68.0) / 68.0 * 100, 1))

    # Percentile placement, the number the whole exercise turns on.
    check("a value above everything ranks 100th",
          percentile_rank(100, [1, 2, 3]), 100.0)
    check("a middling value ranks in the middle",
          percentile_rank(2, [1, 2, 3, 4]), 50.0)

    print("\n" + ("ALL PASS" if not failures
                  else "{0} FAILED: {1}".format(len(failures), failures)))
    return 1 if failures else 0


# ---------------------------------------------------------------- cli


def main():
    parser = argparse.ArgumentParser(
        description="Lane A Phase 1: validate the metric before building on it.")
    parser.add_argument("--axon", action="store_true",
                        help="1.1 only: backtest the reference company")
    parser.add_argument("--control", action="store_true",
                        help="1.2 only: the control set and its distribution")
    parser.add_argument("--sample", type=int, default=CONTROL_SAMPLE,
                        help="control set size (default {0})".format(CONTROL_SAMPLE))
    parser.add_argument("--cik", type=int, default=AXON_CIK,
                        help="reference company (default Axon, {0})".format(AXON_CIK))
    parser.add_argument("--sweep", action="store_true",
                        help="measure the SHIPPED rule across a threshold grid")
    parser.add_argument("--selftest", action="store_true",
                        help="exercise the parsing rules offline, no network")
    args = parser.parse_args()

    if args.selftest:
        return selftest()

    if args.sweep:
        run_sweep(args.sample, args.cik)
        print("\nrequests: {0} ok, {1} absent (404), {2} failed".format(
            STATS["ok"], STATS["missing"], STATS["failed"]))
        return 2 if STATS["ok"] == 0 else 0

    everything = not (args.axon or args.control)
    axon = run_axon(args.cik) if (everything or args.axon) else None
    control, unusable = ([], [])
    if everything or args.control:
        control, unusable = run_control(args.sample)
    if everything:
        report(axon, control, unusable)

    print("\nrequests: {0} ok, {1} absent (404), {2} failed".format(
        STATS["ok"], STATS["missing"], STATS["failed"]))
    for failure in FAILURES[:20]:
        print("  FAILED: {0}".format(failure))

    # A run that fetched nothing must not look like a run that found nothing.
    if STATS["ok"] == 0:
        print("\nNO SUCCESSFUL REQUESTS. This is a transport failure and the "
              "output above means nothing.")
        return 2
    if STATS["failed"]:
        print("\n{0} request(s) failed; the sample above is smaller than it "
              "was asked to be.".format(STATS["failed"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
