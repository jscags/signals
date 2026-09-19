"""Lane A as it was knowable on a past day, not as it reads today.

evaluate_setup() cuts its series by each fact's `end` -- the quarter end. That
is correct for the live lane, where the newest quarter end is always already
public. It is wrong for a backtest, and wrong in the direction that invents
performance.

A quarter ending 2025-06-30 does not become public when June ends. It becomes
public when the 10-Q is filed, which for an accelerated filer is up to 40 days
later and for a smaller reporting company up to 45. Standing on 2025-07-15 and
asking evaluate_setup for the streak returns an answer built on a quarter no
one could read yet. Every entry taken on that basis is an entry taken on
tomorrow's newspaper.

This module closes that by filtering the companyfacts payload itself: drop
every fact whose `filed` is after the as-of date, then hand the survivors to
the UNMODIFIED evaluate_setup. The rule under test stays the shipped rule --
re-implementing it here would measure a lookalike, and any difference between
the two would show up as signal.

The same discipline the ledger work already applies to Form 4, where 29% of
filings arrive three or more days after the transaction and the signal day is
`filed_date`, never `txn_date`.

WRITES NOTHING. Pure functions over a payload the caller supplies.
"""

import json
import sys
from datetime import date

import setup_signal


def _as_date(text):
    try:
        return date.fromisoformat(str(text)[:10])
    except (TypeError, ValueError):
        return None


def facts_filed_by(facts, asof):
    """The companyfacts payload as it stood on `asof`.

    Structure is preserved exactly -- taxonomy, tag, units, and the fact dicts
    themselves -- so evaluate_setup cannot tell it was filtered. Only the list
    membership changes.

    A fact with no `filed` is DROPPED, not kept. Keeping it would mean an
    undated fact silently survives every as-of cut and leaks the future into
    every date at once; dropping it costs a little coverage in the honest
    direction. The count of these is reported by signal_dates so a payload
    that is mostly undated cannot masquerade as a clean run.
    """
    if not isinstance(asof, date):
        asof = _as_date(asof)
    kept_facts = {}
    dropped = undated = 0

    for taxonomy, tags in (facts.get("facts") or {}).items():
        for tag, payload in (tags or {}).items():
            units_out = {}
            for unit, rows in ((payload or {}).get("units") or {}).items():
                keep = []
                for row in rows or []:
                    when = _as_date(row.get("filed"))
                    if when is None:
                        undated += 1
                        continue
                    if when <= asof:
                        keep.append(row)
                    else:
                        dropped += 1
                if keep:
                    units_out[unit] = keep
            if units_out:
                kept_facts.setdefault(taxonomy, {})[tag] = dict(payload or {},
                                                                units=units_out)

    trimmed = dict(facts, facts=kept_facts)
    return trimmed, {"dropped_future": dropped, "dropped_undated": undated}


def filing_dates(facts, concepts_only=True):
    """Every distinct date on which this issuer filed something relevant.

    These are the only days the streak can CHANGE, so they are the only days
    worth evaluating. Walking month-ends instead would both miss a crossing
    that happened mid-month and re-report one that had not moved.
    """
    wanted = None
    if concepts_only:
        wanted = {tag for _tax, tag in
                  (list(setup_signal.LIABILITY_CONCEPTS)
                   + list(setup_signal.REVENUE_CONCEPTS))}
    out = set()
    for _taxonomy, tags in (facts.get("facts") or {}).items():
        for tag, payload in (tags or {}).items():
            if wanted is not None and tag not in wanted:
                continue
            for rows in ((payload or {}).get("units") or {}).values():
                for row in rows or []:
                    when = _as_date(row.get("filed"))
                    if when:
                        out.add(when)
    return sorted(out)


def streak_asof(facts, asof):
    """The shipped rule, run against only what had been filed by `asof`."""
    if not isinstance(asof, date):
        asof = _as_date(asof)
    trimmed, _stats = facts_filed_by(facts, asof)
    verdict = setup_signal.evaluate_setup(trimmed, today=asof)
    return int(verdict.get("streak") or 0), verdict


def signal_dates(facts, threshold=4):
    """Days the streak first reached `threshold`, newest evidence forward.

    A CROSSING, not a level. A company sitting at streak 6 for two years is one
    signal, not five hundred -- counting every day it qualifies would weight
    that company by how long it stayed qualified, which is a property of the
    company rather than of the signal, and would swamp the sample with whoever
    held the condition longest.

    Re-arming is deliberate: a streak that lapses below the threshold and later
    returns fires again, because that genuinely is a second occurrence.
    """
    dates = filing_dates(facts)
    fired, prev, stats = [], 0, {"evaluated": 0, "dropped_future": 0,
                                 "dropped_undated": 0}
    for when in dates:
        trimmed, s = facts_filed_by(facts, when)
        stats["dropped_future"] = max(stats["dropped_future"], s["dropped_future"])
        stats["dropped_undated"] = s["dropped_undated"]
        stats["evaluated"] += 1
        streak = int((setup_signal.evaluate_setup(trimmed, today=when)
                      .get("streak")) or 0)
        if streak >= threshold and prev < threshold:
            fired.append((when, streak))
        prev = streak
    return fired, stats


# ------------------------------------------------------------------ selftest

def _fact(end, val, filed, start=None):
    row = {"end": end, "val": val, "filed": filed, "form": "10-Q"}
    if start:
        row["start"] = start
    return row


def _payload(liab, rev):
    return {"cik": 1, "facts": {
        "us-gaap": {
            "ContractWithCustomerLiabilityCurrent": {"units": {"USD": liab}},
            "RevenueFromContractWithCustomerExcludingAssessedTax":
                {"units": {"USD": rev}},
        }}}


def selftest():
    """Prove the look-ahead is actually closed, before spending any requests."""
    ok = True

    def check(name, got, want):
        nonlocal ok
        good = got == want
        ok = ok and good
        print(f"  {'PASS' if good else 'FAIL'}  {name}")
        if not good:
            print(f"        got {got!r}\n        want {want!r}")

    # A quarter that ENDED before the as-of date but was FILED after it.
    liab = [_fact("2025-06-30", 500, "2025-08-08")]
    rev = [_fact("2025-06-30", 100, "2025-08-08", start="2025-04-01")]
    facts = _payload(liab, rev)

    trimmed, stats = facts_filed_by(facts, date(2025, 7, 15))
    check("quarter filed 2025-08-08 is invisible on 2025-07-15",
          trimmed["facts"], {})
    # Two facts filed that day, one liability and one revenue -- both invisible.
    check("and both that day's facts are counted as dropped",
          stats["dropped_future"], 2)

    trimmed, _ = facts_filed_by(facts, date(2025, 8, 8))
    check("visible on its filing date (boundary is inclusive)",
          len(trimmed["facts"]["us-gaap"]
              ["ContractWithCustomerLiabilityCurrent"]["units"]["USD"]), 1)

    # An undated fact must not survive the cut.
    facts_undated = _payload([{"end": "2025-06-30", "val": 500}], [])
    trimmed, stats = facts_filed_by(facts_undated, date(2020, 1, 1))
    check("undated fact is dropped, not leaked", trimmed["facts"], {})
    check("and is counted separately", stats["dropped_undated"], 1)

    # Structure must survive intact, or evaluate_setup reads a different shape.
    keep = _payload([_fact("2024-03-31", 10, "2024-05-01")],
                    [_fact("2024-03-31", 5, "2024-05-01", start="2024-01-01")])
    trimmed, _ = facts_filed_by(keep, date(2024, 6, 1))
    check("payload keys preserved", sorted(trimmed.keys()), sorted(keep.keys()))
    check("unit nesting preserved",
          list(trimmed["facts"]["us-gaap"]
               ["ContractWithCustomerLiabilityCurrent"]["units"]), ["USD"])

    # A crossing fires once, not once per subsequent filing.
    print("\n  crossing semantics")
    fired_once = [(date(2024, 1, 1), 4), (date(2024, 4, 1), 5)]
    runs = []
    prev = 0
    for _when, streak in fired_once:
        runs.append(streak >= 4 and prev < 4)
        prev = streak
    check("level >= threshold twice yields one crossing", runs, [True, False])

    # The load-bearing empirical claim: closing the look-ahead CHANGES the
    # answer on a realistic issuer, and changes it in the direction that was
    # inventing performance. A mechanism test alone would pass even if the
    # rule never actually read a future quarter.
    print("\n  does it change the answer?")
    from datetime import timedelta
    q_ends = [date(y, m, d) for y in (2022, 2023, 2024, 2025)
              for m, d in ((3, 31), (6, 30), (9, 30), (12, 31))]
    liab_s, rev_s = [], []
    for i, q in enumerate(q_ends):
        filed = (q + timedelta(days=40)).isoformat()     # realistic 10-Q lag
        liab_s.append(_fact(q.isoformat(), 40e6 * (1.18 ** i), filed))
        rev_s.append(_fact(q.isoformat(), 50e6 * (1.04 ** i), filed,
                           start=(q - timedelta(days=89)).isoformat()))
    real = _payload(liab_s, rev_s)

    disagreements = 0
    inflations = set()
    for q in q_ends[8:]:
        for off in (5, 20, 41):
            when = q + timedelta(days=off)
            naive = int((setup_signal.evaluate_setup(real, today=when)
                         .get("streak")) or 0)
            pit, _ = streak_asof(real, when)
            if naive != pit:
                disagreements += 1
                inflations.add(naive - pit)

    check("look-ahead inflates the streak on some days", disagreements > 0, True)
    check("and never deflates it", inflations, {1})

    fired, _stats = signal_dates(real, threshold=6)
    naive_cross = next(
        (q + timedelta(days=off) for q in q_ends[8:] for off in (5, 20, 41)
         if int((setup_signal.evaluate_setup(real, today=q + timedelta(days=off))
                 .get("streak")) or 0) >= 6), None)
    check("point-in-time crossing is LATER than the naive one",
          bool(fired) and naive_cross is not None and fired[0][0] > naive_cross,
          True)
    if fired and naive_cross:
        print(f"        naive {naive_cross}  ->  point-in-time {fired[0][0]}"
              f"   ({(fired[0][0] - naive_cross).days} days early if uncorrected)")

    print("\nSELFTEST " + ("OK" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv[1:]:
        sys.exit(selftest())
    print(__doc__)
    sys.exit(0)
