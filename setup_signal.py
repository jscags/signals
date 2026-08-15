"""Lane A: the setup condition. Contract liabilities outgrowing revenue.

Lanes B and C answer "what was filed today". This answers "what has been true
for several quarters", which is a different question about a different clock,
and it is the only lane that can see a company on a day it filed nothing.

The worked example is Axon. The filing stream's spikes cluster around 2004 --
the top -- and then say nothing for eleven years. The entry that mattered sits
in 2016, in the middle of that silence, and what was visible there was not an
event but a balance: deferred revenue compounding faster than the revenue it
would become, quarter after quarter, as a hardware company turned into a
subscription one. No Form 4, no 8-K, nothing for a daily loop to fire on.

Pure functions over a companyfacts payload. No fetching and no database, so the
metric can be exercised against a saved payload or a synthetic one without a
network -- which is the only reason it could be written and checked here at all.

    ┌─ the two tag families ─────────────────────────────────────────────┐
    │ ASC 606 renamed this line in 2018 and issuers switched on their own │
    │ adoption date, so a series that spans the change is spelled two     │
    │ ways. Reading either tag alone truncates the history at the switch: │
    │ ask for the new one and Axon starts in 2018, ask for the old one    │
    │ and it stops there. Both are read and spliced.                      │
    └────────────────────────────────────────────────────────────────────┘
"""

from datetime import date

# ---------------------------------------------------------------- concepts

# Post-ASC 606. What a company calls the money it has been paid but not earned.
CONTRACT_LIABILITY = (
    ("us-gaap", "ContractWithCustomerLiabilityCurrent"),
    ("us-gaap", "ContractWithCustomerLiability"),
)
# Pre-ASC 606. The same economic quantity under its old name.
DEFERRED_REVENUE = (
    ("us-gaap", "DeferredRevenueCurrent"),
    ("us-gaap", "DeferredRevenue"),
)
# Newer first, so the splice prefers the current spelling wherever both exist.
LIABILITY_CONCEPTS = CONTRACT_LIABILITY + DEFERRED_REVENUE

REVENUE_CONCEPTS = (
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
    ("us-gaap", "Revenues"),
    ("us-gaap", "SalesRevenueNet"),
    ("us-gaap", "SalesRevenueServicesNet"),
    ("us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax"),
)

# ---------------------------------------------------------------- thresholds

# How many consecutive quarters the liability must outgrow revenue.
# Raised from 3 on the Phase 1 sweep. Three quarters is a billing cycle; the
# thesis is about a change in the shape of a business, and eighteen months is
# the shortest window that means it. Measured against 37 control companies
# drawn from end-2015 with no survivorship filter, at a 0.40 materiality floor:
# 3 quarters fires on 18.9%, 4 on 13.5%, 6 on 8.1%, 8 on 5.4%.
MIN_CONSECUTIVE_QUARTERS = 6

# By how much, in percentage points of year-over-year growth. A liability
# growing 0.4pp faster than revenue is noise in the rounding.
MIN_GROWTH_GAP_PP = 5.0

# Ignore quarters where revenue is tiny: a pre-revenue company can post any
# growth ratio at all and none of it means what this metric means. The first
# population probe at $1m returned an antimony miner, a micro-cap e-commerce
# roll-up and a near-pre-revenue device company -- businesses small enough
# that one contract moves the balance sheet.
MIN_QUARTERLY_REVENUE_USD = 10_000_000

# The liability must matter to the business. This is the threshold that does
# the real work, and it is not a size filter: Dave & Buster's fired on eight
# consecutive quarters at $559m of revenue, where the "deferred revenue" is
# unredeemed gift cards worth 3% of a quarter. Paychex's is payroll float,
# Bristol Myers' is rebate and return accruals. A 5pp growth gap on a rounding
# item is arithmetic, not a business changing shape.
#
# The 250-issuer sample split at 0.15 between customer prepayments and
# accounting residue, which is where this started. Axon ran 0.63 to 0.93.
#
# Raised to 0.40 on the Phase 1 sweep, where it does most of the work: at 3
# quarters the control fire-rate runs 51.4% at no floor, 27.0% at 0.15, 18.9%
# at 0.40, 10.8% at 0.60. Combined with 6 quarters this lands at 8.1% -- three
# of 37 -- against 27.0% for the shipped setting.
#
# Read off a curve, not toward an answer. Axon fires in EVERY cell of the grid
# with a streak of 12 here against a requirement of 6, so this is not a filter
# tightened until one name survives -- the check the control set exists to make.
# The companies that clear it alongside Axon are Tableau, PROS Holdings,
# Benefitfocus and Quantum: subscription transitions, which is what the thesis
# says the metric should find.
MIN_LIABILITY_TO_REVENUE = 0.40

# A balance more than this far from its quarter end is not that quarter's.
QUARTER_MATCH_TOLERANCE_DAYS = 45

# Growth is measured year over year, so the series needs this much history
# before the first comparison can be made at all.
LOOKBACK_QUARTERS = 8

# Two balances this close together are one quarter reported twice. Adopting a
# new standard restates the opening balance sheet, so Axon carries both
# 2017-12-31 (70.4m, old basis) and 2018-01-01 (71.3m, ASC 606 basis) -- the
# same instant under two rules. Left alone the pair burns two of the eight
# window slots on one quarter and compares that quarter against itself.
ADJACENT_BALANCE_DAYS = 5


def _as_date(text):
    try:
        return date.fromisoformat(str(text)[:10])
    except (TypeError, ValueError):
        return None


def instant_series(facts, concepts):
    """Dated balances for the first concept family that reports any.

    Balances, not flows: a liability is a point-in-time figure with an `end`
    and no `start`, which the flow reader deliberately discards.

    Spliced across the ASC 606 rename. Each date takes its value from the
    earliest concept in the list that reports one, and since the post-606 tags
    are listed first, a company that reports both during its transition
    quarter is read under the new name -- which is the one it will keep.
    """
    by_date = {}
    for taxonomy, tag in concepts:
        payload = _concept(facts, taxonomy, tag)
        for unit_facts in ((payload or {}).get("units") or {}).values():
            for fact in unit_facts:
                if fact.get("val") is None or fact.get("start") or not fact.get("end"):
                    continue
                when = _as_date(fact["end"])
                if when and when not in by_date:
                    by_date[when] = float(fact["val"])
    return _collapse_adjacent(sorted(by_date.items()))


def _collapse_adjacent(series, within=ADJACENT_BALANCE_DAYS):
    """One point per instant, keeping the earliest date of any close cluster.

    Earliest rather than latest because the quarter end is the date the rest of
    the series is spaced on: the year-ago lookup and the revenue join both aim
    at calendar quarter ends, and a restatement stamped on the first of the
    next month drifts a point off that grid for no gain.
    """
    kept = []
    for when, value in series:
        if kept and (when - kept[-1][0]).days <= within:
            continue
        kept.append((when, value))
    return kept


def flow_series(facts, concepts, max_days=115):
    """Dated quarterly flows for the first concept family that reports any.

    Quarters only. A companyfacts payload carries year-to-date and annual
    periods beside the quarterly ones, and mixing them would compare three
    months of revenue against twelve.
    """
    by_end = {}
    for taxonomy, tag in concepts:
        payload = _concept(facts, taxonomy, tag)
        for unit_facts in ((payload or {}).get("units") or {}).values():
            for fact in unit_facts:
                if fact.get("val") is None or not fact.get("start") or not fact.get("end"):
                    continue
                start, end = _as_date(fact["start"]), _as_date(fact["end"])
                if not start or not end:
                    continue
                span = (end - start).days
                if not 60 <= span <= max_days:
                    continue
                if end not in by_end:
                    by_end[end] = float(fact["val"])
    return sorted(by_end.items())


def _concept(facts, taxonomy, tag):
    try:
        return facts["facts"][taxonomy][tag]
    except (KeyError, TypeError):
        return None


def _nearest(series, when, tolerance=QUARTER_MATCH_TOLERANCE_DAYS):
    """The series value closest to a date, within tolerance.

    Fiscal quarter ends do not line up exactly between a balance and a flow --
    a 13-week retailer's quarter drifts against the calendar -- so an exact
    date join silently drops most of the history.
    """
    best = None
    for when_i, value in series:
        gap = abs((when_i - when).days)
        if gap <= tolerance and (best is None or gap < best[0]):
            best = (gap, value)
    return best[1] if best else None


def _growth(now, then):
    if then is None or now is None or then <= 0:
        return None
    return (now - then) / then * 100.0


def evaluate_setup(facts, today=None):
    """Is the setup condition true, and on what evidence?

    Returns a dict with `setup` (bool), `reason`, and the quarters that were
    compared. Never raises on a shape it does not recognise -- an issuer that
    reports no contract liability at all is the ordinary case, not an error.
    """
    today = today or date.today()
    liabilities = instant_series(facts, LIABILITY_CONCEPTS)
    revenues = flow_series(facts, REVENUE_CONCEPTS)
    if not liabilities or not revenues:
        return {"setup": False, "reason": "no contract-liability or revenue history",
                "quarters": []}

    # Walk quarters newest first, pairing each with the same quarter a year
    # earlier. Year-over-year rather than sequential because deferred revenue
    # is violently seasonal -- an annual-renewal business books most of its
    # billings in one quarter, and a quarter-on-quarter reading would swing
    # between +200% and -60% on a business that is not changing at all.
    quarters = []
    # Cut to the as-of date FIRST, then take the window. Slicing the raw series
    # and filtering afterwards looks equivalent and is not: the last sixteen
    # points of Axon's history are 2022-2026, so a backtest standing in 2016
    # took a window entirely in its own future and discarded all of it, and
    # every quarter came back "not enough overlapping history" off 63 perfectly
    # good liability points. Live it worked, because the tail of the series IS
    # the present -- which is exactly why only a backtest could show it.
    asof_liabilities = [(w, v) for w, v in liabilities if w <= today]
    for when, liability in reversed(asof_liabilities[-LOOKBACK_QUARTERS * 2:]):
        year_ago = date(when.year - 1, when.month, min(when.day, 28))
        prior_liability = _nearest(liabilities, year_ago)
        revenue = _nearest(revenues, when)
        prior_revenue = _nearest(revenues, year_ago)
        if revenue is None or revenue < MIN_QUARTERLY_REVENUE_USD:
            continue
        lg = _growth(liability, prior_liability)
        rg = _growth(revenue, prior_revenue)
        if lg is None or rg is None:
            continue
        quarters.append({
            "quarter_end": when.isoformat(),
            "liability": liability,
            "revenue": revenue,
            "liability_growth_pct": round(lg, 1),
            "revenue_growth_pct": round(rg, 1),
            "gap_pp": round(lg - rg, 1),
            "liability_to_revenue": round(liability / revenue, 3),
        })
        if len(quarters) >= LOOKBACK_QUARTERS:
            break

    if not quarters:
        return {"setup": False, "reason": "not enough overlapping history",
                "quarters": []}

    streak, immaterial = streak_from_quarters(quarters)

    # Consecutive from the most recent quarter backwards. A gap that closed two
    # quarters ago is a condition that has stopped being true, and saying so
    # requires counting from the present rather than anywhere in the window.
    #
    # Both conditions have to hold in the same quarter, so materiality is part
    # of the streak rather than a filter applied before it. Filtering first
    # would let a company whose liability used to matter keep a streak running
    # on quarters where it no longer does.
    if streak < MIN_CONSECUTIVE_QUARTERS:
        if immaterial:
            reason = (f"contract liability is only "
                      f"{quarters[0]['liability_to_revenue']:.0%} of quarterly "
                      f"revenue, below the {MIN_LIABILITY_TO_REVENUE:.0%} floor")
        else:
            reason = (f"liability outgrew revenue for {streak} quarter(s), "
                      f"needs {MIN_CONSECUTIVE_QUARTERS}")
        return {
            "setup": False,
            "reason": reason,
            "quarters": quarters,
            "streak": streak,
        }

    worst = min(q["gap_pp"] for q in quarters[:streak])
    return {
        "setup": True,
        "reason": (f"contract liabilities outgrew revenue for {streak} "
                   f"consecutive quarters, by at least {worst:.0f}pp "
                   f"(through {quarters[0]['quarter_end']})"),
        "quarters": quarters,
        "streak": streak,
    }


def streak_from_quarters(quarters):
    """Consecutive qualifying quarters from the most recent backwards.

    Split out so the collector can re-derive a stored verdict under changed
    thresholds from the evidence it already kept, without fetching anything --
    and so the live path and that repair cannot drift into two rules.

    Returns (streak, immaterial): the count, and whether it stopped because the
    most recent quarter's liability is too small to matter rather than because
    the growth gap closed. Those are different sentences on a card.
    """
    streak = 0
    for q in quarters or ():
        if (q.get("liability_to_revenue") or 0) < MIN_LIABILITY_TO_REVENUE:
            return streak, streak == 0
        if q.get("gap_pp", 0) >= MIN_GROWTH_GAP_PP:
            streak += 1
        else:
            break
    return streak, False


def tag_family(facts):
    """Which spelling this issuer reports, for diagnostics.

    Worth being able to ask directly: the ASC 606 rename is the single most
    likely reason a series looks shorter than it should, and "which tag" is
    the first question when one does.
    """
    present = []
    for taxonomy, tag in LIABILITY_CONCEPTS:
        if _concept(facts, taxonomy, tag):
            present.append(tag)
    return present
