#!/usr/bin/env python3
"""Grade a hand-kept ledger of dated market calls against daily prices.

calls.json holds calls copied (paraphrased) from a daily newsletter, one entry
per call on the day it first appeared. This script prices each one, grades it
twice -- on the close and on the intraday extreme -- and writes a page that
answers one question: which KINDS of call are worth reading?

WHY TWO GRADES
--------------
"Holds 630" is ambiguous. A level that breaks at 10:15 and is reclaimed by the
close held on one reading and failed on the other, and an author grading his
own calls will tend to pick whichever reading flatters him. Both are computed
for every price-testable call and shown side by side; the entry's `basis`
picks which one counts toward the headline, and that choice is made when the
call is entered, not after the outcome is known.

WHAT THIS SCRIPT REFUSES TO DO QUIETLY
--------------------------------------
- A call whose ticker could not be priced is UNPRICED, not a miss and not
  absent. It is counted and listed.
- A call whose window has not closed is PENDING unless the outcome is already
  settled (a hold that has broken, a reach that has touched).
- A malformed ledger fails the run. A silently dropped call looks exactly
  like a call that was never made.

Usage:
  python3 calls_score.py --check            validate calls.json only
  python3 calls_score.py --selftest         grading logic on synthetic bars
  python3 calls_score.py --fetch            refresh prices, grade, write page
  python3 calls_score.py                    grade from prices already stored
"""

import argparse
import html
import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime

TESTS = ("hold", "reach", "under", "up", "down", "manual")
PRICED = ("hold", "reach", "under", "up", "down")
LEVELLED = ("hold", "reach", "under")
CATEGORIES = ("level", "target", "regime", "risk", "ticker", "conditional", "macro")
MANUAL_GRADES = ("hit", "miss", "partial", "pending")
BASES = ("close", "intraday")

HIT, MISS, PARTIAL, PENDING, UNPRICED = "HIT", "MISS", "PARTIAL", "PENDING", "UNPRICED"

CATEGORY_LABEL = {
    "level": "Support / floor levels",
    "target": "Upside targets",
    "regime": "Market regime calls",
    "risk": "Flagged risks",
    "ticker": "Single-name calls",
    "conditional": "If-then reasoning",
    "macro": "Macro / Fed reads",
}


# ------------------------------------------------------------------ validate

def _is_date(s):
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except (TypeError, ValueError):
        return False


def check(data):
    """Every problem in the ledger, as strings. Empty list means usable."""
    problems = []
    calls = data.get("calls") if isinstance(data, dict) else None
    if not isinstance(calls, list) or not calls:
        return ["no calls array, or it is empty"]
    ids = set()
    for i, c in enumerate(calls):
        where = c.get("id") or f"calls[{i}]"
        for field in ("id", "made", "by", "ticker", "test", "category", "say"):
            if not c.get(field) and c.get(field) != 0:
                problems.append(f"{where}: missing {field}")
        if c.get("id") in ids:
            problems.append(f"{where}: duplicate id")
        ids.add(c.get("id"))
        if not _is_date(c.get("made")) or not _is_date(c.get("by")):
            problems.append(f"{where}: made/by must be YYYY-MM-DD")
        elif c["by"] < c["made"]:
            problems.append(f"{where}: by is before made")
        test = c.get("test")
        if test not in TESTS:
            problems.append(f"{where}: test must be one of {TESTS}")
        if c.get("category") not in CATEGORIES:
            problems.append(f"{where}: category must be one of {CATEGORIES}")
        if test in LEVELLED and not isinstance(c.get("level"), (int, float)):
            problems.append(f"{where}: {test} needs a numeric level")
        if test in PRICED and c.get("ticker") in (None, "", "-"):
            problems.append(f"{where}: {test} needs a real ticker")
        if test in PRICED and c.get("basis", "close") not in BASES:
            problems.append(f"{where}: basis must be close or intraday")
        if test == "manual" and c.get("grade") not in MANUAL_GRADES:
            problems.append(f"{where}: manual call needs grade in {MANUAL_GRADES}")
        if test != "manual" and "grade" in c:
            problems.append(f"{where}: grade is only for manual calls; "
                            "priced calls are graded from prices")
        conf = c.get("conf")
        if conf is not None and not (isinstance(conf, (int, float)) and 0 <= conf <= 1):
            problems.append(f"{where}: conf must be 0-1 or null")
    for c in calls:
        sup = c.get("supersedes")
        if sup and sup not in ids:
            problems.append(f"{c.get('id')}: supersedes unknown id {sup}")
    return problems


def load(path="calls.json"):
    with open(path) as fh:
        return json.load(fh)


# ------------------------------------------------------------------ prices

def bars_for(conn, ticker, start=None, end=None):
    q = "SELECT day, open, high, low, close FROM prices WHERE ticker = ?"
    args = [ticker]
    if start:
        q += " AND day >= ?"
        args.append(start)
    if end:
        q += " AND day <= ?"
        args.append(end)
    return [dict(r) for r in conn.execute(q + " ORDER BY day", args)]


def last_day(conn, ticker):
    row = conn.execute("SELECT MAX(day) FROM prices WHERE ticker = ?",
                       (ticker,)).fetchone()
    return row[0] if row else None


def close_on_or_before(conn, ticker, day):
    row = conn.execute(
        "SELECT day, close FROM prices WHERE ticker = ? AND day <= ?"
        " ORDER BY day DESC LIMIT 1", (ticker, day)).fetchone()
    return (row[0], row[1]) if row else (None, None)


# ------------------------------------------------------------------ grading

def _grade_level(test, level, window, complete, basis):
    """One basis of a hold / reach / under call. Returns (status, detail)."""
    for b in window:
        if test == "hold":
            v = b["close"] if basis == "close" else b["low"]
            if v < level:
                return MISS, f"{v:,.2f} on {b['day']}"
        elif test == "under":
            v = b["close"] if basis == "close" else b["high"]
            if v >= level:
                return MISS, f"{v:,.2f} on {b['day']}"
        elif test == "reach":
            v = b["close"] if basis == "close" else b["high"]
            if v >= level:
                return HIT, f"{v:,.2f} on {b['day']}"
    if not complete:
        return PENDING, "window open"
    if test == "reach":
        key = "close" if basis == "close" else "high"
        best = max(b[key] for b in window)
        label = "best close" if basis == "close" else "high"
        return MISS, f"{label} {best:,.2f}"
    key = ("close" if basis == "close" else
           ("low" if test == "hold" else "high"))
    extreme = (min if test == "hold" else max)(b[key] for b in window)
    if basis == "close":
        label = "lowest close" if test == "hold" else "highest close"
    else:
        label = "low" if test == "hold" else "high"
    return HIT, f"{label} {extreme:,.2f}"


def grade(call, conn):
    """Both grades for one call: {'close': (status, detail), 'intraday': ...}."""
    test = call["test"]
    if test == "manual":
        g = {"hit": HIT, "miss": MISS, "partial": PARTIAL,
             "pending": PENDING}[call["grade"]]
        return {"close": (g, ""), "intraday": (g, "")}

    ticker = call["ticker"]
    through = last_day(conn, ticker)
    if through is None:
        return {"close": (UNPRICED, "no prices"), "intraday": (UNPRICED, "no prices")}
    complete = through >= call["by"]

    if test in ("up", "down"):
        bar = bars_for(conn, ticker, call["by"], call["by"])
        if not bar:
            st = (UNPRICED, f"no bar for {call['by']}") if complete else (PENDING, "")
            return {"close": st, "intraday": st}
        prev_day, prev = close_on_or_before(conn, ticker,
                                            _day_before(call["by"]))
        if prev is None:
            st = (UNPRICED, "no prior close")
            return {"close": st, "intraday": st}
        c = bar[0]["close"]
        chg = (c / prev - 1) * 100
        ok = c > prev if test == "up" else c < prev
        st = (HIT if ok else MISS, f"{chg:+.2f}%")
        return {"close": st, "intraday": st}

    window = bars_for(conn, ticker, call["made"], call["by"])
    if not window:
        st = (UNPRICED, "no bars in window") if complete else (PENDING, "window open")
        return {"close": st, "intraday": st}
    level = float(call["level"])
    return {b: _grade_level(test, level, window, complete, b) for b in BASES}


def _day_before(day):
    d = datetime.strptime(day, "%Y-%m-%d").date()
    return date.fromordinal(d.toordinal() - 1).isoformat()


def grade_all(calls, conn):
    out = []
    for c in calls:
        g = grade(c, conn)
        basis = c.get("basis", "close") if c["test"] != "manual" else "close"
        out.append({**c, "grades": g, "headline": g[basis][0]})
    return out


# ------------------------------------------------------------------ stats

def _score(status):
    return {HIT: 1.0, MISS: 0.0, PARTIAL: 0.5}.get(status)


def by_category(graded):
    rows = []
    for cat in CATEGORIES:
        sub = [g for g in graded if g["category"] == cat]
        if not sub:
            continue
        counts = defaultdict(int)
        for g in sub:
            counts[g["headline"]] += 1
        scored = [_score(g["headline"]) for g in sub
                  if _score(g["headline"]) is not None]
        rate = sum(scored) / len(scored) if scored else None
        rows.append({"category": cat, "n": len(sub), "counts": dict(counts),
                     "graded": len(scored), "rate": rate})
    return rows


def basis_gap(graded):
    """How often close and intraday disagree, over priced level tests."""
    both = [g for g in graded if g["test"] in LEVELLED
            and all(g["grades"][b][0] in (HIT, MISS) for b in BASES)]
    split = [g for g in both
             if g["grades"]["close"][0] != g["grades"]["intraday"][0]]
    return len(both), split


def calibration(graded):
    pts = [(g["conf"], _score(g["headline"])) for g in graded
           if g.get("conf") is not None and g["headline"] in (HIT, MISS)]
    if not pts:
        return None
    buckets = [("under 50%", 0, 0.5), ("50%", 0.5, 0.55),
               ("60-70%", 0.55, 0.75), ("80%+", 0.75, 1.01)]
    rows = []
    for name, lo, hi in buckets:
        sub = [(p, o) for p, o in pts if lo <= p < hi]
        if sub:
            rows.append({"bucket": name, "n": len(sub),
                         "stated": sum(p for p, _ in sub) / len(sub),
                         "actual": sum(o for _, o in sub) / len(sub)})
    return {"n": len(pts), "expected": sum(p for p, _ in pts),
            "actual": sum(o for _, o in pts),
            "brier": sum((p - o) ** 2 for p, o in pts) / len(pts),
            "buckets": rows}


def repeated_levels(graded):
    groups = defaultdict(list)
    for g in graded:
        if g["test"] in LEVELLED and g["ticker"] != "-":
            groups[(g["ticker"], round(float(g["level"])))].append(g)
    return {k: sorted(v, key=lambda g: g["made"])
            for k, v in sorted(groups.items()) if len(v) >= 2}


def drift_chains(graded, conn):
    by_id = {g["id"]: g for g in graded}
    successors = {g.get("supersedes") for g in graded if g.get("supersedes")}
    chains = []
    for g in graded:
        if not g.get("supersedes") or g["id"] in successors:
            continue                          # start from the newest link
        chain, cur = [], g
        while cur:
            _, px = (close_on_or_before(conn, cur["ticker"], cur["made"])
                     if cur["ticker"] != "-" else (None, None))
            chain.append((cur, px))
            cur = by_id.get(cur.get("supersedes"))
        chains.append(list(reversed(chain)))
    return chains


# ------------------------------------------------------------------ page

CSS_FALLBACK = """
:root { --ink:#14202B; --paper:#EDF0F2; --card:#FFFFFF; --rule:#C7D0D6;
  --signal:#1B3FD8; --muted:#6B7B87; --warm:#B4630A; --cool:#1736D0; }
@media (prefers-color-scheme: dark) { :root { --ink:#E1E9EF; --paper:#080E13;
  --card:#151C21; --rule:#2B343B; --signal:#7EA2FF; --muted:#8E9AA4;
  --warm:#CE7C21; --cool:#5A86F5; } }
* { box-sizing: border-box; }
body { margin:0; padding:28px 20px 80px; background:var(--paper); color:var(--ink);
  font-family:"IBM Plex Sans",-apple-system,system-ui,sans-serif; font-size:15px;
  line-height:1.45; }
.wrap { max-width:860px; margin:0 auto; }
h1 { font-size:26px; margin:0 0 4px; }
.meta { color:var(--muted); font-size:12.5px; margin:0 0 18px; }
"""

PAGE_CSS = """
.c-h2 { font-size:16px; margin:26px 0 6px; }
.c-note { color:var(--muted); font-size:12.5px; margin:4px 0 10px; }
.c-card { background:var(--card); border:1px solid var(--rule); border-radius:6px;
  padding:12px 14px; margin:0 0 12px; overflow-x:auto; }
table.c { border-collapse:collapse; width:100%; font-size:12.5px; }
table.c th { text-align:left; color:var(--muted); font-weight:500;
  border-bottom:1px solid var(--rule); padding:4px 6px; white-space:nowrap; }
table.c td { border-bottom:1px solid var(--rule); padding:5px 6px;
  vertical-align:top; }
table.c td.n { text-align:right; font-variant-numeric:tabular-nums;
  white-space:nowrap; }
.mono { font-family:"IBM Plex Mono",monospace; font-size:11.5px;
  white-space:nowrap; }
.st { font-family:"IBM Plex Mono",monospace; font-size:10px; letter-spacing:.06em;
  padding:1px 6px; border-radius:2px; white-space:nowrap; display:inline-block;
  min-width:58px; text-align:center; }
.st.HIT { background:var(--cool); color:var(--card); font-weight:600; }
.st.MISS { background:var(--warm); color:var(--card); font-weight:600; }
.st.PARTIAL { color:var(--ink); border:1px solid var(--muted); }
.st.PENDING { color:var(--muted); border:1px solid var(--rule); }
/* UNPRICED must not look like PENDING: one is waiting, the other is an
   outage, and rendering them alike turns a data hole into patience. */
.st.UNPRICED { color:var(--warm); border:1px dashed var(--warm); font-weight:600; }
.bar { height:8px; background:var(--rule); border-radius:4px; min-width:90px;
  position:relative; }
.bar i { position:absolute; left:0; top:0; bottom:0; border-radius:4px;
  background:var(--signal); }
.dim { color:var(--muted); }
.drift { font-size:12.5px; margin:4px 0; }
"""


def _st(status, title=""):
    t = f' title="{html.escape(title)}"' if title else ""
    return f'<span class="st {status}"{t}>{status}</span>'


def _pct(x):
    return "—" if x is None else f"{x * 100:.0f}%"


def render(graded, conn, path):
    try:
        import edgar_discovery as ed            # one stylesheet for the site
        css = ed.CSS
    except Exception:                            # noqa: BLE001 -- page must build
        css = CSS_FALLBACK
    esc = html.escape
    priced = [g for g in graded if g["test"] in PRICED]
    unpriced = [g for g in priced if g["headline"] == UNPRICED]
    pending = [g for g in graded if g["headline"] == PENDING]
    through = max((last_day(conn, t) or "" for t in
                   {g["ticker"] for g in priced}), default="") or "—"

    # categories
    cat_rows = []
    for r in sorted(by_category(graded), key=lambda r: -(r["rate"] or -1)):
        c = r["counts"]
        width = 0 if r["rate"] is None else r["rate"] * 100
        cat_rows.append(
            f"<tr><td>{esc(CATEGORY_LABEL[r['category']])}</td>"
            f"<td class=n>{c.get(HIT, 0)}</td><td class=n>{c.get(MISS, 0)}</td>"
            f"<td class=n>{c.get(PARTIAL, 0)}</td>"
            f"<td class=n>{c.get(PENDING, 0) + c.get(UNPRICED, 0)}</td>"
            f"<td class=n><b>{_pct(r['rate'])}</b></td>"
            f"<td><div class=bar><i style='width:{width:.0f}%'></i></div></td></tr>")
    cat_html = (
        "<table class=c><tr><th>Call type</th><th>Hit</th><th>Miss</th>"
        "<th>Partial</th><th>Open</th><th>Hit rate</th><th></th></tr>"
        + "".join(cat_rows) + "</table>")

    # calibration
    cal = calibration(graded)
    if cal:
        brows = "".join(
            f"<tr><td>{esc(b['bucket'])}</td><td class=n>{b['n']}</td>"
            f"<td class=n>{_pct(b['stated'])}</td>"
            f"<td class=n><b>{_pct(b['actual'])}</b></td></tr>"
            for b in cal["buckets"])
        cal_html = (
            f"<p class=c-note>{cal['n']} calls carried a stated probability. "
            f"Those probabilities summed to <b>{cal['expected']:.1f}</b> expected "
            f"hits; <b>{cal['actual']:.0f}</b> hit. Brier score "
            f"{cal['brier']:.3f} (0 is perfect; always saying 50% scores 0.250). "
            f"Few calls per bucket -- read the direction, not the decimals.</p>"
            "<table class=c><tr><th>Stated</th><th>Calls</th><th>Avg stated</th>"
            f"<th>Actual</th></tr>{brows}</table>")
    else:
        cal_html = "<p class=c-note>No graded calls with a stated probability yet.</p>"

    # close vs intraday
    n_both, split = basis_gap(graded)
    split_rows = "".join(
        f"<tr><td class=mono>{esc(g['made'])}</td><td>{esc(g['say'])}</td>"
        f"<td>{_st(g['grades']['close'][0])}</td>"
        f"<td>{_st(g['grades']['intraday'][0])}</td>"
        f"<td class=dim>{esc(g['grades']['intraday'][1])}</td></tr>"
        for g in split)
    gap_html = (
        f"<p class=c-note>{len(split)} of {n_both} priced level calls grade "
        "differently on the close than intraday. These are the ones a self-graded "
        "scorecard can claim either way.</p>"
        + ("<table class=c><tr><th>Made</th><th>Call</th><th>Close</th>"
           f"<th>Intraday</th><th></th></tr>{split_rows}</table>" if split else ""))

    # repeated levels
    lvl_rows = []
    for (ticker, lvl), items in repeated_levels(graded).items():
        marks = " ".join(_st(g["headline"], f"{g['made']}: {g['say']}")
                         for g in items)
        lvl_rows.append(f"<tr><td class=mono>{esc(ticker)} {lvl:,}</td>"
                        f"<td class=n>{len(items)}</td><td>{marks}</td></tr>")
    lvl_html = (("<table class=c><tr><th>Level</th><th>Times named</th>"
                 "<th>Outcomes, oldest first</th></tr>" + "".join(lvl_rows)
                 + "</table>") if lvl_rows else
                "<p class=c-note>No level named more than once yet.</p>")

    # drift
    drift_html = ""
    for chain in drift_chains(graded, conn):
        steps = " &rarr; ".join(
            f"<b>{float(c['level']):,g}</b> <span class=dim>(said {esc(c['made'])}"
            + (f", price {px:,.2f}" if px is not None else "") + ")</span>"
            for c, px in chain)
        tgt = chain[-1][0]
        drift_html += (f"<p class=drift><span class=mono>{esc(tgt['ticker'])}</span> "
                       f"target by {esc(tgt['by'])}: {steps} "
                       f"{_st(tgt['headline'])}</p>")
    if not drift_html:
        drift_html = "<p class=c-note>No restated targets yet.</p>"

    # ledger
    led_rows = []
    for g in sorted(graded, key=lambda g: (g["made"], g["id"]), reverse=True):
        lvl = (f"{float(g['level']):,g}" if g.get("level") is not None else "")
        gc, gi = g["grades"]["close"], g["grades"]["intraday"]
        intraday = ("" if g["test"] in ("manual", "up", "down")
                    else _st(gi[0], gi[1]))
        hb = g.get("basis", "close") if g["test"] in LEVELLED else "close"
        detail = g.get("note") or g["grades"][hb][1] or ""
        led_rows.append(
            f"<tr><td class=mono>{esc(g['made'])}</td>"
            f"<td class=mono>{esc(g['ticker'])}</td>"
            f"<td>{esc(g['say'])}</td>"
            f"<td class=n>{'' if g.get('conf') is None else _pct(g['conf'])}</td>"
            f"<td class=dim>{esc(g['category'])}</td>"
            f"<td>{_st(gc[0], gc[1])}</td><td>{intraday}</td>"
            f"<td class=dim>{esc(detail)}</td></tr>")
    ledger_html = (
        "<table class=c><tr><th>Made</th><th></th><th>Call</th><th>Conf</th>"
        "<th>Type</th><th>Close</th><th>Intraday</th><th>Evidence</th></tr>"
        + "".join(led_rows) + "</table>")

    unpriced_note = (
        f"<p class=c-note><b>{len(unpriced)} unpriced</b>: "
        + ", ".join(sorted({esc(g['ticker']) for g in unpriced}))
        + ". Counted, not dropped.</p>") if unpriced else ""

    doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Call ledger</title>
<style>{css}{PAGE_CSS}</style>
</head><body><div class="wrap">
<h1>Call ledger</h1>
<p class="meta">{len(graded)} calls &nbsp;·&nbsp; {len(pending)} open &nbsp;·&nbsp;
prices through {esc(through)} &nbsp;·&nbsp; built {datetime.now().strftime("%Y-%m-%d %H:%M")}
&nbsp;·&nbsp; <a href="index.html">Signals</a></p>
<p class="c-note">Dated calls from a daily market newsletter, entered once on the
day they first appeared and graded against prices afterward. Manual grades
(regime, risk, reasoning) cite their evidence in the ledger below.</p>
{unpriced_note}
<h2 class="c-h2">Which kinds of call carry information</h2>
<div class="c-card">{cat_html}</div>
<h2 class="c-h2">Stated confidence versus outcome</h2>
<div class="c-card">{cal_html}</div>
<h2 class="c-h2">Close versus intraday</h2>
<div class="c-card">{gap_html}</div>
<h2 class="c-h2">Levels named more than once</h2>
<div class="c-card">{lvl_html}</div>
<h2 class="c-h2">Target drift</h2>
<div class="c-card">{drift_html}</div>
<h2 class="c-h2">Ledger</h2>
<div class="c-card">{ledger_html}</div>
</div></body></html>"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh:
        fh.write(doc)
    return path


# ------------------------------------------------------------------ selftest

def selftest():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""CREATE TABLE prices (ticker TEXT, day TEXT, open REAL,
        high REAL, low REAL, close REAL, volume REAL,
        PRIMARY KEY (ticker, day));""")
    bars = [  # day, open, high, low, close
        ("2026-11-02", 100, 102, 99, 101),
        ("2026-11-03", 101, 106, 97, 104),     # dips under 98 intraday only
        ("2026-11-04", 104, 105, 95, 96),      # closes under 98
        ("2026-11-05", 96, 99, 94, 97),
    ]
    conn.executemany("INSERT INTO prices VALUES ('X',?,?,?,?,?,0)", bars)

    def call(**kw):
        base = {"id": "t", "made": "2026-11-02", "by": "2026-11-03",
                "ticker": "X", "category": "level", "say": "t", "conf": None}
        base.update(kw)
        return grade(base, conn)

    def both(g):
        return g["close"][0], g["intraday"][0]

    assert both(call(test="hold", level=98)) == (HIT, MISS), "intraday break only"
    assert both(call(test="hold", level=98, by="2026-11-04")) == (MISS, MISS)
    assert both(call(test="reach", level=105)) == (MISS, HIT), "touch, no close"
    assert both(call(test="reach", level=103)) == (HIT, HIT)
    assert both(call(test="under", level=105)) == (HIT, MISS)
    assert both(call(test="up", by="2026-11-03")) == (HIT, HIT)
    assert both(call(test="down", by="2026-11-04")) == (HIT, HIT)
    assert both(call(test="down", by="2026-11-03")) == (MISS, MISS)
    # window past the data: open unless already settled
    assert both(call(test="hold", level=90, by="2026-12-31")) == (PENDING, PENDING)
    assert both(call(test="hold", level=98, by="2026-12-31")) == (MISS, MISS)
    assert both(call(test="reach", level=103, by="2026-12-31")) == (HIT, HIT)
    assert both(call(test="reach", level=200, by="2026-12-31")) == (PENDING, PENDING)
    assert both(call(test="up", by="2026-12-31")) == (PENDING, PENDING)
    # no prices at all is an outage, not a miss
    assert both(call(test="hold", level=1, ticker="NOPE")) == (UNPRICED, UNPRICED)
    # a window inside the data with no bars (e.g. before listing)
    assert both(call(test="hold", level=1, made="2026-10-01",
                     by="2026-10-02")) == (UNPRICED, UNPRICED)
    # manual
    assert both(call(test="manual", grade="partial")) == (PARTIAL, PARTIAL)

    # validator catches the shapes that would silently drop a call
    bad = {"calls": [
        {"id": "a", "made": "2026-11-02", "by": "2026-11-01", "ticker": "X",
         "test": "hold", "category": "level", "say": "s"},
        {"id": "a", "made": "2026-11-02", "by": "2026-11-03", "ticker": "-",
         "test": "reach", "level": 1, "category": "nope", "say": "s"},
        {"id": "b", "made": "2026-11-02", "by": "2026-11-03", "ticker": "X",
         "test": "manual", "category": "risk", "say": "s", "supersedes": "zz"},
    ]}
    msgs = " | ".join(check(bad))
    for needle in ("by is before made", "needs a numeric level", "duplicate id",
                   "needs a real ticker", "category must be",
                   "needs grade", "supersedes unknown"):
        assert needle in msgs, f"validator missed: {needle}"

    # calibration arithmetic
    g = [{"conf": 0.8, "headline": HIT}, {"conf": 0.4, "headline": MISS},
         {"conf": None, "headline": HIT}, {"conf": 0.5, "headline": PENDING}]
    cal = calibration(g)
    assert cal["n"] == 2 and abs(cal["expected"] - 1.2) < 1e-9
    assert abs(cal["brier"] - (0.04 + 0.16) / 2) < 1e-9
    print("selftest ok")


# ------------------------------------------------------------------ main

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--calls", default="calls.json")
    ap.add_argument("--out", default="docs/calls.html")
    ap.add_argument("--check", action="store_true", help="validate only")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--fetch", action="store_true",
                    help="refresh prices for every ticker in the ledger first")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return 0

    data = load(args.calls)
    problems = check(data)
    if problems:
        for p in problems:
            print(f"::error::{p}")
        return 1
    calls = data["calls"]
    print(f"{args.calls}: {len(calls)} calls, valid")
    if args.check:
        return 0

    import market_data as md
    conn = md.connect()
    tickers = sorted({c["ticker"] for c in calls if c["test"] in PRICED})
    if args.fetch:
        fetched, cached, failed = md.refresh(conn, tickers, max_age_days=0)
        print(f"prices: {fetched} fetched, {cached} cached, {failed} failed")

    graded = grade_all(calls, conn)
    counts = defaultdict(int)
    for g in graded:
        counts[g["headline"]] += 1
    print("headline:", ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    for r in by_category(graded):
        print(f"  {r['category']:<12} {_pct(r['rate']):>5}  ({r['graded']} graded)")
    cal = calibration(graded)
    if cal:
        print(f"  calibration: {cal['expected']:.1f} expected, "
              f"{cal['actual']:.0f} actual, brier {cal['brier']:.3f}")
    print("wrote", render(graded, conn, args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
