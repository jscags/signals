"""Is the reconstructed history actually continuous?

MIN(filed_date) .. MAX(filed_date) is a range, not coverage. A backfill that
died in the middle of July leaves a database whose endpoints look perfect and
whose interior has a hole, and every statistic computed over it silently
describes the days that happened to survive. The out-of-sample result this
audit exists to check -- 2025-04-01 to 2026-02-28 -- is exactly that shape of
claim, so the window has to be proven dense before the number means anything.

THIS SCRIPT WRITES NOTHING. It opens the database read-only.

THE THREE CASES
---------------
A weekday with no filings is not one thing, and collapsing them is the same
mistake as reporting a 403 as "no triggers fired":

  NEVER SCANNED   no run_log row. The collector never looked. This is a hole
                  and anything measured across it is wrong.
  SCANNED, EMPTY  a run_log row and zero documents. Market holiday, almost
                  always -- EDGAR publishes no daily index. Not a hole.
  PARTIAL         a run_log row with n_refused > 0. The collector looked and
                  came back with less than the day held. The date is present,
                  the day is incomplete, and nothing downstream can tell.

The third is the dangerous one, because a partial day is indistinguishable
from a quiet day in every table built on top of it.
"""

import os
import sqlite3
import sys
from datetime import date, timedelta


def weekdays(start, end):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def as_ranges(days, index):
    """Collapse sorted dates into (first, last, n) runs, so a three-month hole
    prints as one line rather than sixty.

    Adjacency is measured in WEEKDAY POSITION, not calendar days. A calendar
    test has to allow 3 days so Friday joins Monday -- and that same tolerance
    silently fuses Tuesday with Friday when Wednesday and Thursday are fine,
    reporting one four-day hole where there are two one-day ones.
    """
    out = []
    for d in days:
        if out and index[d] == index[out[-1][1]] + 1:
            out[-1][1] = d
            out[-1][2] += 1
        else:
            out.append([d, d, 1])
    return [(a, b, n) for a, b, n in out]


def audit(path, since, until):
    if not os.path.exists(path):
        print(f"no {path}")
        return 1
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    docs = dict(conn.execute(
        "SELECT filed_date, COUNT(*) FROM documents GROUP BY filed_date"))
    if not docs:
        print("documents table is empty -- nothing to audit")
        return 1

    span_lo, span_hi = min(docs), max(docs)
    print(f"documents span {span_lo} .. {span_hi}  "
          f"({sum(docs.values()):,} rows on {len(docs)} distinct dates)")

    start = date.fromisoformat(since or span_lo)
    end = date.fromisoformat(until or span_hi)
    print(f"auditing    {start} .. {end}\n")

    try:
        log = {r[0]: (r[1], r[2] or 0, r[3] or 0) for r in conn.execute(
            "SELECT run_date, status, n_docs, n_refused FROM run_log "
            "WHERE source = 'edgar_daily'")}
    except sqlite3.OperationalError as exc:
        print(f"!! run_log unreadable ({exc}).")
        print("!! Without it, a day with no filings cannot be told apart from")
        print("!! a day that was never scanned, and this audit cannot answer")
        print("!! its own question. Reporting document dates only.")
        log = None

    expected = list(weekdays(start, end))
    index = {d: i for i, d in enumerate(expected)}
    never, empty, partial, bad_status = [], [], [], []
    for d in expected:
        iso = d.isoformat()
        n_docs = docs.get(iso, 0)
        if log is None:
            if not n_docs:
                never.append(d)
            continue
        row = log.get(iso)
        if row is None:
            never.append(d)
            continue
        status, _logged, refused = row
        if refused:
            partial.append((d, refused))
        if status and status.lower() not in ("ok", "success", "done"):
            bad_status.append((d, status))
        if not n_docs:
            empty.append(d)

    print(f"weekdays in window        {len(expected)}")
    if log is not None:
        print(f"run_log rows in window    "
              f"{sum(1 for d in expected if d.isoformat() in log)}")
    print(f"weekdays with documents   "
          f"{sum(1 for d in expected if docs.get(d.isoformat()))}")
    print()

    verdict = 0
    if never:
        verdict = 1
        label = ("NEVER SCANNED (no run_log row) -- these are real holes"
                 if log is not None else
                 "NO DOCUMENTS (run_log unavailable, cause unknown)")
        print(f"{label}: {len(never)} weekday(s)")
        for a, b, n in as_ranges(never, index):
            print(f"   {a} .. {b}   {n} day(s)")
        print()
    else:
        print("NEVER SCANNED: none. Every weekday in the window was visited.\n")

    if empty:
        print(f"SCANNED, ZERO DOCUMENTS: {len(empty)} day(s) "
              f"-- market holidays unless clustered")
        for a, b, n in as_ranges(empty, index):
            print(f"   {a} .. {b}   {n} day(s)")
        print()

    if partial:
        verdict = 1
        total = sum(n for _d, n in partial)
        print(f"PARTIAL DAYS (n_refused > 0): {len(partial)} day(s), "
              f"{total:,} filing(s) refused")
        print("   the date is present but the day is incomplete, and nothing")
        print("   downstream can tell the difference")
        for d, n in sorted(partial, key=lambda x: -x[1])[:10]:
            print(f"   {d}   {n:,} refused")
        print()

    if bad_status:
        verdict = 1
        print(f"NON-OK STATUS: {len(bad_status)} day(s)")
        for d, s in bad_status[:10]:
            print(f"   {d}   {s}")
        print()

    print("by month  (a month short of ~19-22 document-days is suspect)")
    months = {}
    for d in expected:
        iso = d.isoformat()
        m = iso[:7]
        got, docs_n = months.get(m, (0, 0))
        months[m] = (got + (1 if docs.get(iso) else 0), docs_n + docs.get(iso, 0))
    for m in sorted(months):
        got, n = months[m]
        flag = "  <-- thin" if got < 15 else ""
        print(f"   {m}   {got:>2} day(s)  {n:>7,} docs{flag}")

    print()
    print("VERDICT: " + ("window is dense; the measurement over it stands"
                         if verdict == 0 else
                         "window is NOT clean -- see above before trusting any"
                         " statistic computed across it"))
    return verdict


if __name__ == "__main__":
    sys.exit(audit(os.environ.get("EDGAR_DB", "history.db"),
                   os.environ.get("SINCE", ""), os.environ.get("UNTIL", "")))
