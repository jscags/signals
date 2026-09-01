"""Did the signal find a good entry? Measured honestly, or not at all.

A backtest is a machine for turning bad data into confident numbers, and it
lies in a small number of well-known ways. Each one is refused explicitly here,
because the alternative is a result that looks like an edge and is an artefact.

1. LOOK-AHEAD. A Form 4 reports a transaction up to two business days after it
   happened, and the daily index carrying it does not publish until roughly
   22:00 ET. Nobody could act on the transaction date. Entry is therefore the
   OPEN of the first session strictly after the filing became public -- never
   the transaction date, never the filing day's close.

2. SURVIVORSHIP. A delisted ticker returns no prices. Dropping those entries
   silently measures only the companies that made it, which is exactly the
   population that did well. Every entry that cannot be priced is counted and
   reported beside the result; coverage is part of the answer, not a footnote.

3. THE MARKET. A 3% gain in a month when the market rose 4% is a loss. Every
   horizon is reported as excess return against the benchmark over the
   IDENTICAL sessions, not against zero.

4. TRUNCATION. An entry whose 63-session horizon has not elapsed is excluded
   from the 63-session column rather than measured against the latest close.
   Otherwise long horizons quietly fill with short-horizon results, biased
   toward whatever the last few weeks did.

5. THE MEAN. One 10x drags a mean anywhere. Median and hit rate are reported
   beside it, and a mean that disagrees with its median is saying the result
   rests on a handful of names.

What this module does NOT do is model execution. There is no slippage, no
commission, no position sizing, no liquidity check. Reported numbers are the
return of the security itself, which is an upper bound on what anyone could
have captured.
"""

import json
import statistics
import sys
from datetime import date, datetime, timedelta

import market_data

HORIZONS = (5, 10, 21, 63)


class Entry:
    """One signal, resolved to a tradeable entry or explicitly not."""

    __slots__ = ("ticker", "cik", "rule", "signal_day", "entry_day",
                 "entry_price", "status")

    def __init__(self, ticker, cik, rule, signal_day):
        self.ticker = ticker
        self.cik = cik
        self.rule = rule
        self.signal_day = signal_day
        self.entry_day = None
        self.entry_price = None
        self.status = "unresolved"

    def __repr__(self):
        return (f"<Entry {self.ticker} {self.rule} {self.signal_day} "
                f"{self.status}>")


def resolve(conn, entry, max_gap_days=7):
    """Attach the first tradeable open strictly after the signal day.

    The gap cap matters. If a ticker's next available bar is three months
    after the signal, that is not an entry -- it is a halted or delisted
    security, and pricing it as though someone bought the reopening is how a
    backtest invents its best trades.
    """
    day_after = (date.fromisoformat(entry.signal_day) + timedelta(days=1)).isoformat()
    bar = market_data.next_open_on_or_after(conn, entry.ticker, day_after)
    if bar is None:
        entry.status = "no price"
        return entry
    gap = (date.fromisoformat(bar["day"]) - date.fromisoformat(entry.signal_day)).days
    if gap > max_gap_days:
        entry.status = f"stale ({gap}d gap)"
        return entry
    entry.entry_day = bar["day"]
    entry.entry_price = bar["open"]
    entry.status = "ok"
    return entry


def forward_return(conn, ticker, entry_day, entry_price, sessions):
    """Return over `sessions` trading days, or None if the window is short."""
    bar = market_data.close_n_sessions_after(conn, ticker, entry_day, sessions)
    if bar is None or not entry_price:
        return None
    return (bar["close"] - entry_price) / entry_price


def benchmark_return(conn, entry_day, sessions, benchmark=market_data.BENCHMARK):
    """The market over the same sessions, entered the same way.

    Entered at the benchmark's open on the entry day so the comparison spans
    the identical window; a benchmark measured close-to-close against a
    position entered at the open is a free half-day of drift.
    """
    bar = market_data.next_open_on_or_after(conn, benchmark, entry_day)
    if bar is None:
        return None
    after = market_data.close_n_sessions_after(conn, benchmark, bar["day"], sessions)
    if after is None:
        return None
    return (after["close"] - bar["open"]) / bar["open"]


def measure(conn, entries, horizons=HORIZONS, benchmark=market_data.BENCHMARK):
    """Excess returns per horizon, with the excluded population counted."""
    resolved, unpriced = [], []
    for entry in entries:
        resolve(conn, entry)
        (resolved if entry.status == "ok" else unpriced).append(entry)

    results = {}
    for n in horizons:
        excess, raw, truncated = [], [], 0
        for entry in resolved:
            r = forward_return(conn, entry.ticker, entry.entry_day,
                               entry.entry_price, n)
            b = benchmark_return(conn, entry.entry_day, n, benchmark)
            if r is None or b is None:
                truncated += 1          # window not elapsed: excluded, counted
                continue
            raw.append(r)
            excess.append(r - b)
        results[n] = _summarise(excess, raw, truncated)
    return {
        "entries": len(entries),
        "resolved": len(resolved),
        "unpriced": len(unpriced),
        "unpriced_detail": _reasons(unpriced),
        "coverage": (len(resolved) / len(entries)) if entries else 0.0,
        "horizons": results,
    }


def _reasons(entries):
    out = {}
    for e in entries:
        key = e.status.split(" (")[0]
        out[key] = out.get(key, 0) + 1
    return out


def _summarise(excess, raw, truncated):
    if not excess:
        return {"n": 0, "truncated": truncated}
    return {
        "n": len(excess),
        "truncated": truncated,
        "mean_excess": statistics.fmean(excess),
        "median_excess": statistics.median(excess),
        "hit_rate": sum(1 for x in excess if x > 0) / len(excess),
        "mean_raw": statistics.fmean(raw),
        "median_raw": statistics.median(raw),
        "stdev_excess": statistics.pstdev(excess) if len(excess) > 1 else 0.0,
        "best": max(excess),
        "worst": min(excess),
    }


def format_report(name, result):
    """A table that shows its own weaknesses, because they decide the reading."""
    lines = [f"── {name}",
             f"   entries {result['entries']}, priced {result['resolved']} "
             f"({result['coverage']*100:.0f}% coverage)"]
    if result["unpriced"]:
        detail = ", ".join(f"{k}: {v}" for k, v in
                           sorted(result["unpriced_detail"].items()))
        lines.append(f"   unpriced {result['unpriced']} ({detail})")
    lines.append(f"   {'horizon':>8} {'n':>5} {'excess':>9} {'median':>9} "
                 f"{'hit':>6} {'raw':>9} {'worst':>9}")
    for n, s in sorted(result["horizons"].items()):
        if not s["n"]:
            lines.append(f"   {str(n)+'d':>8} {0:>5}   (no window elapsed"
                         f"{'; ' + str(s['truncated']) + ' pending' if s['truncated'] else ''})")
            continue
        lines.append(
            f"   {str(n)+'d':>8} {s['n']:>5} {s['mean_excess']*100:>8.2f}% "
            f"{s['median_excess']*100:>8.2f}% {s['hit_rate']*100:>5.0f}% "
            f"{s['mean_raw']*100:>8.2f}% {s['worst']*100:>8.2f}%")
    return "\n".join(lines)


def entries_from_ledger(conn, since=None, until=None, apply_sale_override=True):
    """Reconstruct purchase and cluster signals, point-in-time correct.

    The live evaluator filters the ledger by transaction date, which is right
    for it: it runs today and everything filed is already known. A backtest
    cannot do that. A Form 4 reports a trade up to two business days late, and
    in this ledger 29% arrive later than that -- one by 3,075 days. Selecting
    on transaction date alone would let an entry dated March use a filing that
    did not exist until August, which is not a strategy, it is a time machine.

    So every window here is filtered on BOTH: the transaction inside the
    lookback, AND the filing already public on the signal day. The signal day
    is the filing date, never the transaction date, and the entry is the next
    session's open after it.

    `apply_sale_override` mirrors the shipped precedence, where insider selling
    outranks buying. Running it both ways answers a question the live system
    cannot: whether that override is protecting the reader or discarding good
    entries. DISTRIBUTING is 46% of the universe and half of it is one insider
    selling a median $1m, so which of those it is, is worth knowing.
    """
    import signal_state as ss

    where, args = [], []
    if since:
        where.append("d.filed_date >= ?")
        args.append(since)
    if until:
        where.append("d.filed_date <= ?")
        args.append(until)
    clause = (" AND " + " AND ".join(where)) if where else ""

    # Every buy, carrying the date it became public.
    buys = conn.execute(
        "SELECT b.issuer_cik cik, b.ticker, b.owner, b.txn_date, b.value,"
        " d.filed_date FROM insider_buys b"
        " JOIN documents d ON d.accession = b.accession"
        " WHERE COALESCE(b.suspect,0)=0 AND b.txn_date IS NOT NULL"
        " AND d.filed_date IS NOT NULL AND b.issuer_cik IS NOT NULL"
        " AND b.ticker IS NOT NULL AND b.ticker != ''" + clause,
        args).fetchall()
    sales = conn.execute(
        "SELECT s.issuer_cik cik, s.txn_date, s.value, d.filed_date"
        " FROM insider_sales s"
        " JOIN documents d ON d.accession = s.accession"
        " WHERE COALESCE(s.suspect,0)=0 AND s.txn_date IS NOT NULL"
        " AND d.filed_date IS NOT NULL AND s.issuer_cik IS NOT NULL").fetchall()

    by_cik = {}
    for row in buys:
        by_cik.setdefault(row["cik"], []).append(row)
    sales_by_cik = {}
    for row in sales:
        sales_by_cik.setdefault(row["cik"], []).append(row)

    entries, seen = [], set()
    for cik, rows in by_cik.items():
        # One evaluation per day this issuer had a filing become public.
        for day in sorted({r["filed_date"] for r in rows}):
            window_start = (date.fromisoformat(day)
                            - timedelta(days=ss.BUY_WINDOW_DAYS)).isoformat()
            live = [r for r in rows
                    if r["filed_date"] <= day
                    and window_start <= r["txn_date"] <= day]
            if not live:
                continue
            bought = sum(r["value"] or 0 for r in live)
            if bought < ss.MIN_MEANINGFUL_BUY_USD:
                continue

            if apply_sale_override:
                sale_start = (date.fromisoformat(day)
                              - timedelta(days=ss.SALE_WINDOW_DAYS)).isoformat()
                sold = sum(s["value"] or 0 for s in sales_by_cik.get(cik, ())
                           if s["filed_date"] <= day
                           and sale_start <= s["txn_date"] <= day)
                if (sold >= ss.MIN_MEANINGFUL_SALE_USD
                        and bought < ss.SELL_OVERRIDE_RATIO * sold):
                    continue                      # the shipped rule suppresses it

            tight = (date.fromisoformat(day)
                     - timedelta(days=ss.CLUSTER_WINDOW_DAYS)).isoformat()
            buyers = {r["owner"] for r in live
                      if r["owner"] and r["txn_date"] >= tight}
            rule = ("cluster" if len(buyers) >= ss.EXTENDED_MIN_BUYERS
                    else "purchase")

            # One entry per issuer per rule per signal day. A cluster that
            # keeps qualifying on consecutive filings is one idea, and counting
            # it five times would weight whichever names filed most often.
            key = (cik, rule, day)
            if key in seen:
                continue
            seen.add(key)
            entries.append(Entry(live[-1]["ticker"], cik, rule, day))
    entries.sort(key=lambda e: e.signal_day)
    return entries


def entries_from_transitions(conn, rules=("purchase", "cluster"),
                             states=("CONFIRMED", "EXTENDED", "SETUP")):
    """Recorded transitions as entries.

    Only usable for the days the collector has actually run. It is kept
    separate from the reconstructed history because the two are not the same
    evidence: recorded transitions were produced by whatever thresholds were
    live that day, and those changed more than once.
    """
    q = ("SELECT cik, ticker, rule, observed_on FROM state_transitions "
         "WHERE ticker IS NOT NULL AND ticker != '' ")
    args = []
    if rules:
        q += f"AND rule IN ({','.join('?' * len(rules))}) "
        args += list(rules)
    if states:
        q += f"AND to_state IN ({','.join('?' * len(states))}) "
        args += list(states)
    return [Entry(r["ticker"], r["cik"], r["rule"], r["observed_on"])
            for r in conn.execute(q + "ORDER BY observed_on", args)]


# --------------------------------------------------------------- cli

def _tickers_needed(entries):
    return sorted({e.ticker for e in entries if e.ticker})


def main(argv=None):
    import argparse
    import edgar_discovery as ed

    ap = argparse.ArgumentParser(
        description="Measure whether the screener's signals found good entries.")
    ap.add_argument("--source", choices=("ledger", "transitions"),
                    default="ledger",
                    help="reconstruct signals from the filing ledger "
                         "(point-in-time correct) or read recorded transitions")
    ap.add_argument("--since", help="earliest signal date (YYYY-MM-DD)")
    ap.add_argument("--until", help="latest signal date (YYYY-MM-DD)")
    ap.add_argument("--fetch-prices", action="store_true",
                    help="download any prices the entries need before measuring")
    ap.add_argument("--horizons", default=",".join(str(h) for h in HORIZONS))
    ap.add_argument("--json", metavar="PATH", help="write the full result as JSON")
    args = ap.parse_args(argv)

    horizons = tuple(int(h) for h in args.horizons.split(",") if h.strip())
    led = ed.connect()

    if args.source == "ledger":
        shipped = entries_from_ledger(led, args.since, args.until,
                                      apply_sale_override=True)
        no_override = entries_from_ledger(led, args.since, args.until,
                                          apply_sale_override=False)
    else:
        shipped = entries_from_transitions(led)
        no_override = []

    if not shipped and not no_override:
        print("No entries reconstructed. Nothing to measure -- this is a real\n"
              "answer, not an empty one: either the window holds no filings or\n"
              "no issuer cleared the floor.")
        return 1

    prices = market_data.connect()
    if args.fetch_prices:
        need = sorted(set(_tickers_needed(shipped) + _tickers_needed(no_override)))
        need.append(market_data.BENCHMARK)
        print(f"fetching prices for {len(need)} symbols "
              f"(~{len(need) * market_data.REQUEST_PACE / 60:.0f} min)...")
        got, cached, failed = market_data.refresh(prices, need)
        print(f"  {got} fetched, {cached} already current, {failed} unavailable")

    bench = prices.execute(
        "SELECT COUNT(*) FROM prices WHERE ticker = ?",
        (market_data.BENCHMARK,)).fetchone()[0]
    if not bench:
        print(f"\nNo benchmark data ({market_data.BENCHMARK}). Every number "
              "here would be\nan absolute return dressed up as an edge. "
              "Run with --fetch-prices first.")
        return 1

    out = {}
    groups = [("purchase (as shipped)",
               [e for e in shipped if e.rule == "purchase"]),
              ("cluster (as shipped)",
               [e for e in shipped if e.rule == "cluster"])]
    if no_override:
        groups += [("purchase (sale override OFF)",
                    [e for e in no_override if e.rule == "purchase"]),
                   ("cluster (sale override OFF)",
                    [e for e in no_override if e.rule == "cluster"])]

    print()
    for name, group in groups:
        if not group:
            print(f"── {name}\n   no entries\n")
            continue
        result = measure(prices, group, horizons=horizons)
        out[name] = result
        print(format_report(name, result))
        print()

    print("Excess is over " + market_data.BENCHMARK +
          " across the same sessions. No slippage, commission or position\n"
          "sizing is modelled, so these are an upper bound on what was capturable.")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump(out, fh, indent=2, default=str)
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
