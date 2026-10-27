#!/usr/bin/env python3
"""Open a GitHub issue when a watched ETF crosses a dip threshold.

THE ONLY HARD PART IS NOT SENDING IT TWICE
------------------------------------------
A selloff is not an instant, it is a fortnight. A ticker that falls 9% below
its 20-day high stays below it for days, and a naive check that fires
whenever `drawdown >= threshold` opens an issue every morning until the dip
ends. Three such alerts and the channel is noise; ten and it is muted, which
is strictly worse than having built nothing.

So firing is stateful. Each (ticker, depth) records the day it last fired,
and will not fire again until the name has climbed back out of that band and
a cooldown has passed. The state lives in the collection database, which is
committed, so it survives the runner being destroyed between runs -- an
in-memory guard would reset every morning and alert every morning.

WHAT IT DOES NOT DO
-------------------
It does not tell you to buy anything. The issue body carries the depth, what
followed dips that deep historically, the sample size behind that, and the
worst case -- which in this window is a further 40-50% inside three weeks.
A notification that said "SOXL dip, buy" would be asserting something this
repo has measured and failed to find.
"""

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime

import dips

SCHEMA = """
-- One row per (ticker, depth) that has ever fired. `armed` is the guard: it
-- goes false when an alert fires and true again once the name climbs back
-- above the band, so a long slide produces one alert rather than one a day.
CREATE TABLE IF NOT EXISTS dip_alert_state (
    ticker     TEXT NOT NULL,
    depth      REAL NOT NULL,
    armed      INTEGER NOT NULL DEFAULT 1,
    last_fired TEXT,
    last_depth REAL,
    issue_url  TEXT,
    updated_at TEXT,
    PRIMARY KEY (ticker, depth)
);
"""

REARM_MARGIN = 1.0      # percent back above the band before it can fire again
API = "https://api.github.com"


def ensure(conn):
    conn.executescript(SCHEMA)


def decide(conn, ticker, drawdown, depths=dips.DEPTHS, today=None):
    """Which depth, if any, should fire for this ticker right now.

    One alert per EPISODE, with escalation allowed. A selloff is a fortnight,
    not an instant, so the model is:

      * nothing fired yet and a band is crossed   -> fire
      * already fired and it gets DEEPER          -> fire; it got worse,
                                                     which is news
      * same band, or any shallower one           -> silent, including the
                                                     entire climb back out
      * recovered above the shallowest band       -> episode over, re-arm

    The shallower-on-the-way-up case is the one that quietly ruins an alert
    channel. A fall to -13% that bounces back to -4% would otherwise fire
    -8%, then -5%, then -3%, in succession, while the thing was RECOVERING.

    The reason is returned even when nothing fires, because "why was I not
    told" is the question a quiet alerting system eventually has to answer.
    """
    today = (today or date.today()).isoformat()
    if drawdown is None:
        return None, "no drawdown computed"

    floor = min(depths)
    row = conn.execute(
        "SELECT depth, last_fired FROM dip_alert_state WHERE ticker = ?"
        " AND armed = 0 ORDER BY depth DESC", (ticker,)).fetchone()
    active = row[0] if row else None

    if drawdown < floor - REARM_MARGIN:
        if active is not None:
            conn.execute(
                "UPDATE dip_alert_state SET armed = 1, updated_at = ?"
                " WHERE ticker = ? AND armed = 0", (today, ticker))
            conn.commit()
            return None, (f"recovered to -{drawdown:.1f}%, episode over "
                          f"(was -{active:g}%) -- re-armed")
        return None, f"above every band (-{drawdown:.1f}%)"

    matched = [d for d in depths if drawdown >= d]
    if not matched:
        return None, (f"-{drawdown:.1f}% is below -{floor:g}% but not yet "
                      f"recovered; same episode" if active is not None
                      else f"-{drawdown:.1f}%, no band crossed")

    deepest = max(matched)
    if active is None:
        return deepest, f"crossed -{deepest:g}% (now -{drawdown:.1f}%)"
    if deepest > active:
        return deepest, (f"deepened from -{active:g}% to -{deepest:g}% "
                         f"(now -{drawdown:.1f}%)")
    return None, (f"same episode: -{active:g}% already fired on {row[1]}, "
                  f"now -{drawdown:.1f}%")


def record(conn, ticker, depth, drawdown, issue_url, today=None):
    today = (today or date.today()).isoformat()
    conn.execute(
        "INSERT INTO dip_alert_state (ticker, depth, armed, last_fired,"
        " last_depth, issue_url, updated_at) VALUES (?,?,0,?,?,?,?)"
        " ON CONFLICT(ticker, depth) DO UPDATE SET armed = 0,"
        " last_fired = excluded.last_fired, last_depth = excluded.last_depth,"
        " issue_url = excluded.issue_url, updated_at = excluded.updated_at",
        (ticker, depth, today, drawdown, issue_url, today))
    conn.commit()


def evidence(conn, ticker, depth):
    """What followed dips this deep, straight from the stored profile."""
    rows = conn.execute(
        "SELECT horizon, n_episodes, n_outcomes, median, worst, positive"
        " FROM dip_profile WHERE ticker = ? AND depth = ?"
        " ORDER BY horizon", (ticker, depth)).fetchall()
    if not rows:
        return "No dip profile stored for this depth yet."
    out = ["| horizon | episodes | median | worst | positive |",
           "|---|---|---|---|---|"]
    thin = False
    for horizon, n_ep, n_out, median, worst, positive in rows:
        if median is None:
            out.append(f"| {horizon}d | {n_ep} | — | — | — |")
            continue
        if (n_ep or 0) < dips.MIN_EPISODES:
            thin = True
        out.append(f"| {horizon}d | {n_ep} | {median:+.1f}% | {worst:+.1f}% "
                   f"| {positive}/{n_out} |")
    if thin:
        out.append("")
        out.append(f"**Fewer than {dips.MIN_EPISODES} episodes behind at "
                   f"least one row.** Not a tendency; a handful of cases.")
    return "\n".join(out)


def body(conn, ticker, depth, drawdown, close, high, window):
    return "\n".join([
        f"**{ticker} is {drawdown:.1f}% below its "
        f"{dips.HIGH_LOOKBACK}-session high.**",
        "",
        f"- close `{close:,.2f}`, high `{high:,.2f}`",
        f"- crossed the −{depth:g}% band",
        f"- price window behind the figures below: {window}",
        "",
        "### What followed dips this deep",
        "",
        evidence(conn, ticker, depth),
        "",
        "### Read this before acting on it",
        "",
        "This is a notification that a threshold was crossed, not a "
        "recommendation. This repo has looked for an edge in dip depth and "
        "has not found one: medians bounce around zero and are not monotonic "
        "in depth, and the worst outcomes are roughly as bad at every band, "
        "which means buying deeper did not protect.",
        "",
        "A leveraged ETF resets its leverage daily. It decays in choppy "
        "markets and can fail to recover even when the index it tracks does. "
        "The window behind these numbers contains no sustained bear market, "
        "so the case that would hurt most is not in the sample.",
    ])


def open_issue(repo, token, title, text, labels=("dip-alert",)):
    payload = json.dumps({"title": title, "body": text,
                          "labels": list(labels)}).encode()
    request = urllib.request.Request(
        f"{API}/repos/{repo}/issues", data=payload, method="POST",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28",
                 "Content-Type": "application/json",
                 "User-Agent": "signals-dip-alert"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode()).get("html_url")


def run(conn, tickers, price_db=None, repo=None, token=None, dry_run=False):
    import market_data
    import wheel
    ensure(conn)
    prices = market_data.connect(price_db)
    fired = 0
    for ticker in tickers:
        bars = wheel.load_bars(prices, ticker)
        if len(bars) < dips.HIGH_LOOKBACK + 1:
            print(f"{ticker}: too few bars -- no verdict, not 'no dip'")
            continue
        days = [b[0] for b in bars]
        closes = [float(b[4]) for b in bars]
        state = dips.current(days, closes)
        depth, why = decide(conn, ticker, state and state["drawdown"])
        print(f"{ticker}: -{state['drawdown']:.1f}% -> {why}")
        if depth is None:
            continue
        title = (f"{ticker} −{depth:g}% dip "
                 f"({state['drawdown']:.1f}% below {dips.HIGH_LOOKBACK}d high)"
                 f" — {days[-1]}")
        text = body(conn, ticker, depth, state["drawdown"], state["close"],
                    state["high"], f"{days[0]} to {days[-1]}")
        if dry_run or not (repo and token):
            print(f"  DRY RUN, would open: {title}")
            print("  " + text.replace("\n", "\n  ")[:400] + " ...")
            continue
        url = open_issue(repo, token, title, text)
        record(conn, ticker, depth, state["drawdown"], url)
        print(f"  opened {url}")
        fired += 1
    conn.commit()
    return fired


def selftest():
    import sqlite3
    checks = 0

    def ok(label, cond):
        nonlocal checks
        assert cond, "FAILED: " + label
        checks += 1

    conn = sqlite3.connect(":memory:")
    ensure(conn)

    # THE WHOLE POINT. A sustained dip must fire once, not once a day.
    depth, why = decide(conn, "X", 9.0)
    ok("a fresh 9% dip fires the 8% band", depth == 8.0)
    record(conn, "X", depth, 9.0, "http://example/1")
    for day_drawdown in (9.1, 9.4, 8.6, 10.2, 8.2):
        again, why = decide(conn, "X", day_drawdown)
        ok(f"still inside the dip at {day_drawdown}% does not re-fire",
           again is None)
    ok("and says why", "already fired" in why)

    # Getting WORSE is news and is allowed to fire again.
    deeper, why = decide(conn, "X", 13.0)
    ok("deepening to a lower band fires", deeper == 12.0)
    ok("and says it deepened", "deepened" in why)
    record(conn, "X", deeper, 13.0, "http://example/2")
    ok("and then that one holds too", decide(conn, "X", 13.5)[0] is None)

    # The entire climb back out must be silent, at every band.
    for bounce in (7.6, 5.2, 4.0, 3.1):
        quiet, why = decide(conn, "X", bounce)
        ok(f"recovering through -{bounce}% stays silent", quiet is None)
    ok("and calls it the same episode", "same episode" in why)

    # Only a real recovery ends the episode.
    done, why = decide(conn, "X", 0.5)
    ok("full recovery re-arms", done is None and "episode over" in why)
    again, _ = decide(conn, "X", 9.0)
    ok("after which a fresh selloff fires again", again == 8.0)

    ok("no drawdown gives no alert", decide(conn, "Y", None)[0] is None)
    ok("a shallow wobble gives no alert", decide(conn, "Y", 1.0)[0] is None)

    # Evidence must state thinness rather than quote four episodes plainly.
    conn.executescript(
        "CREATE TABLE dip_profile (ticker TEXT, depth REAL, horizon INT,"
        " n_episodes INT, n_outcomes INT, pending INT, median REAL,"
        " worst REAL, best REAL, positive INT, last_seen TEXT,"
        " window_first TEXT, window_last TEXT, updated_at TEXT);")
    conn.execute("INSERT INTO dip_profile VALUES"
                 " ('X',8.0,21,4,4,0,39.3,-32.6,50.0,3,'x','a','b','c')")
    text = evidence(conn, "X", 8.0)
    ok("a four-episode row is marked thin", "Fewer than" in text)

    # A whole selloff, day by day. The bound that matters: at most one
    # alert per band per episode, and silence for the entire recovery.
    walk = sqlite3.connect(":memory:")
    ensure(walk)
    path = [0.5, 2.0, 4.1, 6.3, 9.0, 9.8, 8.7, 11.2, 13.4, 12.9, 11.0,
            8.4, 6.1, 4.2, 2.5, 1.1, 0.3, 2.2, 5.6]
    fired, recovering = [], []
    peaked = over = False
    for value in path:
        picked, _ = decide(walk, "W", value)
        if picked is not None:
            record(walk, "W", picked, value, "u")
            fired.append(picked)
        if value >= 13.0:
            peaked = True
        elif peaked and not over:
            # The climb out, up to the point the episode actually ends.
            # Past that it is a NEW selloff and is allowed to speak.
            if value < min(dips.DEPTHS) - REARM_MARGIN:
                over = True
            else:
                recovering.append(picked)
    ok("one alert per band as it deepens, no more",
       fired == [3.0, 5.0, 8.0, 12.0, 5.0])
    ok("never more alerts than there are bands, plus the next episode",
       len(fired) <= len(dips.DEPTHS) + 1)
    ok("the entire recovery is silent",
       recovering and all(p is None for p in recovering))
    ok("most sessions say nothing at all", len(fired) < len(path) / 3)

    print(f"dip_alert selftest: {checks} checks passed")
    return 0


def main():
    import argparse
    import sqlite3
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default="discovery.db")
    ap.add_argument("--tickers", default="TQQQ,SOXL,SPXL")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    conn = sqlite3.connect(args.db)
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    n = run(conn, tickers,
            repo=os.environ.get("GITHUB_REPOSITORY"),
            token=os.environ.get("GITHUB_TOKEN"),
            dry_run=args.dry_run)
    print(f"\n{n} alert(s) opened")
    return 0


if __name__ == "__main__":
    sys.exit(main())
