#!/usr/bin/env python3
"""The put-selling watchlist page: which tickers are set up, and which are not.

Deliberately a SECOND page rather than a rewrite of the dashboard. The EDGAR
lanes keep running; this answers a different question for a different kind of
trade, and merging them would produce a page that is about nothing in
particular.

It renders steps 1-3 and 8-9 and says plainly that it cannot render 4-7. The
free tier refuses a live options chain, so no strike and no premium appear
here. Pretending otherwise -- modelling a premium and printing it beside a
real price -- would put the least reliable number on the page in the same
typeface as the most reliable one.

The state is stored in the collection database as a small table, which is the
same rule the rest of the repo follows: prices are reproducible and live in a
gitignored file, conclusions are small and get committed.
"""

import argparse
import json
import sys
from datetime import date, datetime

import edgar_discovery as ed
import market_data
import wheel

SCHEMA = """
-- One row per watchlist ticker: the latest verdict and the inputs behind it.
-- Small and committed, unlike the prices it is derived from.
--
-- `why` is stored rather than recomputed at render time because the reason a
-- ticker is NOT set up is the most useful thing on the page, and a verdict
-- without it is a number that cannot be argued with.
-- What followed dips of each depth, per ticker. Small and committed; the
-- prices behind it are reproducible and stay in the gitignored file.
--
-- n_episodes counts SELLOFFS, not days. Consecutive days inside one slide
-- describe the same event, and counting them separately would turn a single
-- drawdown into a body of evidence.
CREATE TABLE IF NOT EXISTS dip_profile (
    ticker     TEXT NOT NULL,
    depth      REAL NOT NULL,
    horizon    INTEGER NOT NULL,
    n_episodes INTEGER,
    n_outcomes INTEGER,
    pending    INTEGER,
    median     REAL,
    worst      REAL,
    best       REAL,
    positive   INTEGER,
    last_seen  TEXT,
    window_first TEXT,
    window_last  TEXT,
    updated_at TEXT,
    PRIMARY KEY (ticker, depth, horizon)
);

CREATE TABLE IF NOT EXISTS wheel_state (
    ticker      TEXT PRIMARY KEY,
    as_of       TEXT,
    close       REAL,
    move_pct    REAL,
    clouds_green INTEGER,      -- NULL means not judgeable, not "no"
    qualifies   INTEGER,
    why         TEXT,
    green_streak INTEGER,
    last_qualified TEXT,
    qualified_count INTEGER,
    sessions    INTEGER,
    status      TEXT,          -- OK, or why there is no verdict
    updated_at  TEXT
);
"""

WHEEL_CSS = """
.w-note { font-size: 12.5px; color: var(--muted); line-height: 1.55;
          border-left: 2px solid var(--warm); padding-left: 10px;
          margin: 0 0 18px; }
.w-grid { display: grid; gap: 8px; margin: 0 0 18px;
          grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); }
.w-card { background: var(--card); border: 1px solid var(--rule);
          border-radius: 3px; padding: 12px 14px; min-width: 0; }
.w-card.set { border-color: var(--cool); }
.w-tick { font-family: "IBM Plex Mono", monospace; font-size: 15px;
          font-weight: 600; letter-spacing: .04em; }
.w-verdict { display: flex; align-items: baseline; gap: 8px;
             margin: 6px 0 8px; }
.w-verdict b { font-size: 22px; font-weight: 600; letter-spacing: -.01em; }
.w-why { font-family: "IBM Plex Mono", monospace; font-size: 11px;
         color: var(--muted); }
.w-rows { font-family: "IBM Plex Mono", monospace; font-size: 11px;
          color: var(--muted); margin-top: 8px; }
.w-rows div { display: flex; justify-content: space-between; gap: 10px;
              padding: 1px 0; }
.w-steps { font-size: 12.5px; line-height: 1.6; color: var(--muted); }
.w-steps li { margin-bottom: 3px; }
.w-steps .done { color: var(--ink); }
.w-find { margin: 0 0 18px; }
.w-find h2 { font-size: 13px; letter-spacing: .08em; text-transform: uppercase;
             color: var(--muted); font-weight: 600; margin: 0 0 8px;
             font-family: "IBM Plex Mono", monospace; }
.w-find table { width: 100%; border-collapse: collapse; font-size: 12px;
                font-family: "IBM Plex Mono", monospace; }
.w-find th, .w-find td { text-align: right; padding: 3px 6px;
                         border-bottom: 1px solid var(--rule);
                         font-variant-numeric: tabular-nums; font-weight: 400; }
.w-find thead th { color: var(--muted); font-size: 10px;
                   letter-spacing: .06em; }
.w-find tbody th { text-align: left; }
.w-find tr.ctrl { color: var(--muted); }
.w-find i { font-style: normal; color: var(--warm); }
.w-find td i { color: var(--muted); font-size: 10px; margin-left: 4px; }
.w-find .thin { color: var(--muted); }
.w-scope { font-size: 11px; color: var(--muted); line-height: 1.55;
           margin: 6px 0 0; }
@media (max-width: 560px) { .w-grid { grid-template-columns: 1fr; } }
"""


def refresh_state(conn, tickers, clouds=wheel.CLOUDS, drop_pct=wheel.DROP_PCT,
                  price_db=None):
    """Recompute each ticker's verdict from fresh prices. Returns the rows.

    A ticker whose prices could not be fetched is recorded with a status
    saying so, never dropped and never rendered as "not set up". Those are
    different statements and only one of them is true.
    """
    conn.executescript(SCHEMA)
    prices = market_data.connect(price_db)
    fetched, cached, failed = market_data.refresh(prices, list(tickers))
    print(f"prices: {fetched} fetched, {cached} cached, {failed} failed")

    out = []
    for ticker in tickers:
        bars = wheel.load_bars(prices, ticker)
        if not bars:
            row = (ticker, None, None, None, None, 0,
                   "no price data", 0, None, 0, 0, "NO PRICE DATA",
                   datetime.now().isoformat(timespec="seconds"))
        else:
            state = wheel.latest_state(bars, clouds, drop_pct)
            row = (ticker, state["day"], state["close"], state["move"],
                   None if state["clouds_green"] is None
                   else int(state["clouds_green"]),
                   int(state["qualifies"]), state["why"],
                   state["green_streak"], state["last_qualified"],
                   state["qualified_count"], len(bars), "OK",
                   datetime.now().isoformat(timespec="seconds"))
        conn.execute(
            "INSERT INTO wheel_state (ticker, as_of, close, move_pct,"
            " clouds_green, qualifies, why, green_streak, last_qualified,"
            " qualified_count, sessions, status, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(ticker) DO UPDATE SET as_of=excluded.as_of,"
            " close=excluded.close, move_pct=excluded.move_pct,"
            " clouds_green=excluded.clouds_green,"
            " qualifies=excluded.qualifies, why=excluded.why,"
            " green_streak=excluded.green_streak,"
            " last_qualified=excluded.last_qualified,"
            " qualified_count=excluded.qualified_count,"
            " sessions=excluded.sessions, status=excluded.status,"
            " updated_at=excluded.updated_at", row)
        out.append(row)
    conn.commit()
    return out


def render_findings(path="wheel_findings.json"):
    """What the backtest found, on the page rather than in an expired log.

    Rendered from the committed file so the page cannot quote a result the
    repository does not contain, and so the scope travels with the numbers.
    A missing file renders nothing at all: an absent backtest is not a
    backtest that found nothing.
    """
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return ""
    runs = data.get("runs") or {}
    if not runs:
        return ""
    # The most realistic rule leads; the others are shown beneath it because
    # the disagreement between them is the point.
    order = [k for k in ("wheel", "hold", "realize") if k in runs]
    lead = runs[order[0]]
    tickers = lead.get("tickers") or {}
    if not tickers:
        return ""
    params = lead.get("params") or {}
    window = lead.get("window") or {}

    rows = []
    for ticker, block in sorted(tickers.items()):
        filt = block.get("filtered")
        if not filt:
            continue
        def cell(d):
            """The headline column is the HONEST total, not the realised one.

            An arm with open positions reads +56.73% realised and -7.72% all
            in. Letting the first of those lead the page would be the same
            trick the harness was built to refuse, committed one layer up in
            the presentation.
            """
            if not d:
                return "<td>—</td><td>—</td><td>—</td>"
            open_n = d.get("open") or 0
            unreal = d.get("unrealised") or 0.0
            flag = (f' <i>+{open_n} open</i>' if open_n else "")
            total = d["sum"] + unreal
            shown = (f'{total:+.2f}%'
                     + (f' <i>({d["sum"]:+.2f}% realised)</i>'
                        if open_n else ""))
            return (f'<td>{d["n"]}{flag}</td>'
                    f'<td>{d["median"]:+.2f}%</td>'
                    f'<td>{shown}</td>')
        hold = filt.get("hold_sum")
        rows.append(
            f'<tr><th>{ed.html.escape(ticker)} filtered</th>{cell(filt)}'
            f'<td>{"—" if hold is None else f"{hold:+.2f}%"}</td></tr>')
        for name, ctrl in sorted((block.get("controls") or {}).items()):
            ch = (ctrl or {}).get("hold_sum")
            rows.append(
                f'<tr class="ctrl"><th>· control: {ed.html.escape(name)}</th>'
                f'{cell(ctrl)}'
                f'<td>{"—" if ch is None else f"{ch:+.2f}%"}</td></tr>')
    if not rows:
        return ""
    return (
        '<section class="w-find"><h2>What the backtest found</h2>'
        '<table><thead><tr><th>arm</th><th>trades</th><th>median</th>'
        '<th>total incl. open</th><th>owning it instead</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table>'
        f'<p class="w-scope">{ed.html.escape(window.get("first") or "?")} to '
        f'{ed.html.escape(window.get("last") or "?")} · clouds '
        f'{ed.html.escape(", ".join("/".join(map(str, c)) for c in params.get("clouds", [])))} '
        f'· {params.get("drop_pct", "?")}% red day · '
        f'{params.get("target_yield_pct", "?")}% target yield · on assignment: '
        f'{ed.html.escape(str(params.get("on_assignment", "?")))}</p>'
        + _rule_rows(runs, order)
        + f'<p class="w-scope"><b>Scope.</b> '
        f'{ed.html.escape(lead.get("scope_note", ""))}</p>'
        '</section>')


def _rule_rows(runs, order):
    """The same trades under each assignment rule, which disagree."""
    if len(order) < 2:
        return ""
    out = []
    for rule in order:
        block = (runs[rule].get("tickers") or {})
        for ticker, arms in sorted(block.items()):
            for name, d in [("filtered", arms.get("filtered"))] + sorted(
                    (arms.get("controls") or {}).items()):
                if not d:
                    continue
                open_n = d.get("open") or 0
                unreal = d.get("unrealised") or 0.0
                unreal_txt = f"{unreal:+.2f}% ({open_n})"
                total = d["sum"] + unreal
                out.append(
                    f'<tr><th>{ed.html.escape(rule)} · '
                    f'{ed.html.escape(ticker)} {ed.html.escape(name)}</th>'
                    f'<td>{d["n"]}</td><td>{d["sum"]:+.2f}%</td>'
                    f'<td>{"—" if not open_n else unreal_txt}</td>'
                    f'<td>{total:+.2f}%</td></tr>')
    return ("<h2>The same trades under each assignment rule</h2>"
            "<table><thead><tr><th>rule · arm</th><th>n</th>"
            "<th>realised</th><th>open</th><th>honest total</th></tr></thead>"
            f"<tbody>{''.join(out)}</tbody></table>")


def refresh_dips(conn, tickers, price_db=None):
    """Compute and store the dip profile for each ticker."""
    import dips
    prices = market_data.connect(price_db)
    now = datetime.now().isoformat(timespec="seconds")
    stored = 0
    for ticker in tickers:
        bars = wheel.load_bars(prices, ticker)
        if len(bars) < dips.HIGH_LOOKBACK + 2:
            print(f"  {ticker}: too few bars for a dip profile")
            continue
        days = [b[0] for b in bars]
        closes = [float(b[4]) for b in bars]
        for row in dips.profile(days, closes):
            for horizon, h in row["horizons"].items():
                conn.execute(
                    "INSERT OR REPLACE INTO dip_profile (ticker, depth,"
                    " horizon, n_episodes, n_outcomes, pending, median,"
                    " worst, best, positive, last_seen, window_first,"
                    " window_last, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ticker, row["depth"], horizon, row["n_episodes"],
                     h.get("n", 0), h.get("pending", 0), h.get("median"),
                     h.get("worst"), h.get("best"), h.get("positive"),
                     row["last"], days[0], days[-1], now))
                stored += 1
    conn.commit()
    print(f"dip profile: {stored} cell(s) stored")
    return stored


def render_dips(conn):
    """Where each name sits, and what followed dips that deep before.

    The sample size travels with every number. A median over four episodes
    and a median over thirty are different kinds of statement and must not
    look alike on a page someone reads in ten seconds.
    """
    import dips
    rows = conn.execute(
        "SELECT ticker, depth, horizon, n_episodes, n_outcomes, pending,"
        " median, worst, best, positive, window_first, window_last"
        " FROM dip_profile ORDER BY ticker, depth, horizon").fetchall()
    if not rows:
        return ""
    by = {}
    span = ("?", "?")
    for r in rows:
        by.setdefault((r["ticker"], r["depth"]), {})[r["horizon"]] = r
        span = (r["window_first"], r["window_last"])

    horizons = sorted({r["horizon"] for r in rows})
    head = "".join(f"<th>{h}d</th>" for h in horizons)
    body = []
    for (ticker, depth), cells in sorted(by.items()):
        any_cell = next(iter(cells.values()))
        n_ep = any_cell["n_episodes"] or 0
        thin = n_ep < dips.MIN_EPISODES
        tds = []
        for h in horizons:
            c = cells.get(h)
            if not c or not c["n_outcomes"]:
                tds.append('<td class="thin">—</td>')
                continue
            cls = ' class="thin"' if thin else ""
            tds.append(
                f'<td{cls}>{c["median"]:+.1f}%'
                f'<i> w {c["worst"]:+.0f}%</i></td>')
        flag = ' <i>n too small</i>' if thin else ""
        body.append(
            f'<tr><th>{ed.html.escape(ticker)} '
            f'−{depth:.0f}%</th><td>{n_ep}{flag}</td>{"".join(tds)}</tr>')
    return (
        '<section class="w-find"><h2>What followed a dip this deep</h2>'
        f'<table><thead><tr><th>name · depth</th><th>episodes</th>{head}'
        '</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table>'
        f'<p class="w-scope">Median forward return, with the worst episode '
        f'beside it. Episodes are distinct selloffs, not days — a long slide '
        f'counts once. Window {ed.html.escape(span[0] or "?")} to '
        f'{ed.html.escape(span[1] or "?")}, which contains no sustained bear '
        f'market: these funds reset leverage daily and can fail to recover '
        f'even when their index does, and nothing in this window shows that '
        f'happening. Fewer than {dips.MIN_EPISODES} episodes is marked and '
        f'should not be read as a tendency.</p></section>')


def render_card(row):
    """One ticker. The reason always travels with the verdict."""
    (ticker, as_of, close, move, green, qualifies, why, streak,
     last_q, n_q, sessions, status, _) = row
    if status != "OK":
        return (f'<div class="w-card"><div class="w-tick">'
                f'{ed.html.escape(ticker)}</div>'
                f'<div class="w-verdict"><b>no data</b></div>'
                f'<div class="w-why">{ed.html.escape(status)} — this is not '
                f'the same as "not set up"</div></div>')
    verdict = "SET UP" if qualifies else "waiting"
    cls = "w-card set" if qualifies else "w-card"
    rows = [("close", f"${close:,.2f}" if close else "—"),
            ("last session", f"{move:+.2f}%" if move is not None else "—"),
            ("clouds", "green" if green else
             ("red" if green == 0 else "not judgeable")),
            ("green run", f"{streak} session(s)" if green else "—"),
            ("last qualified", last_q or "never in window"),
            ("qualified", f"{n_q} time(s) in {sessions} sessions")]
    body = "".join(f"<div><span>{ed.html.escape(k)}</span>"
                   f"<span>{ed.html.escape(str(v))}</span></div>"
                   for k, v in rows)
    return (f'<div class="{cls}"><div class="w-tick">'
            f'{ed.html.escape(ticker)}</div>'
            f'<div class="w-verdict"><b>{verdict}</b></div>'
            f'<div class="w-why">{ed.html.escape(why or "")}</div>'
            f'<div class="w-rows">{body}</div></div>')


def write_page(conn, path="docs/wheel.html", clouds=wheel.CLOUDS,
               drop_pct=wheel.DROP_PCT):
    rows = conn.execute(
        "SELECT ticker, as_of, close, move_pct, clouds_green, qualifies, why,"
        " green_streak, last_qualified, qualified_count, sessions, status,"
        " updated_at FROM wheel_state"
        # No-data last: it is not a verdict, and ranking it above a real
        # "waiting" puts the least informative card in the best seat.
        " ORDER BY qualifies DESC, (status = 'OK') DESC, ticker"
    ).fetchall()
    if not rows:
        raise SystemExit("no wheel_state rows -- refresh first")

    cards = "".join(render_card(tuple(r)) for r in rows)
    n_set = sum(1 for r in rows if r[11] == "OK" and r[5])
    gauges = ed.render_gauges(conn)
    findings = render_findings()
    dip_panel = render_dips(conn)
    cloud_text = ", ".join(f"{f}/{s}" for f, s in clouds)

    steps = (
        '<ol class="w-steps">'
        '<li class="done">Pick 2–3 leveraged ETFs — your list, below</li>'
        f'<li class="done">Both EMA clouds green ({ed.html.escape(cloud_text)})'
        '</li>'
        f'<li class="done">A {drop_pct:g}% red day</li>'
        '<li>Open the chain 30 days out — <b>your broker</b></li>'
        '<li>Find a strike paying ~5% — <b>your broker</b></li>'
        '<li>Sell the cash-secured put — <b>your broker</b></li>'
        '<li>GTC to close at 50% profit — <b>your broker</b></li>'
        '<li class="done">Wait for the next red day — this page</li>'
        '<li class="done">Repeat</li>'
        '</ol>')

    doc = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<title>Setups — {date.today()}</title>
<meta name="color-scheme" content="light dark">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>{ed.CSS}{WHEEL_CSS}</style>
</head><body><div class="wrap">
<h1>Setups</h1>
<p class="meta">{n_set} of {len(rows)} set up &nbsp;·&nbsp; clouds {ed.html.escape(cloud_text)} &nbsp;·&nbsp; red day {drop_pct:g}% &nbsp;·&nbsp; built {datetime.now().strftime("%Y-%m-%d %H:%M")} &nbsp;·&nbsp; <a href="index.html">EDGAR discovery</a></p>
{gauges}
<p class="w-note"><b>This page does steps 1–3, 8 and 9 only.</b> It cannot
quote a strike or a premium: the price feed's free tier refuses a live
options chain, so steps 4–7 are a question for your broker. A premium
modelled here would be the least reliable number on the page wearing the
same typeface as the most reliable one.</p>
<div class="w-grid">{cards}</div>
{dip_panel}
{findings}
{steps}
</div></body></html>"""
    with open(path, "w") as fh:
        fh.write(doc)
    return path, n_set, len(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default="discovery.db")
    ap.add_argument("--out", default="docs/wheel.html")
    ap.add_argument("--tickers", default="TQQQ,SOXL,SPXL")
    ap.add_argument("--clouds", default="5/12,34/50")
    ap.add_argument("--drop", type=float, default=wheel.DROP_PCT)
    ap.add_argument("--no-refresh", action="store_true",
                    help="render from stored state without fetching prices")
    args = ap.parse_args()

    clouds = tuple(tuple(int(n) for n in p.split("/"))
                   for p in args.clouds.split(","))
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    # Opened directly rather than through edgar_discovery.connect(), which
    # takes no path and would also run the collection schema's migrations on
    # a database this script only reads gauges from.
    import sqlite3
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    if not args.no_refresh:
        refresh_state(conn, tickers, clouds, args.drop)
        refresh_dips(conn, tickers)
    path, n_set, n = write_page(conn, args.out, clouds, args.drop)
    print(f"wrote {path} ({n_set} of {n} set up)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
