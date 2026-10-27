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
import sqlite3
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
.w-split { border: 0; border-top: 1px solid var(--rule); margin: 26px 0 14px; }
.w-h2 { font-family: "IBM Plex Mono", monospace; font-size: 13px;
        letter-spacing: .08em; text-transform: uppercase; color: var(--muted);
        font-weight: 600; margin: 0 0 12px; }
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
        for row in dips.profile(days, closes,
                                 depths=dips.SLIDER_DEPTHS):
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


THESIS_CSS = """
.t-wrap { margin: 0 0 18px; }
.t-row { display: flex; align-items: baseline; gap: 10px; padding: 5px 0;
         border-bottom: 1px solid var(--rule); font-size: 12.5px; }
.t-state { font-family: "IBM Plex Mono", monospace; font-size: 10px;
           letter-spacing: .08em; padding: 1px 6px; border-radius: 2px;
           flex: none; min-width: 62px; text-align: center; }
/* STALE must not look like DORMANT. One is a reading, the other is an
   outage, and rendering them alike turns every failure into reassurance. */
.t-state.fired   { background: var(--warm); color: var(--card);
                   font-weight: 600; }
.t-state.dormant { color: var(--muted); border: 1px solid var(--rule); }
.t-state.stale   { color: var(--warm); border: 1px dashed var(--warm);
                   font-weight: 600; }
.t-id { font-family: "IBM Plex Mono", monospace; color: var(--ink); }
/* Breadth, not state: how many named issuers crossed. Recessive by
   default; a lone crosser out of several is the case worth seeing. */
.t-breadth { font-family: "IBM Plex Mono", monospace; font-size: 10px;
             color: var(--muted); margin-left: 6px;
             border: 1px solid var(--rule); border-radius: 2px;
             padding: 0 4px; vertical-align: 1px; }
.t-breadth.lone { color: var(--warm); border-color: var(--warm); }
/* A threshold changed after the evidence was seen. Marked, not hidden:
   a fitted number must not sit unmarked beside pre-registered ones. */
.t-moved { font-family: "IBM Plex Mono", monospace; font-size: 10px;
           color: var(--muted); margin-left: 6px; letter-spacing: .04em;
           border-bottom: 1px dotted var(--muted); cursor: help; }
.t-why { color: var(--muted); margin-left: auto; text-align: right;
         font-size: 11.5px; }
.t-thesis { margin: 14px 0 2px; font-size: 12.5px; }
.t-thesis b { font-size: 13px; }
.t-dis { color: var(--muted); font-size: 11.5px; margin: 2px 0 6px; }
"""


def render_theses(conn, path="theses.json"):
    """The pre-registered triggers and today's reading of each.

    STALE is rendered unmistakably differently from DORMANT. They are
    different sentences -- "we looked and the answer is no" versus "we could
    not look" -- and a page that draws them alike converts every outage into
    a reassuring row.
    """
    try:
        with open(path) as fh:
            spec = json.load(fh)
    except (OSError, ValueError):
        return ""
    # A thesis file with eight triggers and no evaluations is not the same
    # page as a thesis file with nothing in it. If the tracker has never run
    # -- or has not created its table yet -- the panel is still drawn, with
    # every trigger STALE, because returning "" here would publish a page on
    # which pre-registered triggers simply do not appear.
    try:
        rows = conn.execute(
            "SELECT trigger_id, state, reason, detail, as_of FROM thesis_state"
            " WHERE as_of = (SELECT MAX(as_of) FROM thesis_state)").fetchall()
    except sqlite3.OperationalError:
        rows = []
    latest = {r["trigger_id"]: r for r in rows}
    as_of = rows[0]["as_of"] if rows else "never evaluated"

    out = []
    counts = {"FIRED": 0, "DORMANT": 0, "STALE": 0}
    for thesis in spec.get("theses", []):
        body = []
        for trigger in thesis.get("triggers", []):
            r = latest.get(trigger["id"])
            state = (r["state"] if r else "STALE")
            why = (r["reason"] if r else "not evaluated in the latest run")
            counts[state] = counts.get(state, 0) + 1
            body.append(
                f'<div class="t-row">'
                f'<span class="t-state {state.lower()}">{state}</span>'
                f'<span class="t-id">{ed.html.escape(trigger["id"])}'
                f'{breadth_badge(r)}{moved_badge(spec, trigger["id"])}</span>'
                f'<span class="t-why">{ed.html.escape(why)}</span></div>')
        out.append(
            f'<div class="t-thesis"><b>{ed.html.escape(thesis["title"])}</b>'
            f'</div>'
            f'<div class="t-dis">Would be wrong if: '
            f'{ed.html.escape(thesis.get("disconfirms", "—"))}</div>'
            + "".join(body))

    blind = [k for k in (spec.get("_not_measurable") or {})
             if not k.startswith("_")]
    # Say how many thresholds are no longer pre-registrations. The sentence
    # above this one claims they were written down before the evidence was
    # looked at, and for a moved threshold that is no longer true.
    n_moved = sum(1 for v in (spec.get("_threshold_moves") or {}).values()
                  if isinstance(v, dict)
                  and v.get("moved") not in (None, "not moved"))
    moved_note = (
        f' {n_moved} ha{"s" if n_moved == 1 else "ve"} since been moved '
        f'after calibration and {"is" if n_moved == 1 else "are"} marked '
        f'<span class="t-moved">moved</span> — a fitted threshold is '
        f'weaker evidence than one that was never touched.') if n_moved else ""
    return (
        '<section class="w-find t-wrap"><h2>Pre-registered triggers</h2>'
        f'<p class="w-scope">{counts.get("FIRED",0)} fired · '
        f'{counts.get("DORMANT",0)} dormant · <b>{counts.get("STALE",0)} '
        f'could not be evaluated</b> · as of {ed.html.escape(as_of)}. '
        f'Thresholds were written down before the evidence was looked at; '
        f'the file is hand-edited and the pipeline never writes it.'
        f'{moved_note}</p>'
        + "".join(out)
        + f'<p class="w-scope"><b>Not measurable on free data, and therefore '
          f'not tracked at all:</b> {ed.html.escape(", ".join(blind))}. '
          f'Named so their absence stays a stated limit rather than being '
          f'read as absence of evidence.</p></section>')


SLIDER_JS = """
(function () {
  var data = window.__dipProfile || {};
  var el = document.getElementById('dip-slider');
  if (!el) return;
  var out = document.getElementById('dip-readout');
  var tickerSel = document.getElementById('dip-ticker');
  var MIN = Number(el.dataset.minEpisodes || 8);
  var committed = (el.dataset.committed || '').split(',').map(Number);
  // The slider indexes the depths the database actually holds rather than
  // sweeping a numeric range, so every position shows a profile. A position
  // that reads "no profile stored" teaches the reader nothing except that
  // the grid and the control disagree.
  var depths = (el.dataset.depths || '').split(',');

  function cell(text, cls) {
    var td = document.createElement('td');
    td.textContent = text;              // textContent, never innerHTML
    if (cls) td.className = cls;
    return td;
  }

  function draw() {
    var t = tickerSel.value, depth = depths[Number(el.value)];
    var row = (data[t] || {})[depth];
    var label = document.getElementById('dip-depth');
    if (label) label.textContent = '−' + depth + '%';
    out.innerHTML = '';
    var head = document.createElement('div');
    head.className = 'sl-head';
    var isCommitted = committed.indexOf(Number(depth)) !== -1;
    head.textContent = t + ' at \u2212' + depth + '%'
      + (isCommitted ? '  \u00b7 a committed alert band' : '');
    out.appendChild(head);
    if (!row) {
      var p = document.createElement('p');
      p.className = 'sl-none';
      p.textContent = 'no profile stored for this depth';
      out.appendChild(p);
      return;
    }
    var thin = row.n < MIN;
    var warn = document.createElement('p');
    warn.className = thin ? 'sl-thin' : 'sl-ok';
    warn.textContent = thin
      ? row.n + ' episode(s) \u2014 too few to read as a tendency'
      : row.n + ' episodes';
    out.appendChild(warn);
    var tbl = document.createElement('table');
    var tr = document.createElement('tr');
    ['horizon', 'median', 'worst', 'positive'].forEach(function (h) {
      var th = document.createElement('th'); th.textContent = h; tr.appendChild(th);
    });
    tbl.appendChild(tr);
    (row.h || []).forEach(function (h) {
      var r = document.createElement('tr');
      r.appendChild(cell(h[0] + 'd'));
      r.appendChild(cell(h[1] === null ? '\u2014' : h[1].toFixed(1) + '%',
                         thin ? 'sl-dim' : ''));
      r.appendChild(cell(h[2] === null ? '\u2014' : h[2].toFixed(1) + '%',
                         thin ? 'sl-dim' : ''));
      r.appendChild(cell(h[3] === null ? '\u2014' : h[3] + '/' + h[4]));
      tbl.appendChild(r);
    });
    out.appendChild(tbl);
  }
  el.addEventListener('input', draw);
  tickerSel.addEventListener('change', draw);
  draw();
})();
"""

SLIDER_CSS = """
.sl { margin: 0 0 18px; }
.sl-ctl { display: flex; align-items: center; gap: 12px; margin: 8px 0; }
.sl-ctl input[type=range] { flex: 1 1 auto; accent-color: var(--signal); }
.sl-ctl select, .sl-ctl output {
  font-family: "IBM Plex Mono", monospace; font-size: 12px;
  background: var(--field); color: var(--ink);
  border: 1px solid var(--rule); border-radius: 2px; padding: 2px 6px;
}
.sl-head { font-family: "IBM Plex Mono", monospace; font-size: 12px;
           color: var(--ink); margin-bottom: 4px; }
.sl-ok { font-size: 11.5px; color: var(--muted); margin: 0 0 6px; }
.sl-thin { font-size: 11.5px; color: var(--warm); margin: 0 0 6px;
           font-weight: 600; }
.sl-none { font-size: 11.5px; color: var(--muted); }
.sl table { border-collapse: collapse; font-size: 11.5px;
            font-family: "IBM Plex Mono", monospace; width: 100%; }
.sl th { text-align: right; color: var(--muted); font-weight: 400;
         font-size: 10px; letter-spacing: .06em; padding: 2px 6px;
         border-bottom: 1px solid var(--rule); }
.sl td { text-align: right; padding: 2px 6px; font-variant-numeric: tabular-nums;
         border-bottom: 1px solid var(--rule); }
.sl th:first-child, .sl td:first-child { text-align: left; }
.sl .sl-dim { color: var(--muted); }
"""


def moved_badge(spec, trigger_id):
    """Mark a threshold that was changed after the evidence was seen.

    The panel's own blurb says the thresholds were written down before the
    evidence was looked at. For a threshold that has since been moved that
    sentence is no longer true, and a fitted number sitting unmarked beside
    pre-registered ones borrows credibility it is not owed. The move is
    recorded in theses.json; this puts it on the page.
    """
    move = (spec.get("_threshold_moves") or {}).get(trigger_id)
    if not isinstance(move, dict):
        return ""
    when = move.get("moved")
    if not when or when == "not moved":
        return ""
    title = (f"threshold moved {when} after calibration: "
             f"{move.get('from', '?')} -> {move.get('to', '?')}")
    return (f'<span class="t-moved" title="{ed.html.escape(title)}">'
            f'moved</span>')


def breadth_badge(row):
    """How many of a trigger's named issuers crossed, as its own mark.

    These triggers fire on the widest gap across the issuers they name,
    which is how they were pre-registered. One of four firing is a weaker
    finding than four of four, and the page said only "widest gap (AVGO)"
    -- true, and easy to read as a statement about the four semiconductor
    names rather than about one of them. A lone crosser is marked so the
    breadth is visible without reading the sentence.
    """
    if row is None:
        return ""
    try:
        detail = json.loads(row["detail"] or "{}")
    except (ValueError, TypeError):
        return ""
    crossed, n = detail.get("crossed"), detail.get("evaluated")
    if crossed is None or not n:
        return ""
    cls = "t-breadth" + (" lone" if crossed == 1 and n > 1 else "")
    return (f'<span class="{cls}" title="{crossed} of {n} named issuer(s) '
            f'crossed the threshold">{crossed}/{n}</span>')


def render_slider(conn):
    """A calibration slider: what a threshold WOULD have done.

    It changes nothing. The alert bands are committed in code and the thesis
    thresholds are committed in a hand-edited file; this is for choosing a
    number before committing to it, and for seeing how little is behind one
    that has already been chosen.

    The thing it is built to make visible is the sample collapsing. Drag the
    depth out and the episode count falls away underneath the median, which
    keeps printing a confident-looking number long after it has stopped
    meaning anything. That is the failure this repo has hit repeatedly, and
    it is far more obvious as a thing you drag than as a column you read.
    """
    import dips
    rows = conn.execute(
        "SELECT ticker, depth, horizon, n_episodes, median, worst,"
        " positive, n_outcomes FROM dip_profile"
        " ORDER BY ticker, depth, horizon").fetchall()
    if not rows:
        return ""
    data = {}
    for r in rows:
        d = data.setdefault(r["ticker"], {}).setdefault(
            str(int(r["depth"])), {"n": r["n_episodes"], "h": []})
        d["h"].append([r["horizon"], r["median"], r["worst"],
                       r["positive"], r["n_outcomes"]])
    tickers = sorted(data)
    depths = sorted({int(r["depth"]) for r in rows})
    options = "".join(f'<option value="{ed.html.escape(t)}">'
                      f'{ed.html.escape(t)}</option>' for t in tickers)
    committed = ",".join(str(int(d)) for d in dips.DEPTHS)
    # Start on a band that is actually committed, so the first thing on
    # screen is a threshold the alerts really use.
    start = next((i for i, d in enumerate(depths)
                  if d in {int(x) for x in dips.DEPTHS}), 0)
    # Ticks on the track mark the bands the alerts actually use, so the
    # committed thresholds are visible while dragging past them.
    ticks = "".join(
        f'<option value="{i}" label="{d}"></option>'
        for i, d in enumerate(depths) if d in {int(x) for x in dips.DEPTHS})
    return (
        '<section class="w-find sl"><h2>What a different threshold would '
        'have done</h2>'
        '<p class="w-scope">A calibration tool. It records nothing and '
        'changes no alert — the bands that actually fire are committed in '
        'code, and the thesis thresholds in a hand-edited file. Drag it to '
        'see how quickly the sample disappears underneath the median.</p>'
        f'<div class="sl-ctl">'
        f'<select id="dip-ticker">{options}</select>'
        f'<input type="range" id="dip-slider" min="0" '
        f'max="{len(depths) - 1}" step="1" value="{start}" '
        f'aria-label="dip depth" '
        f'data-depths="{",".join(str(d) for d in depths)}" '
        f'data-min-episodes="{dips.MIN_EPISODES}" '
        f'data-committed="{committed}" list="dip-ticks">'
        f'<output id="dip-depth" for="dip-slider"></output>'
        f'<datalist id="dip-ticks">{ticks}</datalist>'
        f'</div><div id="dip-readout"></div>'
        f'<script>window.__dipProfile = {json.dumps(data)};</script>'
        '</section>')


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
    thesis_panel = render_theses(conn)
    slider_panel = render_slider(conn)
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
<style>{ed.CSS}{WHEEL_CSS}{THESIS_CSS}{SLIDER_CSS}</style>
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
{slider_panel}
{thesis_panel}
{findings}
{steps}
</div></body></html>"""
    with open(path, "w") as fh:
        fh.write(doc)
    return path, n_set, len(rows)


def check_placed(panels, doc):
    """Refuse a page that built a panel and then left it out.

    The failure this exists for left no trace: the slider's CSS and its JS
    were both in the merged page while the panel itself was not, so nothing
    errored and the page simply came out short. A panel missing from the
    page is indistinguishable from a panel with nothing to say, which is
    the same confusion as a 403 logged as "no triggers fired".
    """
    missing = sorted(n for n, html in panels.items() if html and html not in doc)
    if missing:
        raise SystemExit(
            f"panel(s) rendered but not placed in the page: "
            f"{', '.join(missing)} -- refusing to publish a page that "
            f"drops content it built")


def write_merged(conn, path="docs/index.html", clouds=wheel.CLOUDS,
                 drop_pct=wheel.DROP_PCT, window_days=14, cap=None):
    """One page: leveraged-ETF state on top, EDGAR transitions beneath.

    The dashboard body comes from edgar_discovery rather than being rebuilt
    here, so there is one builder for it and not two that drift apart. If
    that call fails the merge fails loudly, which is better than publishing
    a page that silently lost half its content.
    """
    rows = conn.execute(
        "SELECT ticker, as_of, close, move_pct, clouds_green, qualifies, why,"
        " green_streak, last_qualified, qualified_count, sessions, status,"
        " updated_at FROM wheel_state"
        " ORDER BY qualifies DESC, (status = 'OK') DESC, ticker"
    ).fetchall()
    cards = "".join(render_card(tuple(r)) for r in rows)
    n_set = sum(1 for r in rows if r[11] == "OK" and r[5])
    body = ed.write_html(conn, window_days=window_days, cap=cap,
                         body_only=True)
    cloud_text = ", ".join(f"{f}/{s}" for f, s in clouds)

    # Each panel renders once, into a named variable, so the template can be
    # checked against them below. The slider and the thesis panel were both
    # wired into write_page and silently left out of this one for a while:
    # their CSS and their JS were here, so nothing errored and the page just
    # came out short. That is the same failure as logging a 403 as "no
    # triggers fired" -- a panel absent from the page is indistinguishable
    # from a panel that had nothing to say -- so it is checked, not trusted.
    panels = {"dips": render_dips(conn),
              "slider": render_slider(conn),
              "theses": render_theses(conn),
              "findings": render_findings()}

    doc = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<title>Signals — {date.today()}</title>
<meta name="color-scheme" content="light dark">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>{ed.CSS}{WHEEL_CSS}{THESIS_CSS}{SLIDER_CSS}</style>
</head><body><div class="wrap">
<h1>Signals</h1>
<p class="meta">{n_set} of {len(rows)} set up &nbsp;·&nbsp; {body["n_moves"]} transition{"" if body["n_moves"] == 1 else "s"} &nbsp;·&nbsp; {body["n_issuers"]:,} issuers tracked &nbsp;·&nbsp; built {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
{ed.render_gauges(conn)}
<p class="w-note"><b>Leveraged ETFs, steps 1–3, 8 and 9 only.</b> No strike
and no premium appear here: the price feed's free tier refuses a live
options chain. These funds also reset leverage daily and can fail to recover
even when their index does — the dip table below says what actually followed
dips of each depth, and over how few episodes.</p>
<div class="w-grid">{cards}</div>
{panels["dips"]}
{panels["slider"]}
{panels["theses"]}
{panels["findings"]}
<hr class="w-split">
<h2 class="w-h2">EDGAR discovery</h2>
{body["sections"]}
{body["watch_html"]}
</div><div id="tip" role="status" aria-live="polite"></div>
<script>{ed.SCRIPT}</script>\n<script>{SLIDER_JS}</script></body></html>"""

    check_placed(panels, doc)

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
    ap.add_argument("--merge", action="store_true",
                    help="write ONE page carrying both lanes")
    ap.add_argument("--no-refresh", action="store_true",
                    help="render from stored state without fetching prices")
    args = ap.parse_args()

    clouds = tuple(tuple(int(n) for n in p.split("/"))
                   for p in args.clouds.split(","))
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    # Opened directly rather than through edgar_discovery.connect(), which
    # takes no path and would also run the collection schema's migrations on
    # a database this script only reads gauges from.
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    if not args.no_refresh:
        refresh_state(conn, tickers, clouds, args.drop)
        refresh_dips(conn, tickers)
    if args.merge:
        path, n_set, n = write_merged(conn, args.out, clouds, args.drop)
    else:
        path, n_set, n = write_page(conn, args.out, clouds, args.drop)
    print(f"wrote {path} ({n_set} of {n} set up)")
    return 0


def selftest():
    """The checks here are all about CONTENT GOING MISSING SILENTLY.

    The slider and the thesis panel were both built, both given their CSS
    and their JS in the merged page, and both left out of its body. Nothing
    raised. The page came out short and looked complete, which is the exact
    failure this repo keeps re-inventing: an absence that reads as a result.
    """
    checks = 0

    def ok(label, cond):
        nonlocal checks
        assert cond, "FAILED: " + label
        checks += 1

    def fresh():
        c = sqlite3.connect(":memory:")
        c.row_factory = sqlite3.Row
        c.executescript(SCHEMA)
        return c

    def seed_dips(c, ticker="T", depths=(3, 5, 8, 12, 20)):
        for d in depths:
            for h in (5, 10, 21, 63):
                c.execute(
                    "INSERT OR REPLACE INTO dip_profile (ticker, depth,"
                    " horizon, n_episodes, n_outcomes, pending, median,"
                    " worst, best, positive, last_seen, window_first,"
                    " window_last, updated_at) VALUES"
                    " (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ticker, float(d), h, 30 - d, 30 - d, 0, 1.5, -9.0, 9.0,
                     15, "2026-10-01", "2024-01-01", "2026-10-01", "now"))
        c.commit()

    # -- the slider indexes stored depths, never a numeric sweep ----------
    conn = fresh()
    seed_dips(conn)
    html = render_slider(conn)
    ok("the slider spans exactly the depths that are stored",
       'min="0"' in html and 'max="4"' in html)
    ok("the stored depths travel with the control",
       'data-depths="3,5,8,12,20"' in html)
    ok("it opens on a committed band",
       any(f'value="{i}"' in html for i in range(5)))
    ok("the committed bands are named so a reader can see which are real",
       'data-committed=' in html)

    conn = fresh()
    seed_dips(conn, depths=range(2, 31))
    html = render_slider(conn)
    ok("a finer grid gives a position per depth and no dead stops",
       'max="28"' in html and 'data-depths="2,3,4' in html)

    ok("an empty profile renders nothing rather than an empty control",
       render_slider(fresh()) == "")

    # -- a panel that cannot be evaluated is still drawn ------------------
    conn = fresh()
    conn.execute("DROP TABLE IF EXISTS thesis_state")
    panel = render_theses(conn)
    ok("a tracker that has never run still shows its pre-registered "
       "triggers, as STALE",
       "STALE" in panel and "never evaluated" in panel)
    ok("and does not quietly report them as dormant",
       panel.count(">DORMANT<") == 0)

    # -- breadth is shown, so one name does not read as a complex ---------
    lone = breadth_badge({"detail": json.dumps(
        {"crossed": 1, "evaluated": 4})})
    ok("a lone crosser out of four is marked", "1/4" in lone)
    ok("and marked distinctly, not just counted", "lone" in lone)
    ok("a unanimous crossing is not marked as lone",
       "lone" not in breadth_badge({"detail": json.dumps(
           {"crossed": 4, "evaluated": 4})}))
    for bad in ({"detail": None}, {"detail": "{}"}, {"detail": "not json"},
                None):
        ok("a trigger with no breadth to report renders nothing",
           breadth_badge(bad) == "")

    # -- a moved threshold is not a pre-registration ----------------------
    spec = {"_threshold_moves": {
        "_note": "not a trigger",
        "a": {"moved": "2026-10-27", "from": "x > 20", "to": "x > 50"},
        "b": {"moved": "not moved", "why": "nothing to calibrate against"}}}
    ok("a moved threshold is marked on the page",
       "moved" in moved_badge(spec, "a"))
    ok("and the move travels in the tooltip",
       "x &gt; 20 -&gt; x &gt; 50" in moved_badge(spec, "a"))
    ok("a threshold deliberately NOT moved is not marked",
       moved_badge(spec, "b") == "")
    ok("an untouched trigger is not marked",
       moved_badge(spec, "c") == "")
    ok("the bookkeeping note is not mistaken for a trigger",
       moved_badge(spec, "_note") == "")
    ok("and no moves means no mark", moved_badge({}, "a") == "")

    # -- the guard that would have caught this bug ------------------------
    panels = {"dips": "<section>a</section>", "slider": "<section>b</section>",
              "theses": "", "findings": "<section>c</section>"}
    whole = "<html>" + "".join(panels.values()) + "</html>"
    raised = ""
    try:
        check_placed(panels, whole)
    except SystemExit as exc:
        raised = str(exc)
    ok("a page carrying every panel is allowed through", raised == "")

    short = whole.replace("<section>b</section>", "")
    try:
        check_placed(panels, short)
    except SystemExit as exc:
        raised = str(exc)
    ok("a page that drops a panel it built is refused, not published",
       "slider" in raised and "not placed" in raised)
    ok("the refusal names the panel that went missing",
       "dips" not in raised and "findings" not in raised)

    # An empty panel is a panel with nothing to say, which is allowed. A
    # NON-empty panel that vanished is the bug.
    try:
        check_placed(panels, whole)
        raised = ""
    except SystemExit as exc:
        raised = str(exc)
    ok("a panel that rendered empty is not mistaken for a dropped one",
       raised == "" and panels["theses"] == "")

    # -- the merged page really does carry both late additions -----------
    src = open(__file__).read()
    merged_body = src.split("def write_merged")[1]
    for name in ("dips", "slider", "theses", "findings"):
        ok(f"write_merged places the {name} panel",
           f'{{panels["{name}"]}}' in merged_body)

    print(f"wheel_page selftest: {checks} checks passed")
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
