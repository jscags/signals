#!/usr/bin/env python3
"""
EDGAR daily-index discovery collector.

Stateless, universe-wide discovery of two signals:
  1. Form 4 open-market insider BUYS (transaction code P)
  2. M&A-inherent form types (SC TO-T, SC 14D9, DEFM14A, SC 13D, S-4, 425)

One fetch of the daily master index gives every filing made that day. We filter
to forms we care about, then fetch only those documents. Everything is keyed on
the EDGAR accession number, which is the natural dedup key.

Zero third-party dependencies (stdlib only). SQLite here so it runs immediately;
swap to Postgres when the dashboard needs concurrent reads.

Usage:
    export EDGAR_USER_AGENT="Your Name your@email.com"   # SEC requires this
    python edgar_discovery.py --date 2026-08-06
    python edgar_discovery.py --backfill 5
    python edgar_discovery.py --list --tier 1
"""

import argparse
import html
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from xml.etree import ElementTree

# ---------------------------------------------------------------- config

DB_PATH = os.environ.get("EDGAR_DB", "discovery.db")
CACHE_DIR = os.environ.get("EDGAR_CACHE", ".edgar_cache")
USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "")

# SEC allows ~10 req/s. Stay comfortably under it.
MIN_REQUEST_INTERVAL = 0.15

# A single purchase at or above this dollar value is Tier 1 on its own.
TIER1_VALUE_USD = 250_000

# Distinct insiders buying the same issuer inside this window = cluster = Tier 1.
CLUSTER_WINDOW_DAYS = 10
CLUSTER_MIN_INSIDERS = 2

# Form types that are inherently M&A. No item-code parsing needed: the form
# type alone is the signal, which is why these are in the first collector.
MA_FORMS_TIER1 = {"SC TO-T", "SC 14D9", "DEFM14A", "SC 13D"}
MA_FORMS_TIER2 = {"S-4", "425", "SC TO-C", "SC 13E3"}
MA_FORMS = MA_FORMS_TIER1 | MA_FORMS_TIER2

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

# ---------------------------------------------------------------- http


_last_request = 0.0


def fetch(url, binary=False):
    """Rate-limited GET. Returns None on 404 (missing index = non-trading day)."""
    global _last_request
    if not USER_AGENT:
        sys.exit(
            "EDGAR_USER_AGENT is not set. The SEC rejects requests without a\n"
            'declared contact. Example: export EDGAR_USER_AGENT="Jane Doe jane@ex.com"'
        )

    elapsed = time.time() - _last_request
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request = time.time()

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        # Only 404 means "this genuinely does not exist". A 403 means the SEC
        # rejected us -- conflating the two hides real failures behind a
        # cheerful "no filings today" message.
        if exc.code == 404:
            return None
        if exc.code == 403:
            raise RuntimeError(
                f"SEC returned 403 for {url}\n"
                f"Usually a malformed User-Agent or rate limiting.\n"
                f"Current value: {USER_AGENT!r}"
            ) from exc
        raise
    return raw if binary else raw.decode("utf-8", errors="replace")


# ---------------------------------------------------------------- ticker map


def load_ticker_map():
    """CIK -> (ticker, title). Doubles as the listed-company filter: any CIK
    absent from this file is a fund, private filer, or foreign entity."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, "company_tickers.json")

    stale = (
        not os.path.exists(path)
        or time.time() - os.path.getmtime(path) > 7 * 86400
    )
    if stale:
        body = fetch(TICKER_MAP_URL)
        if body:
            with open(path, "w") as fh:
                fh.write(body)

    if not os.path.exists(path):
        return {}

    with open(path) as fh:
        data = json.load(fh)

    return {
        int(row["cik_str"]): (row["ticker"], row["title"])
        for row in data.values()
    }


# ---------------------------------------------------------------- daily index


def index_url(day):
    quarter = (day.month - 1) // 3 + 1
    return (
        f"https://www.sec.gov/Archives/edgar/daily-index/"
        f"{day.year}/QTR{quarter}/master.{day:%Y%m%d}.idx"
    )


def parse_master_idx(text):
    """master.idx is pipe-delimited: CIK|Company|Form Type|Date Filed|Filename.

    The preamble is free text of variable length, so rather than skipping a
    fixed number of lines we accept any row with 5 fields and a numeric CIK.
    """
    rows = []
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company, form_type, filed, path = (p.strip() for p in parts)
        if not cik.isdigit():
            continue
        rows.append(
            {
                "cik": int(cik),
                "company": company,
                "form_type": form_type,
                "filed": filed,
                "path": path,
                "accession": accession_from_path(path),
            }
        )
    return rows


def accession_from_path(path):
    m = re.search(r"(\d{10}-\d{2}-\d{6})", path)
    return m.group(1) if m else path


# ---------------------------------------------------------------- form 4


def extract_ownership_xml(submission_text):
    """A full submission .txt bundles several <DOCUMENT> blocks. The ownership
    XML is the one whose root is <ownershipDocument>."""
    for block in re.findall(r"<XML>(.*?)</XML>", submission_text, re.S):
        block = block.strip()
        if "<ownershipDocument" not in block:
            continue
        # Strip anything before the root element (stray XML declarations etc).
        start = block.find("<ownershipDocument")
        try:
            return ElementTree.fromstring(block[start:])
        except ElementTree.ParseError:
            continue
    return None


def _text(node, path, default=None):
    found = node.find(path)
    return found.text.strip() if found is not None and found.text else default


def _num(node, path):
    raw = _text(node, path)
    if raw is None:
        return None
    try:
        return float(raw.replace(",", "").replace("$", ""))
    except ValueError:
        return None


def parse_form4(root):
    """Return open-market purchases only.

    This is where most naive screeners go wrong. Code P is an actual
    open-market purchase. Code A is a grant or award, and M is an option
    exercise -- neither reflects a decision to commit cash at market price.
    We also require acquired/disposed == 'A' to exclude oddities.

    One row per transaction, never one per (transaction x reporting owner).
    A Form 4 carrying several reporting owners is a joint filing by an
    affiliated group -- a fund, its general partner, its managing member --
    reporting the same shares once under shared beneficial ownership. The
    schema offers no link from a transaction to a particular owner, so there
    is nothing to attribute; fanning the transaction out across owners would
    invent N insiders out of one decision and trip the cluster rule on the
    strength of a single filing.
    """
    if root is None:
        return []

    issuer_cik = _text(root, "issuer/issuerCik")
    issuer_name = _text(root, "issuer/issuerName")
    ticker = _text(root, "issuer/issuerTradingSymbol")

    names, titles = [], []
    for owner in root.findall("reportingOwner"):
        names.append(_text(owner, "reportingOwnerId/rptOwnerName", "UNKNOWN"))
        rel = owner.find("reportingOwnerRelationship")
        if rel is None:
            continue
        if _text(rel, "isDirector") in ("1", "true"):
            titles.append("Director")
        if _text(rel, "isOfficer") in ("1", "true"):
            titles.append(_text(rel, "officerTitle") or "Officer")
        if _text(rel, "isTenPercentOwner") in ("1", "true"):
            titles.append("10% Owner")

    # Sorted, so the identity a group files under does not depend on the order
    # the filing agent happened to list them in. An unstable choice here would
    # read as two different insiders across two filings by the same group --
    # exactly the false cluster this function exists to avoid.
    names = sorted(set(names)) or ["UNKNOWN"]
    owner, co_owners = names[0], names[1:]
    # Relationships are per-owner, but the transaction belongs to the group, so
    # the union describes it: a fund plus its director co-filer is both.
    owner_title = ", ".join(dict.fromkeys(titles)) or "Insider"

    buys = []
    for txn in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = _text(txn, "transactionCoding/transactionCode")
        direction = _text(
            txn, "transactionAmounts/transactionAcquiredDisposedCode/value"
        )
        if code != "P" or direction != "A":
            continue

        shares = _num(txn, "transactionAmounts/transactionShares/value")
        price = _num(txn, "transactionAmounts/transactionPricePerShare/value")
        txn_date = _text(txn, "transactionDate/value")
        value = shares * price if shares and price else None

        buys.append(
            {
                "issuer_cik": int(issuer_cik) if issuer_cik else None,
                "issuer": issuer_name,
                "ticker": ticker,
                "owner": owner,
                "owner_title": owner_title,
                "co_owners": co_owners,
                "txn_date": txn_date,
                "shares": shares,
                "price": price,
                "value": value,
            }
        )
    return buys


# ---------------------------------------------------------------- storage

SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    accession   TEXT PRIMARY KEY,
    cik         INTEGER,
    company     TEXT,
    form_type   TEXT,
    filed_date  TEXT,
    path        TEXT,
    fetched_at  TEXT
);

CREATE TABLE IF NOT EXISTS insider_buys (
    accession    TEXT,
    issuer_cik   INTEGER,
    ticker       TEXT,
    issuer       TEXT,
    owner        TEXT,
    owner_title  TEXT,
    txn_date     TEXT,
    shares       REAL,
    price        REAL,
    value        REAL,
    UNIQUE(accession, owner, txn_date, shares, price)
);
CREATE INDEX IF NOT EXISTS idx_buys_issuer ON insider_buys(issuer_cik, txn_date);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id   TEXT,
    entity      TEXT,
    event_type  TEXT,
    tier        INTEGER,
    headline    TEXT,
    detail      TEXT,
    filed_date  TEXT,
    created_at  TEXT,
    reviewed_at TEXT,
    UNIQUE(source_id, entity, event_type)
);

CREATE TABLE IF NOT EXISTS run_log (
    run_date    TEXT,
    source      TEXT,
    status      TEXT,
    n_docs      INTEGER,
    n_events    INTEGER,
    started_at  TEXT,
    finished_at TEXT,
    PRIMARY KEY (run_date, source)
);

-- The watchlist is an OUTPUT of discovery, not an input. Tier 1 findings
-- promote a company into it; the stateful collectors (guidance, trials,
-- contracts) will read from it. Manual entries never expire; auto ones do,
-- so the list stays roughly the size of what is currently interesting.
CREATE TABLE IF NOT EXISTS watchlist (
    ticker       TEXT PRIMARY KEY,
    cik          INTEGER,
    source       TEXT,
    reason       TEXT,
    added_at     TEXT,
    promoted_at  TEXT,
    expires_at   TEXT,
    active       INTEGER DEFAULT 1
);
"""


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def already_processed(conn, accession):
    cur = conn.execute(
        "SELECT 1 FROM documents WHERE accession = ?", (accession,)
    )
    return cur.fetchone() is not None


def emit(conn, *, source_id, entity, event_type, tier, headline, detail, filed):
    """Idempotent on (source_id, entity, event_type) so a manual midday run and
    the scheduled evening run cannot double-report the same thing."""
    cur = conn.execute(
        """INSERT OR IGNORE INTO events
           (source_id, entity, event_type, tier, headline, detail,
            filed_date, created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            source_id,
            entity,
            event_type,
            tier,
            headline,
            json.dumps(detail, default=str),
            filed,
            datetime.utcnow().isoformat(timespec="seconds"),
        ),
    )
    return cur.rowcount


# ---------------------------------------------------------------- tiering


def cluster_insiders(conn, issuer_cik, txn_date):
    """Distinct insiders buying this issuer within the lookback window."""
    if not issuer_cik or not txn_date:
        return []
    try:
        anchor = datetime.strptime(txn_date, "%Y-%m-%d").date()
    except ValueError:
        return []
    since = anchor - timedelta(days=CLUSTER_WINDOW_DAYS)
    cur = conn.execute(
        """SELECT DISTINCT owner FROM insider_buys
           WHERE issuer_cik = ? AND txn_date BETWEEN ? AND ?""",
        (issuer_cik, since.isoformat(), anchor.isoformat()),
    )
    return [r["owner"] for r in cur.fetchall()]


def usd(value):
    if value is None:
        return "undisclosed"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    return f"${value / 1_000:.0f}K"


# ---------------------------------------------------------------- run


def run_day(conn, day, tickers, limit=None):
    started = datetime.utcnow().isoformat(timespec="seconds")
    body = fetch(index_url(day))

    if body is None:
        conn.execute(
            """INSERT OR REPLACE INTO run_log VALUES (?,?,?,?,?,?,?)""",
            (day.isoformat(), "edgar_daily", "no_index", 0, 0, started,
             datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()
        print(f"{day}  no index published (weekend or holiday)")
        return 0, 0

    rows = parse_master_idx(body)
    if not rows:
        print(f"{day}  WARNING: index fetched ({len(body):,} bytes) but parsed "
              f"to 0 rows — the file format may have changed")
    n_docs = n_events = 0
    n_candidates = sum(
        1 for r in rows if r["form_type"] == "4" or r["form_type"] in MA_FORMS
    )

    for row in rows:
        if limit is not None and n_docs >= limit:
            print(f"  (stopped at --limit {limit}; index had {len(rows)} filings)")
            break
        listed = tickers.get(row["cik"])
        is_form4 = row["form_type"] == "4"
        is_ma = row["form_type"] in MA_FORMS

        if not (is_form4 or is_ma):
            continue
        # Form 4 issuer CIK differs from the filer CIK, so we cannot use the
        # ticker map to pre-filter those -- resolve after parsing instead.
        if is_ma and not listed:
            continue
        if already_processed(conn, row["accession"]):
            continue

        if is_form4:
            n_events += handle_form4(conn, row, tickers)
        else:
            n_events += handle_ma(conn, row, listed)

        conn.execute(
            """INSERT OR IGNORE INTO documents
               (accession, cik, company, form_type, filed_date, path, fetched_at)
               VALUES (?,?,?,?,?,?,?)""",
            (row["accession"], row["cik"], row["company"], row["form_type"],
             day.isoformat(), row["path"],
             datetime.utcnow().isoformat(timespec="seconds")),
        )
        n_docs += 1

    conn.execute(
        """INSERT OR REPLACE INTO run_log VALUES (?,?,?,?,?,?,?)""",
        (day.isoformat(), "edgar_daily", "ok", n_docs, n_events, started,
         datetime.utcnow().isoformat(timespec="seconds")),
    )
    conn.commit()
    print(f"{day}  {len(rows):,} filings in index, {n_candidates} of interest, "
          f"{n_docs} processed, {n_events} events")
    return n_docs, n_events


def handle_form4(conn, row, tickers):
    text = fetch(f"https://www.sec.gov/Archives/{row['path']}")
    if not text:
        return 0

    buys = parse_form4(extract_ownership_xml(text))
    if not buys:
        return 0

    emitted = 0
    for buy in buys:
        # Fall back to the ticker map when the XML omits the symbol.
        if not buy["ticker"] and buy["issuer_cik"] in tickers:
            buy["ticker"] = tickers[buy["issuer_cik"]][0]
        if not buy["ticker"]:
            continue  # not a listed issuer

        conn.execute(
            """INSERT OR IGNORE INTO insider_buys
               (accession, issuer_cik, ticker, issuer, owner, owner_title,
                txn_date, shares, price, value)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (row["accession"], buy["issuer_cik"], buy["ticker"], buy["issuer"],
             buy["owner"], buy["owner_title"], buy["txn_date"], buy["shares"],
             buy["price"], buy["value"]),
        )

        peers = cluster_insiders(conn, buy["issuer_cik"], buy["txn_date"])
        big = buy["value"] is not None and buy["value"] >= TIER1_VALUE_USD
        clustered = len(peers) >= CLUSTER_MIN_INSIDERS

        tier = 1 if (big or clustered) else 2
        reason = "cluster" if clustered else ("size" if big else "routine")

        if tier == 1:
            promote(conn, buy["ticker"], buy["issuer_cik"],
                    f"insider buying ({reason})")

        emitted += emit(
            conn,
            source_id=row["accession"],
            entity=buy["ticker"],
            event_type="insider_buy",
            tier=tier,
            headline=(
                f"{buy['ticker']}: {buy['owner']}"
                + (
                    f" +{len(buy['co_owners'])} co-filer"
                    + ("s" if len(buy["co_owners"]) > 1 else "")
                    if buy["co_owners"]
                    else ""
                )
                + f" ({buy['owner_title']}) bought {usd(buy['value'])}"
                + (f" — {len(peers)} insiders buying" if clustered else "")
            ),
            detail={**buy, "cluster_peers": peers, "tier_reason": reason},
            filed=row["filed"],
        )
    return emitted


def handle_ma(conn, row, listed):
    ticker, title = listed
    tier = 1 if row["form_type"] in MA_FORMS_TIER1 else 2

    if tier == 1:
        promote(conn, ticker, row["cik"], f"M&A filing ({row['form_type']})")

    return emit(
        conn,
        source_id=row["accession"],
        entity=ticker,
        event_type=f"ma_{row['form_type'].replace(' ', '_').lower()}",
        tier=tier,
        headline=f"{ticker}: {row['form_type']} filed — {title}",
        detail={"form_type": row["form_type"], "company": row["company"],
                "path": row["path"]},
        filed=row["filed"],
    )


def business_days_back(n):
    days, cursor = [], date.today()
    while len(days) < n:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:
            days.append(cursor)
    return sorted(days)


def show_events(conn, tier=None, limit=50):
    sql = "SELECT * FROM events WHERE reviewed_at IS NULL"
    args = []
    if tier:
        sql += " AND tier = ?"
        args.append(tier)
    sql += " ORDER BY tier, filed_date DESC LIMIT ?"
    args.append(limit)

    current = None
    for row in conn.execute(sql, args):
        if row["tier"] != current:
            current = row["tier"]
            print(f"\n--- TIER {current} " + "-" * 50)
        print(f"  [{row['filed_date']}] {row['headline']}")


WATCH_TTL_DAYS = 90


def promote(conn, ticker, cik, reason, manual=False):
    """Add or refresh a watchlist entry.

    Re-promotion pushes the expiry out, so a name that keeps producing Tier 1
    events stays on the list indefinitely while a one-off drops off after the
    TTL. Manual entries carry no expiry.
    """
    if not ticker:
        return 0
    today = date.today()
    expires = None if manual else (today + timedelta(days=WATCH_TTL_DAYS)).isoformat()

    cur = conn.execute(
        """INSERT INTO watchlist
             (ticker, cik, source, reason, added_at, promoted_at, expires_at, active)
           VALUES (?,?,?,?,?,?,?,1)
           ON CONFLICT(ticker) DO UPDATE SET
             reason      = excluded.reason,
             promoted_at = excluded.promoted_at,
             expires_at  = CASE WHEN watchlist.source = 'manual'
                                THEN NULL ELSE excluded.expires_at END,
             active      = 1""",
        (ticker, cik, "manual" if manual else "auto", reason,
         today.isoformat(), today.isoformat(), expires),
    )
    return cur.rowcount


def unwatch(conn, ticker):
    cur = conn.execute(
        "UPDATE watchlist SET active = 0 WHERE ticker = ?", (ticker.upper(),)
    )
    return cur.rowcount


def prune_watchlist(conn):
    """Retire expired auto-promotions. Manual entries are never touched."""
    cur = conn.execute(
        """UPDATE watchlist SET active = 0
           WHERE active = 1 AND source = 'auto'
             AND expires_at IS NOT NULL AND expires_at < ?""",
        (date.today().isoformat(),),
    )
    return cur.rowcount


def watchlist_rows(conn):
    return conn.execute(
        """SELECT * FROM watchlist WHERE active = 1
           ORDER BY source DESC, promoted_at DESC"""
    ).fetchall()


# ---------------------------------------------------------------- dashboard

CSS = """
:root {
  --ink: #14202B; --paper: #EDF0F2; --card: #FFFFFF;
  --rule: #C7D0D6; --signal: #1B3FD8; --muted: #6B7B87;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 28px 20px 80px; background: var(--paper); color: var(--ink);
  font-family: "IBM Plex Sans", -apple-system, system-ui, sans-serif;
  font-size: 15px; line-height: 1.45; -webkit-text-size-adjust: 100%;
}
.wrap { max-width: 860px; margin: 0 auto; }
h1 {
  font-size: 13px; font-weight: 600; letter-spacing: .16em; text-transform: uppercase;
  margin: 0 0 4px;
}
.meta {
  font-family: "IBM Plex Mono", ui-monospace, monospace;
  font-size: 12px; color: var(--muted); margin: 0 0 32px;
  padding-bottom: 16px; border-bottom: 2px solid var(--ink);
}
.tier-head {
  display: flex; align-items: baseline; gap: 10px;
  margin: 40px 0 14px; padding-bottom: 6px; border-bottom: 1px solid var(--rule);
}
.tier-head b {
  font-size: 12px; letter-spacing: .14em; text-transform: uppercase; font-weight: 600;
}
.tier-head span {
  font-family: "IBM Plex Mono", monospace; font-size: 11px; color: var(--muted);
}
.t1 .tier-head b { color: var(--signal); }
.row {
  display: grid; grid-template-columns: 88px 1fr; gap: 16px;
  background: var(--card); border-left: 3px solid var(--rule);
  padding: 14px 16px; margin-bottom: 8px;
}
.t1 .row { border-left-color: var(--signal); }
.ticker {
  font-family: "IBM Plex Mono", monospace; font-size: 19px; font-weight: 600;
  letter-spacing: -.02em; word-break: break-all;
}
.t1 .ticker { color: var(--signal); }
.headline { margin: 0 0 6px; }
.detail {
  font-family: "IBM Plex Mono", monospace; font-size: 11.5px; color: var(--muted);
  display: flex; flex-wrap: wrap; gap: 4px 14px;
}
.chips { display: flex; gap: 4px; margin-top: 8px; flex-wrap: wrap; }
.chip {
  font-family: "IBM Plex Mono", monospace; font-size: 10px; font-weight: 600;
  border: 1px solid var(--signal); color: var(--signal);
  padding: 2px 6px; border-radius: 2px;
}
.empty {
  font-family: "IBM Plex Mono", monospace; font-size: 12px;
  color: var(--muted); padding: 20px 0;
}
.watch { display: flex; flex-wrap: wrap; gap: 6px; }
.watch div {
  background: var(--card); border: 1px solid var(--rule);
  padding: 7px 10px; display: flex; align-items: baseline; gap: 8px;
}
.watch b {
  font-family: "IBM Plex Mono", monospace; font-size: 13px; font-weight: 600;
}
.watch i {
  font-style: normal; font-size: 10.5px; color: var(--muted);
  font-family: "IBM Plex Mono", monospace;
}
.watch .pin { border-left: 3px solid var(--ink); }
@media (max-width: 520px) {
  .row { grid-template-columns: 1fr; gap: 6px; }
}
@media (prefers-reduced-motion: no-preference) {
  .row { animation: rise .3s ease-out backwards; }
  @keyframes rise { from { opacity: 0; transform: translateY(4px); } }
}
"""


def _initials(name):
    parts = [p for p in re.split(r"[\s,]+", name or "") if p]
    return "".join(p[0].upper() for p in parts[:2]) or "??"


def render_row(row):
    detail = json.loads(row["detail"] or "{}")
    bits = [f"filed {html.escape(row['filed_date'] or '')}"]

    if row["event_type"] == "insider_buy":
        if detail.get("shares") and detail.get("price"):
            bits.append(f"{int(detail['shares']):,} sh @ ${detail['price']:,.2f}")
        if detail.get("txn_date"):
            bits.append(f"traded {detail['txn_date']}")
    else:
        bits.append(html.escape(detail.get("form_type", "")))
    bits.append(html.escape(row["source_id"]))

    # Signature: one chip per distinct insider, so a cluster is visible at a glance.
    peers = detail.get("cluster_peers") or []
    chips = ""
    if len(peers) > 1:
        marks = "".join(
            f'<span class="chip">{html.escape(_initials(p))}</span>' for p in peers[:6]
        )
        chips = f'<div class="chips">{marks}</div>'

    return f"""<div class="row">
  <div class="ticker">{html.escape(row['entity'] or '—')}</div>
  <div>
    <p class="headline">{html.escape(row['headline'] or '')}</p>
    <div class="detail">{"".join(f"<span>{b}</span>" for b in bits)}</div>
    {chips}
  </div>
</div>"""


def write_html(conn, path="dashboard.html"):
    events = conn.execute(
        """SELECT * FROM events WHERE reviewed_at IS NULL
           ORDER BY tier, filed_date DESC, id DESC"""
    ).fetchall()
    runs = conn.execute(
        "SELECT * FROM run_log ORDER BY run_date DESC LIMIT 5"
    ).fetchall()

    sections = []
    for tier, label in ((1, "Act on these"), (2, "Everything else")):
        rows = [e for e in events if e["tier"] == tier]
        body = (
            "".join(render_row(r) for r in rows)
            if rows
            else '<p class="empty">Nothing yet. Run a collection to populate this.</p>'
        )
        sections.append(
            f'<section class="t{tier}"><div class="tier-head">'
            f"<b>Tier {tier} — {label}</b><span>{len(rows)}</span></div>{body}</section>"
        )

    covered = ", ".join(r["run_date"] for r in runs) or "no runs yet"
    docs = sum(r["n_docs"] or 0 for r in runs)

    watched = watchlist_rows(conn)
    if watched:
        cards = []
        for w in watched:
            if w["source"] == "manual":
                note, pin = "pinned", " pin"
            elif w["expires_at"]:
                left = (
                    datetime.strptime(w["expires_at"], "%Y-%m-%d").date()
                    - date.today()
                ).days
                note, pin = f"{left}d left", ""
            else:
                note, pin = "", ""
            cards.append(
                f'<div class="{pin.strip()}"><b>{html.escape(w["ticker"])}</b>'
                f'<i>{html.escape(w["reason"] or "")}</i>'
                f'<i>{note}</i></div>'
            )
        watch_html = (
            '<section><div class="tier-head"><b>Watchlist</b>'
            f'<span>{len(watched)}</span></div>'
            f'<div class="watch">{"".join(cards)}</div></section>'
        )
    else:
        watch_html = ""

    doc = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Discovery — {date.today()}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head><body><div class="wrap">
<h1>EDGAR Discovery</h1>
<p class="meta">{len(events)} open events &nbsp;·&nbsp; {docs} filings scanned &nbsp;·&nbsp; days covered: {covered}</p>
{"".join(sections)}
{watch_html}
</div></body></html>"""

    with open(path, "w") as fh:
        fh.write(doc)
    return path, len(events)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="single day to process (YYYY-MM-DD)")
    ap.add_argument("--backfill", type=int, help="process the last N weekdays")
    ap.add_argument("--list", action="store_true", help="show unreviewed events")
    ap.add_argument("--tier", type=int, choices=[1, 2])
    ap.add_argument("--limit", type=int,
                    help="stop after N documents (use for a quick first run)")
    ap.add_argument("--html", nargs="?", const="dashboard.html",
                    help="write the dashboard to an HTML file")
    ap.add_argument("--watch", metavar="TICKER",
                    help="pin a company to the watchlist (never expires)")
    ap.add_argument("--unwatch", metavar="TICKER",
                    help="remove a company from the watchlist")
    ap.add_argument("--watchlist", action="store_true",
                    help="show the current watchlist")
    args = ap.parse_args()

    conn = connect()

    if args.watch:
        promote(conn, args.watch.upper(), None, "pinned manually", manual=True)
        conn.commit()
        print(f"pinned {args.watch.upper()}")
        return

    if args.unwatch:
        n = unwatch(conn, args.unwatch)
        conn.commit()
        print(f"removed {args.unwatch.upper()}" if n else "not on the watchlist")
        return

    if args.watchlist:
        rows = watchlist_rows(conn)
        if not rows:
            print("watchlist is empty")
        for w in rows:
            tag = "pinned" if w["source"] == "manual" else f"expires {w['expires_at']}"
            print(f"  {w['ticker']:<8} {w['reason'] or '':<32} {tag}")
        return

    if args.list:
        show_events(conn, tier=args.tier)
        return

    if args.html and not (args.date or args.backfill):
        path, n = write_html(conn, args.html)
        print(f"wrote {path} ({n} open events)")
        return

    tickers = load_ticker_map()
    if not tickers:
        print("warning: ticker map unavailable; M&A filtering degraded",
              file=sys.stderr)

    if args.backfill:
        days = business_days_back(args.backfill)
    elif args.date:
        days = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    else:
        days = [date.today()]

    for day in days:
        run_day(conn, day, tickers, limit=args.limit)

    retired = prune_watchlist(conn)
    conn.commit()
    if retired:
        print(f"retired {retired} expired watchlist entries")

    watched = watchlist_rows(conn)
    print(f"watchlist: {len(watched)} active")

    if args.html:
        path, n = write_html(conn, args.html)
        print(f"\nwrote {path} ({n} open events)")
    else:
        print()
        show_events(conn, tier=1)


if __name__ == "__main__":
    main()
