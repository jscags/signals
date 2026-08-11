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
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

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

# A dollar figure alone says nothing about significance: $1M is a rounding
# error at a trillion-dollar company and a serious commitment at a small one.
# Measured against market cap instead, purchases sort onto a scale that means
# the same thing for every issuer. Insider buys are tiny next to a whole
# company by construction, so the working range is basis points, not percent:
# 100 bps (1%) of a company bought by one person is extraordinary.
SIGNIFICANCE_BANDS = (
    (100.0, "major"),
    (25.0, "significant"),
    (5.0, "notable"),
    (1.0, "minor"),
    (0.0, "negligible"),
)

# Relative size promotes on its own. This is the whole point of the scale: a
# $60K purchase in a $20M company moves more of the float than a $5M purchase
# at a mega-cap, and only one of those is a real signal about the company.
TIER1_BPS = 25.0

# A refused document is skipped, but a run of them means the SEC has stopped
# answering us and the honest move is to stop asking rather than push through
# thousands more requests.
MAX_CONSECUTIVE_REFUSALS = 20

# Shelf life for a dashboard event, by tier. Nothing else retires them, so
# without this the feed only grows. Tier 1 is worth acting on for about a
# fortnight; Tier 2 is mostly merger chatter and routine small buys and goes
# stale far sooner.
EVENT_TTL_DAYS = {1: 14, 2: 5}

# Shares outstanding moves slowly; a monthly refresh is plenty.
SHARES_TTL_DAYS = 30

# Not every issuer tags the cover-page concept with a real number. Galaxy
# Digital reports 100 shares outstanding, which values the whole company at
# $1,963 -- so a $100K purchase came out as 51x the company, i.e. 5,100% of
# it, and led Tier 1 flagged major. A reporting issuer with fewer shares than
# this does not exist in practice.
MIN_PLAUSIBLE_SHARES = 50_000

# No open-market Form 4 purchase is a fifth of a company. Above this the
# denominator is wrong, not the buy extraordinary -- a stake that size arrives
# with a 13D, not a routine insider filing. Genuine readings do run high at the
# small end: the largest in the live data is EVGN at 918 bps (9.18% of a $4.4M
# company), so this ceiling sits a little over 2x above the real range, not the
# order of magnitude a mid-cap-only sample would suggest.
MAX_PLAUSIBLE_BPS = 2_000.0

# No insider has ever bought this much stock in one filing. A Form 4 reporting
# more has a bad price -- one live filing quotes $180,000 a share against a
# $1.60 stock -- and the dollar figure must not be shown or tiered on. The
# share-of-company score is unaffected, since price cancels out of that ratio.
MAX_PLAUSIBLE_VALUE_USD = 1_000_000_000

# Cover-page share count first, the us-gaap balance-sheet tag as a fallback for
# issuers that do not tag the dei concept.
XBRL_CONCEPTS = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
)

# Form types that are inherently M&A. No item-code parsing needed: the form
# type alone is the signal, which is why these are in the first collector.
MA_FORMS_TIER1 = {"SC TO-T", "SC 14D9", "DEFM14A", "SC 13D"}
MA_FORMS_TIER2 = {"S-4", "425", "SC TO-C", "SC 13E3"}
MA_FORMS = MA_FORMS_TIER1 | MA_FORMS_TIER2

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

# ---------------------------------------------------------------- http


_last_request = 0.0


class FetchError(RuntimeError):
    """The request did not come back, and trying again later might fix it.

    Covers an outright SEC refusal and the whole family of transport failures
    -- reset connections, timeouts, DNS, a proxy declining the tunnel -- which
    arrive as OSError rather than HTTPError and so slipped past handlers that
    only knew about 403. Callers treat them alike: skip this one, keep the run.
    Subclasses RuntimeError so existing handlers catch it unchanged.
    """


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
            raise FetchError(
                f"SEC returned 403 for {url}\n"
                f"Usually a malformed User-Agent or rate limiting.\n"
                f"Current value: {USER_AGENT!r}"
            ) from exc
        if exc.code >= 500:
            raise FetchError(f"SEC returned {exc.code} for {url}") from exc
        # Anything else in the 4xx range is a bug in the request, not weather.
        raise
    except OSError as exc:
        # HTTPError is caught above; what is left here is transport -- URLError,
        # timeouts, resets, a proxy declining. Same treatment as a refusal.
        raise FetchError(f"could not reach {url}: {exc}") from exc
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
        try:
            body = fetch(TICKER_MAP_URL)
        except RuntimeError:
            # Same reasoning as the document fetch: a refusal here degrades
            # M&A filtering, which main() already warns about, and is not
            # worth ending the run over.
            body = None
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
                "filed": iso_date(filed) or filed,
                "path": path,
                "accession": accession_from_path(path),
            }
        )
    return rows


def iso_date(value):
    """Normalise a filing date to YYYY-MM-DD, or None if it is not a date.

    The daily index writes Date Filed as YYYYMMDD, so events stored it in that
    form, and comparing it against an ISO cutoff silently never matches:
    '20260805' sorts *above* '2026-08-06' because '0' is greater than '-'.
    That one character is why nothing was ever retired.
    """
    if not value:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10] if re.match(r"\d{4}-\d{2}-\d{2}", text) else None


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
        # The position this leaves the insider holding. Free -- it is already in
        # the document -- and it gives the second denominator: a director who
        # just doubled their own stake is saying more than the dollars alone.
        shares_after = _num(
            txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"
        )

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
                "shares_after": shares_after,
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

-- Issuer facts that change slowly and cost a request to learn. Cached here
-- rather than in CACHE_DIR because this file is the state the workflow commits,
-- so the cache survives between runs on a fresh runner.
CREATE TABLE IF NOT EXISTS issuer_facts (
    cik         INTEGER PRIMARY KEY,
    shares_out  REAL,
    as_of       TEXT,
    fetched_at  TEXT
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


# ---------------------------------------------------------------- significance


def shares_outstanding(conn, cik):
    """Shares outstanding for an issuer, cached in the DB for SHARES_TTL_DAYS.

    This is the only term missing from a market cap: the Form 4 already gives
    the price the insider paid, which is the market price that day. So the
    denominator costs one request per issuer per month, from the same host,
    with no market-data vendor involved.

    Returns None when the issuer does not tag either concept -- the scale then
    simply does not apply to that filing, rather than guessing at a cap.
    """
    if not cik:
        return None

    row = conn.execute(
        "SELECT shares_out, fetched_at FROM issuer_facts WHERE cik = ?", (cik,)
    ).fetchone()
    if row and row["fetched_at"]:
        try:
            age = date.today() - date.fromisoformat(row["fetched_at"][:10])
            if age.days < SHARES_TTL_DAYS:
                # Filtered on the way out too, so a bad value already cached by
                # an earlier version stops being served without a refetch.
                return plausible_shares(row["shares_out"])
        except ValueError:
            pass  # unparseable timestamp: fall through and refetch

    value = as_of = None
    for taxonomy, tag in XBRL_CONCEPTS:
        url = (
            f"https://data.sec.gov/api/xbrl/companyconcept/"
            f"CIK{cik:010d}/{taxonomy}/{tag}.json"
        )
        try:
            body = fetch(url)
        except RuntimeError:
            # A refusal on an optional enrichment must not end the run; the
            # filing is still worth reporting without its denominator.
            body = None
        if not body:
            continue
        try:
            points = [
                point
                for unit in json.loads(body).get("units", {}).values()
                for point in unit
                if point.get("val")
            ]
        except (json.JSONDecodeError, AttributeError):
            continue
        if not points:
            continue
        latest = max(points, key=lambda p: (p.get("end") or "", p.get("filed") or ""))
        candidate = plausible_shares(float(latest["val"]))
        if candidate is None:
            continue  # placeholder count; try the other concept
        value, as_of = candidate, latest.get("end")
        break

    # Cache misses too, so an issuer that tags neither concept is not re-asked
    # on every run. The TTL still retires the answer.
    conn.execute(
        """INSERT INTO issuer_facts (cik, shares_out, as_of, fetched_at)
           VALUES (?,?,?,?)
           ON CONFLICT(cik) DO UPDATE SET
             shares_out = excluded.shares_out,
             as_of      = excluded.as_of,
             fetched_at = excluded.fetched_at""",
        (cik, value, as_of, date.today().isoformat()),
    )
    return value


def plausible_shares(shares_out):
    """The share count, or None when it cannot be a real one."""
    if not shares_out or shares_out < MIN_PLAUSIBLE_SHARES:
        return None
    return shares_out


def bps_of_market_cap(value, shares_out, price, shares_bought=None):
    """How much of the company this purchase represents, in basis points.

    Returns None rather than a number whenever the denominator looks wrong. An
    unscored filing is honest; a filing scored against a placeholder share
    count outranks every real signal on the page.
    """
    shares_out = plausible_shares(shares_out)
    if not shares_out:
        return None

    if shares_bought:
        # Share counts, not dollars. The price multiplies both the purchase and
        # the market cap and cancels exactly, so this is the same number by a
        # route that a misreported price cannot corrupt -- one live filing
        # quotes $180,000 a share, and its ratio still comes out right.
        if shares_bought > shares_out:
            # Bought more than the company has: that is not a share count.
            return None
        bps = shares_bought / shares_out * 10_000
    else:
        if not value or not price:
            return None
        market_cap = shares_out * price
        if not market_cap:
            return None
        bps = value / market_cap * 10_000

    return bps if bps <= MAX_PLAUSIBLE_BPS else None


def significance_band(bps):
    """Name the rung on the scale. None when there is no cap to measure against."""
    if bps is None:
        return None
    for floor, name in SIGNIFICANCE_BANDS:
        if bps >= floor:
            return name
    return "negligible"


def format_bps(bps):
    if bps is None:
        return ""
    if bps >= 100:
        return f"{bps / 100:.2f}% of company"
    return f"{bps:.1f} bps of company"


# ---------------------------------------------------------------- run


def log_run(conn, day, status, n_docs, n_events, started):
    conn.execute(
        """INSERT OR REPLACE INTO run_log VALUES (?,?,?,?,?,?,?)""",
        (day.isoformat(), "edgar_daily", status, n_docs, n_events, started,
         datetime.utcnow().isoformat(timespec="seconds")),
    )
    conn.commit()


def run_day(conn, day, tickers, limit=None):
    """Collect one day. Returns 'ok', 'no_index' or 'unavailable'."""
    started = datetime.utcnow().isoformat(timespec="seconds")
    try:
        body = fetch(index_url(day))
    except RuntimeError:
        # The SEC answers 403, not 404, for a daily index that has not been
        # published yet. One day being early is not a reason to abandon the
        # days already collected -- and letting it raise did exactly that,
        # discarding two good days' work before the caller could store it.
        # main() still exits non-zero when EVERY day is refused, which is what
        # an actually blocked client looks like.
        log_run(conn, day, "index_unavailable", 0, 0, started)
        print(f"{day}  index unavailable — not published yet, refused, or unreachable")
        return "unavailable"

    if body is None:
        log_run(conn, day, "no_index", 0, 0, started)
        print(f"{day}  no index published (weekend or holiday)")
        return "no_index"

    rows = parse_master_idx(body)
    if not rows:
        print(f"{day}  WARNING: index fetched ({len(body):,} bytes) but parsed "
              f"to 0 rows — the file format may have changed")
    n_docs = n_events = refused = 0
    status = "ok"
    # Unique accessions, not index rows. EDGAR lists a filing once per filer,
    # so a Form 4 appears under both the issuer and the reporting owner -- about
    # 2.04 rows per filing in practice. Counting rows made the day look twice as
    # big as it is, and made a fully-collected day read as though the collector
    # had stalled.
    n_candidates = len({
        r["accession"] for r in rows
        if r["form_type"] == "4" or r["form_type"] in MA_FORMS
    })

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
            emitted = handle_form4(conn, row, tickers)
            if emitted is None:
                # Refused. Leave the accession unrecorded so a later run
                # retries it, and watch for a run of them: once the SEC starts
                # saying no, asking three thousand more times is the wrong
                # thing to do. Tripping the breaker keeps what we already have.
                refused += 1
                if refused >= MAX_CONSECUTIVE_REFUSALS:
                    status = "partial"
                    print(f"  (stopped after {refused} consecutive refusals; "
                          f"keeping the {n_docs} documents already collected)")
                    break
                continue
            refused = 0
            n_events += emitted
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

    log_run(conn, day, status, n_docs, n_events, started)
    print(f"{day}  {len(rows):,} filings in index, {n_candidates} of interest, "
          f"{n_docs} processed, {n_events} events"
          + ("  [partial: refused]" if status == "partial" else ""))
    return "ok"


def aggregate_buys(buys):
    """Collapse one filing's purchases into the single decision they describe.

    A Form 4 routinely reports a position built across several days as separate
    transaction lines. Emitting only the first understates the commitment and
    tiers on the wrong number: one real filing here reported $220K on 08-05 and
    $438K on 08-06, and the dashboard showed $220K, Tier 2, against a $658K
    total that clears the Tier 1 bar on its own.
    """
    shares = [b["shares"] for b in buys if b["shares"]]
    values = [b["value"] for b in buys if b["value"] is not None]
    dates = sorted(b["txn_date"] for b in buys if b["txn_date"])

    # Volume-weighted over the priced lines only, so the quoted price is the
    # one the reported money actually paid.
    priced = [b for b in buys if b["shares"] and b["price"] is not None]
    volume = sum(b["shares"] for b in priced)
    price = sum(b["shares"] * b["price"] for b in priced) / volume if volume else None

    total_shares = sum(shares) if shares else None

    # The holding after the *last* purchase is the one that stands, so the
    # position before this filing is that figure minus everything bought in it.
    ordered = sorted(buys, key=lambda b: b["txn_date"] or "")
    shares_after = next(
        (b["shares_after"] for b in reversed(ordered) if b.get("shares_after") is not None),
        None,
    )
    position_before = None
    if shares_after is not None and total_shares:
        before = shares_after - total_shares
        position_before = before if before >= 0 else None

    total_value = sum(values) if values else None
    # A price the filing got wrong inflates the dollars and nothing else, so
    # drop the figure rather than print it or tier on it. usd() renders None as
    # "undisclosed", which is exactly what we know.
    value_suspect = total_value is not None and total_value > MAX_PLAUSIBLE_VALUE_USD

    agg = dict(buys[0])
    agg.update(
        {
            "shares": total_shares,
            "price": None if value_suspect else price,
            "value": None if value_suspect else total_value,
            "value_suspect": value_suspect,
            "txn_date": dates[-1] if dates else None,
            "first_txn_date": dates[0] if dates else None,
            "n_purchases": len(buys),
            "shares_after": shares_after,
            "position_before": position_before,
            # A first-ever position has no percentage to grow by; say so rather
            # than dividing by zero or reporting a misleading number.
            "new_position": position_before == 0,
            "pct_position": (
                total_shares / position_before * 100
                if position_before and total_shares
                else None
            ),
        }
    )
    return agg


def score_buy(conn, ticker, buy):
    """Tier an aggregated purchase and describe why.

    Shared by collection and by --rescore so a backfilled event is scored by
    exactly the same rules as a freshly collected one; two copies of this would
    drift apart the first time a threshold moved.
    """
    peers = cluster_insiders(conn, buy["issuer_cik"], buy["txn_date"])
    bps = bps_of_market_cap(
        buy["value"],
        shares_outstanding(conn, buy["issuer_cik"]),
        buy["price"],
        shares_bought=buy.get("shares"),
    )

    big = buy["value"] is not None and buy["value"] >= TIER1_VALUE_USD
    clustered = len(peers) >= CLUSTER_MIN_INSIDERS
    relative = bps is not None and bps >= TIER1_BPS

    # Relative size is the more informative label when both apply: it says what
    # the purchase meant to the company, not just to the buyer.
    reason = (
        "cluster" if clustered
        else "relative size" if relative
        else "size" if big
        else "routine"
    )
    return {
        "tier": 1 if (big or clustered or relative) else 2,
        "reason": reason,
        "peers": peers,
        "bps": bps,
        "band": significance_band(bps),
        "clustered": clustered,
        "headline": buy_headline(ticker, buy, peers, bps, clustered),
    }


def buy_headline(ticker, buy, peers, bps, clustered):
    co_owners = buy.get("co_owners") or []
    return (
        f"{ticker}: {buy['owner']}"
        + (
            f" +{len(co_owners)} co-filer" + ("s" if len(co_owners) > 1 else "")
            if co_owners
            else ""
        )
        + f" ({buy['owner_title']}) bought {usd(buy['value'])}"
        + (
            f" across {buy['n_purchases']} purchases"
            if (buy.get("n_purchases") or 1) > 1
            else ""
        )
        + (f" — {format_bps(bps)}" if bps is not None else "")
        + (f" — {len(peers)} insiders buying" if clustered else "")
    )


def handle_form4(conn, row, tickers):
    """Events emitted for this filing, or None if the SEC refused the document.

    None and 0 mean different things to the caller. 0 is a document we read
    that held no open-market purchase, and it should be recorded so it is never
    fetched again. None is a refusal that says nothing about the contents, so
    the accession stays unrecorded and a later run picks it up.
    """
    try:
        text = fetch(f"https://www.sec.gov/Archives/{row['path']}")
    except RuntimeError:
        return None
    if not text:
        return 0

    buys = parse_form4(extract_ownership_xml(text))
    if not buys:
        return 0

    # The ledger stays per-transaction; only the emitted event is aggregated.
    by_ticker = {}
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
        by_ticker.setdefault(buy["ticker"], []).append(buy)

    emitted = 0
    for ticker, group in by_ticker.items():
        buy = aggregate_buys(group)

        score = score_buy(conn, ticker, buy)

        if score["tier"] == 1:
            promote(conn, ticker, buy["issuer_cik"],
                    f"insider buying ({score['reason']})")

        emitted += emit(
            conn,
            source_id=row["accession"],
            entity=ticker,
            event_type="insider_buy",
            tier=score["tier"],
            headline=score["headline"],
            detail={
                **buy,
                "cluster_peers": score["peers"],
                "tier_reason": score["reason"],
                "bps_of_market_cap": score["bps"],
                "significance": score["band"],
            },
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


def market_today():
    """Today as EDGAR reckons it.

    The collector runs on UTC machines, but filings are stamped in US Eastern
    and a day's index does not appear until roughly 22:00 ET. Any run after
    20:00 ET is already on the next UTC date, so a UTC-based "yesterday" asks
    for a day that, in Eastern terms, has not finished -- which the SEC answers
    with a 403 for a file that does not exist yet. The evening schedule sits
    squarely in that window.
    """
    return datetime.now(ZoneInfo("America/New_York")).date()


def business_days_back(n):
    """The most recent n business days, today included when it is a weekday.

    Today is worth asking for. EDGAR publishes a day's index around 22:00 ET,
    so the evening run can collect that same day rather than leaving it for
    tomorrow morning -- the difference between a filing surfacing in an hour
    and in ten. This used to start from yesterday because an unpublished index
    answered 403 and killed the whole run; run_day now records that as a skip,
    so the early ask costs one request and one honest log line.
    """
    days, cursor = [], market_today()
    while len(days) < n:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
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


def review_events(conn, ticker=None, tier=None):
    """Mark open events reviewed so they leave the dashboard.

    The row is kept -- reviewed_at is a timestamp, not a delete -- so a company
    can be brought back with --unreview and the history stays intact.
    """
    sql = "UPDATE events SET reviewed_at = ? WHERE reviewed_at IS NULL"
    args = [market_today().isoformat()]
    if ticker:
        sql += " AND entity = ?"
        args.append(ticker.upper())
    if tier:
        sql += " AND tier = ?"
        args.append(tier)
    return conn.execute(sql, args).rowcount


def unreview_events(conn, ticker):
    return conn.execute(
        "UPDATE events SET reviewed_at = NULL WHERE entity = ?", (ticker.upper(),)
    ).rowcount


def prune_events(conn):
    """Retire events past their tier's shelf life.

    Nothing ever marked an event reviewed, so the dashboard only grew -- 65
    events one day, 176 two runs later, on the way to thousands. This is the
    same bargain prune_watchlist strikes: the row stays, it just stops being
    shown. Tier 1 keeps a fortnight because that is roughly how long a filing
    is still worth acting on; Tier 2 is largely 425 merger chatter and routine
    small buys, which go stale in days. A filing that mattered has already
    promoted its company to the watchlist, which has its own longer clock.
    """
    today = market_today()
    cutoffs = {
        tier: (today - timedelta(days=days)).isoformat()
        for tier, days in EVENT_TTL_DAYS.items()
    }

    # Compared in Python rather than SQL because filed_date is whatever the
    # index wrote -- YYYYMMDD historically, ISO now -- and a string comparison
    # across both forms is wrong in a way that fails silently.
    stale = []
    for row in conn.execute(
        "SELECT id, tier, filed_date, created_at FROM events WHERE reviewed_at IS NULL"
    ).fetchall():
        cutoff = cutoffs.get(row["tier"])
        when = iso_date(row["filed_date"]) or iso_date(row["created_at"])
        if cutoff and when and when < cutoff:
            stale.append(row["id"])

    for start in range(0, len(stale), 500):
        chunk = stale[start:start + 500]
        conn.execute(
            f"UPDATE events SET reviewed_at = ? WHERE id IN ({','.join('?' * len(chunk))})",
            [today.isoformat(), *chunk],
        )
    return len(stale)


def rescore(conn):
    """Backfill significance onto events emitted before the scale existed.

    emit() is idempotent and already_processed stops the documents being read
    again, so stored events never pick up a bps figure on their own -- they
    would rank below every newer filing forever. The ledger still holds each
    transaction, so both the aggregate and the score can be rebuilt without
    refetching a single document.

    Returns (rescored, promoted). Events whose issuer tags no share count stay
    unscored, and pct_position cannot be recovered because the ledger has no
    column for the post-transaction holding -- only newly collected filings
    carry that.
    """
    rescored = promoted = 0
    for row in conn.execute(
        "SELECT id, source_id, entity, detail FROM events "
        "WHERE event_type = 'insider_buy'"
    ).fetchall():
        detail = json.loads(row["detail"] or "{}")
        stored = detail.get("bps_of_market_cap")
        # Already scored and the score is sane -- nothing to do. A stored figure
        # above the plausible ceiling was computed against a bad share count and
        # needs clearing, so it is deliberately not skipped here.
        if stored is not None and stored <= MAX_PLAUSIBLE_BPS:
            continue

        ledger = conn.execute(
            "SELECT * FROM insider_buys WHERE accession = ? AND ticker = ?",
            (row["source_id"], row["entity"]),
        ).fetchall()
        if not ledger:
            continue

        # Rebuilding from the ledger also repairs any event stored before
        # aggregate_buys existed, which recorded only the first purchase line.
        buy = aggregate_buys(
            [dict(b, co_owners=detail.get("co_owners") or [], shares_after=None)
             for b in ledger]
        )
        buy["pct_position"] = detail.get("pct_position")
        buy["new_position"] = detail.get("new_position", False)

        score = score_buy(conn, row["entity"], buy)
        # Nothing gained and nothing to correct: leave it alone. When there IS a
        # stored figure we fall through even with no new one, so a score built
        # on a placeholder share count is written back out as unscored.
        if score["bps"] is None and stored is None:
            continue

        if score["tier"] == 1:
            promoted += promote(conn, row["entity"], buy["issuer_cik"],
                                f"insider buying ({score['reason']})")

        conn.execute(
            "UPDATE events SET tier = ?, headline = ?, detail = ? WHERE id = ?",
            (
                score["tier"],
                score["headline"],
                json.dumps(
                    {
                        **buy,
                        "cluster_peers": score["peers"],
                        "tier_reason": score["reason"],
                        "bps_of_market_cap": score["bps"],
                        "significance": score["band"],
                    },
                    default=str,
                ),
                row["id"],
            ),
        )
        rescored += 1
    return rescored, promoted


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
/* Tickers link out to Yahoo Finance. Styled to look exactly like the plain
   symbol until you touch it -- the ticker is the label, the link is a
   convenience, and an underline on every card would be visual noise. The
   underline is drawn as a border so it does not shift the baseline. */
.ticker a, .watch b a {
  color: inherit; text-decoration: none;
  border-bottom: 1px solid transparent;
}
.ticker a:hover, .watch b a:hover { border-bottom-color: currentColor; }
.ticker a:focus-visible, .watch b a:focus-visible {
  outline: 2px solid var(--signal); outline-offset: 2px; border-radius: 1px;
}
/* The significance scale. One ramp of weight and colour so the rungs read in
   order at a glance -- negligible recedes into the page, major is the only one
   that fills. Bands carry a word as well as the shading, so the ranking does
   not depend on colour perception. */
.scale { margin-top: 6px; display: flex; flex-direction: column; gap: 3px; }
.band {
  font-family: "IBM Plex Mono", monospace; font-size: 9.5px; font-weight: 600;
  letter-spacing: .08em; text-transform: uppercase; text-align: center;
  padding: 2px 4px; border: 1px solid var(--rule); color: var(--muted);
}
.b-minor      { border-color: var(--muted); color: var(--ink); }
.b-notable    { border-color: var(--ink); color: var(--ink); }
.b-significant{ border-color: var(--signal); color: var(--signal); }
.b-major      { border-color: var(--signal); background: var(--signal); color: #fff; }
.bps {
  font-family: "IBM Plex Mono", monospace; font-size: 9.5px;
  color: var(--muted); text-align: center; letter-spacing: -.01em;
}
/* Secondary filings for a company already shown above. Subordinate to the
   headline on purpose: the card is the story, these are its other filings. */
.more {
  list-style: none; margin: 10px 0 0; padding: 8px 0 0;
  border-top: 1px solid var(--rule);
}
.more li {
  display: flex; justify-content: space-between; align-items: baseline;
  gap: 12px; padding: 3px 0; font-size: 13.5px; color: var(--muted);
}
.more li span {
  font-family: "IBM Plex Mono", monospace; font-size: 11px; white-space: nowrap;
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


YAHOO_QUOTE = "https://finance.yahoo.com/quote/{}"

# Placeholders EDGAR emits when an issuer has no traded symbol. WILSON BANK
# HOLDING CO really does report its trading symbol as the string "none", and
# linking that lands on an empty quote page, so these render as plain text.
NON_TICKERS = {"", "-", "—", "none", "n/a", "na", "null"}


def yahoo_url(ticker):
    """Quote-page URL for a symbol, or None when it is not one.

    Yahoo writes class shares with a hyphen where EDGAR uses a dot, so BRK.B
    has to become BRK-B to resolve. On a phone these are universal links: iOS
    and Android hand them to the Yahoo Finance app when it is installed, and
    fall back to the browser when it is not.
    """
    symbol = (ticker or "").strip()
    if symbol.lower() in NON_TICKERS:
        return None
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9.\-]{0,9}", symbol):
        return None
    return YAHOO_QUOTE.format(
        urllib.parse.quote(symbol.upper().replace(".", "-"), safe="")
    )


def ticker_link(ticker, fallback="—"):
    """The symbol as a link when it resolves, as plain text when it does not."""
    label = html.escape(ticker or fallback)
    url = yahoo_url(ticker)
    if not url:
        return label
    return (
        f'<a href="{html.escape(url)}" target="_blank" rel="noopener noreferrer"'
        f' title="{label} on Yahoo Finance">{label}</a>'
    )


def _initials(name):
    parts = [p for p in re.split(r"[\s,]+", name or "") if p]
    return "".join(p[0].upper() for p in parts[:2]) or "??"


def conviction(event):
    """Sort key inside a tier, as a (class, magnitude) pair.

    Deal filings lead: a definitive merger document is the least ambiguous
    thing this collector finds, and it has no dollar size to compare anyway.
    Buys then rank by share of the company rather than by dollars, which is
    the point of the scale -- $5M at a mega-cap is a smaller statement than
    $60K at a nano-cap. Filings whose issuer tags no share count keep ranking
    by dollars among themselves; they cannot be compared to a bps figure
    honestly, so they are not mixed in with one.
    """
    if event["event_type"] != "insider_buy":
        return (2, 0.0)
    detail = json.loads(event["detail"] or "{}")
    bps = detail.get("bps_of_market_cap")
    if bps is not None:
        return (1, bps)
    return (0, detail.get("value") or 0)


def group_by_company(events):
    """One card per ticker. A company that files three times in a week is one
    story told three times, not three stories."""
    groups = {}
    for event in events:
        groups.setdefault(event["entity"], []).append(event)
    ranked = [
        (entity, sorted(evs, key=conviction, reverse=True))
        for entity, evs in groups.items()
    ]
    ranked.sort(key=lambda g: conviction(g[1][0]), reverse=True)
    return ranked


def _strip_ticker(headline, entity):
    """The ticker is already the card's left column; drop the prefix the
    headline carries for the CLI listing."""
    prefix = f"{entity}: "
    return headline[len(prefix):] if headline.startswith(prefix) else headline


def render_company(entity, events):
    primary = events[0]
    detail = json.loads(primary["detail"] or "{}")
    bits = [f"filed {html.escape(primary['filed_date'] or '')}"]

    if primary["event_type"] == "insider_buy":
        if detail.get("shares") and detail.get("price"):
            bits.append(f"{int(detail['shares']):,} sh @ ${detail['price']:,.2f}")
        # The second denominator: what this did to the buyer's own stake.
        if detail.get("new_position"):
            bits.append("new position")
        elif detail.get("pct_position"):
            bits.append(f"+{detail['pct_position']:,.0f}% to position")
        first, last = detail.get("first_txn_date"), detail.get("txn_date")
        if last:
            span = f"{first} → {last}" if first and first != last else last
            bits.append(f"traded {html.escape(span)}")
    else:
        bits.append(html.escape(detail.get("form_type", "")))
    bits.append(html.escape(primary["source_id"]))

    # Signature: one chip per distinct insider, so a cluster is visible at a
    # glance. Pooled across the card's filings, because each one only recorded
    # the peers visible when it was processed -- the filing that happens to
    # lead on dollar value is often the one that saw the fewest.
    peers = list(
        dict.fromkeys(
            name
            for event in events
            for name in (json.loads(event["detail"] or "{}").get("cluster_peers") or [])
        )
    )
    chips = ""
    if len(peers) > 1:
        marks = "".join(
            f'<span class="chip">{html.escape(_initials(p))}</span>' for p in peers[:6]
        )
        chips = f'<div class="chips">{marks}</div>'

    more = ""
    if len(events) > 1:
        items = "".join(
            f"<li>{html.escape(_strip_ticker(e['headline'] or '', entity))}"
            f"<span>{html.escape(e['filed_date'] or '')}</span></li>"
            for e in events[1:]
        )
        more = f'<ul class="more">{items}</ul>'

    band = detail.get("significance")
    badge = ""
    if band:
        bps = detail.get("bps_of_market_cap")
        badge = (
            f'<span class="band b-{band}">{html.escape(band)}</span>'
            f'<span class="bps">{html.escape(format_bps(bps))}</span>'
        )

    return f"""<div class="row">
  <div class="ticker">{ticker_link(entity)}
    {f'<div class="scale">{badge}</div>' if badge else ''}
  </div>
  <div>
    <p class="headline">{html.escape(_strip_ticker(primary['headline'] or '', entity))}</p>
    <div class="detail">{"".join(f"<span>{b}</span>" for b in bits)}</div>
    {chips}
    {more}
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
        companies = group_by_company(rows)
        body = (
            "".join(render_company(entity, evs) for entity, evs in companies)
            if companies
            else '<p class="empty">Nothing yet. Run a collection to populate this.</p>'
        )
        count = f"{len(companies)}"
        if len(rows) != len(companies):
            count += f" · {len(rows)} filings"
        sections.append(
            f'<section class="t{tier}"><div class="tier-head">'
            f"<b>Tier {tier} — {label}</b><span>{count}</span></div>{body}</section>"
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
                f'<div class="{pin.strip()}"><b>{ticker_link(w["ticker"])}</b>'
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
    ap.add_argument("--review", metavar="TICKER",
                    help="mark a company's open events reviewed (off the dashboard)")
    ap.add_argument("--review-tier", type=int, choices=[1, 2],
                    help="mark every open event in a tier reviewed")
    ap.add_argument("--unreview", metavar="TICKER",
                    help="put a company's events back on the dashboard")
    ap.add_argument("--rescore", action="store_true",
                    help="backfill significance onto events stored before the scale")
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

    if args.review or args.review_tier:
        n = review_events(conn, ticker=args.review, tier=args.review_tier)
        conn.commit()
        scope = args.review.upper() if args.review else f"tier {args.review_tier}"
        print(f"reviewed {n} event(s) for {scope}" if n else "nothing open to review")
        if not args.html:
            return

    if args.unreview:
        n = unreview_events(conn, args.unreview)
        conn.commit()
        print(f"restored {n} event(s) for {args.unreview.upper()}"
              if n else "no events for that ticker")
        if not args.html:
            return

    if args.rescore:
        n, promoted = rescore(conn)
        conn.commit()
        print(f"rescored {n} event(s)"
              + (f", {promoted} promoted to the watchlist" if promoted else ""))
        if not args.html:
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
        days = [market_today()]

    outcomes = [run_day(conn, day, tickers, limit=args.limit) for day in days]
    if outcomes and all(o == "unavailable" for o in outcomes):
        # One refused index is a publication gap; every index refused is us.
        sys.exit(
            "every daily index was refused. That is a blocked client, not a\n"
            "timing gap -- check EDGAR_USER_AGENT and how often this is running."
        )

    aged = prune_events(conn)
    retired = prune_watchlist(conn)
    conn.commit()
    if aged:
        print(f"retired {aged} events past their shelf life")
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
