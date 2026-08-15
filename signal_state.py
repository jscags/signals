"""Ordered state machine for issuer catalyst lifecycle, with a disqualifier layer.

Replaces flat two-tier classification. A tier says how loud a filing was; a
state says where the issuer is in an arc, and an arc can go backwards. The
difference matters most at the end of it: two-tier had no way to express "this
company was interesting and now is not", so a name promoted on insider buying
stayed promoted while the reasons to own it fell away.

    DORMANT -> SETUP -> CONFIRMED -> EXTENDED -> DISTRIBUTING -> DISTRESSED

The order is a lifecycle, not a ranking. DORMANT is the absence of anything;
SETUP is a precursor without confirming action; CONFIRMED is an insider
committing cash at market; EXTENDED is that commitment broadening; DISTRIBUTING
is insiders leaving; DISTRESSED is the company itself in trouble.

The load-bearing property is that DISQUALIFIERS OVERRIDE CATALYSTS. An
open-market purchase at an issuer with an active restatement is not a
promotion, it is a warning -- somebody bought into a company whose numbers are
in question, and the state that describes that is DISTRESSED. Catalyst logic
never gets to argue with it. Every rule below is applied in that order and the
reason string records which one won, so any non-DORMANT state can be traced
back to the filings that produced it.

stdlib only, like the collector it plugs into. Storage is the same SQLite
connection; this module owns four tables and reads two of edgar_discovery's.

Integration instructions are at the bottom of this file.
"""

import json
import sqlite3
import sys
from datetime import date, datetime, timedelta

# ---------------------------------------------------------------- states

DORMANT = "DORMANT"
SETUP = "SETUP"
CONFIRMED = "CONFIRMED"
EXTENDED = "EXTENDED"
DISTRIBUTING = "DISTRIBUTING"
DISTRESSED = "DISTRESSED"

# Index position is the state's rank in the lifecycle. Used for ordering and
# for reporting a transition's direction, never for "better/worse" -- moving
# from CONFIRMED to DISTRIBUTING is a real move forward through the arc and a
# thoroughly bad one for the position.
STATES = (DORMANT, SETUP, CONFIRMED, EXTENDED, DISTRIBUTING, DISTRESSED)
RANK = {name: i for i, name in enumerate(STATES)}

# ---------------------------------------------------------------- windows

# How long a purchase keeps an issuer CONFIRMED without further buying. Beyond
# this the signal has not been contradicted, it has simply gone stale, and the
# issuer falls back rather than sitting CONFIRMED forever.
BUY_WINDOW_DAYS = 45
SALE_WINDOW_DAYS = 45

# Two distinct insiders buying inside this window is the cluster that separates
# EXTENDED from CONFIRMED. Same rule the collector already uses for clusters,
# restated here so the state machine does not silently drift from it.
EXTENDED_MIN_BUYERS = 2
CLUSTER_WINDOW_DAYS = 10

# A restatement or a delisting notice casts a longer shadow than a purchase.
# Six months is long enough to cover a late filing being cured; a disqualifier
# that is genuinely resolved gets cleared explicitly by clear_disqualifier().
DISQUALIFIER_TTL_DAYS = 180

# Selling below this is noise -- tax-withholding sales, small automatic
# dispositions. DISTRIBUTING should mean insiders leaving, not housekeeping.
MIN_MEANINGFUL_SALE_USD = 100_000

# Buying above this multiple of the selling means DISTRIBUTING is the wrong
# reading. Disqualifiers still outrank everything -- a forward split or a
# restatement wins outright, unchanged -- but a lone sale against a buying
# cluster was never a disqualifier, and the rule could not tell them apart.
SELL_OVERRIDE_RATIO = 3.0

# And the same on the buy side, which had no floor at all. "Starr Gayle P
# bought $288", "bought $212", and Horizon Kinetics buying ONE share of TPL at
# $352.62 on three consecutive days were all drawing cards. Those are dividend
# reinvestments and fractional accruals -- a plan executing, not a person
# deciding. An order of magnitude below the sale floor because a purchase is
# the rarer and more informative act, so it earns a lower bar.
#
# Applied to the window total, not to each row: five deliberate $5,000
# purchases are a position being built and should clear, while one $353
# accrual should not.
MIN_MEANINGFUL_BUY_USD = 10_000

# ---------------------------------------------------------------- disqualifiers

# Form types that mean trouble and that cost NOTHING to detect, because the
# daily master index already carries the form type on every row.
#
# What is deliberately missing: the 8-K restatement. Item 4.02 (non-reliance on
# previously issued financials) is the cleanest restatement signal there is,
# and it is NOT in the daily index -- item codes live in the filing header, so
# routing on them costs one fetch per 8-K, thousands a day. That is a real
# collector to build, not a line to add here, so this layer covers the free
# signals and record_disqualifier() stays open for the 8-K scanner to call
# once it exists. See the note in scan_index_row_for_disqualifiers().
DISQUALIFYING_FORMS = {
    # Cannot file on time. The most common precursor to everything else here.
    "NT 10-K": "late annual report",
    "NT 10-Q": "late quarterly report",
    "NT 20-F": "late annual report (foreign issuer)",
    # Exchange has moved to delist, or the issuer is withdrawing.
    "25": "delisting notice",
    "25-NSE": "delisting notice (exchange-initiated)",
    # Deregistration -- the company is leaving the reporting system entirely.
    "15-12B": "deregistration",
    "15-12G": "deregistration",
    "15-15D": "suspension of reporting duty",
}

_USER_AGENT = None


def set_user_agent(agent):
    """Adopt the collector's User-Agent rather than minting a second one.

    Nothing in this module fetches today -- the disqualifier layer reads form
    types the collector has already downloaded. The agent is held for the
    deferred 8-K Item 4.02 scanner, and _require_user_agent() below makes that
    path fail loudly rather than issuing an anonymous request, which the SEC
    would refuse and which would be indistinguishable from "no restatements".
    """
    global _USER_AGENT
    if not agent or not str(agent).strip():
        raise ValueError(
            "signal_state.set_user_agent() needs the collector's User-Agent "
            "string, not an empty value."
        )
    _USER_AGENT = str(agent).strip()


def _require_user_agent():
    if not _USER_AGENT:
        raise RuntimeError(
            "signal_state has no User-Agent. Call set_user_agent(USER_AGENT) "
            "with the collector's string before any path that reaches the SEC."
        )
    return _USER_AGENT


# ---------------------------------------------------------------- storage

SCHEMA = """
-- Where each issuer stands right now. One row per issuer, overwritten in place.
CREATE TABLE IF NOT EXISTS issuer_state (
    cik         INTEGER PRIMARY KEY,
    ticker      TEXT,
    state       TEXT NOT NULL,
    reason      TEXT NOT NULL,
    since       TEXT,
    updated_at  TEXT,
    rule        TEXT
);

-- What CHANGED, and when. The dashboard reads this rather than issuer_state,
-- because a standing state redrawn every twelve hours is not news. The UNIQUE
-- constraint is the idempotence guarantee: the 7am and 9pm passes over the
-- same day cannot both record the same move.
CREATE TABLE IF NOT EXISTS state_transitions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    cik         INTEGER NOT NULL,
    ticker      TEXT,
    from_state  TEXT NOT NULL,
    to_state    TEXT NOT NULL,
    reason      TEXT NOT NULL,
    observed_on TEXT NOT NULL,
    created_at  TEXT,
    rule        TEXT,
    UNIQUE(cik, from_state, to_state, observed_on)
);
CREATE INDEX IF NOT EXISTS idx_transitions_on ON state_transitions(observed_on);

-- Active reasons an issuer cannot be promoted regardless of what insiders do.
CREATE TABLE IF NOT EXISTS disqualifiers (
    cik         INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    detail      TEXT,
    form_type   TEXT,
    accession   TEXT,
    filed       TEXT,
    expires_on  TEXT,
    cleared_at  TEXT,
    UNIQUE(cik, accession, kind)
);
CREATE INDEX IF NOT EXISTS idx_disq_cik ON disqualifiers(cik);

-- The mirror of edgar_discovery's insider_buys. Same shape on purpose: the
-- two are read side by side and a different schema would mean two ways to ask
-- the same question.
CREATE TABLE IF NOT EXISTS insider_sales (
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
    suspect      INTEGER DEFAULT 0,
    UNIQUE(accession, owner, txn_date, shares, price)
);
CREATE INDEX IF NOT EXISTS idx_sales_issuer ON insider_sales(issuer_cik, txn_date);

-- Issuers that must never be evaluated again, whatever their ledger says.
--
-- Deleting an issuer's state is not enough to remove it: the ledger rows are
-- kept on purpose as a record of what was filed, so the very next pass finds
-- them again and re-creates everything. Nine non-traded funds were retired and
-- resurrected inside a single run that way, coming back as cards headed
-- "CIK 2089975" because the rename had nothing to rename them to.
CREATE TABLE IF NOT EXISTS retired_issuers (
    cik        INTEGER PRIMARY KEY,
    reason     TEXT,
    retired_at TEXT
);
"""


def migrate(conn):
    """Create this module's tables on the collector's existing connection.

    Safe to call on every run. Takes the connection rather than opening its
    own so state changes land in the same transaction as the events that
    caused them -- a transition recorded against a filing that was rolled back
    would be a state with no traceable reason, which is the one thing this
    module promises never to produce.
    """
    conn.executescript(SCHEMA)

    # CREATE TABLE IF NOT EXISTS will not add a column to a table that already
    # exists, so a database written before `suspect` needs it grafted on. Same
    # pattern the collector uses for its own tables; NULL reads as 0 via the
    # COALESCE in the readers, so old rows behave as unflagged.
    present = {r[1] for r in conn.execute("PRAGMA table_info(insider_sales)")}
    if "suspect" not in present:
        conn.execute("ALTER TABLE insider_sales ADD COLUMN suspect INTEGER DEFAULT 0")

    conn.commit()
    return conn


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _as_date(value, default=None):
    """Parse YYYY-MM-DD or YYYYMMDD; None when it is not a date."""
    if isinstance(value, date):
        return value
    if not value:
        return default
    text = str(value).strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return default


# ---------------------------------------------------------------- disqualifiers


def record_disqualifier(conn, cik, kind, detail=None, form_type=None,
                        accession=None, filed=None, ttl_days=None):
    """Note a reason this issuer cannot be promoted. Idempotent per accession.

    Open to callers other than the index scanner -- an 8-K Item 4.02 scanner
    would come through here too, which is why kind is a free string rather
    than an enum over the form types this file happens to know about.
    """
    filed_on = _as_date(filed, date.today())
    ttl = DISQUALIFIER_TTL_DAYS if ttl_days is None else ttl_days
    conn.execute(
        """INSERT OR IGNORE INTO disqualifiers
           (cik, kind, detail, form_type, accession, filed, expires_on, cleared_at)
           VALUES (?,?,?,?,?,?,?,NULL)""",
        (int(cik), kind, detail, form_type, accession, filed_on.isoformat(),
         (filed_on + timedelta(days=ttl)).isoformat()),
    )
    return conn.total_changes


def clear_disqualifier(conn, cik, accession=None):
    """Mark a disqualifier resolved before its TTL runs out.

    A late filer that files is no longer late, and waiting six months to say so
    would leave the issuer DISTRESSED through the exact period when the
    resolution is the interesting fact about it.
    """
    if accession:
        cur = conn.execute(
            "UPDATE disqualifiers SET cleared_at = ? "
            "WHERE cik = ? AND accession = ? AND cleared_at IS NULL",
            (_now(), int(cik), accession))
    else:
        cur = conn.execute(
            "UPDATE disqualifiers SET cleared_at = ? "
            "WHERE cik = ? AND cleared_at IS NULL", (_now(), int(cik)))
    return cur.rowcount


def active_disqualifiers(conn, cik, as_of=None):
    """Uncleared, unexpired disqualifiers for an issuer, newest first."""
    today = (as_of or date.today())
    today = today.isoformat() if isinstance(today, date) else str(today)
    return conn.execute(
        """SELECT * FROM disqualifiers
           WHERE cik = ? AND cleared_at IS NULL AND expires_on >= ?
           ORDER BY filed DESC""",
        (int(cik), today),
    ).fetchall()


def scan_index_row_for_disqualifiers(conn, row, watched_ciks=None):
    """Read one daily-index row for trouble. Costs nothing -- no request.

    The row is already in hand from the master index the collector fetched, so
    this is a dictionary lookup on the form type. Gate it on watchlist
    membership at the call site: every issuer in EDGAR files these eventually,
    and a disqualifier table covering the whole market would be large, mostly
    irrelevant, and would slow every evaluate() that scans it.

    Returns 1 if a disqualifier was recorded, else 0.

    Not covered here, and worth knowing: the 8-K restatement (Item 4.02). Item
    codes are not in the daily index -- they live in the filing header -- so
    detecting one costs a fetch per 8-K, which is thousands a day and a
    separate collector's job. When that exists it calls record_disqualifier()
    directly and everything downstream works unchanged.
    """
    form_type = (row.get("form_type") or "").strip().upper()
    kind = DISQUALIFYING_FORMS.get(form_type)
    if not kind:
        return 0

    cik = row.get("cik")
    if not cik:
        return 0
    if watched_ciks is not None and int(cik) not in watched_ciks:
        return 0

    record_disqualifier(
        conn, cik, kind,
        detail=row.get("company"),
        form_type=form_type,
        accession=row.get("accession"),
        filed=row.get("filed"),
    )
    return 1


# ---------------------------------------------------------------- insider sales


# Mirrors edgar_discovery.MAX_PLAUSIBLE_TXN_USD. Duplicated as a literal rather
# than imported because this module must run standalone for its self-test, and a
# circular import to fetch one integer is a worse trade than restating it here.
MAX_PLAUSIBLE_TXN_USD = 2_000_000_000


def retire_issuer(conn, cik, reason):
    """Take an issuer out of the state machine permanently. Idempotent."""
    conn.execute(
        "INSERT OR IGNORE INTO retired_issuers (cik, reason, retired_at) "
        "VALUES (?,?,?)", (int(cik), reason, _now()))
    conn.execute("DELETE FROM state_transitions WHERE cik = ?", (int(cik),))
    conn.execute("DELETE FROM issuer_state WHERE cik = ?", (int(cik),))
    return 1


def is_retired(conn, cik):
    return conn.execute("SELECT 1 FROM retired_issuers WHERE cik = ?",
                        (int(cik),)).fetchone() is not None


def record_insider_sales(conn, sales, accession=None):
    """Store code-S / disposed-D transactions. The mirror of the code-P path.

    Deliberately a separate table and a separate call rather than a flag on
    insider_buys: the purchase ledger is what the collector's tiering, cluster
    detection and significance scale all read, and quietly teaching it to hold
    sales would change every one of those without anyone asking for it.

    Takes the same dicts parse_form4 produces, so the call site is the sale
    branch of the existing parser and nothing needs reshaping.
    """
    n = 0
    for sale in sales or []:
        value = sale.get("value")
        if value is None and sale.get("shares") and sale.get("price"):
            value = sale["shares"] * sale["price"]
        # Same ceiling as the buy side. The sell path was mirrored from the
        # buy parser but not from its guards, so nothing here checked a figure
        # at all -- and DISTRIBUTING is driven by summed dollars.
        suspect = int((value or 0) > MAX_PLAUSIBLE_TXN_USD)
        cur = conn.execute(
            """INSERT OR IGNORE INTO insider_sales
               (accession, issuer_cik, ticker, issuer, owner, owner_title,
                txn_date, shares, price, value, suspect)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            # parse_form4 works from the XML and never sees an accession, so
            # sale.get("accession") was None on every row ever written -- all
            # 1,166 of them -- and a stored sale could not be traced back to the
            # filing it came from. The buy path takes it from the call site the
            # same way; the sale path was mirrored without that argument.
            (accession or sale.get("accession"),
             sale.get("issuer_cik"), sale.get("ticker"),
             sale.get("issuer"), sale.get("owner"), sale.get("owner_title"),
             sale.get("txn_date"), sale.get("shares"), sale.get("price"), value,
             suspect),
        )
        n += cur.rowcount
    return n


# ---------------------------------------------------------------- evaluation


def _window(as_of, days):
    return (as_of - timedelta(days=days)).isoformat(), as_of.isoformat()


def _has_table(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _setup_condition(conn, cik):
    """The collector's cached Lane A verdict, or None.

    Read rather than computed here: the metric needs a companyfacts payload,
    the collector already fetches one per periodic filer, and a second fetch
    for the same document would double the request cost of the whole lane.
    Absent when the table does not exist, which is the standalone self-test.
    """
    if not cik or not _has_table(conn, "issuer_setup"):
        return None
    row = conn.execute(
        "SELECT setup, streak, reason FROM issuer_setup WHERE cik = ?",
        (int(cik),)).fetchone()
    return dict(row) if row else None


def _corporate_events(conn, ticker, as_of, days=BUY_WINDOW_DAYS):
    """Open buyback and M&A events for this issuer, strongest first.

    These live in the collector's events table, which this module reads but
    does not own -- and which is absent when the module runs standalone (the
    self-test), so its absence is an ordinary answer rather than an error.

    Without this, two of the collector's three signals never reached the state
    machine at all: an issuer known only for a buyback or a merger document had
    no insider ledger rows, so it never transitioned, so it never appeared on a
    dashboard built from transitions. Measured on the live database the day the
    dashboard switched over, that was 376 of 517 companies gone -- two thirds
    of the page, dropped by an integration seam rather than by anyone's intent.
    """
    if not ticker or not _has_table(conn, "events"):
        return []
    since, until = _window(as_of, days)
    return conn.execute(
        """SELECT event_type, tier, headline, filed_date FROM events
           WHERE entity = ? AND reviewed_at IS NULL
             AND (event_type = 'buyback' OR event_type LIKE 'ma\\_%' ESCAPE '\\')
             AND filed_date BETWEEN ? AND ?
           ORDER BY tier ASC, filed_date DESC""",
        (ticker, since, until),
    ).fetchall()


def _txn_key(row):
    """What makes two ledger rows the same transaction.

    Owner IS part of it, and the reason is worth recording. The live ledger has
    48 sale groups and 24 buy groups where one (date, shares, price) appears
    under several owner names, which looks like one event filed once per
    co-filer -- and on the sale side, for ATTO, a 23-lot broker sequence
    matching to the cent under two names, it certainly is.

    But the buy side proves the same shape occurs for real: four HEPA directors
    each bought 2,000,000 shares at $0.05 on one day, three TSMC directors each
    bought 47 shares at $73.31, separate people, separate accessions. Keying
    without the owner would have merged those into one buyer and quietly
    dissolved every cluster EXTENDED depends on.

    The two cases are not separable from the ledger as it stands, because
    insider_sales.accession was NULL on all 1,166 rows -- and the UNIQUE
    constraint on that table leads with accession, which is why SQLite, where
    NULL never equals NULL, never deduplicated a single sale. Recording it (see
    record_insider_sales) is what makes the question answerable at all; until
    there is a measurement that separates them, nothing is collapsed.
    """
    return (row["owner"], row["txn_date"], row["shares"], row["price"])


def _cancel_offsetting(buys, sales):
    """Drop same-day purchases and disposals that undo each other.

    STWI's Form 4 reports, on one day, under one owner, an S/D of 3,000,000
    shares at $0.138 and a P/A of 3,000,000 shares at $0.138. Both are really
    in the document -- the parser reads them correctly, one to each ledger --
    and together they are a transfer between holdings rather than a decision
    to buy or to sell. Counting either side made the card announce a purchase
    and a sale of the same $414,000 in the same breath.

    Matched on owner as well as the transaction, because an insider selling
    while a different insider buys the same size on the same day is two
    decisions, and that is exactly the case the D1 precedence rule is for.
    """
    sale_keys = {}
    for sale in sales:
        sale_keys.setdefault((sale["owner"],) + _txn_key(sale), []).append(sale)
    washed = set()
    for buy in buys:
        key = (buy["owner"],) + _txn_key(buy)
        if sale_keys.get(key):
            sale_keys[key].pop()
            washed.add(id(buy))
    kept_sales = [s for group in sale_keys.values() for s in group]
    kept_buys = [b for b in buys if id(b) not in washed]
    return kept_buys, kept_sales


def _buys(conn, cik, as_of, days=BUY_WINDOW_DAYS):
    since, until = _window(as_of, days)
    return conn.execute(
        """SELECT owner, txn_date, shares, price, value FROM insider_buys
           WHERE issuer_cik = ? AND COALESCE(suspect, 0) = 0
             AND txn_date BETWEEN ? AND ?
           ORDER BY txn_date DESC""",
        (int(cik), since, until),
    ).fetchall()


def _sales(conn, cik, as_of, days=SALE_WINDOW_DAYS):
    since, until = _window(as_of, days)
    return conn.execute(
        """SELECT owner, txn_date, shares, price, value FROM insider_sales
           WHERE issuer_cik = ? AND COALESCE(suspect, 0) = 0
             AND txn_date BETWEEN ? AND ?
           ORDER BY txn_date DESC""",
        (int(cik), since, until),
    ).fetchall()


def evaluate(conn, cik, ticker=None, as_of=None,
             has_setup_signal=False, is_crowded=False):
    """The state this issuer is in, and the traceable reason it is in it.

    Rules are applied in a fixed order and the FIRST match wins, which is what
    makes "disqualifiers override catalysts" a property of the code rather
    than a convention someone has to remember:

      1. an active disqualifier            -> DISTRESSED
      2. meaningful insider selling        -> DISTRIBUTING
      3. two or more insiders buying       -> EXTENDED
      4. open-market buying above the floor -> CONFIRMED
      5. a definitive deal document        -> CONFIRMED
      6. the Lane A balance-sheet setup    -> SETUP
      7. a buyback                         -> SETUP
      8. nothing                           -> DORMANT

    has_setup_signal is now an OVERRIDE rather than the only route: passing
    True forces SETUP, and leaving it False lets the cached Lane A verdict
    decide. That keeps the parameter useful for testing a state directly while
    the real signal comes from the collector. is_crowded remains deferred.

    Returns a dict: state, reason, and the evidence the reason was drawn from.
    """
    as_of = as_of or date.today()
    as_of = _as_date(as_of, date.today())

    disqualifiers = active_disqualifiers(conn, cik, as_of)
    if disqualifiers:
        top = disqualifiers[0]
        others = f" (+{len(disqualifiers) - 1} more)" if len(disqualifiers) > 1 else ""
        return {
            "state": DISTRESSED,
            "rule": "disqualifier",
            "reason": f"{top['kind']} filed {top['filed']}"
                      f"{' — ' + top['form_type'] if top['form_type'] else ''}{others}",
            "evidence": {"disqualifiers": [dict(d) for d in disqualifiers]},
        }

    # Both sides are read before either is judged. The sell rule used to return
    # without ever calling _buys, so a single sale outranked any amount of
    # contemporaneous buying: ATTO announced "1 insider sold $2,251,658" on a
    # card that also showed six insiders buying $52m.
    buys, sales = _cancel_offsetting(_buys(conn, cik, as_of),
                                     _sales(conn, cik, as_of))
    sold = sum(s["value"] or 0 for s in sales)
    bought = sum(b["value"] or 0 for b in buys)

    # Buying this far above the selling means the sale is not the story. The
    # ratio is read off the live distribution rather than chosen: across the
    # 266 DISTRIBUTING issuers only 9 had any buying at all, and their
    # buy-to-sell ratios run 0.0, 0.1, 0.3, 0.5, 0.7 and then nothing whatever
    # until 10.1, 11.4, 11.6. Anything from 2x to 5x separates those groups
    # identically, so 3x sits in empty space and is insensitive to where in it.
    if sales and sold >= MIN_MEANINGFUL_SALE_USD and bought < SELL_OVERRIDE_RATIO * sold:
        sellers = {s["owner"] for s in sales if s["owner"]}
        return {
            "state": DISTRIBUTING,
            "rule": "selling",
            "reason": f"{len(sellers)} insider(s) sold ${sold:,.0f} "
                      f"since {(as_of - timedelta(days=SALE_WINDOW_DAYS)).isoformat()}",
            "evidence": {"sales": len(sales), "value": sold},
        }
    if buys and bought >= MIN_MEANINGFUL_BUY_USD:
        buyers = {b["owner"] for b in buys if b["owner"]}
        # A cluster is distinct buyers inside the tight window, not merely
        # several purchases -- one director buying weekly is conviction, but it
        # is one person's conviction, and that is CONFIRMED not EXTENDED.
        recent = as_of - timedelta(days=CLUSTER_WINDOW_DAYS)
        cluster = {b["owner"] for b in buys
                   if b["owner"] and _as_date(b["txn_date"], date.min) >= recent}
        if len(cluster) >= EXTENDED_MIN_BUYERS:
            return {
                "state": EXTENDED,
                "rule": "cluster",
                "reason": f"{len(cluster)} insiders bought ${bought:,.0f} "
                          f"within {CLUSTER_WINDOW_DAYS} days"
                          + (" — crowded" if is_crowded else ""),
                "evidence": {"buyers": sorted(cluster), "value": bought},
            }
        who = sorted(buyers)[0] if buyers else "an insider"
        return {
            "state": CONFIRMED,
            "rule": "purchase",
            "reason": f"{who} bought ${bought:,.0f} on {buys[0]['txn_date']}",
            "evidence": {"buyers": sorted(buyers), "value": bought},
        }

    # The company acting on its own stock, or a deal document. Checked after
    # the insider rules because an insider is a person putting their own money
    # in and that is the stronger statement -- but checked at all, which it was
    # not: buybacks and M&A produce no insider ledger rows, so an issuer known
    # only for those never left DORMANT and never reached a page built from
    # transitions. Two of the collector's three signals, invisible.
    # Lane A, before the corporate branch. Both land in SETUP, and when an
    # issuer has each of them the balance-sheet condition is the one worth
    # naming: a repurchase authorisation is one board meeting, while contract
    # liabilities compounding past revenue for three quarters is the shape of
    # the business changing. Checked here rather than at the has_setup_signal
    # line below so it cannot be buried by the buyback.
    setup = _setup_condition(conn, cik) if has_setup_signal is False else None
    if has_setup_signal or (setup and setup.get("setup")):
        return {
            "state": SETUP,
            # The rule is RECORDED, not inferred later from the reason string.
            # Every Lane A hit on the live page also carries a buyback, so a
            # renderer keying on words -- or on which filing is loudest -- draws
            # all three as buyback cards and the balance-sheet lane disappears.
            "rule": "lane_a",
            "reason": (setup or {}).get("reason")
                      or "setup signal present, no confirming purchase",
            "evidence": {"setup": setup},
        }

    corporate = _corporate_events(conn, ticker, as_of)
    if corporate:
        top = corporate[0]
        more = f" (+{len(corporate) - 1} more)" if len(corporate) > 1 else ""
        # A deal document and a buyback are not the same strength of claim, and
        # collapsing them into one state was the reason 431 of 804 issuers --
        # 54% of the universe -- sat in CONFIRMED, most of them megacaps running
        # a standing repurchase programme.
        #
        # A definitive merger document is a dated, consequential, largely
        # irreversible act, so it CONFIRMS. A board-authorised repurchase is a
        # ceiling on future buying, not a purchase anyone was obliged to make,
        # and nobody committed personal money to it -- which is precisely what
        # SETUP already means here: a precursor with no confirming action.
        #
        # SETUP was unreachable until now because the only thing meant to
        # produce it was deferred. It has two producers when that lands; both
        # say the same thing, which is that something happened and no insider
        # has yet backed it.
        if top["event_type"] == "buyback":
            return {
                "state": SETUP,
                "rule": "buyback",
                # "no insider buying" stated an absence as though it were
                # evidence. Nothing was observed; that is not an observation.
                "reason": f"repurchasing stock, filed {top['filed_date']}{more}",
                "evidence": {"events": [dict(e) for e in corporate]},
            }
        return {
            "state": CONFIRMED,
            "rule": "deal",
            "reason": f"deal filing ({top['event_type'][3:].upper()}) "
                      f"filed {top['filed_date']}{more}",
            "evidence": {"events": [dict(e) for e in corporate]},
        }

    return {"state": DORMANT, "rule": "none",
            "reason": "no activity in window", "evidence": {}}


# ---------------------------------------------------------------- application


def current_state(conn, cik):
    row = conn.execute("SELECT * FROM issuer_state WHERE cik = ?",
                       (int(cik),)).fetchone()
    return row["state"] if row else DORMANT


def apply_state(conn, cik, ticker, verdict, observed_on=None):
    """Store the verdict, and record a transition only if something MOVED.

    This is where "the same issuer should not reappear every twelve hours"
    is enforced. A standing state is rewritten in place and produces no
    transition row; only a genuine change writes to state_transitions. The
    UNIQUE constraint then catches the remaining case -- two passes over the
    same day that both see the same move -- so re-running a day is a no-op
    rather than a duplicate.

    Returns the transition dict if one was recorded, else None.
    """
    observed_on = _as_date(observed_on or date.today(), date.today()).isoformat()
    was = current_state(conn, cik)
    now = verdict["state"]

    conn.execute(
        """INSERT INTO issuer_state
             (cik, ticker, state, reason, since, updated_at, rule)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(cik) DO UPDATE SET
             ticker = excluded.ticker,
             state = excluded.state,
             reason = excluded.reason,
             rule = excluded.rule,
             since = CASE WHEN issuer_state.state = excluded.state
                          THEN issuer_state.since ELSE excluded.since END,
             updated_at = excluded.updated_at""",
        (int(cik), ticker, now, verdict["reason"], observed_on, _now(),
         verdict.get("rule")),
    )

    if was == now:
        return None

    cur = conn.execute(
        """INSERT OR IGNORE INTO state_transitions
           (cik, ticker, from_state, to_state, reason, observed_on, created_at,
            rule)
           VALUES (?,?,?,?,?,?,?,?)""",
        (int(cik), ticker, was, now, verdict["reason"], observed_on, _now(),
         verdict.get("rule")),
    )
    if not cur.rowcount:
        return None
    return {"cik": int(cik), "ticker": ticker, "from_state": was,
            "to_state": now, "reason": verdict["reason"],
            "observed_on": observed_on, "rule": verdict.get("rule")}


def issuers_to_evaluate(conn, as_of=None):
    """Every issuer with anything worth re-reading: bought, sold, disqualified,
    or already carrying a state that may have gone stale."""
    as_of = _as_date(as_of or date.today(), date.today())
    since = (as_of - timedelta(days=max(BUY_WINDOW_DAYS, SALE_WINDOW_DAYS))).isoformat()
    rows = conn.execute(
        """SELECT issuer_cik AS cik, ticker FROM insider_buys WHERE txn_date >= ?
           UNION
           SELECT issuer_cik AS cik, ticker FROM insider_sales WHERE txn_date >= ?
           UNION
           SELECT cik, NULL AS ticker FROM disqualifiers WHERE cleared_at IS NULL
           UNION
           SELECT cik, ticker FROM issuer_state WHERE state != ?""",
        (since, since, DORMANT),
    ).fetchall()

    # A CIK can arrive from several branches with the ticker set in only one of
    # them; keep the first non-empty rather than letting a NULL overwrite it.
    out = {}
    for row in rows:
        if not row["cik"]:
            continue
        cik = int(row["cik"])
        if row["ticker"] or cik not in out:
            out[cik] = row["ticker"] or out.get(cik)

    # Issuers known only for a buyback or a deal document. They have no ledger
    # rows, so none of the branches above sees them -- which is what silently
    # kept two thirds of the collector's output off a transition-driven page.
    #
    # The CIK comes from two places because the two event types record it
    # differently: a buyback carries it in its detail JSON, while a deal filing
    # carries only the accession, which the documents table maps back. An
    # issuer whose CIK resolves from neither is skipped rather than given a
    # made-up one -- issuer_state is keyed on CIK and a fabricated key would
    # collide with a real issuer sooner or later.
    for cik, ticker in _corporate_issuers(conn, since):
        if ticker or cik not in out:
            out[cik] = ticker or out.get(cik)

    # Issuers known ONLY for the balance-sheet condition. Lane A exists to see
    # a company on a day it filed nothing, and none of the branches above can:
    # they all start from an event. Every Lane A hit on the live page so far
    # also carried a buyback and arrived through the corporate branch, which
    # hid this completely -- tightening the thresholds left two issuers
    # qualifying on Lane A alone, and neither was ever evaluated.
    if _has_table(conn, "issuer_setup"):
        for row in conn.execute(
                "SELECT cik FROM issuer_setup WHERE setup = 1").fetchall():
            if row["cik"] and int(row["cik"]) not in out:
                out[int(row["cik"])] = None

    # Retired issuers are dropped last, so nothing upstream has to remember to
    # check. Their ledger rows stay and keep finding their way into the unions
    # above; this is the one place that has to say no.
    retired = {r[0] for r in conn.execute("SELECT cik FROM retired_issuers")}
    return sorted((cik, t) for cik, t in out.items() if cik not in retired)


def _corporate_issuers(conn, since):
    """(cik, ticker) for issuers with an open buyback or deal filing."""
    if not _has_table(conn, "events"):
        return []
    rows = conn.execute(
        """SELECT e.entity AS ticker,
                  COALESCE(json_extract(e.detail, '$.cik'), d.cik) AS cik
             FROM events e
             LEFT JOIN documents d ON d.accession = e.source_id
            WHERE e.reviewed_at IS NULL
              AND (e.event_type = 'buyback'
                   OR e.event_type LIKE 'ma\\_%' ESCAPE '\\')
              AND e.filed_date >= ?""",
        (since,),
    ).fetchall()
    return [(int(r["cik"]), r["ticker"]) for r in rows if r["cik"]]


def classify_all(conn, as_of=None):
    """Evaluate every issuer worth evaluating. Returns (n_evaluated, transitions)."""
    as_of = _as_date(as_of or date.today(), date.today())
    transitions = []
    pairs = issuers_to_evaluate(conn, as_of)
    for cik, ticker in pairs:
        verdict = evaluate(conn, cik, ticker, as_of=as_of)
        moved = apply_state(conn, cik, ticker, verdict, observed_on=as_of)
        if moved:
            transitions.append(moved)
    conn.commit()
    return len(pairs), transitions


# ---------------------------------------------------------------- reading out


def transitions_since(conn, since=None, limit=200):
    """What changed, newest first. The dashboard's data source.

    Standing state is deliberately not what this returns. An issuer that has
    been CONFIRMED for three weeks is not news on day twenty-two, and a
    dashboard rebuilt twice a day off issuer_state would say it is.
    """
    if since is None:
        since = date.today() - timedelta(days=14)
    since = _as_date(since, date.today() - timedelta(days=14)).isoformat()
    return conn.execute(
        """SELECT * FROM state_transitions
           WHERE observed_on >= ?
           ORDER BY observed_on DESC, id DESC LIMIT ?""",
        (since, limit),
    ).fetchall()


def state_counts(conn):
    """How many issuers sit in each state. Every state present, zeros included --
    a counts panel that silently omits DISTRESSED because it happens to be
    empty reads as though the category does not exist."""
    counts = {name: 0 for name in STATES}
    for row in conn.execute(
            "SELECT state, COUNT(*) AS n FROM issuer_state GROUP BY state"):
        if row["state"] in counts:
            counts[row["state"]] = row["n"]
    return counts


def state_reason(conn, cik):
    """The traceable reason string behind an issuer's current state."""
    row = conn.execute("SELECT state, reason, since FROM issuer_state WHERE cik = ?",
                       (int(cik),)).fetchone()
    return dict(row) if row else {"state": DORMANT, "reason": "never evaluated",
                                  "since": None}


# ---------------------------------------------------------------- self-test


def _selftest():
    """Drive one issuer through four transitions against an in-memory DB.

    The last of them is the point of the module: a purchase is on the books and
    stays on the books, and the restatement still wins.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # The two tables this module reads but does not own, so the self-test can
    # run without importing the collector.
    conn.executescript("""
        CREATE TABLE insider_buys (accession TEXT, issuer_cik INTEGER,
          ticker TEXT, issuer TEXT, owner TEXT, owner_title TEXT,
          txn_date TEXT, shares REAL, price REAL, value REAL,
          suspect INTEGER DEFAULT 0);
    """)
    migrate(conn)
    set_user_agent("signal_state selftest selftest@example.com")

    CIK, TICKER = 320193, "ACME"
    day = date(2026, 8, 10)

    def step(label, on):
        verdict = evaluate(conn, CIK, TICKER, as_of=on)
        moved = apply_state(conn, CIK, TICKER, verdict, observed_on=on)
        conn.commit()
        if moved:
            print(f"  {moved['from_state']:>12} -> {moved['to_state']:<12} "
                  f"{moved['observed_on']}  {moved['reason']}")
        else:
            print(f"  {'(no change)':>12}    {label}")
        return moved

    def buy(owner, on, value):
        conn.execute(
            "INSERT INTO insider_buys (accession, issuer_cik, ticker, owner, "
            "txn_date, shares, price, value) VALUES (?,?,?,?,?,?,?,?)",
            (f"acc-{owner}-{on}", CIK, TICKER, owner, on.isoformat(),
             value / 10, 10.0, value))

    print("transitions")
    moves = []

    # 1. one director buys -> CONFIRMED
    buy("HOVDE STEVEN D", day, 500_000)
    moves.append(step("first purchase", day))

    # 2. a second insider joins inside the cluster window -> EXTENDED
    buy("NGUYEN MAI", day + timedelta(days=2), 300_000)
    moves.append(step("cluster forms", day + timedelta(days=2)))

    # 3. insiders start selling -> DISTRIBUTING
    record_insider_sales(conn, [{
        "accession": "acc-sale-1", "issuer_cik": CIK, "ticker": TICKER,
        "owner": "HOVDE STEVEN D", "owner_title": "Director",
        "txn_date": (day + timedelta(days=20)).isoformat(),
        "shares": 40_000.0, "price": 25.0, "value": 1_000_000.0}])
    moves.append(step("selling begins", day + timedelta(days=20)))

    # 4. a restatement lands. The purchases above are still on the books and
    #    the disqualifier still wins -- this is the whole design property.
    record_disqualifier(conn, CIK, "late annual report",
                        detail="ACME CORP", form_type="NT 10-K",
                        accession="acc-nt10k",
                        filed=(day + timedelta(days=25)).isoformat())
    moves.append(step("disqualifier lands", day + timedelta(days=25)))

    # Idempotence: the same day re-evaluated must add nothing.
    before = conn.execute("SELECT COUNT(*) FROM state_transitions").fetchone()[0]
    step("re-run of the same day", day + timedelta(days=25))
    after = conn.execute("SELECT COUNT(*) FROM state_transitions").fetchone()[0]

    counts = state_counts(conn)
    print(f"counts {counts}")

    recorded = [m for m in moves if m]
    problems = []
    if len(recorded) != 4:
        problems.append(f"expected 4 transitions, recorded {len(recorded)}")
    path = [m["to_state"] for m in recorded]
    if path != [CONFIRMED, EXTENDED, DISTRIBUTING, DISTRESSED]:
        problems.append(f"unexpected path: {path}")
    if before != after:
        problems.append(f"not idempotent: {before} -> {after} rows on re-run")
    if counts[DISTRESSED] != 1:
        problems.append(f"issuer should be DISTRESSED, counts={counts}")
    if sum(counts.values()) != 1:
        problems.append(f"one issuer expected, counts={counts}")
    # The buys must still be there -- DISTRESSED is an override, not a delete.
    if not _buys(conn, CIK, day + timedelta(days=25)):
        problems.append("purchases vanished; disqualifier must override, not erase")
    if not state_reason(conn, CIK)["reason"]:
        problems.append("state has no traceable reason")

    if problems:
        for p in problems:
            print(f"FAIL {p}")
        return 1
    print("OK  4 transitions, disqualifier overrode a live purchase, re-run added 0")
    return 0


# ---------------------------------------------------------------- integration
#
# Three edits to edgar_discovery.py, plus two scanner call sites.
#
# 1. Alongside the existing imports:
#
#        import signal_state
#
# 2. After the connection is open and USER_AGENT is built -- reuse that string,
#    do not construct a second one:
#
#        signal_state.set_user_agent(USER_AGENT)
#        signal_state.migrate(conn)
#
# 3. Replace the two-tier assignment at the end of the per-run classification
#    pass. Both prints stay: they separate "the collector produced nothing"
#    from "classification found nothing", which nothing else can tell apart.
#
#        n_issuers, moves = signal_state.classify_all(conn, as_of=day)
#        print(f"CLASSIFIED {n_issuers} issuer(s), {len(moves)} transition(s)")
#        print(f"STATE COUNTS {signal_state.state_counts(conn)}")
#
# Scanner call site A -- inside the daily-master-index loop. Free, no request.
# Gate it on watchlist membership so the table stays small:
#
#        signal_state.scan_index_row_for_disqualifiers(conn, row, watched)
#
# Scanner call site B -- in parse_form4's transaction loop, the mirror of the
# code-P branch. Leave the purchase logic alone:
#
#        if code == "S" and acquired_disposed == "D":
#            sales.append(...)          # same dict shape as a buy
#    ...then after parsing:
#        signal_state.record_insider_sales(conn, sales)
#
# Dashboard: read signal_state.transitions_since(conn) instead of the tier
# list, and add a counts panel from signal_state.state_counts(conn).

if __name__ == "__main__":
    sys.exit(_selftest())
