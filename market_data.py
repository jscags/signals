"""Daily prices, kept apart from the collection database on purpose.

The screener's database is committed to the repository twice a day. Prices for
a few hundred tickers over several years run to hundreds of thousands of rows,
and committing that on the same cadence would bloat the history for data that
is (a) reproducible from the source at any time and (b) not evidence of
anything the screener claims. So this lives in its own file, which is
gitignored and rebuilt on demand. Only the small result tables are committed.

The source is Stooq's daily CSV endpoint: no key, no registration, one request
per ticker, and it serves adjusted daily bars going back years. It is not the
SEC, so nothing here goes through edgar_discovery.fetch() -- that carries the
SEC's declared user agent and pacing, which do not belong on another host.

WHAT THIS MODULE REFUSES TO DO QUIETLY
--------------------------------------
A backtest is a machine for producing confident numbers from bad data, and the
usual way it lies is by silently dropping what it could not price. A delisted
ticker returns nothing here; if those entries were skipped without a count,
every result would be measured on survivors only and would read better than
the truth. So every ticker asked for is recorded in `price_meta` with what
happened to it, and the harness reports coverage alongside any return it
quotes. An unpriceable entry is a known unknown, never an absence.
"""

import csv
import io
import os
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta

PRICE_DB = os.environ.get("PRICE_DB", "prices.db")
STOOQ_URL = "https://stooq.com/q/d/l/?s={symbol}&i=d"
# Stooq is a free service answering one request per ticker. This pace is a
# courtesy, not a documented limit -- there is no published rate to respect,
# so the run is deliberately unhurried rather than as fast as it can go.
REQUEST_PACE = 0.35
TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# The benchmark. Every return this project quotes is quoted beside the market
# over the identical window, because a number that is not is mostly telling you
# what the market did.
BENCHMARK = "^spx"

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
    ticker TEXT NOT NULL,
    day    TEXT NOT NULL,
    open   REAL,
    high   REAL,
    low    REAL,
    close  REAL,
    volume REAL,
    PRIMARY KEY (ticker, day)
);
CREATE INDEX IF NOT EXISTS idx_prices_day ON prices(day);

-- One row per ticker ASKED FOR, including the ones that returned nothing.
-- This is what makes missing data countable instead of invisible.
CREATE TABLE IF NOT EXISTS price_meta (
    ticker     TEXT PRIMARY KEY,
    status     TEXT,
    n_days     INTEGER,
    first_day  TEXT,
    last_day   TEXT,
    fetched_at TEXT,
    note       TEXT
);
"""


class PriceError(RuntimeError):
    """A fetch or parse that produced nothing usable."""


def connect(path=None):
    conn = sqlite3.connect(path or PRICE_DB)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def stooq_symbol(ticker):
    """Stooq wants us equities suffixed, and indices bare.

    Class shares are dotted on EDGAR and dashed on Stooq (BRK.B -> brk-b).
    """
    t = (ticker or "").strip().lower()
    if not t:
        raise PriceError("empty ticker")
    if t.startswith("^"):
        return t
    return t.replace(".", "-") + ".us"


def parse_csv(body):
    """Stooq's daily CSV to rows. Raises rather than returning an empty list.

    A delisted or unknown symbol answers with a body that is not a price CSV
    at all -- historically the literal string "No data". Returning [] for that
    would make "this ticker does not exist" and "this ticker had no trades"
    the same answer, and only one of them should count against coverage.
    """
    text = (body or "").strip()
    if not text or text.lower().startswith("no data"):
        raise PriceError("no data for symbol")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "Date" not in reader.fieldnames:
        raise PriceError(f"unexpected columns: {reader.fieldnames}")
    out = []
    for row in reader:
        day = (row.get("Date") or "").strip()
        if len(day) != 10:
            continue
        try:
            bar = (day,
                   float(row["Open"]), float(row["High"]),
                   float(row["Low"]), float(row["Close"]),
                   float(row.get("Volume") or 0))
        except (TypeError, ValueError, KeyError):
            continue                      # a blank day, not a broken feed
        if bar[4] <= 0:
            continue                      # a zero close is not a price
        out.append(bar)
    if not out:
        raise PriceError("csv carried no usable bars")
    out.sort()
    return out


def fetch_prices(ticker):
    symbol = stooq_symbol(ticker)
    request = urllib.request.Request(
        STOOQ_URL.format(symbol=urllib.request.quote(symbol, safe="^-.")),
        headers={"User-Agent": USER_AGENT, "Accept": "text/csv, */*"})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            body = response.read().decode("utf-8", "replace")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        raise PriceError(f"{symbol}: {exc}") from exc
    return parse_csv(body)


def store(conn, ticker, bars):
    conn.executemany(
        "INSERT INTO prices (ticker, day, open, high, low, close, volume)"
        " VALUES (?,?,?,?,?,?,?)"
        " ON CONFLICT(ticker, day) DO UPDATE SET open=excluded.open,"
        " high=excluded.high, low=excluded.low, close=excluded.close,"
        " volume=excluded.volume",
        [(ticker, *bar) for bar in bars])


def _note(conn, ticker, status, bars=None, note=""):
    conn.execute(
        "INSERT INTO price_meta (ticker, status, n_days, first_day, last_day,"
        " fetched_at, note) VALUES (?,?,?,?,?,?,?)"
        " ON CONFLICT(ticker) DO UPDATE SET status=excluded.status,"
        " n_days=excluded.n_days, first_day=excluded.first_day,"
        " last_day=excluded.last_day, fetched_at=excluded.fetched_at,"
        " note=excluded.note",
        (ticker, status, len(bars or ()), bars[0][0] if bars else None,
         bars[-1][0] if bars else None,
         datetime.now().isoformat(timespec="seconds"), note))


def refresh(conn, tickers, max_age_days=3, pace=REQUEST_PACE, verbose=True):
    """Fetch each ticker once. Returns (fetched, cached, failed) counts.

    A ticker already stored and recent enough is left alone, so re-running the
    backfill after an interruption costs only what is missing.
    """
    today = date.today()
    fetched = cached = failed = 0
    for ticker in tickers:
        row = conn.execute(
            "SELECT status, last_day, fetched_at FROM price_meta WHERE ticker = ?",
            (ticker,)).fetchone()
        if row and row["status"] == "ok" and row["fetched_at"]:
            age = (today - datetime.fromisoformat(row["fetched_at"]).date()).days
            if age <= max_age_days:
                cached += 1
                continue
        try:
            bars = fetch_prices(ticker)
        except PriceError as exc:
            # Recorded, not skipped. An unpriceable ticker has to stay
            # countable or every result silently becomes survivors-only.
            _note(conn, ticker, "unavailable", note=str(exc)[:200])
            failed += 1
            if verbose:
                print(f"  no prices: {ticker} ({exc})")
        else:
            store(conn, ticker, bars)
            _note(conn, ticker, "ok", bars)
            fetched += 1
        conn.commit()
        time.sleep(pace)
    return fetched, cached, failed


# ------------------------------------------------------------ lookups

def trading_days(conn, ticker, since=None):
    q = "SELECT day, open, close FROM prices WHERE ticker = ?"
    args = [ticker]
    if since:
        q += " AND day >= ?"
        args.append(since)
    return conn.execute(q + " ORDER BY day", args).fetchall()


def next_open_on_or_after(conn, ticker, day):
    """The first tradeable open at or after `day`.

    Entry is an OPEN, never a close. A signal derived from a filing is not
    actionable until the next session, and marking the entry at the close of
    the day the filing appeared would hand the backtest several hours of
    hindsight it never had.
    """
    return conn.execute(
        "SELECT day, open FROM prices WHERE ticker = ? AND day >= ? AND open > 0"
        " ORDER BY day LIMIT 1", (ticker, day)).fetchone()


def close_n_sessions_after(conn, ticker, day, n):
    """The close n trading sessions after the entry day, or None if not yet.

    Counted in SESSIONS, not calendar days, so a horizon means the same thing
    across holidays. Returns None when the window has not elapsed -- an entry
    whose horizon runs past the data must be excluded from that horizon rather
    than measured against the last price available, which would silently turn
    a 21-day return into a 4-day one.
    """
    rows = conn.execute(
        "SELECT day, close FROM prices WHERE ticker = ? AND day > ?"
        " ORDER BY day LIMIT ?", (ticker, day, n)).fetchall()
    if len(rows) < n:
        return None
    return rows[-1]
