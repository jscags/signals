"""Daily prices, kept apart from the collection database on purpose.

The screener's database is committed to the repository twice a day. Prices for
a few hundred tickers over several years run to hundreds of thousands of rows,
and committing that on the same cadence would bloat the history for data that
is (a) reproducible from the source at any time and (b) not evidence of
anything the screener claims. So this lives in its own file, which is
gitignored and rebuilt on demand. Only the small result tables are committed.

Prices come from Polygon (a free key, automated access explicitly permitted)
or from Stooq (no key, but its bot protection refuses datacenter IPs, so it is
only usable from a personal machine). Neither is the SEC, so nothing here goes
through edgar_discovery.fetch() -- that carries the SEC's declared user agent
and pacing, which do not belong on another host.

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
import json
import os
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

PRICE_DB = os.environ.get("PRICE_DB", "prices.db")
TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# ---------------------------------------------------------------- providers
#
# Two, because neither works everywhere.
#
# Stooq needs no key and serves a plain CSV, which makes it the obvious choice
# from a personal machine. It is unusable from CI: its bot protection answers a
# datacenter IP with a JavaScript challenge page, and a run from a GitHub
# runner gets 231 of 231 tickers refused. That is the host declining automated
# access from that address, and the answer is to ask somewhere that permits it
# rather than to disguise the request.
#
# Polygon's free tier permits automated access explicitly, in exchange for a
# key and a hard 5 requests/minute. Slow, but sanctioned and repeatable.
POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")
PROVIDER = os.environ.get("PRICE_PROVIDER") or ("polygon" if POLYGON_KEY else "stooq")

STOOQ_URL = "https://stooq.com/q/d/l/?s={symbol}&i=d"
POLYGON_URL = ("https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
               "{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={key}")

# Stooq has no published rate, so the pace is a courtesy. Polygon publishes
# five per minute on the free tier, so the pace is arithmetic: 12.5s leaves a
# margin under it rather than riding the edge and collecting 429s.
STOOQ_PACE = 0.35
POLYGON_PACE = 12.5
REQUEST_PACE = POLYGON_PACE if PROVIDER == "polygon" else STOOQ_PACE

# How far back a backfill asks for. Polygon's free tier serves two years.
HISTORY_START = os.environ.get("PRICE_START", "2024-01-01")

# The benchmark. Every return this project quotes is quoted beside the market
# over the identical window, because a number that is not is mostly telling you
# what the market did.
#
# SPY rather than the S&P index itself: Polygon's free tier does not carry
# index data, and an ETF is the better comparison anyway -- it is the thing a
# reader could actually have bought instead.
BENCHMARK = os.environ.get("BENCHMARK") or ("SPY" if PROVIDER == "polygon" else "^spx")

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


def parse_polygon(body):
    """Polygon's aggregates JSON to the same row shape as the CSV path.

    A key that is wrong, exhausted, or unauthorised comes back as a JSON
    document with a status rather than an HTTP error, so the status is read.
    Otherwise an expired key would look exactly like a delisted ticker and
    would be silently counted as missing data for every symbol.
    """
    try:
        data = json.loads(body or "")
    except (json.JSONDecodeError, TypeError) as exc:
        raise PriceError(f"not json: {exc}") from exc
    status = (data.get("status") or "").upper()
    if status in ("ERROR", "NOT_AUTHORIZED"):
        raise PriceError(f"polygon refused: {data.get('message') or status}")
    results = data.get("results") or []
    if not results:
        raise PriceError("no results for symbol")
    out = []
    for bar in results:
        try:
            day = datetime.fromtimestamp(
                bar["t"] / 1000, tz=timezone.utc).date().isoformat()
            row = (day, float(bar["o"]), float(bar["h"]),
                   float(bar["l"]), float(bar["c"]), float(bar.get("v") or 0))
        except (KeyError, TypeError, ValueError, OSError):
            continue
        if row[4] <= 0:
            continue
        out.append(row)
    if not out:
        raise PriceError("payload carried no usable bars")
    out.sort()
    return out


def _get(url, accept):
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": accept})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        # 429 is the free tier's rate limit, and it is worth naming: a run
        # that silently records every ticker as unavailable because it went
        # too fast would report a coverage problem that is really a pacing bug.
        if exc.code == 429:
            raise PriceError("rate limited (429) -- the pace is too fast") from exc
        raise PriceError(f"HTTP {exc.code}") from exc
    except (urllib.error.URLError, OSError) as exc:
        raise PriceError(str(exc)) from exc


def fetch_prices(ticker, provider=None, start=None, end=None):
    provider = provider or PROVIDER
    if provider == "polygon":
        if not POLYGON_KEY:
            raise PriceError(
                "POLYGON_API_KEY is not set. Add it as a repository secret, or "
                "set PRICE_PROVIDER=stooq to fetch from a machine Stooq allows.")
        symbol = (ticker or "").strip().upper().replace(".", "-")
        if not symbol:
            raise PriceError("empty ticker")
        url = POLYGON_URL.format(
            symbol=urllib.request.quote(symbol, safe="-"),
            start=start or HISTORY_START,
            end=end or date.today().isoformat(), key=POLYGON_KEY)
        return parse_polygon(_get(url, "application/json"))

    symbol = stooq_symbol(ticker)
    return parse_csv(_get(
        STOOQ_URL.format(symbol=urllib.request.quote(symbol, safe="^-.")),
        "text/csv, */*"))


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


# ------------------------------------------------- bulk (grouped) fetch
#
# One request per DAY covering every US ticker, instead of one per ticker
# covering every day. For a backtest this is the difference between feasible
# and not: ~1,500 distinct tickers over six months is 1,500 requests at five a
# minute, over five hours. The same window as grouped days is ~190 requests,
# under forty minutes, and it prices tickers that were delisted before today --
# which the per-ticker endpoint will not do, and which is exactly the
# population a survivorship-blind backtest silently loses.

POLYGON_GROUPED_URL = (
    "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{day}"
    "?adjusted=true&apiKey={key}")


def parse_grouped(body):
    """One day of bars for every ticker: {ticker: (day, o, h, l, c, v)}."""
    try:
        data = json.loads(body or "")
    except (json.JSONDecodeError, TypeError) as exc:
        raise PriceError(f"not json: {exc}") from exc
    status = (data.get("status") or "").upper()
    if status in ("ERROR", "NOT_AUTHORIZED"):
        raise PriceError(f"polygon refused: {data.get('message') or status}")
    out = {}
    for bar in data.get("results") or []:
        try:
            ticker = (bar["T"] or "").strip().upper()
            day = datetime.fromtimestamp(
                bar["t"] / 1000, tz=timezone.utc).date().isoformat()
            row = (day, float(bar["o"]), float(bar["h"]),
                   float(bar["l"]), float(bar["c"]), float(bar.get("v") or 0))
        except (KeyError, TypeError, ValueError, OSError):
            continue
        if not ticker or row[4] <= 0:
            continue
        out[ticker] = row
    return out                    # empty is legitimate: a market holiday


def refresh_grouped(conn, start, end, pace=None, verbose=True):
    """Fetch every US daily bar between two dates, one request per day.

    Returns (days_fetched, days_empty, days_failed). A holiday returning no
    bars is not a failure and is counted separately, because conflating "the
    market was shut" with "the request broke" would hide a real outage inside
    a normal-looking count.
    """
    if PROVIDER != "polygon":
        raise PriceError("grouped fetch needs the polygon provider")
    if not POLYGON_KEY:
        raise PriceError("POLYGON_API_KEY is not set")
    pace = POLYGON_PACE if pace is None else pace
    day = date.fromisoformat(start)
    last = date.fromisoformat(end)
    got = empty = failed = 0
    while day <= last:
        if day.weekday() >= 5:
            day += timedelta(days=1)
            continue
        stamp = day.isoformat()
        done = conn.execute(
            "SELECT 1 FROM price_meta WHERE ticker = ?",
            (f"__day__{stamp}",)).fetchone()
        if done:
            day += timedelta(days=1)
            continue
        try:
            bars = parse_grouped(_get(
                POLYGON_GROUPED_URL.format(day=stamp, key=POLYGON_KEY),
                "application/json"))
        except PriceError as exc:
            failed += 1
            if verbose:
                print(f"  {stamp}: {exc}")
            # A failed day is NOT marked done, so a rerun retries it rather
            # than leaving a hole that later reads as a market closure.
        else:
            if bars:
                conn.executemany(
                    "INSERT INTO prices (ticker, day, open, high, low, close,"
                    " volume) VALUES (?,?,?,?,?,?,?)"
                    " ON CONFLICT(ticker, day) DO UPDATE SET"
                    " open=excluded.open, high=excluded.high,"
                    " low=excluded.low, close=excluded.close,"
                    " volume=excluded.volume",
                    [(t, *row) for t, row in bars.items()])
                got += 1
            else:
                empty += 1
            # The day is recorded either way, so a rerun does not re-ask for
            # holidays. The marker is namespaced so it cannot collide with a
            # real ticker.
            conn.execute(
                "INSERT OR REPLACE INTO price_meta (ticker, status, n_days,"
                " first_day, last_day, fetched_at, note)"
                " VALUES (?,?,?,?,?,?,?)",
                (f"__day__{stamp}", "ok" if bars else "no bars", len(bars),
                 stamp, stamp, datetime.now().isoformat(timespec="seconds"),
                 "grouped daily"))
            conn.commit()
            if verbose and got % 20 == 0 and bars:
                print(f"  {stamp}: {len(bars)} tickers "
                      f"({got} days fetched, {empty} empty, {failed} failed)")
        time.sleep(pace)
        day += timedelta(days=1)
    return got, empty, failed
