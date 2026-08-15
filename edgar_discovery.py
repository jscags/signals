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
import collections
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

import setup_signal
import signal_state

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

# Bumped whenever the rules for deriving a cached XBRL figure change. A cached
# row carries the version it was derived under, and a mismatch is treated as
# stale however recent it is -- otherwise a corrected rule sits inert behind a
# thirty-day TTL, which is exactly how the multi-year buyback periods survived
# the fix that was supposed to remove them.
# Bumped to 4 for Lane A. Every cached row was written by rules that did not
# compute the setup condition, and _fresh() would have gone on serving them for
# the full 30-day TTL -- so the new lane would have produced nothing at all for
# a month while looking perfectly healthy. Sixth time in this file that a rule
# has shipped without reaching what was already stored; the version exists
# precisely so it does not have to be a seventh.
XBRL_DERIVATION = 4

# How many stale issuers a single run re-derives. Bumping XBRL_DERIVATION
# invalidates every cached row at once, and refetching them all in one run
# would add minutes; this drains the backlog over a few runs instead, oldest
# rules first, while a normal run is unaffected because nothing is stale.
STALE_REFRESH_BUDGET = 150

# How many state transitions the dashboard will read before it stops and says
# so. Transitions, not cards: the page collapses an issuer's moves into one
# card, so 1,219 transitions here draw 575 cards.
#
# Measured rather than picked. At 600 the live page drew 378 cards in 352KB
# and silently held back half the history; the whole of it is 575 cards in
# 543KB, which is an ordinary size for a local file with no network fetch in
# it. The ceiling that matters is the number of issuers that can move inside
# the window -- bounded by the tracked universe, ~800 today and growing toward
# the 10-Q filing population -- so this leaves room for that to roughly triple
# before anything is held back again. The truncation note stays either way,
# because a page that quietly ends is the failure this was built against.
TRANSITION_CAP = 2500

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

# Per-TRANSACTION ceiling, applied where a row enters the ledger.
#
# The aggregate ceiling above guards the emitted event; it does not guard the
# ledger, and the state machine reads the ledger. So Reborn Coffee -- a
# sub-dollar microcap whose Form 4 reported $180,000 a share -- had its event
# correctly suppressed while the state machine went on saying "bought
# $23,649,660,000", which is the figure that reached the page.
#
# Set above the largest legitimate figure the collector has seen by a wide
# margin. Real sponsor block sales run to the hundreds of millions: Argon
# Holdco's $491M in CRBG, Leonard Green's $221M in LTH, TPG's $145M in LFST.
# Those are exactly the kind of thing this must NOT reject, so the bar sits an
# order of magnitude above them.
#
# Form 4 does not carry a currency element -- transactionPricePerShare is a
# bare number and the schema assumes USD -- so a foreign-currency price cannot
# be detected from the XML. Prose footnotes sometimes say so. Until that is
# parsed, an implausible figure is all the signal there is, which is why this
# flags rather than converts.
MAX_PLAUSIBLE_TXN_USD = 2_000_000_000

# A Form 4 is due within two business days. Beyond this, the transaction is
# being reported so long after the fact that it says nothing about now, and the
# card should say so rather than printing an eight-year-old date unremarked.
#
# Not treated as a parse error, because it is not one. The Cheesecake Factory
# filing that prompted this reports a 2018 purchase at $49.51 alongside 2026
# transactions at $106-110 -- and CAKE really did trade near $49 in March 2018,
# so the price corroborates the date. Genuinely late reports are rare (2 of 516
# rows here) and legal under Rule 16a-3; they are a fact about the filer, not a
# fault in the parser.
#
# Set well beyond the 45-day scoring window, so anything flagged here was
# already outside the state machine's reach and this only affects what is said
# on the card.
MAX_REPORTING_LAG_DAYS = 180

# Cover-page share count first, the us-gaap balance-sheet tag as a fallback for
# issuers that do not tag the dei concept.
XBRL_CONCEPTS = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
)

# Periodic reports. A company filing one is the trigger to look up what it
# repurchased -- the filing itself is never downloaded, since the numbers are
# served as structured XBRL from the same host.
PERIODIC_FORMS = {"10-Q", "10-K"}

# Candidate repurchase concepts. Measured coverage over 60 recent listed
# filers: the cash-flow tag 83%, treasury value 55%, the share-count tags
# 42/42/30%, and 92% report at least one of them.
#
# Share counts are tried first even though they are less common. A buyback is
# only meaningful against the size of the company, and shares retired over
# shares outstanding needs no price -- which matters because a company filing a
# 10-Q has given us no price the way a Form 4 does.
BUYBACK_SHARE_CONCEPTS = (
    ("us-gaap", "StockRepurchasedAndRetiredDuringPeriodShares"),
    ("us-gaap", "StockRepurchasedDuringPeriodShares"),
    ("us-gaap", "TreasuryStockSharesAcquired"),
)
BUYBACK_VALUE_CONCEPTS = (
    ("us-gaap", "PaymentsForRepurchaseOfCommonStock"),
    ("us-gaap", "PaymentsForRepurchaseOfEquity"),
    ("us-gaap", "TreasuryStockValueAcquiredCostMethod"),
)

# A repurchase tag simply stops appearing once a company stops buying, so the
# newest observation can be years old: Watsco last tagged one for 2008 and
# still files quarterly. Anything older than this is history, not news.
# An ETF or a commodity pool "repurchases" shares continuously -- that is the
# redemption half of the creation/redemption mechanism, not a decision anyone
# made about the price. The XBRL is correct and the arithmetic is correct; the
# category is wrong, so no plausibility band can catch it. ProShares Trust II
# alone tagged $26.8bn against a 131m share count, which is the largest figure
# the collector has ever produced.
#
# There is a proper answer -- the SEC assigns every filer a SIC code, and
# commodity pools and investment offices sit in their own -- and it belongs in
# a later pass, one request per issuer against data.sec.gov/submissions. This
# pattern is the offline stand-in, and it was measured rather than guessed:
# across the 322 issuers then holding a buyback event it matched 17 (5.3%),
# every one a pooled vehicle, with no false positive. Deliberately narrow.
# A bare FUND|TRUST would have caught Medical Properties Trust and RLJ Lodging
# Trust, which are REITs running real buybacks, so it names sponsors and
# vehicle types instead of those two words on their own.
#
# Known to fall through, and left alone rather than guessed at: non-traded
# vehicles whose "share repurchase program" is really an investor redemption
# facility -- Blackstone Real Estate Income Trust, MSC Income Fund. The SIC
# lookup settles those; a name pattern cannot.
FUND_VEHICLE = re.compile(
    r"\b(ETF|ETNS?|PROSHARES|ISHARES|SPDR|GRAYSCALE|BITWISE|INVESCO DB"
    r"|INDEX TRACKING|COMMODITY|CURRENCYSHARES|TRUST I{2,3})\b",
    re.IGNORECASE,
)

# The second family: non-traded REITs, BDCs and commodity trusts. Same category
# error as an ETF and a different set of names, so a separate pattern.
#
# These run a continuous share repurchase programme as a standing liquidity
# feature -- it is how an investor gets money out of a vehicle with no
# secondary market. The quarterly figure is a redemption queue clearing, not a
# board deciding the stock is cheap, and it turns up looking enormous because
# the denominator is small: Hashdex Commodities Trust at "31.8% of public
# float", Starwood at 4.8% of shares outstanding, every quarter, forever.
#
# The right discriminator is the SEC's SIC code, which needs one request per
# issuer against data.sec.gov/submissions and belongs in the same later pass as
# the ETF version of this. Measured offline instead, over the 250 issuers then
# holding a buyback event: 9 matched (3.6%), every one a non-traded vehicle.
#
# One candidate pattern was tried and DROPPED: \bPROPERT\w+ TRUST\b would have
# caught Medical Properties Trust, which is NYSE-listed and buys back stock for
# the ordinary reason. That is the whole hazard here -- listed REITs and
# non-traded REITs share most of their vocabulary, and only the specific
# constructions below separate them.
#
# Known to fall through: Franklin BSP Capital Corp, a non-traded BDC whose name
# carries no marker at all. Left alone rather than guessed at.
NONTRADED_VEHICLE = re.compile(
    r"\bREIT\b"                       # Lightstone Value Plus REIT IV
    r"|\bBDC\b"                       # Kayne Anderson BDC
    r"|\bINCOME\s+TRUST\b"            # Ares / Blackstone / Starwood Real Estate
    r"|\bCOMMODIT\w*\s+TRUST\b"       # Hashdex Commodities Trust
    r"|\bTRUST\s+(?:I{1,3}|IV|VI{0,3}|IX|XI{0,3})\b",   # Strategic Storage VI
    re.IGNORECASE,
)

BUYBACK_MAX_AGE_DAYS = 400

# Reported periods are fiscal year-to-date, not discrete quarters -- one filer
# reports 2026-01-01..2026-06-30 and the next 2025-10-01..2026-06-30 -- so
# consecutive observations overlap and must never be summed. A single period
# shorter than a year is scaled up to an annual rate instead.
BUYBACK_MIN_PERIOD_DAYS = 60

# And never much longer than one. TreasuryStockSharesAcquired is often tagged
# cumulatively -- AdvanSix reports 2018-05-04..2026-06-30 -- and dividing eight
# years of buying by eight would invent an annual rate the company may never
# have run: the whole amount could have been spent in the first year. Such a
# period says nothing about the current year and is dropped.
BUYBACK_MAX_PERIOD_DAYS = 500

# Annualised repurchases as a percent of shares outstanding. A different scale
# from insider buying by an order of magnitude: buybacks run in whole percent,
# and anything under 1% is usually just mopping up option dilution.
BUYBACK_BANDS = (
    (10.0, "major"),
    (6.0, "significant"),
    (3.0, "notable"),
    (1.0, "minor"),
    (0.0, "negligible"),
)

# Retiring this much of yourself in a year is a real capital-allocation
# decision rather than housekeeping, and promotes on its own.
TIER1_BUYBACK_PCT = 5.0

# Above this it is a tender offer or a bad denominator, not a buyback.
MAX_PLAUSIBLE_BUYBACK_PCT = 50.0

# Only 22% of filers tag a repurchase share count, so most buybacks arrived as
# dollars with no way to size them. The cover page carries the answer: the
# aggregate market value of stock held by non-affiliates, reported by 98% of
# the sample and already inside the companyfacts document we fetch. It is a
# dollar denominator, so no share price is needed at all.
#
# Two things it is not. It is the FLOAT, excluding insider and affiliate
# holdings, so a percentage against it runs higher than against full market
# cap and must be labelled as such. And it is stamped as of the last business
# day of the prior second fiscal quarter, so it is old by construction --
# fine for sizing a company, wrong for anything price-sensitive.
MIN_PLAUSIBLE_FLOAT_USD = 1_000_000
FLOAT_MAX_AGE_DAYS = 800

# Form types that are inherently M&A. No item-code parsing needed: the form
# type alone is the signal, which is why these are in the first collector.
MA_FORMS_TIER1 = {"SC TO-T", "SC 14D9", "DEFM14A", "SC 13D"}
MA_FORMS_TIER2 = {"S-4", "425", "SC TO-C", "SC 13E3"}
MA_FORMS = MA_FORMS_TIER1 | MA_FORMS_TIER2

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

# ---------------------------------------------------------------- http


_last_request = 0.0

# Added to the interval between requests once the SEC has said 429, and kept
# for the rest of the run. Backing off only for the retry treats throttling as
# one unlucky request; it is the pace that was wrong, so the pace changes.
_throttle_penalty = 0.0

# How many times to wait and try again before giving up on one URL. The SEC
# sends Retry-After sometimes; when it does not, these double from the base.
THROTTLE_RETRIES = 3
THROTTLE_BACKOFF = 4.0
THROTTLE_PENALTY_STEP = 0.20
MAX_THROTTLE_PENALTY = 1.50


class FetchError(RuntimeError):
    """The request did not come back, and trying again later might fix it.

    Covers an outright SEC refusal and the whole family of transport failures
    -- reset connections, timeouts, DNS, a proxy declining the tunnel -- which
    arrive as OSError rather than HTTPError and so slipped past handlers that
    only knew about 403. Callers treat them alike: skip this one, keep the run.
    Subclasses RuntimeError so existing handlers catch it unchanged.
    """


class Throttled(FetchError):
    """The SEC said 429. Retryable, and the run should slow down permanently.

    Distinct from a plain refusal because it is the one failure that says the
    collector itself caused the problem. It used to fall through `fetch`'s
    final `raise` as a bare HTTPError, which is not a FetchError, so nothing
    upstream caught it: a single 429 anywhere in a 150-issuer refresh took the
    whole daily run down instead of costing one issuer.
    """

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def fetch(url, binary=False):
    """Rate-limited GET, retried through throttling.

    Returns None on 404 (a missing index is a non-trading day, not a failure).
    A 429 is waited out rather than raised on the first sight of it, because
    the SEC's limit is a moving target and a bulk pass -- 150 companyfacts in
    a refresh, more in a probe -- will find it sooner or later.
    """
    if not USER_AGENT:
        sys.exit(
            "EDGAR_USER_AGENT is not set. The SEC rejects requests without a\n"
            'declared contact. Example: export EDGAR_USER_AGENT="Jane Doe jane@ex.com"'
        )

    for attempt in range(THROTTLE_RETRIES + 1):
        try:
            return _fetch_once(url, binary)
        except Throttled as exc:
            if attempt == THROTTLE_RETRIES:
                raise FetchError(
                    f"SEC returned 429 for {url} after "
                    f"{THROTTLE_RETRIES + 1} attempts; the run is being "
                    f"throttled faster than it can back off"
                ) from exc
            wait = exc.retry_after or THROTTLE_BACKOFF * (2 ** attempt)
            print(f"WARNING: SEC returned 429; waiting {wait:.0f}s "
                  f"(attempt {attempt + 1}/{THROTTLE_RETRIES})", flush=True)
            time.sleep(wait)
    raise AssertionError("unreachable")


def _fetch_once(url, binary=False):
    global _last_request, _throttle_penalty

    interval = MIN_REQUEST_INTERVAL + _throttle_penalty
    elapsed = time.time() - _last_request
    if elapsed < interval:
        time.sleep(interval - elapsed)
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
        if exc.code == 429:
            # Slow every later request too, not just the retry of this one.
            # Throttling says the pace was wrong, and the next thousand
            # requests are the ones that have to live with the answer.
            _throttle_penalty = min(_throttle_penalty + THROTTLE_PENALTY_STEP,
                                    MAX_THROTTLE_PENALTY)
            raise Throttled(f"SEC returned 429 for {url}",
                            retry_after=_retry_after(exc)) from exc
        if exc.code >= 500:
            raise FetchError(f"SEC returned {exc.code} for {url}") from exc
        # Anything else in the 4xx range is a bug in the request, not weather.
        raise
    except OSError as exc:
        # HTTPError is caught above; what is left here is transport -- URLError,
        # timeouts, resets, a proxy declining. Same treatment as a refusal.
        raise FetchError(f"could not reach {url}: {exc}") from exc
    return raw if binary else raw.decode("utf-8", errors="replace")


def _retry_after(exc, ceiling=120.0):
    """Seconds the server asked us to wait, if it named a number.

    Capped: a Retry-After of an hour is not something a daily run can honour,
    and waiting it out would be indistinguishable from hanging.
    """
    try:
        value = float((exc.headers or {}).get("Retry-After", ""))
    except (TypeError, ValueError):
        return None
    return min(value, ceiling) if value > 0 else None


USASPENDING_SEARCH = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
DOD_CONTRACTS = "https://www.defense.gov/News/Contracts/"
SAM_OPPORTUNITIES = "https://api.sam.gov/opportunities/v2/search"

# Corporate boilerplate that differs between how a company registers with the
# SEC and how it appears on a federal award. Stripped from both sides before
# comparing, because "LOCKHEED MARTIN CORPORATION" and "Lockheed Martin Corp"
# are the same company and no exact match will ever say so.
NAME_SUFFIXES = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LLC", "LP",
    "LLP", "LTD", "LIMITED", "PLC", "HOLDINGS", "HOLDING", "GROUP", "THE",
    "AND", "NV", "SA", "AG", "SE", "TRUST", "PARTNERS", "INTERNATIONAL",
}


def normalize_company(name):
    """A company name reduced to the words that identify it."""
    text = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper())
    return " ".join(w for w in text.split() if w and w not in NAME_SUFFIXES)


def http_json(url, payload=None, agent=None):
    """Rate-limited JSON request. POSTs when given a payload.

    Separate from fetch() because these are not SEC endpoints: they want a
    different contact string, some need POST, and a failure here must never
    look like an EDGAR refusal.
    """
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request = time.time()

    body = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=body,
        headers={"User-Agent": agent or USER_AGENT or "signals-research",
                 "Content-Type": "application/json",
                 "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace")), resp.status
    except urllib.error.HTTPError as exc:
        return None, exc.code
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def http_text(url, agent=None):
    """Rate-limited GET returning text, for pages that are not APIs."""
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request = time.time()
    req = urllib.request.Request(
        url, headers={"User-Agent": agent or USER_AGENT or "signals-research"}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8", errors="replace"), resp.status
    except urllib.error.HTTPError as exc:
        return None, exc.code
    except OSError as exc:
        return None, f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------- ticker map


def load_ticker_map():
    """CIK -> (ticker, title). Doubles as the listed-company filter: any CIK
    absent from this file is a fund, private filer, or foreign entity.

    This map is load-bearing in a way that is easy to miss. M&A and buyback
    detection both gate on the filer being in it, so an EMPTY map does not
    produce an empty run -- it produces a run that quietly drops two of the
    three collectors and keeps going, because Form 4 resolves its issuer after
    parsing and carries on regardless. The old code returned {} when the fetch
    failed and there was no cache, which is exactly that: about a third of the
    normal output, no error anywhere, and a log that reads like a quiet day.

    So the three outcomes are now separated. A usable cache is used. A stale
    cache that could not be refreshed is used, loudly. Nothing usable at all is
    fatal, because every downstream answer would be a lie of omission.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, "company_tickers.json")
    cached = os.path.exists(path)

    stale = not cached or time.time() - os.path.getmtime(path) > 7 * 86400
    if stale:
        try:
            body = fetch(TICKER_MAP_URL)
            if not body:
                raise FetchError(f"{TICKER_MAP_URL} returned nothing")
            with open(path, "w") as fh:
                fh.write(body)
        except RuntimeError as exc:
            if not cached:
                raise FetchError(
                    f"could not load the ticker map and no cached copy exists: {exc}\n"
                    "Without it every M&A and buyback filing is discarded as "
                    "unlisted, so the run would silently collect insider buys "
                    "only. Refusing to run rather than under-report."
                ) from exc
            age = (time.time() - os.path.getmtime(path)) / 86400
            print(f"WARNING: ticker map refresh failed ({exc}); "
                  f"falling back to the cached copy, {age:.0f} days old")

    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        # A half-written cache from an interrupted run reads as valid JSON far
        # less often than it reads as a truncated file. Either way it is not a
        # ticker map, and pretending otherwise is the silent path again.
        raise FetchError(f"the cached ticker map at {path} is unreadable: {exc}\n"
                         "Delete it and rerun to fetch a fresh copy.") from exc

    # company_tickers.json is one row per SYMBOL, and a company with warrants,
    # units or preferred series appears several times under one CIK. Keying the
    # dict directly let whichever row came last win, which is how a Bakkt 10-Q
    # buyback came to be filed under BKKT-WT instead of BKKT.
    candidates = {}
    for row in data.values():
        try:
            candidates.setdefault(int(row["cik_str"]), []).append(
                (row["ticker"], row["title"]))
        except (KeyError, TypeError, ValueError):
            continue
    tickers = {cik: min(pairs, key=lambda p: (derivative_rank(p[0]), len(p[0]), p[0]))
               for cik, pairs in candidates.items()}

    if not tickers:
        raise FetchError(
            f"the ticker map at {path} parsed to zero companies.\n"
            "The SEC's schema for company_tickers.json has probably changed."
        )
    return tickers


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


class MalformedFiling(RuntimeError):
    """The document arrived but could not be read.

    Distinct from FetchError, which means it never arrived, and distinct from
    a filing that simply holds nothing we want. This one is a defect -- ours,
    or a schema change at the SEC -- and the caller must not record the
    accession as processed on the strength of it.
    """


def extract_ownership_xml(submission_text):
    """The ownership XML out of a full submission .txt, or None if it has none.

    A submission bundles several <DOCUMENT> blocks and only one of them is the
    Form 4 itself. Returning None means "this submission carries no ownership
    document" -- a real and unremarkable answer, since the caller fetches by
    form type and EDGAR occasionally files something else under it.

    A block that announces itself as an ownershipDocument and then fails to
    parse is a different matter and now raises. Swallowing it returned None,
    which parse_form4 turned into [], which handle_form4 returned as 0 -- and 0
    means "read it, nothing there", so the accession was recorded as processed
    and the filing was never fetched again. One unreadable byte and a purchase
    disappeared permanently, silently, with no way to find it afterwards.
    """
    for block in re.findall(r"<XML>(.*?)</XML>", submission_text, re.S):
        block = block.strip()
        if "<ownershipDocument" not in block:
            continue
        # Strip anything before the root element (stray XML declarations etc).
        start = block.find("<ownershipDocument")
        try:
            return ElementTree.fromstring(block[start:])
        except ElementTree.ParseError as exc:
            raise MalformedFiling(f"ownership XML did not parse: {exc}") from exc
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


def parse_form4(root, want_code="P", want_direction="A"):
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
        if code != want_code or direction != want_direction:
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
    suspect      INTEGER DEFAULT 0,
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
    as_of        TEXT,
    fetched_at   TEXT,
    derived_v    INTEGER,
    public_float REAL,
    float_as_of  TEXT
);

-- Lane A: the setup condition. Cached alongside the buyback figures because it
-- is derived from the SAME companyfacts document -- the fetch that already
-- happens for every listed 10-Q/10-K filer carries revenue and contract
-- liabilities in it, so this costs no additional request.
CREATE TABLE IF NOT EXISTS issuer_setup (
    cik         INTEGER PRIMARY KEY,
    setup       INTEGER,
    streak      INTEGER,
    reason      TEXT,
    quarters    TEXT,
    tags        TEXT,
    fetched_at  TEXT,
    derived_v   INTEGER
);

-- Annualised repurchase activity per issuer, cached on the same terms as the
-- share count: slow-moving, one small request, and worth surviving between
-- runs on a fresh runner.
CREATE TABLE IF NOT EXISTS issuer_buybacks (
    cik          INTEGER PRIMARY KEY,
    shares       REAL,
    value        REAL,
    concept      TEXT,
    period_start TEXT,
    period_end   TEXT,
    annualised   INTEGER,
    fetched_at   TEXT,
    derived_v    INTEGER
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
    # CREATE TABLE IF NOT EXISTS will not add a column to a table that already
    # exists, so databases written before derived_v need it grafted on. A NULL
    # there reads as version 0 and forces one refresh.
    wanted = {
        "issuer_facts": (("derived_v", "INTEGER"), ("public_float", "REAL"),
                         ("float_as_of", "TEXT")),
        "issuer_buybacks": (("derived_v", "INTEGER"),),
        # n_docs alone cannot answer the only question anyone asks of this
        # table. See log_run().
        "run_log": (("n_candidates", "INTEGER"), ("n_skipped", "INTEGER"),
                    ("n_refused", "INTEGER")),
        "insider_buys": (("suspect", "INTEGER DEFAULT 0"),),
    }
    for table, columns in wanted.items():
        present = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        for name, kind in columns:
            if name not in present:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}")

    # Events stored before iso_date() existed kept the index's YYYYMMDD form,
    # so the table holds two formats at once and any ordering by filed_date
    # interleaves them wrongly -- '20260806' sorts above every ISO date because
    # '0' > '-'. Normalising in place is cheap and permanent; doing it at read
    # time would leave the next date control to rediscover the same trap.
    conn.execute(
        "UPDATE events SET filed_date = "
        "substr(filed_date,1,4) || '-' || substr(filed_date,5,2) || '-' || "
        "substr(filed_date,7,2) "
        "WHERE filed_date GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
    )
    conn.commit()
    # The state machine's tables belong to the connection, not to main(): the
    # dashboard reads state_transitions, and leaving the migration in the
    # command path meant any other caller -- a script, a test, anything
    # importing this module -- got a connection whose schema was half there.
    signal_state.migrate(conn)
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
    """Dollars at the precision a reader needs, over eight orders of magnitude.

    The unit is chosen from the ROUNDED figure rather than the raw one, which
    is the difference between "$1000K" and "$1.0M": a $999,999.75 purchase is
    below the million mark by a quarter of a dollar and used to print in
    thousands with four digits. Under a thousand it prints in whole dollars --
    a $31 purchase is a real thing a director occasionally files, and "$0K" is
    not a number.
    """
    if value is None:
        return "undisclosed"
    if value < 1_000:
        return f"${value:,.0f}"
    for unit, size in (("K", 1e3), ("M", 1e6), ("B", 1e9)):
        scaled = value / size
        if round(scaled, 0 if unit == "K" else 1) < 1000:
            return f"${scaled:,.0f}K" if unit == "K" else f"${scaled:,.1f}{unit}"
    return f"${value / 1e12:,.1f}T"


# Failures that are correct to survive individually and alarming in bulk.
#
# An issuer whose XBRL will not load is reported without a denominator, which
# is the right call for one filing -- the purchase still happened and is still
# worth seeing. But nothing anywhere counted them, so the difference between
# "three issuers tag nothing" and "data.sec.gov refused four hundred times"
# was a dashboard with more unscored cards than usual and no way to tell which
# it was. Tallied here and printed once at the end of a run.
DEGRADED = collections.Counter()


def note_degraded(kind):
    DEGRADED[kind] += 1


def report_degraded():
    """One line per kind of survivable failure, or silence if there were none."""
    for kind, n in sorted(DEGRADED.items(), key=lambda kv: -kv[1]):
        print(f"  degraded: {n} × {kind}")
    DEGRADED.clear()


# ---------------------------------------------------------------- significance


def company_facts(cik):
    """Every XBRL fact an issuer reports, in one request.

    The alternative is companyconcept, one request per tag, and this collector
    wants up to eight of them per issuer: two for the share count and six
    walking the repurchase concepts. The rate limiter charges per request
    rather than per byte, so a bigger single document is markedly cheaper than
    the walk -- the first buyback run took twelve minutes against two and a
    half, almost all of it waiting between small requests for facts that live
    in the same file.
    """
    if not cik:
        return None
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
    try:
        body = fetch(url)
    except RuntimeError:
        # A refusal on an optional enrichment must not end the run; the filing
        # is still worth reporting without its denominator. Counted, though --
        # one is weather, four hundred is the SEC shutting us out, and until
        # now both looked like issuers that simply tag nothing.
        note_degraded("issuer XBRL unavailable (companyfacts)")
        return None
    if not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        note_degraded("issuer XBRL unreadable (companyfacts)")
        return None


def facts_concept(facts, taxonomy, tag):
    """One concept out of a companyfacts payload, shaped like companyconcept."""
    try:
        return facts["facts"][taxonomy][tag]
    except (KeyError, TypeError):
        return None


def refresh_issuer_xbrl(conn, cik):
    """Derive every cached XBRL figure for an issuer from a single fetch.

    Both caches are filled together even when only one was asked for, so an
    issuer is fetched once per run however many of its facts get used.
    """
    facts = company_facts(cik)

    shares_out = as_of = None
    for taxonomy, tag in XBRL_CONCEPTS:
        points = [
            point
            for unit in ((facts_concept(facts, taxonomy, tag) or {}).get("units")
                         or {}).values()
            for point in unit
            if point.get("val")
        ]
        if not points:
            continue
        latest = max(points, key=lambda p: (p.get("end") or "", p.get("filed") or ""))
        candidate = plausible_shares(float(latest["val"]))
        if candidate is None:
            continue  # placeholder count; try the other concept
        shares_out, as_of = candidate, latest.get("end")
        break

    floated = latest_instant(
        facts_concept(facts, "dei", "EntityPublicFloat"),
        max_age_days=FLOAT_MAX_AGE_DAYS,
    )
    public_float = plausible_float(floated["value"]) if floated else None

    # Cache misses too, so an issuer that tags nothing is not re-asked on every
    # run. The TTL still retires the answer.
    conn.execute(
        """INSERT INTO issuer_facts
             (cik, shares_out, as_of, fetched_at, derived_v, public_float,
              float_as_of)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(cik) DO UPDATE SET
             shares_out   = excluded.shares_out,
             as_of        = excluded.as_of,
             fetched_at   = excluded.fetched_at,
             derived_v    = excluded.derived_v,
             public_float = excluded.public_float,
             float_as_of  = excluded.float_as_of""",
        (cik, shares_out, as_of, date.today().isoformat(), XBRL_DERIVATION,
         public_float, floated["as_of"] if floated else None),
    )

    shares = _first_flow_from(facts, BUYBACK_SHARE_CONCEPTS)
    value = _first_flow_from(facts, BUYBACK_VALUE_CONCEPTS)
    best = shares or value
    conn.execute(
        """INSERT INTO issuer_buybacks
             (cik, shares, value, concept, period_start, period_end,
              annualised, fetched_at, derived_v)
           VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(cik) DO UPDATE SET
             shares=excluded.shares, value=excluded.value,
             concept=excluded.concept, period_start=excluded.period_start,
             period_end=excluded.period_end, annualised=excluded.annualised,
             fetched_at=excluded.fetched_at, derived_v=excluded.derived_v""",
        (cik,
         shares["value"] if shares else None,
         value["value"] if value else None,
         (best or {}).get("concept"),
         (best or {}).get("start"),
         (best or {}).get("end"),
         int(bool((best or {}).get("annualised"))),
         date.today().isoformat(), XBRL_DERIVATION),
    )

    # Lane A, off the same payload. The design memo treats this as a second
    # pipeline needing its own universe, and the reasoning is right about the
    # WATCHLIST -- which is populated by catalysts and therefore only contains
    # companies that already did something loud. But the periodic lane does not
    # read the watchlist: handle_periodic fires on every listed 10-Q/10-K filer
    # in the daily index, which is 1,102 issuers here that are on no watchlist
    # at all. The universe and the quarterly clock already exist; a 10-Q IS the
    # quarterly tick. So this rides them rather than duplicating them.
    verdict = setup_signal.evaluate_setup(facts)
    conn.execute(
        """INSERT INTO issuer_setup
             (cik, setup, streak, reason, quarters, tags, fetched_at, derived_v)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(cik) DO UPDATE SET
             setup=excluded.setup, streak=excluded.streak,
             reason=excluded.reason, quarters=excluded.quarters,
             tags=excluded.tags, fetched_at=excluded.fetched_at,
             derived_v=excluded.derived_v""",
        (cik, int(bool(verdict["setup"])), verdict.get("streak") or 0,
         verdict["reason"], json.dumps(verdict.get("quarters") or []),
         ",".join(setup_signal.tag_family(facts)),
         date.today().isoformat(), XBRL_DERIVATION),
    )
    return shares_out


def setup_condition(conn, cik):
    """The cached Lane A verdict for an issuer, or None if never derived."""
    if not cik:
        return None
    row = conn.execute("SELECT * FROM issuer_setup WHERE cik = ?", (cik,)).fetchone()
    return dict(row) if row else None


def _fresh(row):
    """True when a cached row is inside its TTL and derived by current rules."""
    if not row or not row["fetched_at"]:
        return False
    try:
        derived = row["derived_v"] or 0
    except (IndexError, KeyError):
        derived = 0          # a row selected without the column, or written
                             # before it existed: treat as an old derivation
    if derived != XBRL_DERIVATION:
        return False
    try:
        return (date.today() - date.fromisoformat(row["fetched_at"][:10])).days \
            < SHARES_TTL_DAYS
    except ValueError:
        return False


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
        "SELECT shares_out, fetched_at, derived_v FROM issuer_facts WHERE cik = ?",
        (cik,)
    ).fetchone()
    if _fresh(row):
        # Filtered on the way out too, so a bad value already cached by an
        # earlier version stops being served without a refetch.
        return plausible_shares(row["shares_out"])

    return plausible_shares(refresh_issuer_xbrl(conn, cik))




def plausible_shares(shares_out):
    """The share count, or None when it cannot be a real one."""
    if not shares_out or shares_out < MIN_PLAUSIBLE_SHARES:
        return None
    return shares_out


def xbrl_concept(cik, taxonomy, tag):
    """Raw companyconcept payload for one tag, or None if it is not reported."""
    if not cik:
        return None
    url = (
        f"https://data.sec.gov/api/xbrl/companyconcept/"
        f"CIK{cik:010d}/{taxonomy}/{tag}.json"
    )
    try:
        body = fetch(url)
    except RuntimeError:
        note_degraded("issuer XBRL unavailable (companyconcept)")
        return None
    if not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        note_degraded("issuer XBRL unreadable (companyconcept)")
        return None


def concept_points(payload):
    """Flatten a companyconcept payload into dated observations.

    Only closed periods with both a start and an end, so instantaneous
    balances and open-ended facts do not get summed as if they were flows.
    """
    if not payload:
        return []
    points = []
    for unit, facts in (payload.get("units") or {}).items():
        for fact in facts:
            if fact.get("val") is None or not fact.get("start") or not fact.get("end"):
                continue
            points.append({
                "unit": unit,
                "val": float(fact["val"]),
                "start": fact["start"],
                "end": fact["end"],
                "form": fact.get("form"),
                "fy": fact.get("fy"),
                "fp": fact.get("fp"),
            })
    points.sort(key=lambda p: (p["end"], p["start"]))
    return points


def _period_days(point):
    try:
        start = date.fromisoformat(point["start"])
        end = date.fromisoformat(point["end"])
    except (ValueError, TypeError):
        return None
    return (end - start).days or None


def recent_flow(payload, today=None):
    """The most recent usable period from a flow concept, as reported.

    Picks one observation rather than summing: the reported periods are fiscal
    year-to-date and overlap each other, so adding them would count the same
    dollars several times. Prefers the longest period among those ending on the
    latest date, since a full year needs no scaling and a stub quarter does.
    """
    today = today or market_today()
    usable = []
    for point in concept_points(payload):
        span = _period_days(point)
        if not span or not BUYBACK_MIN_PERIOD_DAYS <= span <= BUYBACK_MAX_PERIOD_DAYS:
            continue
        try:
            end = date.fromisoformat(point["end"])
        except ValueError:
            continue
        if (today - end).days > BUYBACK_MAX_AGE_DAYS or end > today:
            continue
        usable.append((point, span, end))
    if not usable:
        return None

    latest_end = max(end for _, _, end in usable)
    point, span, end = max(
        (u for u in usable if u[2] == latest_end), key=lambda u: u[1]
    )
    if not point["val"]:
        return None  # a tagged zero is not a buyback

    # NOT annualised. Scaling a half-year by two asserts the company will keep
    # repurchasing at the same rate, which the filing does not say and the data
    # cannot support -- a board authorisation is a ceiling, not a run-rate, and
    # programmes are routinely front-loaded or paused. Grindr's six months at
    # 21.9% of its shares became "annualised 43.9%", a figure no company has
    # ever posted, and the doubling was ours.
    #
    # The period is reported instead, so a reader can do the extrapolation
    # themselves if they want it and can see what they are extrapolating from.
    return {
        "value": point["val"],
        "reported": point["val"],
        "start": point["start"],
        "end": point["end"],
        "period_days": span,
        "annualised": 0,   # retired; kept so old rows and the schema still read
        "unit": point["unit"],
    }


def latest_instant(payload, today=None, max_age_days=None):
    """The most recent point-in-time value of a concept.

    concept_points deliberately discards facts without a start date, because a
    balance is not a flow and summing one would be wrong. Cover-page figures
    like the public float are exactly that shape, so they need their own
    reader rather than a loosened version of the flow one.
    """
    today = today or market_today()
    best = None
    for facts in ((payload or {}).get("units") or {}).values():
        for fact in facts:
            if fact.get("val") is None or not fact.get("end"):
                continue
            try:
                end = date.fromisoformat(fact["end"])
            except (ValueError, TypeError):
                continue
            if end > today:
                continue
            if max_age_days is not None and (today - end).days > max_age_days:
                continue
            if best is None or end > best[0]:
                best = (end, float(fact["val"]))
    return {"value": best[1], "as_of": best[0].isoformat()} if best else None


def plausible_float(value):
    """The public float, or None when it cannot be a real one."""
    if not value or value < MIN_PLAUSIBLE_FLOAT_USD:
        return None
    return value


def _first_flow_from(facts, concepts):
    """The first usable flow among a list of concepts, out of one payload."""
    for taxonomy, tag in concepts:
        flow = recent_flow(facts_concept(facts, taxonomy, tag))
        if flow:
            return {**flow, "concept": tag}
    return None


def buyback_activity(conn, cik):
    """Repurchases for an issuer as reported, cached like any other slow fact.

    Both this and the share count come out of one companyfacts document, so
    whichever is asked for first pays the single request and the other is
    already waiting in the cache.
    """
    if not cik:
        return None

    row = conn.execute(
        "SELECT * FROM issuer_buybacks WHERE cik = ?", (cik,)
    ).fetchone()
    if not _fresh(row):
        refresh_issuer_xbrl(conn, cik)
        row = conn.execute(
            "SELECT * FROM issuer_buybacks WHERE cik = ?", (cik,)
        ).fetchone()

    if not row or not (row["shares"] or row["value"]):
        return None
    activity = dict(row)
    # Derived rather than stored: the reporting period is already here as two
    # dates, and the length is what the headline needs now that the figure is
    # no longer scaled to a year. Deriving it also reaches rows written before
    # anything wanted it.
    activity["period_days"] = _span_days(row["period_start"], row["period_end"])
    return activity


def _span_days(start, end):
    try:
        return (date.fromisoformat(end) - date.fromisoformat(start)).days
    except (TypeError, ValueError):
        return None


def unannualise_buybacks(conn):
    """Undo the scaling on cached rows written while it was still applied.

    Exact arithmetic, no refetch: a part-year figure was multiplied by
    365/span, so dividing by the same factor recovers what the issuer actually
    filed. Doing it this way rather than by bumping XBRL_DERIVATION matters --
    that would force a fresh companyfacts request for every issuer, and the
    number needed to change today.
    """
    fixed = 0
    for row in conn.execute(
        "SELECT cik, shares, value, period_start, period_end FROM issuer_buybacks "
        "WHERE annualised = 1"
    ).fetchall():
        span = _span_days(row["period_start"], row["period_end"])
        if not span or span >= 300:
            continue
        factor = 365.0 / span
        conn.execute(
            "UPDATE issuer_buybacks SET shares = ?, value = ?, annualised = 0 "
            "WHERE cik = ?",
            (row["shares"] / factor if row["shares"] else row["shares"],
             row["value"] / factor if row["value"] else row["value"],
             row["cik"]),
        )
        fixed += 1
    return fixed


def public_float(conn, cik):
    """Cached public float for an issuer, or None."""
    if not cik:
        return None
    row = conn.execute(
        "SELECT public_float, fetched_at, derived_v FROM issuer_facts WHERE cik = ?",
        (cik,),
    ).fetchone()
    if not _fresh(row):
        refresh_issuer_xbrl(conn, cik)
        row = conn.execute(
            "SELECT public_float FROM issuer_facts WHERE cik = ?", (cik,)
        ).fetchone()
    return plausible_float(row["public_float"]) if row else None


def buyback_float_pct(activity, floated):
    """Annualised repurchase dollars as a percent of the public float."""
    floated = plausible_float(floated)
    if not activity or not floated:
        return None
    spent = activity.get("value")
    if not spent:
        return None
    pct = spent / floated * 100.0
    return pct if 0 < pct <= MAX_PLAUSIBLE_BUYBACK_PCT else None


def buyback_measure(activity, shares_out, floated):
    """The share of the company repurchased, and what it was measured against.

    Share counts first: that ratio is exact and needs no price. The float is
    the fallback and a different denominator, not a substitute one -- it
    excludes affiliate holdings, so the same buyback reads larger against it.
    Callers label which was used rather than presenting them as one number.
    """
    pct = buyback_pct(activity, shares_out)
    if pct is not None:
        return pct, "shares outstanding"
    pct = buyback_float_pct(activity, floated)
    if pct is not None:
        return pct, "public float"
    return None, None


def buyback_pct(activity, shares_out):
    """Annualised repurchases as a percent of the company.

    Share counts where the issuer tags them, since that ratio needs no price.
    Otherwise dollars against a market cap -- which requires a price we only
    have when the same issuer has also filed a Form 4, so many filings carry a
    dollar figure and no percentage. Saying so beats inventing a denominator.
    """
    shares_out = plausible_shares(shares_out)
    if not activity or not shares_out:
        return None
    retired = activity.get("shares")
    if not retired:
        return None
    pct = retired / shares_out * 100.0
    return pct if 0 < pct <= MAX_PLAUSIBLE_BUYBACK_PCT else None


def is_fund_vehicle(name):
    """True for a pooled vehicle whose "repurchases" are share redemptions.

    Covers both families: exchange-traded funds and commodity pools, and
    non-traded REITs and BDCs. They differ in how they are named and not at all
    in why they are wrong here -- neither is a company whose board decided its
    own stock was cheap.
    """
    if not name:
        return False
    return bool(FUND_VEHICLE.search(name) or NONTRADED_VEHICLE.search(name))


def buyback_band(pct):
    if pct is None:
        return None
    for floor, name in BUYBACK_BANDS:
        if pct >= floor:
            return name
    return "negligible"


def handle_periodic(conn, row, listed):
    """A 10-Q or 10-K is only the trigger; the numbers come from XBRL.

    The filing itself is never downloaded. The index says this company reported,
    and one small JSON per issuer -- cached for a month -- says what it bought
    back, instead of several megabytes of HTML and a different Item 5(c) table
    layout for every filer.
    """
    ticker, title = listed
    # Checked before the XBRL request, not after: a fund is not a company whose
    # management is saying anything, so there is nothing here worth a fetch.
    if is_fund_vehicle(row["company"]) or is_fund_vehicle(title):
        return 0

    activity = buyback_activity(conn, row["cik"])
    if not activity:
        return 0

    pct, basis = buyback_measure(
        activity,
        shares_outstanding(conn, row["cik"]),
        public_float(conn, row["cik"]),
    )
    band = buyback_band(pct)
    tier = 1 if (pct is not None and pct >= TIER1_BUYBACK_PCT) else 2

    if tier == 1:
        promote(conn, ticker, row["cik"], f"buyback ({pct:.1f}% of {basis})")

    detail = {**activity, "pct_of_shares": pct, "pct_basis": basis,
              "significance": band, "company": row["company"],
              "form_type": row["form_type"]}

    # Keyed on the reporting period, not the accession: the same quarter is
    # reported again in the next 10-K, and that is one fact, not two.
    return emit(
        conn,
        source_id=f"{row['cik']}:{activity['period_end']}",
        entity=ticker,
        event_type="buyback",
        tier=tier,
        headline=buyback_headline(ticker, detail),
        detail=detail,
        filed=row["filed"],
    )


def probe_setup(cik=1069183, start_year=2013, end_year=2018):
    """Backtest Lane A against one issuer with a known outcome.

    Runs on the GitHub runner rather than here, because this sandbox's egress
    policy denies sec.gov and the runner's does not. One companyfacts fetch
    carries the issuer's whole history, so the condition can be evaluated at
    successive as-of dates from a single request -- which is what makes a
    walk-forward backtest cost one call rather than twenty.

    Default CIK is Axon (TASR/AXON), the worked example: the filing stream is
    silent from 2005 to 2016 and the entry sits in the middle of that silence,
    so if Lane A cannot see 2016 here it cannot see anything.

    Writes nothing. Prints a quarter-by-quarter table so the thresholds can be
    read off real numbers instead of reasoned at.
    """
    facts = company_facts(cik)
    if not facts:
        print(f"no companyfacts for CIK {cik}")
        return
    print(f"entity      : {facts.get('entityName')}  (CIK {cik})")
    print(f"tags present: {', '.join(setup_signal.tag_family(facts)) or 'NONE'}")

    liabilities = setup_signal.instant_series(facts, setup_signal.LIABILITY_CONCEPTS)
    revenues = setup_signal.flow_series(facts, setup_signal.REVENUE_CONCEPTS)
    print(f"liability points: {len(liabilities)}"
          + (f"  {liabilities[0][0]} .. {liabilities[-1][0]}" if liabilities else ""))
    print(f"revenue points  : {len(revenues)}"
          + (f"  {revenues[0][0]} .. {revenues[-1][0]}" if revenues else ""))

    # Walk forward. The question is not "is it true today" but "when did it
    # first become true", which is the only version of the question a backtest
    # can answer usefully.
    print(f"\n{'as of':>12}  {'state':>7}  streak  reason")
    first_true = None
    for year in range(start_year, end_year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            as_of = date(year, month, day)
            v = setup_signal.evaluate_setup(facts, today=as_of)
            flag = "SETUP" if v["setup"] else "-"
            if v["setup"] and first_true is None:
                first_true = as_of
            print(f"{as_of.isoformat():>12}  {flag:>7}  {v.get('streak', 0):>6}  "
                  f"{v['reason'][:64]}")

    print(f"\nfirst true: {first_true or 'never in window'}")
    print("thresholds in force: "
          f"{setup_signal.MIN_CONSECUTIVE_QUARTERS} consecutive quarters, "
          f"{setup_signal.MIN_GROWTH_GAP_PP}pp gap, "
          f"${setup_signal.MIN_QUARTERLY_REVENUE_USD:,.0f} revenue floor, "
          f"{setup_signal.MIN_LIABILITY_TO_REVENUE:.0%} liability materiality")

    # The evidence, so a threshold can be moved on numbers rather than feel.
    latest = setup_signal.evaluate_setup(facts, today=date(end_year, 12, 31))
    if latest.get("quarters"):
        print(f"\n{'quarter':>12} {'liability':>16} {'revenue':>16} "
              f"{'liab %':>8} {'rev %':>8} {'gap pp':>8}")
        for q in reversed(latest["quarters"]):
            print(f"{q['quarter_end']:>12} {q['liability']:>16,.0f} "
                  f"{q['revenue']:>16,.0f} {q['liability_growth_pct']:>8.1f} "
                  f"{q['revenue_growth_pct']:>8.1f} {q['gap_pp']:>8.1f}")


def probe_form4(accession):
    """Print every non-derivative transaction in one Form 4, unfiltered.

    Read-only, writes nothing. Exists because the ledger cannot answer the
    question: `parse_form4` returns no accession, so `record_insider_sales`
    stores NULL for all 1,166 sale rows and a stored sale cannot be traced back
    to the filing it came from. The document itself is the only witness.

    Prints the codes and directions as filed rather than the parser's view of
    them, since the question is whether one transaction is being read twice or
    whether the filing genuinely carries both a purchase and a disposal.
    """
    plain = accession.replace("-", "")
    cik = None
    for row in connect().execute(
            "SELECT cik FROM documents WHERE accession = ?", (accession,)):
        cik = row["cik"]
    if cik is None:
        print(f"accession {accession} is not in documents; trying the index")
        return
    path = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{plain}/"
            f"{accession}-index.htm")
    print(f"accession : {accession}\nissuer cik: {cik}\nindex     : {path}")

    text = fetch(f"https://www.sec.gov/Archives/edgar/data/{cik}/{plain}.txt")
    if not text:
        print("no document body returned")
        return
    root = extract_ownership_xml(text)
    if root is None:
        print("no ownership XML in the submission")
        return

    print("\nreporting owners:")
    for owner in root.findall("reportingOwner"):
        print("   ", _text(owner, "reportingOwnerId/rptOwnerName", "UNKNOWN"))

    print(f"\n{'code':>5} {'A/D':>4} {'date':>12} {'shares':>14} {'price':>10} "
          f"{'value':>14}")
    for txn in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = _text(txn, "transactionCoding/transactionCode")
        direction = _text(
            txn, "transactionAmounts/transactionAcquiredDisposedCode/value")
        shares = _num(txn, "transactionAmounts/transactionShares/value") or 0
        price = _num(txn, "transactionAmounts/transactionPricePerShare/value") or 0
        print(f"{code or '?':>5} {direction or '?':>4} "
              f"{_text(txn, 'transactionDate/value') or '?':>12} "
              f"{shares:>14,.0f} {price:>10,.4f} {shares * price:>14,.2f}")

    print("\nparser's view:")
    print(f"   buys  (P/A): {len(parse_form4(root))}")
    print(f"   sales (S/D): {len(parse_form4(root, want_code='S', want_direction='D'))}")


def probe_setup_population(sample=150):
    """How selective is Lane A across the universe, not just on its poster child?

    The Axon backtest answers "does the condition fire on a company we know
    changed shape". It cannot answer "does it fire on everyone", and those are
    different failures: a metric true for 23 consecutive quarters on Axon is
    either a business turning into a subscription company or a threshold set so
    low that half the market clears it. Only counting tells the two apart.

    SETUP is the bottom rung -- it promotes a quiet issuer out of DORMANT so a
    later insider buy lands on a name already being watched -- so it is allowed
    to be a state rather than an event, and allowed to persist. What it is not
    allowed to be is most of the market, because a watchlist that holds
    everything holds nothing.

    Deterministic stride sample over sorted CIKs rather than a random one, so
    two runs of the same size are comparable and a threshold change can be read
    against the same companies rather than against sampling noise.
    """
    tickers = load_ticker_map()
    ciks = sorted(tickers)
    stride = max(1, len(ciks) // sample)
    picked = ciks[::stride][:sample]
    print(f"universe: {len(ciks)} CIKs, sampling every {stride}th -> {len(picked)}")
    print("thresholds in force: "
          f"{setup_signal.MIN_CONSECUTIVE_QUARTERS} consecutive quarters, "
          f"{setup_signal.MIN_GROWTH_GAP_PP}pp gap, "
          f"${setup_signal.MIN_QUARTERLY_REVENUE_USD:,.0f} revenue floor, "
          f"{setup_signal.MIN_LIABILITY_TO_REVENUE:.0%} liability materiality\n")

    tally = {"no facts": 0, "no liability tag": 0, "no history": 0,
             "streak too short": 0, "SETUP": 0}
    hits, streaks, judged = [], [], []
    for n, cik in enumerate(picked, 1):
        symbol, title = tickers[cik]
        facts = company_facts(cik)
        if not facts:
            tally["no facts"] += 1
            continue
        if not setup_signal.tag_family(facts):
            tally["no liability tag"] += 1
            continue
        verdict = setup_signal.evaluate_setup(facts)
        streak = verdict.get("streak", 0)
        if verdict.get("quarters"):
            # Scale, recorded for every issuer the metric was able to judge --
            # not just the ones it liked. A floor can only be set from the
            # distribution it has to cut, and the hits alone do not show it.
            recent = verdict["quarters"][0]
            judged.append((symbol, title, streak, verdict["setup"],
                           recent["revenue"], recent["liability"]))
        if verdict["setup"]:
            tally["SETUP"] += 1
            worst = min(q["gap_pp"] for q in verdict["quarters"][:streak])
            hits.append((symbol, title, streak, worst))
        elif not verdict.get("quarters"):
            tally["no history"] += 1
        else:
            tally["streak too short"] += 1
            streaks.append(streak)
        if n % 25 == 0:
            print(f"  ... {n}/{len(picked)}", flush=True)

    print(f"\n{'outcome':>20}  count   share")
    for key, count in tally.items():
        print(f"{key:>20}  {count:>5}  {count / len(picked) * 100:>5.1f}%")

    # "no facts" covers both an issuer that tags nothing and one the SEC
    # refused us, and those mean opposite things about the measurement. A
    # sample thinned by throttling is a smaller sample, not a finding.
    report_degraded()

    reporting = tally["SETUP"] + tally["streak too short"]
    if reporting:
        print(f"\nof the {reporting} issuers with enough history to judge, "
              f"{tally['SETUP'] / reporting * 100:.0f}% are SETUP")

    # The list itself is the check the counts cannot make. If it reads as banks
    # and REITs, the metric is measuring float and not deferred revenue.
    if hits:
        print(f"\n{'ticker':>8}  {'streak':>6}  {'min gap':>8}  company")
        for symbol, title, streak, worst in sorted(hits, key=lambda h: -h[2]):
            print(f"{symbol:>8}  {streak:>6}  {worst:>8.1f}  {title[:44]}")

    # Where the near-misses sit says which way the threshold should move: a pile
    # at 2 means 3 is cutting real cases, a pile at 0 means it is nowhere close.
    if streaks:
        print("\nnear misses by streak length:")
        for length in range(0, setup_signal.MIN_CONSECUTIVE_QUARTERS):
            print(f"  {length} quarter(s): {streaks.count(length)}")

    # The scale distribution, which is the thing the first population run could
    # not show. Its four hits were an antimony miner, a micro-cap e-commerce
    # roll-up and a near-pre-revenue device company -- companies where a single
    # contract moves the balance sheet, not companies changing shape. Axon in
    # the years that mattered ran ~$80m a quarter against a liability worth
    # around 60% of it, and the revenue floor sits thirty times below that.
    if judged:
        print(f"\n{'ticker':>8}  {'setup':>5}  {'streak':>6}  {'revenue $m':>10}  "
              f"{'liab/rev':>8}  company")
        for symbol, title, streak, is_setup, revenue, liability in sorted(
                judged, key=lambda j: -j[4]):
            ratio = liability / revenue if revenue else 0.0
            print(f"{symbol:>8}  {'YES' if is_setup else '-':>5}  {streak:>6}  "
                  f"{revenue / 1e6:>10,.1f}  {ratio:>8.2f}  {title[:36]}")


def probe_contracts(days=30, sample=200):
    """Measure whether federal award data can be tied to listed companies.

    The APIs working is not the question -- the question is entity resolution.
    USAspending names legal entities and the DoD page names them in prose;
    neither carries a ticker or a CIK. If award recipients cannot be matched to
    listed issuers at a decent rate, a contract collector produces a stream of
    names nobody can trade against, and the design has to change before it is
    built rather than after.
    """
    tickers = load_ticker_map()
    listed = {}
    for cik, (ticker, title) in tickers.items():
        key = normalize_company(title)
        if key:
            listed.setdefault(key, (ticker, title, cik))
    print(f"ticker map: {len(tickers)} issuers, {len(listed)} distinct normalised names\n")

    since = (market_today() - timedelta(days=days)).isoformat()
    payload = {
        "filters": {
            "time_period": [{"start_date": since,
                             "end_date": market_today().isoformat()}],
            "award_type_codes": ["A", "B", "C", "D"],   # definitive contracts
        },
        "fields": ["Award ID", "Recipient Name", "Award Amount",
                   "Awarding Agency", "Start Date"],
        "sort": "Award Amount", "order": "desc",
        "limit": min(sample, 100), "page": 1,
    }
    print(f"=== USAspending: contract awards since {since} ===")
    data, status = http_json(USASPENDING_SEARCH, payload,
                             agent="signals-research contracts probe")
    if not data:
        print(f"  request failed: {status}")
        results = []
    else:
        results = data.get("results") or []
        print(f"  HTTP {status}, {len(results)} awards returned")

    matched = unmatched = 0
    examples, misses = [], []
    for row in results:
        name = row.get("Recipient Name") or ""
        hit = listed.get(normalize_company(name))
        if hit:
            matched += 1
            if len(examples) < 8:
                examples.append((hit[0], name, row.get("Award Amount"),
                                 (row.get("Awarding Agency") or "")[:28]))
        else:
            unmatched += 1
            if len(misses) < 10:
                misses.append((name, row.get("Award Amount")))

    total = matched + unmatched
    if total:
        print(f"  recipients matched to a listed ticker: {matched}/{total} "
              f"({matched / total * 100:.0f}%)\n")
        print("  matched:")
        for tk, name, amt, agency in examples:
            print(f"    {tk:7} {name[:38]:40} ${(amt or 0):>15,.0f}  {agency}")
        print("\n  unmatched (the resolution problem, in the raw):")
        for name, amt in misses:
            print(f"    {'':7} {name[:38]:40} ${(amt or 0):>15,.0f}")

    print(f"\n=== DoD daily contract announcements ===")
    page, status = http_text(DOD_CONTRACTS, agent="signals-research contracts probe")
    if not page:
        print(f"  request failed: {status}")
    else:
        print(f"  HTTP {status}, {len(page):,} bytes")
        dollars = re.findall(r"\$[\d,]{7,}", page)
        links = re.findall(r'href="([^"]*contract[^"]*)"', page, re.I)
        print(f"  dollar figures on the index page : {len(dollars)}")
        print(f"  links that look like daily posts : {len(set(links))}")
        print(f"  sample: {dollars[:5]}")

    print(f"\n=== SAM.gov without an API key ===")
    _, status = http_json(SAM_OPPORTUNITIES + "?limit=1",
                          agent="signals-research contracts probe")
    print(f"  HTTP {status}  (a key requirement would break the no-credentials property)")


def probe_buybacks(days=3, sample=60):
    """Report how many recent periodic filers actually tag repurchase data.

    Written before building the collector rather than after: if the cash-flow
    concept is tagged by a small minority, the whole XBRL approach is wrong and
    the Item 5(c) table in the filing itself is the only route. Cheap to answer,
    and it decides the design.
    """
    tickers = load_ticker_map()
    filers = {}
    for day in business_days_back(days):
        try:
            body = fetch(index_url(day))
        except RuntimeError:
            continue
        if not body:
            continue
        for row in parse_master_idx(body):
            if row["form_type"] in PERIODIC_FORMS and row["cik"] in tickers:
                filers.setdefault(row["cik"], (tickers[row["cik"]][0],
                                               row["form_type"], day.isoformat()))
            if len(filers) >= sample:
                break
        if len(filers) >= sample:
            break

    print(f"sampled {len(filers)} listed {'/'.join(sorted(PERIODIC_FORMS))} filers "
          f"over the last {days} business days\n")
    if not filers:
        print("no periodic filings found in the window -- try more days")
        return

    candidates = BUYBACK_SHARE_CONCEPTS + BUYBACK_VALUE_CONCEPTS
    hits = {tag: 0 for _, tag in candidates}
    examples = []
    for cik, (ticker, form, day) in filers.items():
        found = {}
        for taxonomy, tag in candidates:
            points = concept_points(xbrl_concept(cik, taxonomy, tag))
            if points:
                hits[tag] += 1
                found[tag] = points[-1]
        if found:
            examples.append((ticker, form, found))

    print(f"{'CONCEPT':46} {'FILERS':>7} {'COVERAGE':>9}")
    for _, tag in candidates:
        print(f"{tag:46} {hits[tag]:>7} {hits[tag] / len(filers) * 100:>8.0f}%")

    print(f"\nany repurchase concept at all: {len(examples)}/{len(filers)} "
          f"({len(examples) / len(filers) * 100:.0f}%)\n")

    # What could turn a dollar figure into a share of the company?
    print("\ncandidate denominators:")
    denom = {"EntityPublicFloat": 0, "shares outstanding": 0,
             "implied price (paired value+shares)": 0, "our own Form 4 price": 0}
    for cik, (ticker, form, day) in filers.items():
        facts = company_facts(cik)
        if facts_concept(facts, "dei", "EntityPublicFloat"):
            denom["EntityPublicFloat"] += 1
        if any(facts_concept(facts, t, g) for t, g in XBRL_CONCEPTS):
            denom["shares outstanding"] += 1
        has_v = any(facts_concept(facts, t, g) for t, g in BUYBACK_VALUE_CONCEPTS)
        has_s = any(facts_concept(facts, t, g) for t, g in BUYBACK_SHARE_CONCEPTS)
        if has_v and has_s:
            denom["implied price (paired value+shares)"] += 1
    with connect() as conn:
        known = {r[0] for r in conn.execute(
            "SELECT DISTINCT issuer_cik FROM insider_buys WHERE price IS NOT NULL")}
    denom["our own Form 4 price"] = sum(1 for cik in filers if cik in known)
    for name, hit in denom.items():
        print(f"  {name:38} {hit:>4}/{len(filers)} {hit / len(filers) * 100:>5.0f}%")

    print("\nsample of the most recent observation per filer:")
    for ticker, form, found in examples[:12]:
        tag, point = next(iter(found.items()))
        print(f"  {ticker:7} {form:5} {tag[:38]:38} "
              f"{point['val']:>18,.0f} {point['unit']:<5} "
              f"{point['start']}..{point['end']}")


def usd_scaled(value):
    """Compact dollars for figures spanning nano-caps to mega-caps."""
    if value is None:
        return ""
    for unit, size in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
        if value >= size:
            return f"${value / size:,.1f}{unit}"
    return f"${value:,.0f}"


def market_cap(shares_out, price):
    """Shares outstanding times the price on the filing.

    Not a live quote. The price is the one the insider paid on the transaction
    date, so this is the company's size as of that trade rather than today's.
    That is enough to tell a nano-cap from a mega-cap, which is the whole point
    of showing it next to a purchase.
    """
    shares_out = plausible_shares(shares_out)
    if not shares_out or not price:
        return None
    return shares_out * price


def trusted_price(detail):
    """The filing's price, or None when it cannot be believed.

    Reborn Coffee quotes $180,000 a share against a $1.60 stock. aggregate_buys
    withholds a total built on such a price, but events stored before that
    guard existed still carry one, and multiplying it by the share count put a
    $1.5T market cap on a nano-cap. The implied purchase value is the tell, and
    it is checked here rather than trusted from storage.
    """
    price, shares = detail.get("price"), detail.get("shares")
    if not price:
        return None
    if detail.get("value_suspect"):
        return None
    if shares and shares * price > MAX_PLAUSIBLE_VALUE_USD:
        return None
    return price


def cached_shares_outstanding(conn, cik):
    """Share count from the cache only, never fetching.

    shares_outstanding() refreshes a stale row, which is right during
    collection and wrong while rendering: writing the dashboard must not
    depend on the network or quietly take minutes.
    """
    if not cik:
        return None
    row = conn.execute(
        "SELECT shares_out FROM issuer_facts WHERE cik = ?", (cik,)
    ).fetchone()
    return plausible_shares(row["shares_out"]) if row else None


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


def log_run(conn, day, status, n_docs, n_events, started,
            n_candidates=0, n_skipped=0, n_refused=0):
    """Record what a day's pass did, in terms that survive being read later.

    n_docs counts NEWLY fetched documents, and the workflow rescans a three-day
    window twice a day, so the second pass over a day legitimately fetches
    nothing. Stored alone that is indistinguishable from a collector that found
    nothing at all -- both read `docs=0 events=0` against status `ok` -- and
    the two call for opposite responses. Every row in this table looked like a
    dead pipeline, and at least once was read as one.

    The denominators settle it. n_candidates is how many filings in the day's
    index were of a form we collect; n_skipped is how many of those were
    already in the documents table. docs=0 with candidates=1300 skipped=1300 is
    a day fully collected; docs=0 with candidates=0 is a day with nothing in it;
    docs=0 with candidates=1300 skipped=0 is the pipeline actually being dry.

    n_refused is the day's TOTAL refusals. run_day also keeps a consecutive
    count for the circuit breaker, but that one resets on every success, so a
    day quietly losing every third filing tripped nothing and recorded nothing.
    """
    conn.execute(
        """INSERT OR REPLACE INTO run_log
           (run_date, source, status, n_docs, n_events, started_at, finished_at,
            n_candidates, n_skipped, n_refused)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (day.isoformat(), "edgar_daily", status, n_docs, n_events, started,
         datetime.utcnow().isoformat(timespec="seconds"),
         n_candidates, n_skipped, n_refused),
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
    n_docs = n_events = refused = n_skipped = n_refused = 0
    seen = set()          # accessions this pass recorded
    status = "ok"
    # Unique accessions, not index rows. EDGAR lists a filing once per filer,
    # so a Form 4 appears under both the issuer and the reporting owner -- about
    # 2.04 rows per filing in practice. Counting rows made the day look twice as
    # big as it is, and made a fully-collected day read as though the collector
    # had stalled.
    n_candidates = len({
        r["accession"] for r in rows
        if r["form_type"] == "4" or r["form_type"] in MA_FORMS
        or r["form_type"] in PERIODIC_FORMS
    })

    # Watchlist CIKs, resolved once per day rather than per row. The scanner
    # is free -- the form type is already in hand -- but the disqualifier table
    # would cover the whole market if it were left ungated, and every
    # evaluate() scans it.
    watched_ciks = {
        r["cik"] for r in watchlist_rows(conn) if r["cik"] is not None
    }

    for row in rows:
        # Before the form-type filter below: NT 10-K, Form 25 and Form 15 are
        # not forms this collector wants, so gating this behind it would mean
        # the scanner never saw one.
        signal_state.scan_index_row_for_disqualifiers(conn, row, watched_ciks)

        if limit is not None and n_docs >= limit:
            print(f"  (stopped at --limit {limit}; index had {len(rows)} filings)")
            break
        listed = tickers.get(row["cik"])
        is_form4 = row["form_type"] == "4"
        is_ma = row["form_type"] in MA_FORMS
        is_periodic = row["form_type"] in PERIODIC_FORMS

        if not (is_form4 or is_ma or is_periodic):
            continue
        # Form 4 issuer CIK differs from the filer CIK, so we cannot use the
        # ticker map to pre-filter those -- resolve after parsing instead.
        if (is_ma or is_periodic) and not listed:
            continue
        if already_processed(conn, row["accession"]):
            # Only count it as skipped if an EARLIER pass collected it. EDGAR
            # lists a Form 4 under both the issuer and the reporting owner, so
            # the second row for an accession this pass just recorded would
            # otherwise register as "already collected" -- putting skips on a
            # day that was in fact freshly collected, which is exactly the
            # confusion this column exists to remove.
            if row["accession"] not in seen:
                n_skipped += 1
            continue

        if is_form4:
            emitted = handle_form4(conn, row, tickers)
            if emitted is None:
                # Refused. Leave the accession unrecorded so a later run
                # retries it, and watch for a run of them: once the SEC starts
                # saying no, asking three thousand more times is the wrong
                # thing to do. Tripping the breaker keeps what we already have.
                #
                # Two counters, because they answer different questions.
                # `refused` is CONSECUTIVE and resets below on any success, so
                # it only ever describes a stretch -- which meant a day losing
                # every third filing reported nothing at all, forever. n_refused
                # is the day's total and is what gets written down.
                refused += 1
                n_refused += 1
                if refused >= MAX_CONSECUTIVE_REFUSALS:
                    status = "partial"
                    print(f"  (stopped after {refused} consecutive refusals; "
                          f"keeping the {n_docs} documents already collected)")
                    break
                continue
            refused = 0
            n_events += emitted
        elif is_ma:
            n_events += handle_ma(conn, row, listed)
        else:
            n_events += handle_periodic(conn, row, listed)

        conn.execute(
            """INSERT OR IGNORE INTO documents
               (accession, cik, company, form_type, filed_date, path, fetched_at)
               VALUES (?,?,?,?,?,?,?)""",
            (row["accession"], row["cik"], row["company"], row["form_type"],
             day.isoformat(), row["path"],
             datetime.utcnow().isoformat(timespec="seconds")),
        )
        seen.add(row["accession"])
        n_docs += 1

    log_run(conn, day, status, n_docs, n_events, started,
            n_candidates=n_candidates, n_skipped=n_skipped,
            n_refused=n_refused)

    # n_skipped counts index ROWS, n_candidates counts unique accessions, and
    # EDGAR lists a Form 4 under both the issuer and the reporting owner. So a
    # fully-collected day skips more rows than it had candidates, and comparing
    # the two directly would read as a miscount. Nothing new and nothing
    # refused is what "already collected" actually looks like.
    if n_docs == 0:
        if n_candidates == 0:
            why = " — nothing of interest filed"
        elif n_skipped and not refused:
            why = f" — all {n_candidates} already collected on an earlier pass"
        else:
            why = " — NOTHING COLLECTED, and not because it was already done"
    else:
        why = ""

    print(f"{day}  {len(rows):,} filings in index, {n_candidates} of interest, "
          f"{n_docs} processed, {n_events} events{why}"
          + (f", {n_refused} refused" if n_refused else "")
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

    try:
        root = extract_ownership_xml(text)
        buys = parse_form4(root)
        # The mirror of the purchase path: code S, disposed D. Stored in its own
        # ledger so none of the tiering, clustering or scoring that reads
        # insider_buys starts quietly seeing sales.
        signal_state.record_insider_sales(
            conn, parse_form4(root, want_code="S", want_direction="D"))
    except MalformedFiling as exc:
        # Says nothing about the contents, so it takes the same lane as a
        # refusal: unrecorded, retried next run, and counted toward the breaker
        # -- which is what makes a schema change stop the run instead of
        # quietly emptying it one filing at a time. Printed here rather than
        # left to the breaker, because a single one of these is already a fact
        # worth seeing and the breaker only speaks at twenty.
        print(f"  MALFORMED {row['accession']} ({row['company']}): {exc}")
        return None
    if not buys:
        return 0

    # The ledger stays per-transaction; only the emitted event is aggregated.
    by_ticker = {}
    for buy in buys:
        # Fall back to the ticker map when the XML has no usable symbol.
        #
        # "No usable symbol" is not the same as "empty". Wilson Bank Holding
        # really does report its trading symbol as the string "none", and since
        # that is truthy the fallback never ran -- so the ledger, the events,
        # the state machine and the card all carried a company called NONE,
        # sitting on the dashboard above a $27M purchase at 8.47% of the
        # company. The placeholders were already enumerated for link rendering;
        # they just were not consulted this early.
        if not real_ticker(buy["ticker"]):
            buy["ticker"] = (tickers.get(buy["issuer_cik"]) or (None,))[0]
        if not real_ticker(buy["ticker"]):
            continue  # not a listed issuer, or no symbol we can name it by

        suspect = int((buy["value"] or 0) > MAX_PLAUSIBLE_TXN_USD)
        if suspect:
            note_degraded("transaction above the plausible ceiling")
            print(f"  SUSPECT {row['accession']} {buy['ticker']}: "
                  f"{buy['shares']:,.0f} sh @ ${buy['price']:,.2f} = "
                  f"{usd(buy['value'])} — held out of scoring for review")
        conn.execute(
            """INSERT OR IGNORE INTO insider_buys
               (accession, issuer_cik, ticker, issuer, owner, owner_title,
                txn_date, shares, price, value, suspect)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (row["accession"], buy["issuer_cik"], buy["ticker"], buy["issuer"],
             buy["owner"], buy["owner_title"], buy["txn_date"], buy["shares"],
             buy["price"], buy["value"], suspect),
        )
        if not suspect:
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


def refresh_stale_facts(conn, budget=STALE_REFRESH_BUDGET):
    """Re-derive cached figures left behind by a rules change.

    Versioning alone only helps issuers that happen to file again, which for a
    quarterly filer can be months. This walks the backlog directly so a
    corrected rule reaches the whole cache without waiting on the calendar.
    """
    stale = [
        row["cik"] for row in conn.execute(
            """SELECT cik FROM issuer_buybacks
               WHERE derived_v IS NULL OR derived_v <> ?
               ORDER BY cik LIMIT ?""",
            (XBRL_DERIVATION, budget),
        ).fetchall()
    ]
    for cik in stale:
        refresh_issuer_xbrl(conn, cik)
    return len(stale)


def buyback_headline(entity, detail):
    """The one place a buyback headline is written, so it cannot fork."""
    pct, basis = detail.get("pct_of_shares"), detail.get("pct_basis")
    days = detail.get("period_days")
    over = f" over {days} days" if days else ""
    if pct is not None:
        scale = f" — {pct:.1f}% of {basis}{over}"
    elif detail.get("value"):
        scale = f" — {usd(detail['value'])}{over}, share of company unknown"
    else:
        scale = ""
    return (f"{entity}: repurchased stock{scale}"
            + (f" — {detail['company']}" if detail.get("company") else ""))


def flag_suspect_transactions(conn):
    """Flag stored ledger rows above the per-transaction ceiling.

    The guard at the write site only sees new rows. Reborn Coffee's
    $23,649,660,000 was already in the ledger, and the state machine reads the
    ledger -- so the event stayed correctly suppressed while the page went on
    saying an insider bought twenty-three billion dollars of a coffee-shop
    microcap. Fifth time a rule has shipped without reaching stored data.
    """
    flagged = 0
    for table in ("insider_buys", "insider_sales"):
        cur = conn.execute(
            f"UPDATE {table} SET suspect = 1 "
            "WHERE COALESCE(suspect, 0) = 0 AND value > ?",
            (MAX_PLAUSIBLE_TXN_USD,),
        )
        flagged += cur.rowcount
    return flagged


def repair_placeholder_tickers(conn, tickers):
    """Re-resolve issuers stored under a placeholder symbol.

    Guarding handle_form4 stops new ones; this reaches what is already stored,
    which is the half that has been forgotten four times in this file.

    Two outcomes, because the placeholders turn out to mean two different
    things. A listed company that simply reports its symbol oddly resolves from
    the ticker map and is renamed everywhere -- Wilson Bank Holding files as
    "none" and is WBHC. Everything else with no symbol has none because it does
    not trade: interval funds, BDCs, private credit vehicles. Those are retired
    rather than renamed. Insiders buying into a fund nobody can buy is not a
    signal, and leaving it on the page as a company called NONE was worse.

    The ledger rows are kept in both cases. It is a record of what was filed,
    and deleting history to tidy a display is how you lose the ability to
    explain a number later.
    """
    renamed = retired = 0
    rows = conn.execute(
        "SELECT DISTINCT issuer_cik AS cik, ticker FROM insider_buys "
        "UNION SELECT cik, ticker FROM issuer_state"
    ).fetchall()

    for row in rows:
        cik, stored = row["cik"], row["ticker"]
        if not cik:
            continue
        preferred = real_ticker((tickers.get(int(cik)) or (None,))[0])
        # Two reasons to re-resolve: no usable symbol at all, or a symbol that
        # is a claim on the common when the common is also listed. A Bakkt 10-Q
        # buyback belongs under BKKT; it was filed under BKKT-WT because the
        # ticker map was collapsed by whichever row happened to come last.
        needs_name = not real_ticker(stored)
        outranked = bool(preferred and preferred != stored
                         and derivative_rank(stored) > derivative_rank(preferred))
        if not (needs_name or outranked):
            continue
        resolved = preferred

        if resolved:
            for sql in (
                "UPDATE insider_buys SET ticker = ? WHERE issuer_cik = ?",
                "UPDATE insider_sales SET ticker = ? WHERE issuer_cik = ?",
                "UPDATE OR IGNORE events SET entity = ? WHERE entity = ?",
                "UPDATE issuer_state SET ticker = ? WHERE cik = ?",
                "UPDATE state_transitions SET ticker = ? WHERE cik = ?",
                "UPDATE OR IGNORE watchlist SET ticker = ? WHERE ticker = ?",
            ):
                key = stored if "entity = ?" in sql or "watchlist" in sql else cik
                conn.execute(sql, (resolved, key))
            renamed += 1
            continue

        if outranked:
            continue   # a derivative with no common to move to: leave it be

        # Untradeable. Off the dashboard, out of the state machine, ledger kept.
        #
        # Counted by what actually changed, not by how many issuers were
        # examined. The ledger rows keep the placeholder on purpose, so these
        # issuers come back round on every run -- and a count that incremented
        # regardless would report retiring the same nine funds twice a day
        # forever, which is a made-up number in a status line.
        already = signal_state.is_retired(conn, cik)
        touched = 0
        touched += conn.execute(
            "UPDATE events SET reviewed_at = ? WHERE entity = ? AND reviewed_at IS NULL",
            (datetime.utcnow().isoformat(timespec="seconds"), stored),
        ).rowcount
        touched += conn.execute(
            "DELETE FROM watchlist WHERE ticker = ?", (stored,)).rowcount
        # Recorded, not just deleted. Deleting the state left the ledger rows
        # in place -- which is correct, they are the record -- and the next
        # classification pass read them straight back and rebuilt everything.
        signal_state.retire_issuer(conn, cik, f"no trading symbol ({stored})")
        if touched or not already:
            retired += 1

    return renamed, retired


def refresh_headlines(conn):
    """Rewrite stored headlines with the current formatters.

    A headline is written once, at collection, and then never again unless the
    event happens to be rescored -- so a change to how a figure READS never
    reaches the page. Fixing usd() so that $999,999.75 stops printing as
    "$1000K" and a $31 purchase stops printing as "$0K" changed nothing at all
    on the dashboard, because both strings were already baked into 561 stored
    rows. This makes the headline a derived value: rebuilt from the detail that
    is already stored, using whatever the formatters say today.

    Nothing is refetched and no score is touched -- only the wording.
    """
    changed = 0
    for row in conn.execute(
        "SELECT id, entity, event_type, headline, detail FROM events"
    ).fetchall():
        detail = json.loads(row["detail"] or "{}")
        if row["event_type"] == "insider_buy":
            peers = detail.get("cluster_peers") or []
            rebuilt = buy_headline(
                row["entity"], detail, peers,
                detail.get("bps_of_market_cap"),
                len(peers) >= CLUSTER_MIN_INSIDERS,
            )
        elif row["event_type"] == "buyback":
            rebuilt = buyback_headline(row["entity"], detail)
        else:
            continue  # deal filings quote no figures
        if rebuilt != row["headline"]:
            conn.execute("UPDATE events SET headline = ? WHERE id = ?",
                         (rebuilt, row["id"]))
            changed += 1
    return changed


def rescore_buybacks(conn):
    """Recompute stored buyback events from the current cache.

    emit() is idempotent, so an event keeps whatever it was written with --
    AdvanSix stayed at 23.7% of itself, off an eight-year cumulative period,
    after the rule that rejects such periods had already shipped. Repairing the
    cache is not enough; what was published has to be recomputed too.
    """
    # Events for pooled vehicles are dropped rather than rescored: there is no
    # figure that makes a fund redemption a buyback. The document stays recorded
    # as processed, so nothing re-emits them, and the collector now declines
    # them before it spends a request. Doing this here rather than leaving it to
    # "the next run will be clean" is the whole lesson of the last four rule
    # changes -- a guard that never reaches what is already stored isn't shipped.
    dropped = 0
    for row in conn.execute(
        "SELECT id, detail FROM events WHERE event_type = 'buyback'"
    ).fetchall():
        if is_fund_vehicle(json.loads(row["detail"] or "{}").get("company")):
            conn.execute("DELETE FROM events WHERE id = ?", (row["id"],))
            dropped += 1

    fixed = 0
    for row in conn.execute(
        "SELECT id, entity, detail FROM events WHERE event_type = 'buyback'"
    ).fetchall():
        detail = json.loads(row["detail"] or "{}")
        cik = detail.get("cik")
        if not cik:
            continue
        activity = buyback_activity(conn, cik)
        pct, basis = buyback_measure(
            activity, shares_outstanding(conn, cik), public_float(conn, cik)
        )
        # period_days joins the skip test because the figure is no longer
        # annualised, so the period is what tells a reader whether 30% is a
        # year of buying or six months of it. A row whose percentage happens to
        # be unchanged still needs it merged in the first time.
        if (pct == detail.get("pct_of_shares")
                and basis == detail.get("pct_basis")
                and detail.get("period_days") == (activity or {}).get("period_days")):
            continue

        band = buyback_band(pct)
        tier = 1 if (pct is not None and pct >= TIER1_BUYBACK_PCT) else 2
        merged = {**detail, **(activity or {}), "pct_of_shares": pct,
                  "pct_basis": basis, "significance": band}
        conn.execute(
            "UPDATE events SET tier = ?, headline = ?, detail = ? WHERE id = ?",
            (tier, buyback_headline(row["entity"], merged),
             json.dumps(merged, default=str), row["id"]),
        )
        fixed += 1
    return fixed, dropped


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
        #
        # So is an implausible dollar total. aggregate_buys() drops a value that
        # a bad reported price inflated, but the bps it produces can still land
        # inside the ceiling and hide the event from this skip: REBN sat on the
        # dashboard as a $23.6bn "major" purchase at a coffee-shop microcap,
        # because the Form 4 reported $180,000 a share and the resulting 160 bps
        # looked perfectly ordinary.
        suspect = (detail.get("value") or 0) > MAX_PLAUSIBLE_VALUE_USD
        if stored is not None and stored <= MAX_PLAUSIBLE_BPS and not suspect:
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
  --field: #FFFFFF; --sunk: #E2E7EA;
  /* The significance ramp: one hue, five steps, light to dark, so the rungs
     read in order without reading the words. Stepped in OKLCH at the signal
     hue and checked against the card surface -- monotone lightness, >= 0.06
     between steps, and the palest step clears 2:1 on white so "negligible"
     is still a mark and not a smudge. Dark mode re-steps the same hue for
     the dark surface rather than inverting these. */
  --r1: #91AEF0; --r2: #6F94EE; --r3: #4E79EC; --r4: #305AE1; --r5: #1736D0;
  --on-r1: #14202B; --on-r2: #14202B; --on-r3: #FFFFFF;
  --on-r4: #FFFFFF; --on-r5: #FFFFFF;
}
@media (prefers-color-scheme: dark) {
  :root {
    --ink: #E1E9EF; --paper: #080E13; --card: #151C21;
    --rule: #2B343B; --signal: #7EA2FF; --muted: #8E9AA4;
    --field: #101820; --sunk: #1E272E;
    --r1: #3561E8; --r2: #4F7BF2; --r3: #6C95F9; --r4: #8AADFF; --r5: #A7C5FF;
    --on-r1: #FFFFFF; --on-r2: #08101C; --on-r3: #08101C;
    --on-r4: #08101C; --on-r5: #08101C;
  }
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
.row {
  display: grid; grid-template-columns: 88px 1fr; gap: 16px;
  background: var(--card); border-left: 3px solid var(--rule);
  padding: 14px 16px; margin-bottom: 8px;
}
.row[data-tier="1"] { border-left-color: var(--signal); }
.ticker {
  font-family: "IBM Plex Mono", monospace; font-size: 19px; font-weight: 600;
  letter-spacing: -.02em; word-break: break-all;
}
.row[data-tier="1"] .ticker { color: var(--signal); }
/* The tier tag repeats on the card what the section heading says, so the fact
   survives a sort that dissolves the sections. */
.row[data-tier="1"] .tag { border-color: var(--signal); color: var(--signal); }
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
/* The significance scale. The word names the rung and the ramp step shades it,
   so the ranking never depends on colour perception alone, and the exact
   figure is printed underneath so nothing has to be read off a colour.
   (A five-segment meter sat here briefly. At 88px wide it drew as a dashed
   rule -- noise, and a third encoding of a rung the badge already carries in
   both a word and a shade.) */
.scale { margin-top: 6px; display: flex; flex-direction: column; gap: 3px; }
.band {
  font-family: "IBM Plex Mono", monospace; font-size: 9.5px; font-weight: 600;
  letter-spacing: .08em; text-transform: uppercase; text-align: center;
  padding: 2px 4px; border: 1px solid var(--rule); color: var(--muted);
  border-radius: 2px;
}
/* .b-unscored deliberately adds nothing: the base style above -- grey hairline,
   muted text, no fill -- IS the off-the-ramp look. "We could not measure this"
   is a different statement from "this is small", and the page must not let the
   two resemble each other. */
.b-negligible { border-color: var(--r1); color: var(--ink); }
.b-minor      { border-color: var(--r2); background: var(--r2); color: var(--on-r2); }
.b-notable    { border-color: var(--r3); background: var(--r3); color: var(--on-r3); }
.b-significant{ border-color: var(--r4); background: var(--r4); color: var(--on-r4); }
.b-major      { border-color: var(--r5); background: var(--r5); color: var(--on-r5); }
.bps {
  font-family: "IBM Plex Mono", monospace; font-size: 9.5px;
  color: var(--muted); text-align: center; letter-spacing: -.01em;
}
/* ---- controls: one row above everything they scope ---- */
.controls {
  display: flex; flex-wrap: wrap; gap: 8px 10px; align-items: center;
  margin: 0 0 18px;
}
.controls label { font-size: 12px; color: var(--muted); }
#q, #sort {
  font-family: inherit; font-size: 13px; color: var(--ink);
  background: var(--field); border: 1px solid var(--rule);
  border-radius: 3px; padding: 7px 10px;
}
#q { flex: 1 1 220px; min-width: 0; }
#q::placeholder { color: var(--muted); }
#q:focus-visible, #sort:focus-visible, .chipbtn:focus-visible,
.dist-row:focus-visible {
  outline: 2px solid var(--signal); outline-offset: 2px;
}
.chipset { display: flex; gap: 6px; flex-wrap: wrap; }
.chipbtn {
  font-family: "IBM Plex Mono", monospace; font-size: 11px; font-weight: 600;
  letter-spacing: .04em; cursor: pointer;
  background: var(--field); color: var(--muted);
  border: 1px solid var(--rule); border-radius: 3px; padding: 6px 9px;
}
.chipbtn small { font-weight: 400; opacity: .75; margin-left: 5px; }
.chipbtn[aria-pressed="true"] {
  background: var(--signal); border-color: var(--signal); color: var(--card);
}
/* ---- significance distribution: the overview, and the band filter ---- */
.dist {
  background: var(--card); border: 1px solid var(--rule); border-radius: 3px;
  padding: 8px 14px; margin: 0 0 18px;
}
.dist-row {
  cursor: pointer; background: none; border: 0; font: inherit; color: inherit;
  text-align: left; width: 100%; border-radius: 2px;
  /* The hit target is the whole row, not the 9px bar -- padding included, it
     clears the 24px minimum at every count. */
  display: grid; grid-template-columns: 84px 1fr 44px; gap: 10px;
  align-items: center; padding: 6px 4px;
}
.dist-label {
  font-family: "IBM Plex Mono", monospace; font-size: 10.5px; font-weight: 600;
  letter-spacing: .06em; text-transform: uppercase; color: var(--muted);
}
.dist-row[aria-pressed="true"] .dist-label { color: var(--ink); }
.dist-row[aria-pressed="true"] .dist-label::before { content: "\\2713\\00a0"; }
.dist-track { display: block; }
.dist-bar {
  display: block; height: 9px; border-radius: 0 2px 2px 0; min-width: 2px;
  background: var(--r3); transition: width .18s ease-out;
}
.dist-n {
  font-family: "IBM Plex Mono", monospace; font-size: 11px;
  color: var(--muted); text-align: right; font-variant-numeric: tabular-nums;
}
.dist-row:hover .dist-bar { filter: brightness(1.08); }
.dist-row[data-band="major"]       .dist-bar { background: var(--r5); }
.dist-row[data-band="significant"] .dist-bar { background: var(--r4); }
.dist-row[data-band="notable"]     .dist-bar { background: var(--r3); }
.dist-row[data-band="minor"]       .dist-bar { background: var(--r2); }
.dist-row[data-band="negligible"]  .dist-bar { background: var(--r1); }
.dist-row[data-band="unscored"]    .dist-bar { background: var(--rule); }
#tip {
  position: fixed; z-index: 9; pointer-events: none; opacity: 0;
  background: var(--ink); color: var(--paper); border-radius: 3px;
  padding: 6px 9px; font-size: 12px; line-height: 1.35;
  transition: opacity .12s; max-width: 240px;
}
#tip b { font-size: 13px; }
/* ---- KPI row ---- */
.kpis { display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 18px; }
.kpi {
  background: var(--card); border: 1px solid var(--rule); border-radius: 3px;
  padding: 10px 14px; flex: 1 1 120px;
}
.kpi b { display: block; font-size: 26px; font-weight: 600; letter-spacing: -.02em; }
.kpi.lead b { font-size: 38px; color: var(--signal); line-height: 1.05; }
.kpi span {
  font-family: "IBM Plex Mono", monospace; font-size: 10px;
  letter-spacing: .1em; text-transform: uppercase; color: var(--muted);
}
.tag {
  font-family: "IBM Plex Mono", monospace; font-size: 9.5px; font-weight: 600;
  letter-spacing: .08em; text-transform: uppercase; color: var(--muted);
  border: 1px solid var(--rule); border-radius: 2px; padding: 1px 5px;
  margin-left: 8px; vertical-align: 1px; white-space: nowrap;
}
/* The move itself. Specificity is deliberate: .row[data-tier="1"] .tag also
   sets a colour, and at equal weight it won -- painting the badge's text the
   same blue as its own background. */
.row .tag.state-confirmed    { border-color: var(--r3); color: var(--r3); }
.row .tag.state-extended     { border-color: var(--r5); background: var(--r5);
                               color: var(--on-r5); }
.row .tag.state-distributing { border-color: var(--muted); color: var(--muted); }
.row .tag.state-distressed   { border-color: #D03B3B; background: #D03B3B;
                               color: #FFFFFF; font-weight: 700; }
.row .tag.state-setup        { border-color: var(--r1); color: var(--ink); }
.row .tag[class*="state-"]   { margin-left: 0; margin-right: 8px; }
.because { color: var(--muted); }
.none { display: none !important; }
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
  /* Stacked, the 88px column becomes the full width and the badge with it --
     a saturated bar across the card, shouting far louder than the rung it
     names. Laid on its side it stays a badge. */
  .scale { flex-direction: row; align-items: baseline; gap: 8px; margin-top: 0; }
  .band { padding: 2px 8px; }
  .bps { text-align: left; }
  .kpi.lead { flex-basis: 100%; }
}
@media (prefers-reduced-motion: no-preference) {
  /* Entry only. Sorting reorders by re-inserting every node, which restarts a
     CSS animation -- so with the animation left on, changing the sort made all
     500-odd cards fade in at once and the whole page flashed. The script adds
     .settled once the first paint is done. */
  #list:not(.settled) .row { animation: rise .3s ease-out backwards; }
  @keyframes rise { from { opacity: 0; transform: translateY(4px); } }
}
"""


"""Filtering and sorting, applied over the cards already on the page.

Progressive enhancement on purpose. Every card, count and bar is rendered by
Python and correct on its own; this script only narrows and reorders what is
already there. With scripting off the controls stay hidden and the page is
exactly the dashboard it was before -- there is no second copy of the data and
no template that can disagree with the server's.

Reordering 400-odd nodes with appendChild is well under a frame, so there is no
virtualisation here and no need for any.
"""
SCRIPT = """
(function () {
  var list = document.getElementById('list');
  if (!list) return;
  var cards = [].slice.call(list.querySelectorAll('.row'));
  if (!cards.length) return;

  var origin = [].slice.call(list.children);   // server order, headings included
  var heads = [].slice.call(list.querySelectorAll('.tier-head'));
  // The transition list has no per-tier headings -- the order is recency, not
  // conviction -- so these are absent and the sort branch below must cope.
  var allHead = document.querySelector('[data-head="all"]');
  var allCount = document.getElementById('allcount');
  var nohits = document.getElementById('nohits');
  var dist = document.getElementById('dist');
  var distRows = [].slice.call(dist.querySelectorAll('.dist-row'));
  var famBtns = [].slice.call(document.querySelectorAll('.chipbtn[data-fam]'));
  var t1 = document.getElementById('t1');
  var q = document.getElementById('q');
  var sort = document.getElementById('sort');
  var tip = document.getElementById('tip');
  var controls = document.getElementById('controls');
  controls.hidden = false;

  var fams = new Set(), bands = new Set(), tierOnly = false, needle = '';

  function num(card, key) { return parseFloat(card.getAttribute(key)); }

  // Everything except the band filter. The distribution has to count what the
  // OTHER filters leave standing, or narrowing by band would collapse every bar
  // but the one you picked and the overview would stop being an overview.
  function passesBase(card) {
    if (tierOnly && card.getAttribute('data-tier') !== '1') return false;
    if (fams.size) {
      var has = card.getAttribute('data-fam').split(' ');
      if (!has.some(function (f) { return fams.has(f); })) return false;
    }
    if (needle && card.getAttribute('data-find').indexOf(needle) < 0) return false;
    return true;
  }

  var SORTS = {
    conviction: null,
    mag:    function (a, b) { return num(b, 'data-mag') - num(a, 'data-mag'); },
    usd:    function (a, b) { return num(b, 'data-usd') - num(a, 'data-usd'); },
    filed:  function (a, b) {
      return a.getAttribute('data-filed') < b.getAttribute('data-filed') ? 1 : -1;
    },
    ticker: function (a, b) {
      return a.getAttribute('data-find').localeCompare(b.getAttribute('data-find'));
    }
  };

  function apply() {
    var counts = Object.create(null), widest = 0, shown = 0;

    cards.forEach(function (card) {
      var base = passesBase(card);
      var band = card.getAttribute('data-band');
      if (base) {
        counts[band] = (counts[band] || 0) + 1;
        if (counts[band] > widest) widest = counts[band];
      }
      var visible = base && (!bands.size || bands.has(band));
      card.classList.toggle('none', !visible);
      if (visible) shown++;
    });

    distRows.forEach(function (row) {
      var n = counts[row.getAttribute('data-band')] || 0;
      row.querySelector('.dist-bar').style.width =
        (widest ? n / widest * 100 : 0) + '%';
      row.querySelector('.dist-n').textContent = n.toLocaleString();
    });

    var cmp = SORTS[sort.value];
    if (cmp) {
      heads.forEach(function (h) { h.classList.add('none'); });
      if (allHead) { allHead.classList.toggle('none', shown === 0); }
      if (allCount) { allCount.textContent = shown.toLocaleString(); }
      cards.slice().sort(cmp).forEach(function (card) { list.appendChild(card); });
    } else {
      if (allHead) { allHead.classList.add('none'); }
      // Back to the order Python wrote, headings and all.
      origin.forEach(function (node) { list.appendChild(node); });
      heads.forEach(function (head) {
        var tier = head.getAttribute('data-head');
        var n = cards.filter(function (card) {
          return !card.classList.contains('none') &&
                 card.getAttribute('data-tier') === tier;
        }).length;
        head.classList.toggle('none', n === 0);
        head.querySelector('span').textContent = n.toLocaleString();
      });
    }
    nohits.classList.toggle('none', shown > 0);
  }

  function toggle(button, set, key) {
    var value = button.getAttribute(key);
    if (set.has(value)) { set.delete(value); } else { set.add(value); }
    button.setAttribute('aria-pressed', set.has(value) ? 'true' : 'false');
    apply();
  }

  famBtns.forEach(function (b) {
    b.addEventListener('click', function () { toggle(b, fams, 'data-fam'); });
  });
  distRows.forEach(function (b) {
    b.addEventListener('click', function () { toggle(b, bands, 'data-band'); });
  });
  t1.addEventListener('click', function () {
    tierOnly = !tierOnly;
    t1.setAttribute('aria-pressed', tierOnly ? 'true' : 'false');
    apply();
  });
  sort.addEventListener('change', apply);
  q.addEventListener('input', function () {
    needle = q.value.trim().toLowerCase();
    apply();
  });

  // The bars carry their counts as text already, so the tooltip is the share of
  // the list -- something the page does not otherwise say. Keyboard focus shows
  // the same readout as the pointer.
  function showTip(row) {
    var band = row.getAttribute('data-band');
    var n = parseInt(row.querySelector('.dist-n').textContent.replace(/,/g, ''), 10);
    var total = cards.filter(passesBase).length;
    tip.textContent = '';
    var strong = document.createElement('b');
    strong.textContent = n.toLocaleString() + (n === 1 ? ' company' : ' companies');
    tip.appendChild(strong);
    tip.appendChild(document.createTextNode(
      ' \\u00b7 ' + band + (total ? ' \\u00b7 ' + Math.round(n / total * 100) + '% of the list' : '')
    ));
    var box = row.getBoundingClientRect();
    tip.style.opacity = '1';
    tip.style.left = Math.min(box.left + 90, window.innerWidth - tip.offsetWidth - 8) + 'px';
    tip.style.top = Math.max(8, box.top - tip.offsetHeight - 6) + 'px';
  }
  function hideTip() { tip.style.opacity = '0'; }
  distRows.forEach(function (row) {
    row.addEventListener('pointerenter', function () { showTip(row); });
    row.addEventListener('focus', function () { showTip(row); });
    row.addEventListener('pointerleave', hideTip);
    row.addEventListener('blur', hideTip);
  });

  apply();
  // First paint is over; stop the entry animation so re-sorting (which
  // re-inserts every node) does not replay it across the whole list.
  requestAnimationFrame(function () { list.classList.add('settled'); });
})();
"""


YAHOO_QUOTE = "https://finance.yahoo.com/quote/{}"

# Placeholders EDGAR emits when an issuer has no traded symbol. WILSON BANK
# HOLDING CO really does report its trading symbol as the string "none", and
# linking that lands on an empty quote page, so these render as plain text.
NON_TICKERS = {"", "-", "—", "none", "n/a", "na", "null"}


# Symbol shapes that are a claim on the common rather than the common itself.
# Applied ONLY to choose between symbols that share a CIK, never to reject one:
# if a company's sole listed symbol is a warrant then a warrant is what it
# trades as, and calling it something else would be an invention. Used as a
# tiebreak the heuristic cannot do damage, which is what makes the loose
# trailing-letter rules below safe to have.
_DERIVATIVE_SUFFIX = (
    (re.compile(r"[-.](WT|WS|RT|U)$", re.I), 3),        # explicit: BKKT-WT
    (re.compile(r"[-.]P[A-Z]$", re.I), 3),              # preferred: SCHW-PJ
    (re.compile(r"^[A-Z]{4}[WUR]$"), 2),                # Nasdaq 5-letter warrant/unit/right
    (re.compile(r"^[A-Z]{4}[LMNOP]$"), 1),              # Nasdaq 5-letter preferred/notes
)


def derivative_rank(symbol):
    """0 for common stock, higher the further from it. Lower sorts first.

    Deliberately ordinal rather than boolean: given BKKT, BKKT-WT and nothing
    else, the plain symbol wins on 0; given only BKKT-WT it still wins its own
    comparison and is used, because it is what that CIK trades as.
    """
    text = (symbol or "").strip().upper()
    for pattern, rank in _DERIVATIVE_SUFFIX:
        if pattern.search(text):
            return rank
    return 0


def real_ticker(symbol):
    """The symbol, or None when it is a placeholder standing in for one.

    EDGAR carries these verbatim from the filing, so they are data, not gaps --
    which is why `if not ticker` never caught them.
    """
    text = (symbol or "").strip()
    return text if text.lower() not in NON_TICKERS else None


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
    """One card per ticker, across every tier.

    A company that files three times in a week is one story told three times,
    not three stories -- and grouping inside a tier only got that half right.
    A ticker with both a Tier 1 and a Tier 2 filing drew a card in each
    section, so it appeared twice on the page: 22 of them on the live
    dashboard, which is the duplication the rollup exists to remove.

    Each company is placed once, at the best tier any of its filings earned,
    with the rest nested underneath. The leading event is the best-tier one
    and, within that, the loudest -- the filing that put the company on the
    page is the one that should headline its card. Callers split the returned
    list on the leading event's tier.
    """
    def rank(event):
        # Ascending tier, descending conviction, in one sort key.
        return (event["tier"], *(-part for part in conviction(event)))

    groups = {}
    for event in events:
        groups.setdefault(event["entity"], []).append(event)

    ranked = [
        (entity, sorted(evs, key=rank)) for entity, evs in groups.items()
    ]
    ranked.sort(key=lambda group: rank(group[1][0]))
    return ranked


BAND_RUNGS = ("negligible", "minor", "notable", "significant", "major")

# The three families the collector emits, as the page names them. Grouped so a
# reader filtering for "M&A" gets every deal document without knowing that a
# 425 and an SC TO-T are different forms.
FAMILIES = (
    ("insider", "Insider buys", lambda t: t == "insider_buy"),
    ("buyback", "Buybacks", lambda t: t == "buyback"),
    ("ma", "M&A", lambda t: t.startswith("ma_")),
)

# Families for cards that exist because an issuer's STATE moved rather than
# because it filed something. Keyed off the destination state rather than off
# the reason text: the reason is prose written for a reader and parsing it back
# out to decide a filter would break the first time it was reworded.
EVIDENCE_FREE_FAMILIES = (
    ("setup", "Setups"),
    ("state", "Other state moves"),
)


def band_rung(band):
    """Position on the significance ladder, 1-5.

    0 means the issuer tags no share count to measure against, which is a
    different statement from "small" and is kept separate everywhere: unscored
    filings never mix into the ramp, they sit at the end of it.
    """
    return BAND_RUNGS.index(band) + 1 if band in BAND_RUNGS else 0


def family_of(event_type):
    return next((key for key, _, test in FAMILIES if test(event_type)), "other")


def share_of_company(event_type, detail):
    """Magnitude in basis points of the company, whatever the filing is.

    An insider buy is already measured that way. A buyback is measured in whole
    percent, so it multiplies up by 100. They are not the same *kind* of
    statement -- one is a single purchase, the other a year of repurchases --
    but both answer "how much of this company", which is the axis the page
    sorts on, and putting them in one unit is what makes the sort mean anything.
    """
    if event_type == "insider_buy":
        return detail.get("bps_of_market_cap")
    if event_type == "buyback" and detail.get("pct_of_shares") is not None:
        return detail["pct_of_shares"] * 100
    return None


def card_facets(entity, events, order):
    """The card's sortable and filterable facts, as data-* attributes.

    Written onto the card itself rather than shipped alongside as a JSON blob,
    so the page holds one copy of every figure and the controls cannot come to
    disagree with what is drawn under them.
    """
    primary = events[0]
    detail = json.loads(primary["detail"] or "{}")
    band = detail.get("significance") or "unscored"

    # Filtering by kind looks at every filing on the card -- a company shown for
    # its insider buying should still appear under "Buybacks" when it also
    # bought back stock. Rank, band and magnitude come from the leading filing
    # alone, because that is the filing the card is placed by.
    families = " ".join(
        dict.fromkeys(family_of(e["event_type"]) for e in events)
    )

    haystack = {entity.lower()}
    for event in events:
        d = json.loads(event["detail"] or "{}")
        for key in ("issuer", "company", "owner"):
            if d.get(key):
                haystack.add(str(d[key]).lower())
        for name in (d.get("cluster_peers") or []) + (d.get("co_owners") or []):
            haystack.add(str(name).lower())

    return {
        "data-ord": str(order),
        "data-fam": families,
        "data-tier": str(primary["tier"]),
        "data-band": band,
        "data-rung": str(band_rung(detail.get("significance"))),
        "data-mag": f"{share_of_company(primary['event_type'], detail) or -1:.4f}",
        "data-usd": f"{detail.get('value') or -1:.2f}",
        "data-filed": primary["filed_date"] or "",
        "data-find": " ".join(sorted(haystack)),
    }


def _strip_ticker(headline, entity):
    """The ticker is already the card's left column; drop the prefix the
    headline carries for the CLI listing."""
    prefix = f"{entity}: "
    return headline[len(prefix):] if headline.startswith(prefix) else headline


def render_company(entity, events, conn=None, order=0):
    primary = events[0]
    detail = json.loads(primary["detail"] or "{}")
    bits = [f"filed {html.escape(primary['filed_date'] or '')}"]

    if primary["event_type"] == "insider_buy":
        if detail.get("shares") and detail.get("price"):
            bits.append(f"{int(detail['shares']):,} sh @ ${detail['price']:,.2f}")
        if conn is not None:
            cap = market_cap(
                cached_shares_outstanding(conn, detail.get("issuer_cik")),
                trusted_price(detail),
            )
            if cap:
                bits.append(f"mkt cap {usd_scaled(cap)}")
        # The second denominator: what this did to the buyer's own stake.
        if detail.get("new_position"):
            bits.append("new position")
        elif detail.get("pct_position"):
            bits.append(f"+{detail['pct_position']:,.0f}% to position")
        first, last = detail.get("first_txn_date"), detail.get("txn_date")
        if last:
            span = f"{first} → {last}" if first and first != last else last
            lag = _span_days(first or last, primary["filed_date"])
            late = (f" — reported {lag:,} days late"
                    if lag and lag > MAX_REPORTING_LAG_DAYS else "")
            bits.append(f"traded {html.escape(span)}{late}")
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
    if not band:
        # An empty column reads as an unfinished card. Say the thing instead:
        # this issuer tags no share count, so there is nothing to measure the
        # filing against -- which is why it ranks where it does.
        scale = (
            '<div class="scale"><span class="band b-unscored" '
            'title="the issuer reports no share count to measure this against">'
            "unscored</span></div>"
        )
    else:
        # The column is 88px wide, so the figure has to say which denominator
        # it used without spelling out "of shares outstanding" over two lines.
        # The headline beside it carries the full phrasing.
        if primary["event_type"] == "buyback":
            basis = "float" if "float" in (detail.get("pct_basis") or "") else "shares"
            figure = f"{detail['pct_of_shares']:.1f}% of {basis}"
        else:
            figure = format_bps(detail.get("bps_of_market_cap")).replace(
                " of company", ""
            )
        scale = (
            f'<div class="scale">'
            f'<span class="band b-{band}">{html.escape(band)}</span>'
            f'<span class="bps">{html.escape(figure)}</span></div>'
        )

    attrs = " ".join(
        f'{k}="{html.escape(v)}"' for k, v in card_facets(entity, events, order).items()
    )
    tier_tag = '<span class="tag">tier 1</span>' if primary["tier"] == 1 else ""

    return f"""<div class="row" {attrs}>
  <div class="ticker">{ticker_link(entity)}
    {scale}
  </div>
  <div>
    <p class="headline">{html.escape(_strip_ticker(primary['headline'] or '', entity))}{tier_tag}</p>
    <div class="detail">{"".join(f"<span>{b}</span>" for b in bits)}</div>
    {chips}
    {more}
  </div>
</div>"""


def render_controls(grouped, evidence_free=None):
    """The filter row, the significance distribution, and the KPI tiles.

    `evidence_free` counts the cards drawn from a transition with no open
    filing behind it, keyed by the family they carry. Lane A is the reason it
    exists: a contract-liability setup is a statement about eight quarters of
    balance sheet, so there is no filing to group it under, and a chipset built
    only from filing families left those cards uncounted and unselectable --
    present until any chip was pressed, then gone, with nothing to press to
    bring them back.

    All three are one thing: a way through 400-odd cards. The distribution is
    both the overview and the band filter, which is why it is a chart rather
    than another row of chips -- where the mass sits is the first useful fact
    about a day's collection, and the answer is also the control.

    Every count here is rendered server-side and correct without JavaScript.
    The script only narrows them.
    """
    cards = [
        (entity, evs, json.loads(evs[0]["detail"] or "{}")) for entity, evs in grouped
    ]

    evidence_free = evidence_free or {}
    chips = "".join(
        f'<button class="chipbtn" type="button" data-fam="{key}" aria-pressed="false">'
        f"{label}<small>{sum(1 for _, evs, _ in cards if any(test(e['event_type']) for e in evs)):,}</small>"
        f"</button>"
        for key, label, test in FAMILIES
    )
    chips += "".join(
        f'<button class="chipbtn" type="button" data-fam="{key}" aria-pressed="false">'
        f"{label}<small>{evidence_free[key]:,}</small></button>"
        for key, label in EVIDENCE_FREE_FAMILIES if evidence_free.get(key)
    )

    counts = collections.Counter(d.get("significance") or "unscored" for _, _, d in cards)
    # Those cards are drawn unscored, so the band they can be filtered by has
    # to count them or the row says one number and shows another.
    counts["unscored"] += sum(evidence_free.values())
    # `or 1` because the line above can put a zero-valued key into an otherwise
    # empty Counter, and max() then returns that 0 rather than the default --
    # which divides by zero on a page with nothing on it.
    widest = max(counts.values(), default=1) or 1
    rows = "".join(
        f'<button class="dist-row" type="button" data-band="{band}" aria-pressed="false">'
        f'<span class="dist-label">{band}</span>'
        f'<span class="dist-track"><span class="dist-bar" '
        f'style="width:{counts.get(band, 0) / widest * 100:.1f}%"></span></span>'
        f'<span class="dist-n">{counts.get(band, 0):,}</span></button>'
        for band in tuple(reversed(BAND_RUNGS)) + ("unscored",)
    )

    return f"""<div class="controls" id="controls" hidden>
  <input id="q" type="search" placeholder="Ticker, company or person"
         aria-label="Filter by ticker, company or person" autocomplete="off">
  <div class="chipset">{chips}
    <button class="chipbtn" type="button" id="t1" aria-pressed="false">Tier 1 only</button>
  </div>
  <label for="sort">Sort</label>
  <select id="sort">
    <option value="conviction">Conviction</option>
    <option value="mag">Share of company</option>
    <option value="usd">Dollar size</option>
    <option value="filed">Most recent</option>
    <option value="ticker">Ticker A–Z</option>
  </select>
</div>
<div class="dist" id="dist" role="group" aria-label="Filter by significance">{rows}</div>"""


def render_state_panel(counts, n_moves, window_days, truncated=False,
                       n_decayed=0):
    """Where every issuer stands, beside how many moved. Both are needed.

    The counts are standing state and the list below is change, so a run where
    nothing moved shows a full panel over an empty list -- which is the correct
    reading of a quiet week, and is exactly what a page built only from
    standing state could never say.
    """
    tiles = "".join(
        f'<div class="kpi{" lead" if name == signal_state.EXTENDED else ""}">'
        f"<b>{counts.get(name, 0):,}</b>"
        f'<span>{html.escape(name.lower())}</span></div>'
        for name in signal_state.STATES
        if counts.get(name, 0) or name in (signal_state.CONFIRMED,
                                           signal_state.EXTENDED,
                                           signal_state.DISTRESSED)
    )
    return (
        f'<div class="kpis">{tiles}</div>'
        f'<div class="tier-head"><b>Moved in the last {window_days} days</b>'
        f"<span>{n_moves}{' (showing the most recent)' if truncated else ''}"
        f"{f' · {n_decayed} decayed, not shown' if n_decayed else ''}"
        f"</span></div>"
    )


def write_html(conn, path="dashboard.html", window_days=14, cap=None):
    """The page is built from TRANSITIONS, not from standing state.

    An issuer that has been CONFIRMED for three weeks is not news on day
    twenty-two, and the previous version -- a list of every open event, rebuilt
    twice a day -- said it was. The same names reappeared every twelve hours
    whether or not anything had happened to them, which is the fastest way to
    train someone to stop reading a dashboard.

    Each transition is still drawn with the filing detail behind it, because
    "GBFH moved to EXTENDED" is a fact about the state machine and "two
    directors bought $400K" is the fact a person acts on. The card body is the
    one render_company() already produces; the state is what earns it a place.
    """
    events = conn.execute(
        """SELECT * FROM events WHERE reviewed_at IS NULL
           ORDER BY tier, filed_date DESC, id DESC"""
    ).fetchall()
    runs = conn.execute(
        "SELECT * FROM run_log ORDER BY run_date DESC LIMIT 5"
    ).fetchall()

    since = market_today() - timedelta(days=window_days)
    # The cap is asked for explicitly and one over, so the page can tell the
    # reader it was truncated rather than just ending. A first run transitions
    # every issuer at once -- 544 of them here -- and silently drawing the
    # first 200 of those would be the same silent-shortfall bug in a new place.
    cap = cap or TRANSITION_CAP
    moves = signal_state.transitions_since(conn, since=since, limit=cap + 1)
    truncated = len(moves) > cap
    moves = moves[:cap]
    counts = signal_state.state_counts(conn)

    # Filings for the issuers that moved, so a transition card can show what
    # was actually filed. Grouped by ticker the same way as before.
    by_entity = dict(group_by_company(events))

    # One card per ISSUER, not per hop. An issuer can move twice in a day --
    # DORMANT to CONFIRMED on a purchase, then CONFIRMED to EXTENDED when a
    # second insider joins -- and both rows are correct history. Drawn as two
    # cards they read as the page contradicting itself, which is how OVBC came
    # to show "confirmed → extended" directly above "dormant → confirmed".
    #
    # Keyed on CIK rather than ticker, because a placeholder symbol is shared
    # by every issuer that has one: eight distinct companies were all called
    # NONE, and collapsing on the ticker merged them into a single card while
    # collapsing on nothing drew eleven.
    #
    # The pair kept spans the whole day: the earliest from_state and the latest
    # to_state, so a double hop reads as the net move it was.
    net = {}
    for move in moves:                       # newest first, from the query
        seen = net.get(move["cik"])
        if seen is None:
            net[move["cik"]] = dict(move)
        else:
            seen["from_state"] = move["from_state"]   # older row, earlier state
    # An issuer that went out and came back has not moved. REBN was CONFIRMED
    # on a purchase, the purchase was flagged implausible, and it returned to
    # DORMANT -- two true rows whose net is nothing, and "dormant → dormant" is
    # not a thing to tell anyone. The history stays in the table either way.
    moves = [m for m in net.values() if m["from_state"] != m["to_state"]]

    # Decay is bookkeeping, not news. An issuer returning to DORMANT means its
    # purchases aged past the window -- nobody did anything, and the feed was
    # filling with "no activity in window" over the stale filing that had
    # stopped counting. Still written to state_transitions, because the arc
    # going backwards is exactly what two tiers could never express and it
    # matters when reading an issuer's history; just not surfaced as an item.
    decayed = [m for m in moves if m["to_state"] == signal_state.DORMANT]
    moves = [m for m in moves if m["to_state"] != signal_state.DORMANT]

    body = []
    evidence_free = collections.Counter()
    for order, move in enumerate(moves):
        entity = real_ticker(move["ticker"]) or f"CIK {move['cik']}"
        evs = by_entity.get(move["ticker"]) if real_ticker(move["ticker"]) else None
        if evs:
            card = render_company(entity, evs, conn, order)
            # The headline render_company picks is the loudest FILING on the
            # card, which is not the same thing as the reason this issuer
            # moved. STWI moved to DISTRIBUTING on a $414K sale and the card
            # announced a purchase, because the purchase was the louder filing.
            # The transition's own reason leads now; the filing stays beneath it
            # as the evidence, which is the order a reader needs them in.
            card = card.replace(
                '<p class="headline">',
                f'<p class="headline">{html.escape(move["reason"])}'
                f'<span class="because"> — </span>', 1)
        else:
            # Moved on evidence that is not an open event -- a disqualifier, or
            # selling, or a purchase already reviewed away, or Lane A, whose
            # whole point is an issuer that filed nothing. Still a transition,
            # so it still gets a card; the reason string carries it.
            fam = "setup" if move["to_state"] == signal_state.SETUP else "state"
            evidence_free[fam] += 1
            card = (
                f'<div class="row" data-ord="{order}" data-tier="2" '
                f'data-fam="{fam}" data-band="unscored" data-rung="0" '
                f'data-mag="-1" data-usd="-1" '
                f'data-filed="{html.escape(move["observed_on"])}" '
                f'data-find="{html.escape(entity.lower())}">'
                f'<div class="ticker">{ticker_link(entity)}</div>'
                f'<div><p class="headline">{html.escape(move["reason"])}</p>'
                f'<div class="detail"><span>observed '
                f'{html.escape(move["observed_on"])}</span></div></div></div>'
            )
        # The move itself, stamped onto the card that explains it.
        badge = (f'<span class="tag state-{move["to_state"].lower()}">'
                 f'{html.escape(move["from_state"].lower())} → '
                 f'{html.escape(move["to_state"].lower())}</span>')
        card = card.replace('<p class="headline">', f'<p class="headline">{badge} ', 1)
        body.append(card)

    if not moves:
        body.append('<p class="empty">Nothing changed state in the last '
                    f'{window_days} days. {sum(counts.values()):,} issuers are '
                    'being tracked; the panel above shows where they stand.</p>')

    sections = [
        render_state_panel(counts, len(moves), window_days, truncated,
                           len(decayed)),
        f'<section id="list">{"".join(body)}</section>',
        '<p class="empty none" id="nohits">No company matches these filters.</p>',
    ]
    grouped = [(m["ticker"] or f"CIK {m['cik']}",
                by_entity.get(m["ticker"]) or []) for m in moves]
    sections = [render_controls([g for g in grouped if g[1]],
                                evidence_free)] + sections

    covered = ", ".join(r["run_date"] for r in runs) or "no runs yet"
    # Candidates, not n_docs. n_docs counts what a pass newly fetched, which is
    # zero on every rescan of a day already collected -- so the line read
    # "0 filings scanned" on a dashboard built from 3,900 of them.
    docs = sum(r["n_candidates"] or 0 for r in runs)

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
<meta name="color-scheme" content="light dark">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head><body><div class="wrap">
<h1>EDGAR Discovery</h1>
<p class="meta">{len(moves)} transition{"" if len(moves) == 1 else "s"} &nbsp;·&nbsp; {sum(counts.values()):,} issuers tracked &nbsp;·&nbsp; days covered: {covered}{f" &nbsp;·&nbsp; {docs} filings scanned" if docs else ""}</p>
{"".join(sections)}
{watch_html}
</div><div id="tip" role="status" aria-live="polite"></div>
<script>{SCRIPT}</script></body></html>"""

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
    ap.add_argument("--probe-contracts", type=int, metavar="N", nargs="?", const=100,
                    help="diagnostic: can federal award recipients be matched "
                         "to listed tickers (sample size N)")
    ap.add_argument("--probe-buybacks", type=int, metavar="N", nargs="?", const=60,
                    help="diagnostic: how many recent 10-Q/10-K filers tag "
                         "repurchase data (sample size N)")
    ap.add_argument("--probe-setup", type=int, metavar="CIK", nargs="?",
                    const=1069183,
                    help="diagnostic: backtest Lane A against one issuer "
                         "(default 1069183, Axon)")
    ap.add_argument("--probe-setup-population", type=int, metavar="N", nargs="?",
                    const=150,
                    help="diagnostic: what share of a sample of N issuers "
                         "clears the Lane A condition today")
    ap.add_argument("--probe-form4", metavar="ACCESSION",
                    help="diagnostic: print one Form 4's transactions as filed")
    ap.add_argument("--transition-cap", type=int, metavar="N",
                    help=f"how many state transitions the dashboard reads "
                         f"before truncating (default {TRANSITION_CAP}); "
                         f"raise it when the page says it held moves back")
    ap.add_argument("--rescore", action="store_true",
                    help="backfill significance onto events stored before the scale")
    args = ap.parse_args()

    conn = connect()
    signal_state.set_user_agent(USER_AGENT)

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

    if args.probe_contracts:
        probe_contracts(sample=args.probe_contracts)
        return

    if args.probe_setup:
        probe_setup(cik=args.probe_setup)
        return

    if args.probe_form4:
        probe_form4(args.probe_form4)
        return

    if args.probe_setup_population:
        probe_setup_population(sample=args.probe_setup_population)
        return

    if args.probe_buybacks:
        probe_buybacks(sample=args.probe_buybacks)
        return

    if args.rescore:
        # Both halves. The flag has only ever rescored insider buys, so a
        # buyback rule change could not be applied to stored events without a
        # full collection run -- which needs the network, and needs the SEC to
        # be answering, neither of which a repair should depend on.
        n, promoted = rescore(conn)
        fixed, dropped = rescore_buybacks(conn)
        reworded = refresh_headlines(conn)
        conn.commit()
        print(f"rescored {n} event(s)"
              + (f", {promoted} promoted to the watchlist" if promoted else ""))
        if dropped:
            print(f"dropped {dropped} fund redemption(s) miscounted as buybacks")
        if fixed:
            print(f"rescored {fixed} buyback event(s)")
        if reworded:
            print(f"reworded {reworded} headline(s)")
        if not args.html:
            return

    if args.list:
        show_events(conn, tier=args.tier)
        return

    if args.html and not (args.date or args.backfill):
        # Classify before rendering. The page is built from transitions, so a
        # rebuild that skipped this drew an empty dashboard over a full
        # database -- which is precisely the "collector produced nothing" /
        # "classification found nothing" confusion the two lines below exist to
        # end, arrived at from the third direction: nothing had been asked.
        # No network here; it reads the tables the collector already filled.
        n_issuers, moves = signal_state.classify_all(conn, as_of=market_today())
        print(f"CLASSIFIED {n_issuers} issuer(s), {len(moves)} transition(s)")
        print(f"STATE COUNTS {signal_state.state_counts(conn)}")
        path, n = write_html(conn, args.html, cap=args.transition_cap)
        print(f"wrote {path} ({n} open events)")
        return

    # No emptiness check here any more: load_ticker_map() either returns a
    # populated map or raises. The warning that used to sit here called the
    # situation "degraded", which undersold it -- an empty map does not degrade
    # M&A filtering, it discards every M&A and buyback filing as unlisted.
    tickers = load_ticker_map()

    if args.backfill:
        days = business_days_back(args.backfill)
    elif args.date:
        days = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    else:
        days = [market_today()]

    outcomes = [run_day(conn, day, tickers, limit=args.limit) for day in days]
    # Printed after the days rather than inside them: these are enrichment
    # failures, they cross day boundaries via the issuer cache, and a per-day
    # tally would read as noise where a single total reads as a symptom.
    report_degraded()
    if outcomes and all(o == "unavailable" for o in outcomes):
        # One refused index is a publication gap; every index refused is us.
        sys.exit(
            "every daily index was refused. That is a blocked client, not a\n"
            "timing gap -- check EDGAR_USER_AGENT and how often this is running."
        )

    # Housekeeping every run, not on request. A score computed against a bad
    # denominator sat on the published dashboard reading 5,100% of the company
    # because clearing it needed someone to remember a flag; the run should
    # repair its own output. Cheap after the first pass -- sane scores are
    # skipped, and share counts come from the cache.
    drained = refresh_stale_facts(conn)
    unscaled = unannualise_buybacks(conn)
    flagged = flag_suspect_transactions(conn)
    renamed, retired_funds = repair_placeholder_tickers(conn, tickers)
    fixed, newly_promoted = rescore(conn)
    fixed_buybacks, dropped_funds = rescore_buybacks(conn)
    reworded = refresh_headlines(conn)
    aged = prune_events(conn)
    retired = prune_watchlist(conn)
    conn.commit()
    if drained:
        print(f"re-derived {drained} stale issuer(s)")
    if unscaled:
        print(f"un-annualised {unscaled} cached buyback figure(s)")
    if flagged:
        print(f"flagged {flagged} transaction(s) above the plausible ceiling")
    if renamed or retired_funds:
        print(f"resolved {renamed} placeholder ticker(s); "
              f"retired {retired_funds} untradeable issuer(s)")
    if dropped_funds:
        print(f"dropped {dropped_funds} fund redemption(s) miscounted as buybacks")
    if fixed_buybacks:
        print(f"rescored {fixed_buybacks} buyback event(s)")
    if reworded:
        print(f"reworded {reworded} headline(s)")
    if fixed:
        print(f"rescored {fixed} event(s)"
              + (f", {newly_promoted} promoted" if newly_promoted else ""))
    if aged:
        print(f"retired {aged} events past their shelf life")
    if retired:
        print(f"retired {retired} expired watchlist entries")

    # Both prints stay. They separate "the collector produced nothing" from
    # "classification found nothing", which no other line in this run can tell
    # apart -- the first is a broken pipeline, the second is a quiet week, and
    # for a long time they looked identical from the outside.
    n_issuers, moves = signal_state.classify_all(conn, as_of=days[-1])
    print(f"CLASSIFIED {n_issuers} issuer(s), {len(moves)} transition(s)")
    print(f"STATE COUNTS {signal_state.state_counts(conn)}")

    watched = watchlist_rows(conn)
    print(f"watchlist: {len(watched)} active")

    if args.html:
        path, n = write_html(conn, args.html, cap=args.transition_cap)
        print(f"\nwrote {path} ({n} open events)")
    else:
        print()
        show_events(conn, tier=1)


if __name__ == "__main__":
    main()
