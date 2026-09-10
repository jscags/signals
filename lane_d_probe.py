"""Lane D, Phase 0: measure whether the sources are good enough to build on.

THIS SCRIPT WRITES NOTHING. No database, no state, no files. It fetches,
extracts, and prints. Phase 0 exists to find out whether Lane D is worth
building at all, and a measurement pass that quietly creates schema has
already assumed the answer.

It reuses the PATTERN of the existing collector -- declared user agent, paced
requests, a four-way outcome on every call -- without importing it, so Lane D
stays independent of Lane A/B/C as the work order requires.

THE FOUR-WAY OUTCOME
--------------------
Every network call resolves to exactly one of: OK, HTTP_ERROR, EMPTY,
PARSE_FAIL. This is the point of the whole exercise. The repo's known failure
mode is a 403 or an empty payload arriving at the far end as "nothing fired",
and a monitor that cannot tell "the trigger did not fire" from "I could not
look" is worse than no monitor, because it is quietly reassuring.

THE EXTRACTION TAXONOMY
-----------------------
Every filing lands in exactly one bucket:

  HIT       the extractor found a figure AND the surrounding text confirms it
            is consolidated backlog
  WRONG     the extractor found a figure but the context says it is something
            else -- a segment, a prior-year comparative, a bookings number
  MISSED    the document talks about backlog and the extractor found nothing.
            THIS IS THE DANGEROUS ONE. A miss looks exactly like an absence
            to anything downstream.
  ABSENT    the document genuinely does not mention backlog. Not a failure.

HIT + WRONG + MISSED + ABSENT = every filing examined. A taxonomy whose parts
do not sum to the whole is hiding something.
"""

import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "edgar-discovery 52y9fp5njf@privaterelay.appleid.com"
SEC_PACE = 0.15                      # under the SEC's 10/sec
TIMEOUT = 30

OK, HTTP_ERROR, EMPTY, PARSE_FAIL = "OK", "HTTP_ERROR", "EMPTY", "PARSE_FAIL"


class Fetched:
    """One network call and exactly what became of it."""

    __slots__ = ("url", "status", "code", "bytes", "body", "note")

    def __init__(self, url, status, code=None, body="", note=""):
        self.url = url
        self.status = status
        self.code = code
        self.body = body
        self.bytes = len(body or "")
        self.note = note

    def __repr__(self):
        return f"<{self.status} {self.code or ''} {self.bytes}B {self.url[:60]}>"


def fetch(url, accept="*/*", pace=SEC_PACE):
    """Never raises. Returns a Fetched carrying which of the four it was."""
    request = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": accept,
        "Accept-Encoding": "identity",
    })
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            body = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return Fetched(url, HTTP_ERROR, exc.code, "", str(exc.reason))
    except (urllib.error.URLError, OSError) as exc:
        return Fetched(url, HTTP_ERROR, None, "", str(exc))
    finally:
        time.sleep(pace)
    if not body.strip():
        return Fetched(url, EMPTY, 200, "", "200 with an empty body")
    return Fetched(url, OK, 200, body)


# ------------------------------------------------------------- extraction

# Backlog is written a dozen ways across filers. This is deliberately the
# NAIVE extractor the work order asks about -- the point is to measure how
# often a reasonable first attempt is right, not to hand-tune it until the
# measurement flatters the plan.
BACKLOG_RE = re.compile(
    r"(consolidated\s+)?backlog[^.$]{0,120}?"
    r"\$\s*([0-9][0-9,.]*)\s*(billion|million|bn|mm|m\b)?",
    re.I | re.S)

# Context words that mean the number found is NOT the consolidated figure.
DISQUALIFY = re.compile(
    r"\b(segment|prior[- ]year|a year ago|compared|previously|"
    r"as of [A-Z][a-z]+ \d{1,2}, 20[0-2]\d.{0,40}(was|were))\b", re.I)

MENTIONS_BACKLOG = re.compile(r"backlog", re.I)


def strip_tags(html):
    """Crude, and enough: press releases are mostly prose in shallow markup."""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = (text.replace("&nbsp;", " ").replace("&amp;", "&")
                .replace("&#160;", " ").replace("&#8217;", "'")
                .replace("&rsquo;", "'").replace("&mdash;", "—"))
    return re.sub(r"\s+", " ", text).strip()


def to_usd(number, unit):
    try:
        value = float(number.replace(",", ""))
    except ValueError:
        return None
    unit = (unit or "").lower()
    if unit in ("billion", "bn"):
        return value * 1e9
    if unit in ("million", "mm", "m"):
        return value * 1e6
    # A bare figure in a press release is nearly always already in dollars
    # when it is this large, and a mis-scaled backlog is the difference
    # between a trigger firing and not.
    return value


def extract_backlog(text):
    """Every candidate, with the surrounding words that justify or damn it."""
    out = []
    for m in BACKLOG_RE.finditer(text):
        start, end = max(0, m.start() - 90), min(len(text), m.end() + 90)
        context = text[start:end]
        out.append({
            "matched": m.group(0)[:160],
            "value": to_usd(m.group(2), m.group(3)),
            "consolidated_word": bool(m.group(1)),
            "disqualified_by": (DISQUALIFY.search(context).group(0)
                                if DISQUALIFY.search(context) else None),
            "context": context,
        })
    return out


def classify(text, candidates):
    """One filing, one bucket. See the module docstring."""
    if not MENTIONS_BACKLOG.search(text):
        return "ABSENT", None
    if not candidates:
        return "MISSED", None
    clean = [c for c in candidates if not c["disqualified_by"] and c["value"]]
    if not clean:
        return "WRONG", candidates[0]
    # Prefer one that literally says "consolidated"; else the largest, which
    # is the consolidated figure in every filing shape seen so far.
    named = [c for c in clean if c["consolidated_word"]]
    best = max(named or clean, key=lambda c: c["value"])
    return "HIT", best


# ------------------------------------------------------------------ edgar

def submissions(cik):
    padded = str(cik).lstrip("0").zfill(10)
    return fetch(f"https://data.sec.gov/submissions/CIK{padded}.json",
                 accept="application/json")


def recent_8ks(subs_body, limit=12):
    """Accessions of recent 8-Ks, newest first. Raises nothing."""
    try:
        data = json.loads(subs_body)
        recent = data["filings"]["recent"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    rows = []
    forms = recent.get("form") or []
    for i, form in enumerate(forms):
        if form != "8-K":
            continue
        rows.append({
            "accession": recent["accessionNumber"][i],
            "filed": recent["filingDate"][i],
            "doc": recent.get("primaryDocument", [None] * len(forms))[i],
            "items": (recent.get("items") or [""] * len(forms))[i],
        })
        if len(rows) >= limit:
            break
    return rows, None


def exhibit_991(cik, accession):
    """The EX-99.1 press release inside one 8-K, or why there wasn't one."""
    plain = accession.replace("-", "")
    base = (f"https://www.sec.gov/Archives/edgar/data/"
            f"{str(cik).lstrip('0')}/{plain}")
    index = fetch(f"{base}/index.json", accept="application/json")
    if index.status != OK:
        return None, index, "index unavailable"
    try:
        items = json.loads(index.body)["directory"]["item"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        return None, Fetched(index.url, PARSE_FAIL, 200, index.body,
                             f"{type(exc).__name__}"), "index unparseable"
    # EX-99.1 is not consistently named; press releases are the .htm files
    # that are not the primary 8-K document and not graphics.
    cands = [i["name"] for i in items
             if i["name"].lower().endswith((".htm", ".html"))
             and not re.search(r"(?i)^0*\d+\.htm|R\d+\.htm", i["name"])]
    if not cands:
        return None, index, "no htm exhibits in the filing index"
    # Prefer something that names itself an exhibit.
    ex = [c for c in cands if re.search(r"(?i)ex.?99", c)]
    pick = (ex or cands)[-1] if not ex else ex[0]
    doc = fetch(f"{base}/{pick}", accept="text/html")
    return pick, doc, None


# --------------------------------------------------------------- ir feeds

FEED_HINT = re.compile(
    r'<link[^>]+type=["\']application/(rss|atom)\+xml["\'][^>]*>', re.I)
HREF = re.compile(r'href=["\']([^"\']+)["\']', re.I)


def discover_feed(site):
    """Ask the IR page what feed it advertises, rather than guessing URLs.

    Guessing produces a list of 404s that proves nothing. A site either
    declares a feed in its head or it does not.
    """
    page = fetch(site, accept="text/html", pace=0.4)
    if page.status != OK:
        return None, page, "IR page not reachable"
    links = []
    for tag in FEED_HINT.findall(page.body) or []:
        pass
    for m in re.finditer(FEED_HINT, page.body):
        href = HREF.search(m.group(0))
        if href:
            links.append(href.group(1))
    if not links:
        return None, page, "no feed declared in the page head"
    url = links[0]
    if url.startswith("/"):
        root = re.match(r"(https?://[^/]+)", site)
        url = (root.group(1) if root else "") + url
    feed = fetch(url, accept="application/rss+xml, application/xml", pace=0.4)
    return url, feed, None


def feed_titles(body, limit=10):
    titles = re.findall(r"(?is)<title[^>]*>(.*?)</title>", body or "")
    out = []
    for t in titles:
        t = strip_tags(t).strip()
        if t and t.lower() not in ("rss", "atom"):
            out.append(t[:120])
        if len(out) >= limit + 1:
            break
    return out[1:] if len(out) > limit else out


# -------------------------------------------------------------- quotes

# Free, no-key candidates. The work order says STOP and report if a key is
# required, so this measures whether that point has been reached.
QUOTE_CANDIDATES = [
    ("stooq csv", "https://stooq.com/q/l/?s={t}.us&f=sd2t2ohlc&h&e=csv"),
    ("stooq daily", "https://stooq.com/q/d/l/?s={t}.us&i=d"),
    ("yahoo chart", "https://query1.finance.yahoo.com/v8/finance/chart/{t}"
                    "?range=5d&interval=1d"),
]


def probe_quotes(ticker):
    rows = []
    for name, tmpl in QUOTE_CANDIDATES:
        got = fetch(tmpl.format(t=ticker.lower()), pace=0.5)
        verdict = got.status
        detail = got.note
        if got.status == OK:
            head = got.body.lstrip()[:200].replace("\n", " ")
            if head.lower().startswith("<!doctype html") or "<html" in head.lower():
                verdict = PARSE_FAIL
                detail = "HTML, not data — bot challenge or error page"
            elif "no data" in head.lower():
                verdict = EMPTY
                detail = "explicit no-data reply"
            else:
                detail = head[:120]
        rows.append((name, verdict, got.code, got.bytes, detail))
    return rows


# ---------------------------------------------------------------- report

def probe_ticker(ticker, cik, quarters=6):
    print(f"\n{'='*74}\n{ticker}  (CIK {cik})\n{'='*74}")

    subs = submissions(cik)
    print(f"submissions: {subs.status} {subs.code or ''} {subs.bytes}B"
          + (f"  {subs.note}" if subs.note else ""))
    if subs.status != OK:
        print("  -> cannot enumerate filings; every count below would be a lie")
        return None

    filings, err = recent_8ks(subs.body, limit=quarters * 3)
    if err:
        print(f"  -> PARSE_FAIL on submissions json: {err}")
        return None
    print(f"8-Ks found: {len(filings)} (examining up to {quarters * 3})")

    tally = {"HIT": 0, "WRONG": 0, "MISSED": 0, "ABSENT": 0}
    fetch_fail = 0
    examined = 0
    print(f"\n{'filed':<12}{'bucket':<9}{'value':>16}  extracted string")
    print("-" * 74)
    for f in filings:
        if examined >= quarters * 2:
            break
        name, doc, why = exhibit_991(cik, f["accession"])
        if doc is None or doc.status != OK:
            fetch_fail += 1
            code = doc.code if doc is not None else "-"
            print(f"{f['filed']:<12}{'FETCH':<9}{'':>16}  "
                  f"{why or doc.status} ({code})")
            continue
        examined += 1
        text = strip_tags(doc.body)
        cands = extract_backlog(text)
        bucket, best = classify(text, cands)
        tally[bucket] += 1
        value = (f"${best['value']/1e9:.2f}bn" if best and best.get("value")
                 else "")
        shown = (best or (cands[0] if cands else {})).get("matched", "")
        print(f"{f['filed']:<12}{bucket:<9}{value:>16}  {shown[:120]}")
        if bucket == "MISSED":
            hit = MENTIONS_BACKLOG.search(text)
            around = text[max(0, hit.start()-100):hit.start()+160]
            print(f"{'':12}{'':9}{'':>16}  ...{around}...")
        if bucket == "WRONG" and best:
            print(f"{'':12}{'':9}{'':>16}  rejected by: {best['disqualified_by']}")

    total = sum(tally.values())
    print("-" * 74)
    print(f"examined {examined} press releases, {fetch_fail} could not be fetched")
    print(f"  HIT     {tally['HIT']:>3}   extractor right, context confirms")
    print(f"  WRONG   {tally['WRONG']:>3}   found a figure, context says it is "
          f"not consolidated backlog")
    print(f"  MISSED  {tally['MISSED']:>3}   document discusses backlog, "
          f"extractor found nothing  <-- the dangerous bucket")
    print(f"  ABSENT  {tally['ABSENT']:>3}   no backlog discussion at all "
          f"(not a failure)")
    print(f"  sums to {total} of {examined} examined "
          f"{'OK' if total == examined else '*** MISMATCH ***'}")
    return tally


def main(argv):
    pairs = []
    for arg in argv[1:]:
        if ":" in arg:
            t, c = arg.split(":", 1)
            pairs.append((t.strip().upper(), c.strip()))
    if not pairs:
        pairs = [("AGX", "0000100591")]

    print("LANE D — PHASE 0 SOURCE RELIABILITY PROBE")
    print(f"run at {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    print("this script writes nothing: no database, no state, no files")

    for ticker, cik in pairs:
        probe_ticker(ticker, cik)

    print(f"\n{'='*74}\nQUOTE SOURCES (free, no key) — from this runner's IP")
    print("=" * 74)
    print(f"{'source':<14}{'verdict':<12}{'code':>5}{'bytes':>9}  detail")
    print("-" * 74)
    for name, verdict, code, nbytes, detail in probe_quotes(pairs[0][0]):
        print(f"{name:<14}{verdict:<12}{str(code or '-'):>5}{nbytes:>9}  "
              f"{detail[:70]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
