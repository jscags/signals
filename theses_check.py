#!/usr/bin/env python3
"""Validate theses.json. Reads it, never writes it.

The thesis file is hand-edited, which is the point and also the risk. A
mistyped trigger type, a missing threshold, or a duplicated id would not
crash a tracker -- it would quietly drop a trigger, and a trigger that is
silently absent looks exactly like a trigger that never fired. This repo has
been bitten by that shape of bug repeatedly, so the file is checked before
anything reads it.

Run it in CI before the tracker. A malformed thesis file should fail the
run, not produce a confident empty report.
"""

import json
import re
import sys

TYPES = ("metric", "price", "event")
# Every trigger has to state a direction and a number. "Capex slows" is not
# a trigger; "yoy_growth_pct < 0" is. This is the cheapest possible check
# that someone wrote a threshold rather than a sentiment.
CONDITION = re.compile(r"[<>]=?\s*-?\d|\bbetween\b")


def check(path="theses.json"):
    problems, warnings = [], []
    try:
        with open(path) as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return [f"{path} does not exist"], [], 0
    except json.JSONDecodeError as exc:
        return [f"{path} is not valid JSON: {exc}"], [], 0

    theses = data.get("theses")
    if not isinstance(theses, list) or not theses:
        return ["no theses array, or it is empty"], [], 0

    seen_thesis, seen_trigger, n_triggers = set(), set(), 0
    for i, thesis in enumerate(theses):
        where = f"thesis[{i}]"
        tid = thesis.get("id")
        if not tid:
            problems.append(f"{where}: no id")
        elif tid in seen_thesis:
            problems.append(f"{where}: duplicate thesis id {tid!r}")
        else:
            seen_thesis.add(tid)
            where = f"thesis {tid!r}"

        for field in ("title", "statement"):
            if not thesis.get(field):
                problems.append(f"{where}: no {field}")

        # A thesis that cannot be wrong is not a thesis.
        if not thesis.get("disconfirms"):
            problems.append(
                f"{where}: no 'disconfirms' -- state what would show this "
                f"thesis to be wrong, or it is not falsifiable")

        triggers = thesis.get("triggers")
        if not isinstance(triggers, list) or not triggers:
            problems.append(f"{where}: no triggers")
            continue

        for j, trigger in enumerate(triggers):
            n_triggers += 1
            gid = trigger.get("id") or f"{where}.triggers[{j}]"
            if trigger.get("id"):
                if gid in seen_trigger:
                    problems.append(f"trigger {gid!r}: duplicate id")
                seen_trigger.add(gid)
            else:
                problems.append(f"{where}.triggers[{j}]: no id")

            kind = trigger.get("type")
            if kind not in TYPES:
                problems.append(
                    f"trigger {gid!r}: type {kind!r} is not one of {TYPES}")

            for field in ("what", "source", "fires_when"):
                if not trigger.get(field):
                    problems.append(f"trigger {gid!r}: no {field}")

            fires = trigger.get("fires_when") or ""
            if fires and not CONDITION.search(fires):
                problems.append(
                    f"trigger {gid!r}: fires_when {fires!r} states no "
                    f"threshold -- a direction and a number are required")

            if kind == "metric" and not trigger.get("ciks"):
                problems.append(
                    f"trigger {gid!r}: a metric trigger needs ciks")
            if kind == "price" and not (trigger.get("tickers")
                                        or trigger.get("source")):
                problems.append(
                    f"trigger {gid!r}: a price trigger needs tickers")
            # An event trigger that counts issuers must say WHICH issuers.
            # "The AI complex" is not a set a tracker can evaluate, and a
            # numeric threshold over an undefined universe is the same
            # vagueness as "capex slows" one level up.
            if kind == "event" and not (trigger.get("tickers")
                                        or trigger.get("ciks")):
                problems.append(
                    f"trigger {gid!r}: an event trigger needs an explicit "
                    f"tickers or ciks universe, not a described one")

    # Not a failure, but worth saying out loud every run.
    unmeasurable = data.get("_not_measurable") or {}
    named = [k for k in unmeasurable if not k.startswith("_")]
    if not named:
        warnings.append(
            "no _not_measurable section: every tracker has blind spots and "
            "unnamed ones get mistaken for absence of evidence")

    return problems, warnings, n_triggers


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "theses.json"
    problems, warnings, n = check(path)
    print(f"{path}: {n} trigger(s)")
    for w in warnings:
        print(f"  WARNING  {w}")
    for p in problems:
        print(f"  PROBLEM  {p}")
    if problems:
        print(f"\n{len(problems)} problem(s). A malformed thesis file is "
              f"refused rather than read partially: a silently dropped "
              f"trigger is indistinguishable from one that never fired.")
        return 1
    print("  ok")
    return 0


def selftest():
    import tempfile, os
    checks = 0

    def ok(label, cond):
        nonlocal checks
        assert cond, "FAILED: " + label
        checks += 1

    def write(obj):
        fd, p = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as fh:
            json.dump(obj, fh)
        return p

    good = {"theses": [{"id": "t", "title": "T", "statement": "S",
                        "disconfirms": "D",
                        "triggers": [{"id": "g", "type": "price",
                                      "what": "W", "source": "S",
                                      "tickers": ["X"],
                                      "fires_when": "drawdown_pct > 20"}]}],
            "_not_measurable": {"x": "y"}}
    problems, _, n = check(write(good))
    ok("a well-formed file passes", not problems and n == 1)

    bad = json.loads(json.dumps(good))
    bad["theses"][0]["triggers"][0]["type"] = "sentiment"
    ok("an invented trigger type is refused",
       any("not one of" in p for p in check(write(bad))[0]))

    bad = json.loads(json.dumps(good))
    bad["theses"][0]["triggers"][0]["fires_when"] = "capex slows"
    ok("a threshold-free condition is refused",
       any("states no threshold" in p for p in check(write(bad))[0]))

    bad = json.loads(json.dumps(good))
    del bad["theses"][0]["disconfirms"]
    ok("a thesis that cannot be wrong is refused",
       any("disconfirms" in p for p in check(write(bad))[0]))

    bad = json.loads(json.dumps(good))
    bad["theses"][0]["triggers"].append(
        dict(bad["theses"][0]["triggers"][0]))
    ok("a duplicated trigger id is caught",
       any("duplicate id" in p for p in check(write(bad))[0]))

    bad = json.loads(json.dumps(good))
    bad["theses"][0]["triggers"][0]["type"] = "event"
    bad["theses"][0]["triggers"][0].pop("tickers")
    ok("an event trigger over an undefined universe is refused",
       any("explicit" in p for p in check(write(bad))[0]))

    bad = json.loads(json.dumps(good))
    bad["theses"][0]["triggers"][0]["type"] = "metric"
    bad["theses"][0]["triggers"][0].pop("tickers")
    ok("a metric trigger without ciks is refused",
       any("needs ciks" in p for p in check(write(bad))[0]))

    fd, p = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as fh:
        fh.write("{not json")
    ok("malformed JSON is refused, not ignored",
       any("not valid JSON" in x for x in check(p)[0]))
    ok("a missing file is refused", check("/nonexistent.json")[0])

    print(f"theses_check selftest: {checks} checks passed")
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
