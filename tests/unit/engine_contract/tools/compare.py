#!/usr/bin/env python3
"""Compare the contract observations of the SpiderMonkey and QuickJS runs.

Usage: compare.py SM.jsonl QJS.jsonl [known-differences.toml]

Both inputs are JSON Lines produced by `engine_contract_tests`. Cases are matched by `id`; the
`engine` field is ignored. SpiderMonkey is the reference: a difference listed in
`known-differences.toml` is reported as a known QuickJS defect and does not fail the run; an
unlisted difference does. Entries are never silent skips.
"""

import json
import sys
import tomllib


def load(path):
    cases = {}
    with open(path, encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()
            if not line:
                continue
            case = json.loads(line)
            case.pop("engine", None)
            case_id = case["id"]
            if case_id in cases:
                raise SystemExit(f"{path}:{line_number}: duplicate case id {case_id!r}")
            cases[case_id] = case
    return cases


def load_differences(path):
    if not path:
        return {}
    with open(path, "rb") as file:
        document = tomllib.load(file)
    differences = {}
    for entry in document.get("difference", []):
        differences[entry["id"]] = set(entry["fields"])
    return differences


def matches_difference(left, right, fields):
    if not fields:
        return False
    remaining_left = {key: value for key, value in left.items() if key not in fields}
    remaining_right = {key: value for key, value in right.items() if key not in fields}
    return remaining_left == remaining_right


def main(argv):
    if len(argv) not in (3, 4):
        raise SystemExit(__doc__)
    sm = load(argv[1])
    qjs = load(argv[2])
    differences = load_differences(argv[3] if len(argv) == 4 else None)

    missing_in_qjs = sorted(set(sm) - set(qjs))
    missing_in_sm = sorted(set(qjs) - set(sm))
    mismatches = []
    known = []
    for case_id in sorted(set(sm) & set(qjs)):
        left = sm[case_id]
        right = qjs[case_id]
        if left == right:
            continue
        fields = differences.get(case_id)
        if matches_difference(left, right, fields):
            known.append((case_id, left, right, fields))
        else:
            mismatches.append((case_id, left, right))

    for case_id, left, right, fields in known:
        print(f"known QuickJS defect: {case_id} (fields: {', '.join(sorted(fields))})")
        print(f"  SpiderMonkey: {json.dumps(left, sort_keys=True)}")
        print(f"  QuickJS:      {json.dumps(right, sort_keys=True)}")
    for case_id in missing_in_qjs:
        print(f"missing in QuickJS: {case_id}")
    for case_id in missing_in_sm:
        print(f"missing in SpiderMonkey: {case_id}")
    for case_id, left, right in mismatches:
        print(f"mismatch: {case_id}")
        print(f"  SpiderMonkey: {json.dumps(left, sort_keys=True)}")
        print(f"  QuickJS:      {json.dumps(right, sort_keys=True)}")

    failed = bool(missing_in_qjs or missing_in_sm or mismatches)
    print(
        f"compared {len(set(sm) & set(qjs))} cases: "
        f"{len(mismatches)} mismatches, "
        f"{len(known)} known QuickJS defects, "
        f"{len(missing_in_qjs)} missing in QuickJS, "
        f"{len(missing_in_sm)} missing in SpiderMonkey"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
