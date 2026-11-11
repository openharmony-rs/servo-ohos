#!/usr/bin/env python3
"""Check that every eligible mozjs entry point is exercised by the contract run.

Usage: check_api_coverage.py MANIFEST.jsonl MANIFEST.toml

The JSON Lines input is one of the files written by `CONTRACT_OUT`. Each case must list the mozjs
entry points it calls in its `apis` field. A name is accepted only if the manifest classifies it
as `implemented`; listing an `empty` or `todo` entry is an error, because the suite must not claim
to test known-unimplemented behavior. Coverage is reported per engine module so the gap is
actionable.
"""

import collections
import json
import sys
import tomllib


def load_manifest(path):
    with open(path, "rb") as file:
        document = tomllib.load(file)
    return document["api"]


def load_coverage(path):
    covered = set()
    cases = []
    with open(path, encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()
            if not line:
                continue
            case = json.loads(line)
            case_id = case["id"]
            apis = case.get("apis")
            if not isinstance(apis, list) or not apis:
                raise SystemExit(f"{path}:{line_number}: case {case_id!r} has no apis")
            unknown = [api for api in apis if not isinstance(api, str)]
            if unknown:
                raise SystemExit(f"{path}:{line_number}: case {case_id!r} has non-string apis")
            cases.append((case_id, apis))
            covered.update(apis)
    return cases, covered


def main(argv):
    if len(argv) != 3:
        raise SystemExit(__doc__)
    manifest_path, coverage_path = argv[1], argv[2]
    entries = load_manifest(manifest_path)
    cases, covered = load_coverage(coverage_path)

    by_name = collections.defaultdict(list)
    for entry in entries:
        by_name[entry["name"]].append(entry)

    errors = []
    covered_eligible = set()
    for case_id, apis in cases:
        for name in apis:
            matches = by_name.get(name)
            if not matches:
                errors.append(f"{case_id}: {name} is not in the mozjs manifest")
            elif not any(entry["status"] == "implemented" for entry in matches):
                statuses = ", ".join(sorted({entry["status"] for entry in matches}))
                errors.append(f"{case_id}: {name} is classified {statuses}, not implemented")
            else:
                covered_eligible.add(name)

    eligible = {
        entry["name"]
        for entry in entries
        if entry["status"] == "implemented"
    }
    missing = eligible - covered_eligible
    if missing:
        by_file = collections.Counter()
        for entry in entries:
            if entry["name"] in missing:
                by_file[entry.get("file", "?")] += 1
        errors.append(f"{len(missing)} eligible mozjs entry points have no contract case")
        for path, count in sorted(by_file.items()):
            errors.append(f"  {path.removeprefix('engine/')}: {count}")

    if errors:
        for error in errors:
            print("error:", error, file=sys.stderr)
        return 1

    print(
        f"API coverage ok: {len(covered_eligible)}/{len(eligible)} eligible entries "
        f"covered by {len(cases)} cases"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
