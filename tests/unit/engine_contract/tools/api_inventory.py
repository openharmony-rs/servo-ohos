#!/usr/bin/env python3
"""Generate or check the mozjs C API manifest used by the engine contract suite.

The manifest is derived from the same bindgen output and the same name/arity rule as
servo-quickjs/mozjs/stub_bindings.py. It deliberately does not try to implement that rule a second
time: when a servo-quickjs checkout is supplied, its parser is imported and reused.

Usage:
  api_inventory.py generate --bindgen-dir DIR --engine-src DIR --output FILE \
      [--quickjs-revision REV] [--stub-bindings FILE]
  api_inventory.py check --manifest FILE
  api_inventory.py verify --manifest FILE --bindgen-dir DIR --engine-src DIR \
      [--stub-bindings FILE]

``--engine-src`` normally points at ``servo-quickjs/mozjs/src/engine``. A function whose body is
empty or only ignores arguments is classified as ``empty`` because the generated forwarding stub
has no observable engine semantics to exercise. Everything else present in the engine tree is
``implemented``; everything else is ``todo``.
"""

import argparse
import glob
import hashlib
import importlib.util
import os
import re
import sys
import tomllib


TODO_REASON = "qjs_todo!: calling this entry panics until servo-quickjs implements it"
EMPTY_REASON = "silent no-op: the shim has no observable engine semantics for this entry"


def load_stub_module(path):
    spec = importlib.util.spec_from_file_location("servo_quickjs_stub_bindings", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def read_text(path):
    with open(path, encoding="utf-8") as file:
        return file.read()


def find_matching(text, index, opening, closing):
    depth = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            index = skip_string(text, index)
            continue
        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return -1


def skip_string(text, index):
    index += 1
    while index < len(text) and text[index] != '"':
        index += 2 if text[index] == "\\" else 1
    return index + 1


CONSTANT = r"(true|false|0|\(\)|None|::std::ptr::null_mut\(\)|::core::ptr::null_mut\(\)|ptr::null_mut\(\))"


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def is_empty_body(body):
    normalized = re.sub(r"\s+", " ", strip_comments(body)).strip()
    statements = [part.strip() for part in re.findall(r"[^;]+;", normalized)]
    rest = re.sub(r"[^;]+;", "", normalized).strip()
    if not all(re.fullmatch(r"let _ = .*;", statement) for statement in statements):
        return False
    return rest == "" or re.fullmatch(CONSTANT + r"( as .+)?", rest) is not None


def split_params(params):
    """Split a Rust parameter list at top-level commas."""
    result = []
    depth = 0
    current = []
    for char in params + ",":
        if char in "([<{":
            depth += 1
        elif char in ")]>}":
            depth -= 1
        if char == "," and depth == 0:
            parameter = "".join(current).strip()
            if parameter:
                result.append(parameter)
            current = []
        else:
            current.append(char)
    return result


def engine_functions(engine_src, stub):
    functions = {}
    for path in sorted(glob.glob(os.path.join(engine_src, "**", "*.rs"), recursive=True)):
        text = read_text(path)
        for match in re.finditer(r"pub\(crate\) unsafe fn (\w+)\(", text):
            name = match.group(1)
            params_end = find_matching(text, match.end() - 1, "(", ")")
            if params_end < 0:
                continue
            arity = len(stub.param_names(text[match.end() : params_end]))
            body_start = params_end + 1
            while body_start < len(text) and text[body_start] not in "{;":
                body_start += 1
            empty = False
            if body_start < len(text) and text[body_start] == "{":
                body_end = find_matching(text, body_start, "{", "}")
                if body_end >= 0:
                    empty = is_empty_body(text[body_start + 1 : body_end])
            line = text.count("\n", 0, match.start()) + 1
            functions.setdefault((name, arity), []).append(
                {
                    "file": os.path.relpath(path, os.path.dirname(engine_src)),
                    "line": line,
                    "empty": empty,
                }
            )
    return functions


def bindgen_entries(bindgen_dir, stub):
    entries = []
    for source in ("jsapi", "gluebindings"):
        path = os.path.join(bindgen_dir, source + ".rs")
        text = read_text(path)
        text = re.sub(
            r"#\[allow\(clippy::unnecessary_operation, clippy::identity_op\)\]\s*const _: \(\) = \{.*?\n\s*\};\n",
            "",
            text,
            flags=re.S,
        )
        text = re.sub(
            r"pub struct Value \{\s*pub asBits_: u64,\s*\}",
            "pub struct Value { pub asBits_: u64, pub tag: i64 }",
            text,
        )
        pos = 0
        block = re.compile(r'(?m)^(?P<indent>[ \t]*)(?:unsafe )?extern "C" \{')
        while True:
            match = block.search(text, pos)
            if not match:
                break
            brace = match.end() - 1
            end = stub.find_block_end(text, brace)
            for item in stub.split_items(text[brace + 1 : end]):
                function = stub.FN.match(item.strip())
                if function is None:
                    continue
                params = function.group("params")
                if re.search(r"(^|,)\s*\.\.\.\s*$", params):
                    continue
                parameters = split_params(params)
                parameter_types = []
                for parameter in parameters:
                    _, _, parameter_type = parameter.partition(":")
                    parameter_types.append(" ".join(parameter_type.split()))
                return_type = (function.group("ret") or "").strip()
                return_type = return_type.removeprefix("->").strip() or "()"
                signature = f"({', '.join(parameter_types)}) -> {return_type}"
                entries.append(
                    {
                        "name": function.group("name"),
                        "arity": len(parameters),
                        "source": source,
                        "signature": signature,
                    }
                )
            pos = end + 1
    return entries


def quote(value):
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def write_manifest(path, entries, metadata):
    lines = ["[manifest]"]
    for key in (
        "mozjs_version",
        "bindgen_sha256",
        "quickjs_revision",
        "bindgen_definitions",
        "unique_names",
        "variadic",
        "implemented",
        "empty",
        "todo",
        "eligible",
    ):
        value = metadata[key]
        if isinstance(value, int):
            lines.append(f"{key} = {value}")
        else:
            lines.append(f"{key} = {quote(value)}")
    lines.append("")
    for entry in entries:
        lines.append("[[api]]")
        lines.append(f"name = {quote(entry['name'])}")
        lines.append(f"arity = {entry['arity']}")
        lines.append(f"source = {quote(entry['source'])}")
        lines.append(f"status = {quote(entry['status'])}")
        lines.append(f"signature = {quote(entry['signature'])}")
        if entry.get("file"):
            lines.append(f"file = {quote(entry['file'])}")
        if entry.get("line"):
            lines.append(f"line = {entry['line']}")
        if entry.get("reason"):
            lines.append(f"reason = {quote(entry['reason'])}")
        lines.append("")
    with open(path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines))


def generate_entries(bindgen_dir, engine_src, stub):
    definitions = bindgen_entries(bindgen_dir, stub)
    functions = engine_functions(engine_src, stub)

    entries = []
    for definition in definitions:
        function = functions.get((definition["name"], definition["arity"]))
        entry = dict(definition)
        if function:
            location = function[0]
            entry["file"] = location["file"]
            entry["line"] = location["line"]
            if location["empty"]:
                entry["status"] = "empty"
                entry["reason"] = EMPTY_REASON
            else:
                entry["status"] = "implemented"
        else:
            entry["status"] = "todo"
            entry["reason"] = TODO_REASON
        entries.append(entry)

    entries.sort(key=lambda entry: (entry["name"], entry["arity"], entry["source"]))
    counts = {
        "bindgen_definitions": len(entries),
        "unique_names": len({entry["name"] for entry in entries}),
        "variadic": 0,
        "implemented": sum(entry["status"] == "implemented" for entry in entries),
        "empty": sum(entry["status"] == "empty" for entry in entries),
        "todo": sum(entry["status"] == "todo" for entry in entries),
    }
    counts["eligible"] = counts["implemented"]

    # Variadic declarations are not converted into stubs by stub_bindings.py, but they are part
    # of the C surface. Count them from the source for the record only.
    for source in ("jsapi", "gluebindings"):
        text = read_text(os.path.join(bindgen_dir, source + ".rs"))
        pos = 0
        block = re.compile(r'(?m)^(?P<indent>[ \t]*)(?:unsafe )?extern "C" \{')
        while True:
            match = block.search(text, pos)
            if not match:
                break
            brace = match.end() - 1
            end = stub.find_block_end(text, brace)
            for item in stub.split_items(text[brace + 1 : end]):
                function = stub.FN.match(item.strip())
                if function and re.search(r"(^|,)\s*\.\.\.\s*$", function.group("params")):
                    counts["variadic"] += 1
            pos = end + 1

    return entries, counts


def generate(args):
    stub = load_stub_module(args.stub_bindings)
    bindgen_dir = args.bindgen_dir
    entries, counts = generate_entries(bindgen_dir, args.engine_src, stub)

    digest = hashlib.sha256()
    for source in ("jsapi", "gluebindings"):
        digest.update(sha256(os.path.join(bindgen_dir, source + ".rs")).encode())
    metadata = {
        "mozjs_version": args.mozjs_version,
        "bindgen_sha256": digest.hexdigest(),
        "quickjs_revision": args.quickjs_revision,
        **counts,
    }
    write_manifest(args.output, entries, metadata)
    print(
        f"wrote {args.output}: {counts['bindgen_definitions']} definitions, "
        f"{counts['eligible']} eligible, {counts['empty']} empty, {counts['todo']} todo, "
        f"{counts['variadic']} variadic"
    )


def load_manifest(path):
    with open(path, "rb") as file:
        document = tomllib.load(file)
    metadata = document.get("manifest")
    entries = document.get("api", [])
    if not metadata or not entries:
        raise SystemExit(f"{path}: manifest is missing [manifest] or [[api]]")
    missing_signatures = [entry for entry in entries if "signature" not in entry]
    if missing_signatures:
        raise SystemExit(
            f"{path}: manifest has no signatures; regenerate it with api_inventory.py generate"
        )
    return metadata, entries


def inventory(entries):
    """The part of the manifest that coverage depends on: which entries exist and how they are
    classified. Line numbers and reasons may move without changing coverage."""
    return {
        (entry["name"], entry["arity"], entry["source"]): (
            entry.get("signature", ""),
            entry["status"],
        )
        for entry in entries
    }


def check(args):
    metadata, entries = load_manifest(args.manifest)
    problems = []
    ids = set()
    for entry in entries:
        key = (entry["name"], entry["arity"], entry["source"])
        if key in ids:
            problems.append(f"duplicate entry {key}")
        ids.add(key)
        status = entry["status"]
        if status not in ("implemented", "empty", "todo"):
            problems.append(f"{key}: unknown status {status}")
        if status in ("empty", "todo") and not entry.get("reason"):
            problems.append(f"{key}: excluded entry lacks a reason")
    counts = {
        "bindgen_definitions": len(entries),
        "unique_names": len({entry["name"] for entry in entries}),
        "implemented": sum(entry["status"] == "implemented" for entry in entries),
        "empty": sum(entry["status"] == "empty" for entry in entries),
        "todo": sum(entry["status"] == "todo" for entry in entries),
    }
    for key, value in counts.items():
        if metadata.get(key) != value:
            problems.append(f"{key}: manifest says {metadata.get(key)}, entries say {value}")
    if metadata.get("eligible") != counts["implemented"]:
        problems.append("eligible must equal implemented")
    if problems:
        for problem in problems:
            print("error:", problem, file=sys.stderr)
        return 1
    print(
        f"manifest ok: {counts['bindgen_definitions']} definitions, "
        f"{counts['implemented']} eligible, {counts['empty']} empty, {counts['todo']} todo"
    )
    return 0


def diff_inventory(recorded, generated):
    removed = sorted(set(recorded) - set(generated))
    added = sorted(set(generated) - set(recorded))
    changed = sorted(
        key for key in set(recorded) & set(generated)
        if recorded[key] != generated[key]
    )
    lines = []
    for key in added:
        lines.append(f"  + {key[0]}({key[1]}) [{key[2]}]: {generated[key][1]}")
    for key in removed:
        lines.append(f"  - {key[0]}({key[1]}) [{key[2]}]: {recorded[key][1]}")
    for key in changed:
        if recorded[key][1] != generated[key][1]:
            lines.append(
                f"  ~ {key[0]}({key[1]}) [{key[2]}]: status "
                f"{recorded[key][1]} -> {generated[key][1]}"
            )
        if recorded[key][0] != generated[key][0]:
            lines.append(f"  ~ {key[0]}({key[1]}) [{key[2]}]: signature")
            lines.append(f"      recorded:  {recorded[key][0]}")
            lines.append(f"      generated: {generated[key][0]}")
    return lines


def verify(args):
    """Fail when the checked-in manifest no longer describes the bindgen and shim inputs."""
    _, recorded_entries = load_manifest(args.manifest)
    stub = load_stub_module(args.stub_bindings)
    recorded = inventory(recorded_entries)
    generated_entries = generate_entries(
        args.bindgen_dir, args.engine_src, stub
    )[0]
    generated = inventory(generated_entries)
    if recorded == generated:
        print(
            f"manifest ok: {len(recorded)} entries match "
            f"{args.bindgen_dir} and {args.engine_src}"
        )
        return 0
    print(
        f"error: {args.manifest} is stale for the current mozjs bindgen or QuickJS shim",
        file=sys.stderr,
    )
    for line in diff_inventory(recorded, generated):
        print(line, file=sys.stderr)
    print(
        "Regenerate the manifest, add or update contract cases, then rerun coverage.",
        file=sys.stderr,
    )
    return 1


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("--bindgen-dir", required=True)
    generate_parser.add_argument("--engine-src", required=True)
    generate_parser.add_argument("--stub-bindings", required=True)
    generate_parser.add_argument("--output", required=True)
    generate_parser.add_argument("--mozjs-version", default="0.26.14")
    generate_parser.add_argument("--quickjs-revision", default="unknown")
    generate_parser.set_defaults(func=generate)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--manifest", required=True)
    check_parser.set_defaults(func=check)

    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--manifest", required=True)
    verify_parser.add_argument("--bindgen-dir", required=True)
    verify_parser.add_argument("--engine-src", required=True)
    verify_parser.add_argument("--stub-bindings", required=True)
    verify_parser.set_defaults(func=verify)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
