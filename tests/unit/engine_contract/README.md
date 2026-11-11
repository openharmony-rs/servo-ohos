# engine contract tests

Shared contract tests for the `mozjs` API. SpiderMonkey is the reference implementation: every
case asserts SpiderMonkey behavior, and the same source also runs against the QuickJS shim to
expose divergences. The engine is selected at build time by Servo's `[patch.crates-io]`, so the
test source never names an engine and never uses `mozjs::quickjs` or `qjs::`.

## Scope

The suite currently runs 76 cases over the implemented mozjs surface:

| Area | Cases | Source |
| --- | --- | --- |
| Buffer and typed arrays | 26 | `buffer_test.rs`, `typed_array_test.rs` |
| Objects and properties | 11 | `objects_test.rs`, `objects_extra_test.rs`, `property_test.rs` |
| Modules and clone | 11 | `module_test.rs`, `clone_test.rs` |
| Strings, eval and stencil | 5 | `strings_test.rs`, `strings_extra_test.rs`, `eval_test.rs`, `eval_ext_test.rs`, `stencil_test.rs` |
| Promises, realm and runtime | 5 | `promises_test.rs`, `realm_test.rs`, `runtime_test.rs` |
| Class, misc and errors | 5 | `class_test.rs`, `misc_test.rs`, `errors_test.rs` |
| Elements, calls and rooters | 5 | `contract_test.rs` |
| Proxies, realms and principals | 8 | `proxy_test.rs` |

Every case records the `apis` it calls. The coverage gate compares those names with
`api-manifest.toml`; the current release run covers all 631 `implemented` entries.

Known-unimplemented entries are outside this suite's scope until their capability is enabled.

`coverage` first regenerates the inventory from the `servo-quickjs` checkout in `QJS_DIR` and
compares it with `api-manifest.toml`. It fails if a bindgen entry was added, removed, or changed
status or signature without regenerating the manifest. The signature records the parameter and
return types, so changing a type, adding or removing a parameter, or reordering same-typed
parameters fails; renaming a parameter does not. This makes a mozjs upgrade that adds entry points
fail before coverage is computed: the manifest must be updated, then every new `implemented` entry
needs a case. Line-number and file movements in the shim do not fail this check.

## Tool tests

Run the manifest tool tests with:

```bash
python3 -m unittest discover -s tests/unit/engine_contract/tools/tests
```

## Engine policy

- This suite is Servo's source of engine contract cases, and every case must pass on
  SpiderMonkey. It is the diagnostic reference, not the QuickJS upgrade gate.
- The QuickJS upgrade gate lives in `servo-quickjs/engine-contract`: `qjs-baseline.jsonl` is the
  strong gate, and a SpiderMonkey difference is review evidence rather than an automatic failure.
- Do not weaken an assertion to make a shim green. Accepted QuickJS behavior changes are recorded
  in the QJS baseline after review.

## Running

```bash
tests/unit/engine_contract/tools/run.sh sm       # SpiderMonkey reference
tests/unit/engine_contract/tools/run.sh qjs      # local servo-quickjs checkout
tests/unit/engine_contract/tools/run.sh diff
tests/unit/engine_contract/tools/run.sh manifest # api-manifest.toml matches the shim and bindgen
tests/unit/engine_contract/tools/run.sh coverage # manifest check plus API coverage
```

Set `QJS_DIR` if `servo-quickjs` is not next to this checkout. Set `OUT_DIR` to change where the
JSONL files are written. The script always uses absolute paths for `CONTRACT_OUT`.

When `CONTRACT_OUT` is unset the tests still assert their expectations, but no JSONL is written.
The file is truncated at the start of each run and appended per case.

Cases are isolated from each other. A failed assertion records
`{"fields": {"panic": "<message>"}, "apis": []}` for that case and the run continues with the
next case. The test still fails after every case has run, and the failure summary lists each
failed case, so one engine defect cannot hide the cases that follow it.

CI runs `coverage` after the unit tests, with `CONTRACT_OUT` pointing at the JSONL of that run. It
checks out the `servo-quickjs` revision pinned in `support/quickjs/cargo-config.toml` and passes it
as `QJS_DIR`, so a mozjs or shim update cannot land with a stale manifest or without cases for the
newly implemented entry points.

## Comparing

Each case records its normalized value, the pending exception name, and the `apis` it calls. A
case that panics records a `panic` field instead.
`compare.py` ignores the `engine` field. `known-differences.toml` remains for the legacy
SpiderMonkey/QuickJS comparison; the QuickJS upgrade gate is the baseline checker in
`servo-quickjs/engine-contract/tools/check_baseline.py`.
