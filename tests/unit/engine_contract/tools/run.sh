#!/usr/bin/env bash
# Run the engine contract suite against SpiderMonkey (the reference) or QuickJS, then compare.
#
# Usage:
#   tools/run.sh sm       [cargo test args...]
#   tools/run.sh qjs      [cargo test args...]
#   tools/run.sh diff
#   tools/run.sh manifest   check api-manifest.toml against the current mozjs bindgen and shim
#   tools/run.sh coverage   check the coverage of api-manifest.toml in sm.jsonl
#   tools/run.sh all
#
# Environment:
#   QJS_DIR  local servo-quickjs checkout (default: ../servo-quickjs)
#   OUT_DIR  output directory (default: target/contract)

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../../.." && pwd)"
out_dir="${OUT_DIR:-$repo_root/target/contract}"
qjs_dir="${QJS_DIR:-$repo_root/../servo-quickjs}"

run_sm() {
    mkdir -p "$out_dir"
    cd "$repo_root"
    CONTRACT_ENGINE=spidermonkey CONTRACT_OUT="$out_dir/sm.jsonl" \
        cargo test --release -p engine_contract_tests "$@"
}

run_qjs() {
    mkdir -p "$out_dir"
    cd "$repo_root"
    CONTRACT_ENGINE=quickjs CONTRACT_OUT="$out_dir/qjs.jsonl" \
        cargo test --release -p engine_contract_tests --features js-quickjs \
        --config "patch.crates-io.mozjs.path=\"$qjs_dir/mozjs\"" "$@"
}

run_diff() {
    [[ -f "$out_dir/sm.jsonl" && -f "$out_dir/qjs.jsonl" ]] || {
        echo "missing $out_dir/sm.jsonl or $out_dir/qjs.jsonl; run '$0 sm' and '$0 qjs' first" >&2
        return 1
    }
    cd "$repo_root"
    python3 tests/unit/engine_contract/tools/compare.py \
        "$out_dir/sm.jsonl" "$out_dir/qjs.jsonl" \
        tests/unit/engine_contract/known-differences.toml
}

run_manifest() {
    [[ -f "$qjs_dir/mozjs/upstream-bindgen/jsapi.rs" ]] || {
        echo "missing $qjs_dir/mozjs/upstream-bindgen; set QJS_DIR to a servo-quickjs checkout" >&2
        return 1
    }
    cd "$repo_root"
    python3 tests/unit/engine_contract/tools/api_inventory.py verify \
        --manifest tests/unit/engine_contract/api-manifest.toml \
        --bindgen-dir "$qjs_dir/mozjs/upstream-bindgen" \
        --engine-src "$qjs_dir/mozjs/src/engine" \
        --stub-bindings "$qjs_dir/mozjs/stub_bindings.py"
}

run_coverage() {
    run_manifest
    [[ -f "$out_dir/sm.jsonl" ]] || {
        echo "missing $out_dir/sm.jsonl; run '$0 sm' first" >&2
        return 1
    }
    cd "$repo_root"
    python3 tests/unit/engine_contract/tools/check_api_coverage.py \
        tests/unit/engine_contract/api-manifest.toml "$out_dir/sm.jsonl"
}

command="${1:-all}"
if [[ $# -gt 0 ]]; then
    shift
fi

case "$command" in
    sm) run_sm "$@" ;;
    qjs) run_qjs "$@" ;;
    diff) run_diff ;;
    manifest) run_manifest ;;
    coverage) run_coverage ;;
    all)
        run_sm
        run_qjs
        run_diff
        run_coverage
        ;;
    *)
        echo "usage: $0 {sm|qjs|diff|manifest|coverage|all} [cargo test args...]" >&2
        exit 2
        ;;
esac
