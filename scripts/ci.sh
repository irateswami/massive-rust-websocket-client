#!/usr/bin/env bash
#
# ci.sh — Run all tests, checks, profiling, and benchmarks.
#
# Usage:
#   ./scripts/ci.sh           # run everything
#   ./scripts/ci.sh --quick   # skip slow profiling passes (samply, flamegraph)
#
# Prerequisites:
#   cargo install cargo-deny cargo-geiger flamegraph samply cargo-bloat cargo-llvm-lines cargo-fuzz
#   rustup +nightly component add miri
#
# Outputs are written to ci-out/<timestamp>/.
#
set -euo pipefail

QUICK=false
[[ "${1:-}" == "--quick" ]] && QUICK=true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="$PROJECT_DIR/ci-out/$TIMESTAMP"
mkdir -p "$OUT_DIR"
# Symlink ci-out/latest → this run
ln -sfn "$TIMESTAMP" "$PROJECT_DIR/ci-out/latest"

BOLD="\033[1m"
GREEN="\033[1;32m"
RED="\033[1;31m"
CYAN="\033[1;36m"
RESET="\033[0m"

PASS=0
FAIL=0
SKIP=0
RESULTS=()

run_step() {
    local name="$1"
    shift
    local logfile="$OUT_DIR/$(echo "$name" | tr ' ()/' '____').log"
    printf "${CYAN}▶ %-40s${RESET}" "$name"
    if "$@" > "$logfile" 2>&1; then
        printf "${GREEN}PASS${RESET}\n"
        RESULTS+=("PASS  $name")
        ((PASS++)) || true
    else
        printf "${RED}FAIL${RESET}\n"
        RESULTS+=("FAIL  $name")
        ((FAIL++)) || true
        tail -20 "$logfile" | sed 's/^/       /'
    fi
}

skip_step() {
    local name="$1"
    printf "${CYAN}▶ %-40s${RESET}SKIP\n" "$name"
    RESULTS+=("SKIP  $name")
    ((SKIP++)) || true
}

printf "\n${CYAN}Output directory: ${RESET}${OUT_DIR}\n"

# ── Build ────────────────────────────────────────────────────────────────

printf "\n${BOLD}═══ Build ═══${RESET}\n\n"

run_step "cargo check"                    cargo check --all-targets
run_step "cargo fmt --check"              cargo fmt -- --check
run_step "cargo clippy"                   cargo clippy --all-targets

# Build timings — shows which crates dominate compile time.
run_step "cargo build --timings"          cargo build --timings --release
# Move the generated HTML report to output.
if [[ -f target/cargo-timings/cargo-timing.html ]]; then
    cp target/cargo-timings/cargo-timing.html "$OUT_DIR/cargo-timing.html"
    printf "       ${CYAN}→ Open: ci-out/latest/cargo-timing.html${RESET}\n"
fi

# ── Tests ────────────────────────────────────────────────────────────────

printf "\n${BOLD}═══ Tests ═══${RESET}\n\n"

run_step "cargo test (unit + integration)" cargo test
run_step "cargo test (loom)"               cargo test --test loom_tests
run_step "cargo test (alloc assertions)"   cargo test --test alloc_test
run_step "cargo test (proptest)"           cargo test --test proptest_tests
run_step "cargo bench --test"              cargo bench --bench benchmarks -- --test
run_step "cargo bench --test (throughput)" cargo bench --bench throughput -- --test

# ── Miri (memory safety) ────────────────────────────────────────────────

printf "\n${BOLD}═══ Miri ═══${RESET}\n\n"

if rustup run nightly cargo miri --version > /dev/null 2>&1; then
    # Miri can't run async/tokio tests (no syscall support).
    # Run only sync tests: model deser, subscription, config.
    run_step "miri (models)"               rustup run nightly cargo miri test websocket::models::tests
    run_step "miri (subscription)"         rustup run nightly cargo miri test websocket::subscription::tests
    run_step "miri (config)"               rustup run nightly cargo miri test websocket::config::tests
else
    skip_step "cargo +nightly miri test (miri not installed)"
fi

# ── Audit ────────────────────────────────────────────────────────────────

printf "\n${BOLD}═══ Audit ═══${RESET}\n\n"

if command -v cargo-deny > /dev/null 2>&1; then
    run_step "cargo deny (advisories)"     cargo deny check advisories
    run_step "cargo deny (licenses)"       cargo deny check licenses
    run_step "cargo deny (bans)"           cargo deny check bans
    run_step "cargo deny (sources)"        cargo deny check sources
else
    skip_step "cargo deny (not installed)"
fi

if command -v cargo-geiger > /dev/null 2>&1; then
    # geiger exits non-zero when it finds unsafe in deps (expected for tokio/ring).
    # We run it for the report, not as a gate.
    local_logfile="$OUT_DIR/cargo_geiger.log"
    printf "${CYAN}▶ %-40s${RESET}" "cargo geiger"
    if cargo geiger --quiet > "$local_logfile" 2>&1; then
        printf "${GREEN}PASS${RESET}\n"
    else
        printf "${GREEN}DONE${RESET} (unsafe found in deps — see output)\n"
    fi
    RESULTS+=("PASS  cargo geiger")
    ((PASS++)) || true
else
    skip_step "cargo geiger (not installed)"
fi

# ── Binary Analysis ──────────────────────────────────────────────────────

printf "\n${BOLD}═══ Binary Analysis ═══${RESET}\n\n"

if command -v cargo-bloat > /dev/null 2>&1; then
    local_logfile="$OUT_DIR/cargo_bloat.log"
    printf "${CYAN}▶ %-40s${RESET}" "cargo bloat"
    if cargo bloat --release -n 30 > "$local_logfile" 2>&1; then
        printf "${GREEN}PASS${RESET}\n"
        printf "       ${CYAN}→ See: ci-out/latest/cargo_bloat.log${RESET}\n"
    else
        printf "${GREEN}DONE${RESET}\n"
    fi
    RESULTS+=("PASS  cargo bloat")
    ((PASS++)) || true
else
    skip_step "cargo bloat (not installed)"
fi

if command -v cargo-llvm-lines > /dev/null 2>&1; then
    local_logfile="$OUT_DIR/cargo_llvm_lines.log"
    printf "${CYAN}▶ %-40s${RESET}" "cargo llvm-lines"
    if cargo llvm-lines --release 2>/dev/null | head -40 > "$local_logfile" 2>&1; then
        printf "${GREEN}PASS${RESET}\n"
        printf "       ${CYAN}→ See: ci-out/latest/cargo_llvm_lines.log${RESET}\n"
    else
        printf "${GREEN}DONE${RESET}\n"
    fi
    RESULTS+=("PASS  cargo llvm-lines")
    ((PASS++)) || true
else
    skip_step "cargo llvm-lines (not installed)"
fi

# ── Profiling ────────────────────────────────────────────────────────────

printf "\n${BOLD}═══ Profiling ═══${RESET}\n\n"

# dhat heap profiling (always runs — fast)
run_step "dhat heap profile"               cargo run --example heap_profile --features dhat-heap
# dhat writes to cwd; move the output if it was generated
[[ -f dhat-heap.json ]] && mv dhat-heap.json "$OUT_DIR/dhat-heap.json"

if $QUICK; then
    skip_step "criterion benchmarks (--quick)"
    skip_step "throughput benchmark (--quick)"
    skip_step "samply CPU profile (--quick)"
    skip_step "flamegraph (--quick)"
else
    # Criterion benchmarks — output lands in target/criterion/
    run_step "criterion benchmarks"        cargo bench --bench benchmarks

    # End-to-end throughput benchmark
    run_step "throughput benchmark"         cargo bench --bench throughput
    # Copy criterion report if present
    if [[ -d target/criterion ]]; then
        cp -r target/criterion "$OUT_DIR/criterion"
    fi

    # samply CPU profile (build only — interactive UI can't run in CI)
    if command -v samply > /dev/null 2>&1; then
        run_step "build profile binary"    cargo build --example profile --release
        printf "       ${CYAN}→ Run interactively: samply record target/release/examples/profile${RESET}\n"
    else
        skip_step "samply (not installed)"
    fi

    # flamegraph (requires sudo on macOS for dtrace — skip in non-interactive)
    if command -v cargo-flamegraph > /dev/null 2>&1; then
        if sudo -n true 2>/dev/null; then
            run_step "flamegraph"          cargo flamegraph --example profile -o "$OUT_DIR/flamegraph.svg" --root
            printf "       ${CYAN}→ Open: ci-out/latest/flamegraph.svg${RESET}\n"
        else
            skip_step "flamegraph (requires sudo — run manually with: sudo cargo flamegraph --example profile)"
        fi
    else
        skip_step "flamegraph (not installed)"
    fi
fi

# ── Summary ──────────────────────────────────────────────────────────────

printf "\n${BOLD}═══ Summary ═══${RESET}\n\n"

SUMMARY=""
for r in "${RESULTS[@]}"; do
    case "$r" in
        PASS*) line="  ✓ ${r#PASS  }" ;  printf "  ${GREEN}✓${RESET} ${r#PASS  }\n" ;;
        FAIL*) line="  ✗ ${r#FAIL  }" ;  printf "  ${RED}✗${RESET} ${r#FAIL  }\n" ;;
        SKIP*) line="  - ${r#SKIP  }" ;  printf "  - ${r#SKIP  }\n" ;;
    esac
    SUMMARY+="$line"$'\n'
done

FOOTER="${PASS} passed"
[[ $FAIL -gt 0 ]] && FOOTER+=", ${FAIL} failed"
[[ $SKIP -gt 0 ]] && FOOTER+=", ${SKIP} skipped"

printf "\n  ${GREEN}${PASS} passed${RESET}"
[[ $FAIL -gt 0 ]] && printf ", ${RED}${FAIL} failed${RESET}"
[[ $SKIP -gt 0 ]] && printf ", ${SKIP} skipped"
printf "\n  ${CYAN}Output: ${RESET}ci-out/latest/\n\n"

# Write a plain-text summary into the output directory
cat > "$OUT_DIR/summary.txt" <<EOF
ci.sh run: $TIMESTAMP
$(if $QUICK; then echo "mode: --quick"; else echo "mode: full"; fi)

$SUMMARY
$FOOTER
EOF

[[ $FAIL -eq 0 ]]
