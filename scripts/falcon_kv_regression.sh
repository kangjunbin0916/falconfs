#!/usr/bin/env bash
# ============================================================================
# FalconFS KV cache regression harness (v6.5)
# ============================================================================
# Single fail-fast wrapper that runs the entire KV-cache regression in one
# command:
#
#   1. Build (calls scripts/falcon_distributed_test.sh build)
#   2. ctest (FalconKVPrimitivesUT, label kv-unit)
#   3. Cluster up (1 CN + 2 DNs + 2 Clients)
#   4. FalconFS distributed file ops — `falcon_distributed_test.sh test` (client
#      mounts: read/write/cross-visibility/large file/concurrent writes)
#   5. C++ E2E suites (same driver):
#         kv-test, kv-meta-stress-test, kv-cluster-test,
#         kv-fault-test, kv-cluster-fault-test
#   6. When REGRESSION_PROFILE=full only (P8 final gate):
#         kv-cluster-failover-test, kv-topology-test, kv-mixed-colocation-test,
#         kv-cluster-promote-test
#   7. Python unittest under vllm_kv_cache/test/ (excluding vllm/ subfolder), including
#         ``test_offloading_manager_cluster_mixed_e2e`` (vLLM OffloadingManager meta+data
#         BRPC against 3 DNs + 4 stores). Full profile fails fast if that topology is down.
#   8. Cluster down
#   9. Coloured summary
#
# Knobs (all env-driven, all optional):
#   REGRESSION_PROFILE   - smoke (default) | full (adds failover/topology/mixed-
#                          colocation/promote suites; defaults KV_THREE_DNS=1 and
#                          STORE_COUNT=4 for 3-DN + 4-store topology)
#   KEEP_LOGS            - 1 to keep logs even on success (default: 0)
#   STOP_ON_FAILURE      - 0 to keep cluster up after first failure for debug
#                          (default: 1; cleanup runs in trap)
#   SKIP_BUILD           - 1 to skip the build step (use when iterating)
#   SKIP_CLUSTER         - 1 to skip cluster start/stop (e.g. cluster already up)
#   PYTHON_BIN           - override python interpreter (default: python3)
#
# Exit codes:
#   0  - all suites passed
#   1+ - first failed suite's order (see main() in this file). Logs:
#        logs/regression/<ts>/
# ============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DRIVER="$SCRIPT_DIR/falcon_distributed_test.sh"
BUILD_DIR="${BUILD_DIR:-$PROJECT_DIR/build}"

PROFILE="${REGRESSION_PROFILE:-smoke}"
KEEP_LOGS="${KEEP_LOGS:-0}"
STOP_ON_FAILURE="${STOP_ON_FAILURE:-1}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_CLUSTER="${SKIP_CLUSTER:-0}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_ROOT="${REGRESSION_LOG_ROOT:-$PROJECT_DIR/logs/regression}"
LOG_DIR="$LOG_ROOT/$TS"
mkdir -p "$LOG_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log_step() { echo -e "${BLUE}${BOLD}[STEP]${NC} $*"; }
log_info() { echo -e "${GREEN}[ OK ]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}${BOLD}[FAIL]${NC} $*"; }

# Suite results: NAME|STATUS|DURATION_S|LOG
SUITE_RESULTS=()
SUITE_FAILED=""
SUITE_FAILED_ORDER=0
START_EPOCH="$(date +%s)"

cleanup() {
    local rc=$?
    if [ -n "$SUITE_FAILED" ] && [ "$STOP_ON_FAILURE" = "0" ]; then
        log_warn "Failure detected ($SUITE_FAILED); STOP_ON_FAILURE=0 keeping cluster up"
    elif [ "$SKIP_CLUSTER" = "0" ]; then
        # Best-effort stop, ignore exit code since we're already in cleanup.
        if bash "$DRIVER" stop >>"$LOG_DIR/zz_cleanup_stop.log" 2>&1; then
            :
        fi
    fi
    print_summary
    exit $rc
}
trap cleanup EXIT
trap 'echo; log_warn "Interrupted"; exit 130' INT TERM

run_suite() {
    local order="$1"
    local name="$2"
    shift 2
    local logfile="$LOG_DIR/${order}_${name}.log"
    local t0 t1 dur rc
    log_step "[$order] $name"
    t0="$(date +%s)"
    if "$@" >>"$logfile" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    t1="$(date +%s)"
    dur=$((t1 - t0))
    if [ $rc -eq 0 ]; then
        SUITE_RESULTS+=("$name|PASS|${dur}|$logfile")
        log_info "[$order] $name passed in ${dur}s"
    else
        SUITE_RESULTS+=("$name|FAIL|${dur}|$logfile")
        SUITE_FAILED="$name"
        SUITE_FAILED_ORDER="$order"
        log_err "[$order] $name failed in ${dur}s (rc=$rc); see $logfile"
        # Echo last 30 lines of the log for quick triage.
        echo "----- tail $logfile -----"
        tail -n 30 "$logfile" || true
        echo "----- end tail -----"
        return $rc
    fi
    return 0
}

print_summary() {
    local end_epoch total_dur
    end_epoch="$(date +%s)"
    total_dur=$((end_epoch - START_EPOCH))
    echo
    echo "============================================================"
    echo " FalconFS KV regression summary  (profile: $PROFILE)"
    echo " logs:  $LOG_DIR"
    echo " total: ${total_dur}s"
    echo "============================================================"
    if [ ${#SUITE_RESULTS[@]} -eq 0 ]; then
        log_warn "No suites ran"
        return
    fi
    local pass_count=0 fail_count=0
    for entry in "${SUITE_RESULTS[@]}"; do
        local name="${entry%%|*}"
        local rest="${entry#*|}"
        local status="${rest%%|*}"
        rest="${rest#*|}"
        local dur="${rest%%|*}"
        local logf="${rest#*|}"
        if [ "$status" = "PASS" ]; then
            printf " ${GREEN}PASS${NC}  %-30s %5ss  %s\n" "$name" "$dur" "$logf"
            pass_count=$((pass_count + 1))
        else
            printf " ${RED}FAIL${NC}  %-30s %5ss  %s\n" "$name" "$dur" "$logf"
            fail_count=$((fail_count + 1))
        fi
    done
    echo "------------------------------------------------------------"
    printf " %d passed, %d failed\n" "$pass_count" "$fail_count"
    echo "============================================================"
    if [ "$fail_count" -eq 0 ] && [ "$KEEP_LOGS" = "0" ]; then
        # Trim noise on the happy path: keep the directory but remove
        # individual log files so the next run starts clean.
        find "$LOG_DIR" -maxdepth 1 -name '*.log' -delete 2>/dev/null || true
    fi
}

# ============================================================================
# Suite definitions
# ============================================================================

suite_build() {
    bash "$DRIVER" build
}

suite_ctest_unit() {
    if [ ! -d "$BUILD_DIR/tests/falcon_kv" ]; then
        echo "BUILD_DIR/tests/falcon_kv missing: $BUILD_DIR/tests/falcon_kv"
        return 1
    fi
    cd "$BUILD_DIR/tests/falcon_kv"
    ctest --output-on-failure -L kv-unit -j"$(nproc)"
}

suite_cluster_up() {
    if [ "$PROFILE" = "full" ]; then
        export KV_THREE_DNS="${KV_THREE_DNS:-1}"
        export STORE_COUNT="${STORE_COUNT:-4}"
        log_step "full profile: KV_THREE_DNS=${KV_THREE_DNS} STORE_COUNT=${STORE_COUNT}"
    fi
    bash "$DRIVER" start
}

# `falcon_distributed_test.sh test` — exercises FalconFS through the two client
# FUSE mounts (not KV-specific). Must pass before KV E2E assumes a healthy cluster.
suite_distributed_fs_test() {
    bash "$DRIVER" test
}

suite_kv_test() {
    bash "$DRIVER" kv-test
}

suite_kv_meta_stress_test() {
    # Smoke profile keeps the existing default (1 iteration). Full profile
    # bumps iterations to exercise more concurrency permutations.
    if [ "$PROFILE" = "full" ]; then
        export FALCON_KV_STRESS_ITERATIONS=4
    fi
    bash "$DRIVER" kv-meta-stress-test
}

suite_kv_cluster_test() {
    bash "$DRIVER" kv-cluster-test
}

suite_kv_fault_test() {
    bash "$DRIVER" kv-fault-test
}

suite_kv_cluster_fault_test() {
    bash "$DRIVER" kv-cluster-fault-test
}

suite_kv_cluster_failover_test() {
    bash "$DRIVER" kv-cluster-failover-test
}

suite_kv_topology_test() {
    bash "$DRIVER" kv-topology-test
}

suite_kv_mixed_colocation_test() {
    bash "$DRIVER" kv-mixed-colocation-test
}

suite_kv_cluster_promote_test() {
    bash "$DRIVER" kv-cluster-promote-test
}

suite_python_unittest() {
    cd "$PROJECT_DIR"
    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:$PROJECT_DIR/vllm_kv_cache/test:${PYTHONPATH:-}"
    if [ "$PROFILE" = "full" ]; then
        log_step "  full profile: require vLLM mixed-cluster topology (3 DNs + 4 stores) for Python E2E"
        if ! $PYTHON_BIN -c "
import sys
sys.path.insert(0, 'vllm_kv_cache/python')
sys.path.insert(0, 'vllm_kv_cache/test')
import test_offloading_manager_cluster_mixed_e2e as m
if not m.mixed_topology_available():
    print(
        'REGRESSION_FAIL: mixed topology unreachable (need KV_THREE_DNS=1, STORE_COUNT=4, '
        'CN + DN1/2/3 poolers + four falcon_kv_store BRPC ports). '
        'See vllm_kv_cache/test/test_offloading_manager_cluster_mixed_e2e.py',
        file=sys.stderr,
    )
    sys.exit(1)
"; then
            return 1
        fi
        export FALCON_MIX_E2E_QUICK="${FALCON_MIX_E2E_QUICK:-0}"
        log_step "  full profile: FALCON_MIX_E2E_QUICK=${FALCON_MIX_E2E_QUICK} (0 = larger mixed E2E defaults)"
    fi
    # All Python tests directly under vllm_kv_cache/test/ are
    # unittest.TestCase-based; avoid a pytest dependency. The vllm/
    # subfolder requires vLLM at import time and is opt-in.
    # The cluster_brpc test self-skips if BRPC endpoints are unreachable.
    local modules=()
    while IFS= read -r f; do
        local base
        base="$(basename "$f" .py)"
        modules+=("$base")
    done < <(find "$PROJECT_DIR/vllm_kv_cache/test" -maxdepth 1 -type f -name 'test_*.py' | sort)
    if [ ${#modules[@]} -eq 0 ]; then
        echo "No Python test modules found"
        return 1
    fi
    echo "Running unittest modules: ${modules[*]}"
    $PYTHON_BIN -m unittest -v "${modules[@]}"
}

suite_cluster_down() {
    bash "$DRIVER" stop
}

# ============================================================================
# Driver
# ============================================================================

main() {
    log_step "FalconFS KV regression harness  (profile: $PROFILE)"
    log_step "Logs: $LOG_DIR"

    if [ "$SKIP_BUILD" = "1" ]; then
        log_warn "[1] build SKIPPED (SKIP_BUILD=1)"
    else
        run_suite 1 build suite_build || exit 1
    fi

    run_suite 2 ctest_unit suite_ctest_unit || exit 2

    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[3] cluster_up SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite 3 cluster_up suite_cluster_up || exit 3
        if [ -f /tmp/falcon_kv_store_env.sh ]; then
            set -a
            # shellcheck disable=SC1090
            . /tmp/falcon_kv_store_env.sh
            set +a
        fi
    fi

    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[4] distributed_fs_test SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite 4 distributed_fs_test suite_distributed_fs_test || exit 4
    fi

    run_suite 5 kv_test               suite_kv_test               || exit 5
    run_suite 6 kv_meta_stress_test   suite_kv_meta_stress_test   || exit 6
    run_suite 7 kv_cluster_test       suite_kv_cluster_test       || exit 7
    run_suite 8 kv_fault_test         suite_kv_fault_test         || exit 8
    run_suite 9 kv_cluster_fault_test suite_kv_cluster_fault_test || exit 9

    local py_order=10
    if [ "$PROFILE" = "full" ]; then
        run_suite 10 kv_cluster_failover_test suite_kv_cluster_failover_test || exit 10
        run_suite 11 kv_topology_test suite_kv_topology_test || exit 11
        run_suite 12 kv_mixed_colocation_test suite_kv_mixed_colocation_test || exit 12
        run_suite 13 kv_cluster_promote_test suite_kv_cluster_promote_test || exit 13
        py_order=14
    fi

    run_suite "$py_order" python_unittest suite_python_unittest || exit "$py_order"

    local down_order=$((py_order + 1))
    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[${down_order}] cluster_down SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite "$down_order" cluster_down suite_cluster_down || exit "$down_order"
    fi
}

main "$@"
