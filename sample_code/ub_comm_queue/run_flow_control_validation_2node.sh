#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
SAMPLE_DIR="${ROOT_DIR}/sample_code/ub_comm_queue"
SRC="${SAMPLE_DIR}/flow_control_queue_test.cpp"

MODE="${MODE:-help}"
BIN="${BIN:-${SAMPLE_DIR}/flow_control_queue_test}"
OUT="${OUT:-${BIN}}"
SHM_CREATOR="${SHM_CREATOR:-./node_ubsm_shm_creator}"

TEST_ID="${TEST_ID:-manual}"
SENDER_SHM="${SENDER_SHM:-flow_sender_${TEST_ID}}"
RECEIVER_SHM="${RECEIVER_SHM:-flow_receiver_${TEST_ID}}"
SENDER_HOST="${SENDER_HOST:-slot1}"
RECEIVER_HOST="${RECEIVER_HOST:-slot2}"

CASE_NAME="${CASE_NAME:-all}"
CAPACITY="${CAPACITY:-64}"
THRESHOLD="${THRESHOLD:-50}"
PERF_MESSAGES="${PERF_MESSAGES:-20000}"
WAIT_SECONDS="${WAIT_SECONDS:-20}"
LOG_LEVEL="${LOG_LEVEL:-3}"

CXX="${CXX:-g++}"
CXXFLAGS_EXTRA="${CXXFLAGS_EXTRA:-}"
LDFLAGS_EXTRA="${LDFLAGS_EXTRA:-}"
COMM_INCLUDE="${COMM_INCLUDE:-${ROOT_DIR}/include}"
COMM_SRC_INCLUDE="${COMM_SRC_INCLUDE:-${ROOT_DIR}/src/ub_comm_queue}"
UBSM_INCLUDE="${UBSM_INCLUDE:-/usr/local/ubs_mem/include}"
COMM_LIB_DIR="${COMM_LIB_DIR:-/usr/lib64}"
UBSM_LIB_DIR="${UBSM_LIB_DIR:-/usr/local/ubs_mem/lib}"

usage()
{
    cat <<EOF
Two-node ub_comm_queue flow-control validation.

This script is meant to be copied to both nodes together with flow_control_queue_test.
It does not ssh to the other node.

Common environment:
  TEST_ID=${TEST_ID}
  SENDER_HOST=${SENDER_HOST}
  RECEIVER_HOST=${RECEIVER_HOST}
  SENDER_SHM=${SENDER_SHM}
  RECEIVER_SHM=${RECEIVER_SHM}
  BIN=${BIN}
  SHM_CREATOR=${SHM_CREATOR}

Modes:
  MODE=build      Compile flow_control_queue_test on a build machine.
  MODE=create     Create slot1/slot2 exported shared memory.
  MODE=run-b      Run role B. Execute this on ${RECEIVER_HOST}.
  MODE=run-a      Run role A. Execute this on ${SENDER_HOST} after role B is ready.
  MODE=cleanup    Delete slot1/slot2 exported shared memory.

Build example on build machine:
  MODE=build OUT=/tmp/flow_control_queue_test bash $0

Copy example:
  scp /tmp/flow_control_queue_test ${SENDER_HOST}:/path/to/flow_control_queue_test
  scp /tmp/flow_control_queue_test ${RECEIVER_HOST}:/path/to/flow_control_queue_test
  scp $0 ${SENDER_HOST}:/path/to/
  scp $0 ${RECEIVER_HOST}:/path/to/

Two-node run example:
  # On either node that can run node_ubsm_shm_creator:
  TEST_ID=t001 MODE=create SHM_CREATOR=./node_ubsm_shm_creator bash $0

  # On ${RECEIVER_HOST}:
  TEST_ID=t001 MODE=run-b BIN=/path/to/flow_control_queue_test bash $0

  # On ${SENDER_HOST}:
  TEST_ID=t001 MODE=run-a BIN=/path/to/flow_control_queue_test CASE_NAME=all bash $0

  # Cleanup:
  TEST_ID=t001 MODE=cleanup SHM_CREATOR=./node_ubsm_shm_creator bash $0

Hot-update interval case:
  TEST_ID=t002 MODE=create CAPACITY=2048 bash $0
  TEST_ID=t002 MODE=run-b CAPACITY=2048 BIN=/path/to/flow_control_queue_test bash $0
  TEST_ID=t002 MODE=run-a CAPACITY=2048 CASE_NAME=hotupdate BIN=/path/to/flow_control_queue_test bash $0
  TEST_ID=t002 MODE=cleanup bash $0
EOF
}

require_binary()
{
    if [[ ! -x "${BIN}" ]]; then
        echo "[error] flow_control_queue_test not found or not executable: ${BIN}" >&2
        echo "        Build it with MODE=build, copy it to this node, or set BIN=/path/to/binary." >&2
        exit 1
    fi
}

require_creator()
{
    if [[ ! -x "${SHM_CREATOR}" ]]; then
        echo "[error] node_ubsm_shm_creator not found or not executable: ${SHM_CREATOR}" >&2
        echo "        Set SHM_CREATOR=/path/to/node_ubsm_shm_creator." >&2
        exit 1
    fi
}

build_binary()
{
    mkdir -p "$(dirname "${OUT}")"
    echo "[build] ${OUT}"
    "${CXX}" -O2 -g -std=c++17 ${CXXFLAGS_EXTRA} -o "${OUT}" "${SRC}" \
        -I"${COMM_INCLUDE}" \
        -I"${COMM_SRC_INCLUDE}" \
        -I"${UBSM_INCLUDE}" \
        -L"${COMM_LIB_DIR}" \
        -L"${UBSM_LIB_DIR}" \
        ${LDFLAGS_EXTRA} \
        -lubturbo_tdsql \
        -lubsm_sdk \
        -lpthread \
        -Wl,-rpath,"${UBSM_LIB_DIR}"
}

create_shm()
{
    require_creator
    echo "[shm] recreate ${SENDER_HOST}:${SENDER_SHM}"
    "${SHM_CREATOR}" delete "${SENDER_HOST}" "${SENDER_SHM}" >/dev/null 2>&1 || true
    "${SHM_CREATOR}" create "${SENDER_HOST}" "${SENDER_SHM}"

    echo "[shm] recreate ${RECEIVER_HOST}:${RECEIVER_SHM}"
    "${SHM_CREATOR}" delete "${RECEIVER_HOST}" "${RECEIVER_SHM}" >/dev/null 2>&1 || true
    "${SHM_CREATOR}" create "${RECEIVER_HOST}" "${RECEIVER_SHM}"
}

cleanup_shm()
{
    require_creator
    echo "[shm] delete ${SENDER_HOST}:${SENDER_SHM}"
    "${SHM_CREATOR}" delete "${SENDER_HOST}" "${SENDER_SHM}" >/dev/null 2>&1 || true
    echo "[shm] delete ${RECEIVER_HOST}:${RECEIVER_SHM}"
    "${SHM_CREATOR}" delete "${RECEIVER_HOST}" "${RECEIVER_SHM}" >/dev/null 2>&1 || true
}

run_role()
{
    local role=$1
    require_binary
    echo "[run] role=${role} sender_shm=${SENDER_SHM} receiver_shm=${RECEIVER_SHM} case=${CASE_NAME} capacity=${CAPACITY}"
    "${BIN}" --role "${role}" \
        -s "${SENDER_SHM}" \
        -r "${RECEIVER_SHM}" \
        --case "${CASE_NAME}" \
        --capacity "${CAPACITY}" \
        --threshold "${THRESHOLD}" \
        --perf-messages "${PERF_MESSAGES}" \
        --wait "${WAIT_SECONDS}" \
        --log-level "${LOG_LEVEL}"
}

case "${MODE}" in
    build)
        build_binary
        ;;
    create)
        create_shm
        ;;
    run-b)
        run_role B
        ;;
    run-a)
        run_role A
        ;;
    cleanup)
        cleanup_shm
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        echo "[error] unknown MODE=${MODE}" >&2
        usage
        exit 1
        ;;
esac
