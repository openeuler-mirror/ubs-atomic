#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
SAMPLE_DIR="${ROOT_DIR}/sample_code/ub_comm_queue"
BUILD_DIR="${BUILD_DIR:-/tmp/ub_comm_queue_flow_validation}"
BIN="${BIN:-${SAMPLE_DIR}/flow_control_queue_test}"
SRC="${SAMPLE_DIR}/flow_control_queue_test.cpp"
SHM_CREATOR="${SHM_CREATOR:-./node_ubsm_shm_creator}"
CXX="${CXX:-g++}"
BUILD_LOCAL="${BUILD_LOCAL:-0}"

SENDER_SHM="${SENDER_SHM:-flow_sender_$$}"
RECEIVER_SHM="${RECEIVER_SHM:-flow_receiver_$$}"
SENDER_HOST="${SENDER_HOST:-slot1}"
RECEIVER_HOST="${RECEIVER_HOST:-slot2}"
CASE_NAME="${CASE_NAME:-all}"
CAPACITY="${CAPACITY:-64}"
THRESHOLD="${THRESHOLD:-50}"
PERF_MESSAGES="${PERF_MESSAGES:-20000}"
WAIT_SECONDS="${WAIT_SECONDS:-20}"
LOG_LEVEL="${LOG_LEVEL:-3}"
RUN_MATRIX="${RUN_MATRIX:-0}"
CAPACITY_MATRIX="${CAPACITY_MATRIX:-64 256 2048}"
PERF_MESSAGES_MATRIX="${PERF_MESSAGES_MATRIX:-4096 8192 4096}"

CXXFLAGS_EXTRA="${CXXFLAGS_EXTRA:-}"
LDFLAGS_EXTRA="${LDFLAGS_EXTRA:-}"
COMM_INCLUDE="${COMM_INCLUDE:-${ROOT_DIR}/include}"
COMM_SRC_INCLUDE="${COMM_SRC_INCLUDE:-${ROOT_DIR}/src/ub_comm_queue}"
UBSM_INCLUDE="${UBSM_INCLUDE:-/usr/local/ubs_mem/include}"
COMM_LIB_DIR="${COMM_LIB_DIR:-/usr/lib64}"
UBSM_LIB_DIR="${UBSM_LIB_DIR:-/usr/local/ubs_mem/lib}"
CREATED_SHMS=()

cleanup()
{
    local code=$?
    if [[ -n "${B_PID:-}" ]]; then
        if kill -0 "${B_PID}" 2>/dev/null; then
            kill "${B_PID}" 2>/dev/null || true
        fi
        wait "${B_PID}" 2>/dev/null || true
    fi
    if [[ -x "${SHM_CREATOR}" ]]; then
        for item in "${CREATED_SHMS[@]:-}"; do
            local host="${item%%:*}"
            local shm="${item#*:}"
            "${SHM_CREATOR}" delete "${host}" "${shm}" >/dev/null 2>&1 || true
        done
    fi
    exit "${code}"
}
trap cleanup EXIT

if [[ "${BUILD_LOCAL}" == "1" ]]; then
    mkdir -p "$(dirname "${BIN}")"

    echo "[build] ${BIN}"
    "${CXX}" -O2 -g -std=c++17 ${CXXFLAGS_EXTRA} -o "${BIN}" "${SRC}" \
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
else
    echo "[binary] ${BIN}"
fi

if [[ ! -x "${BIN}" ]]; then
    echo "[error] flow_control_queue_test not found or not executable: ${BIN}" >&2
    echo "        Copy it from the build machine, or set BUILD_LOCAL=1 to compile locally." >&2
    echo "        You can also set BIN=/path/to/flow_control_queue_test." >&2
    exit 1
fi

if [[ ! -x "${SHM_CREATOR}" ]]; then
    echo "[error] node_ubsm_shm_creator not found or not executable: ${SHM_CREATOR}" >&2
    echo "        Set SHM_CREATOR=/path/to/node_ubsm_shm_creator and retry." >&2
    exit 1
fi

run_once()
{
    local case_name=$1
    local capacity=$2
    local perf_messages=$3
    local suffix=$4
    local sender_shm="${SENDER_SHM}_${suffix}"
    local receiver_shm="${RECEIVER_SHM}_${suffix}"

    CREATED_SHMS+=("${SENDER_HOST}:${sender_shm}" "${RECEIVER_HOST}:${receiver_shm}")

    echo "[shm] create ${SENDER_HOST}:${sender_shm} ${RECEIVER_HOST}:${receiver_shm}"
    "${SHM_CREATOR}" delete "${SENDER_HOST}" "${sender_shm}" >/dev/null 2>&1 || true
    "${SHM_CREATOR}" delete "${RECEIVER_HOST}" "${receiver_shm}" >/dev/null 2>&1 || true
    "${SHM_CREATOR}" create "${SENDER_HOST}" "${sender_shm}"
    "${SHM_CREATOR}" create "${RECEIVER_HOST}" "${receiver_shm}"

    echo "[run] role B case=${case_name} capacity=${capacity}"
    "${BIN}" --role B \
        -s "${sender_shm}" \
        -r "${receiver_shm}" \
        --capacity "${capacity}" \
        --threshold "${THRESHOLD}" \
        --wait "${WAIT_SECONDS}" \
        --log-level "${LOG_LEVEL}" &
    B_PID=$!

    sleep 2

    echo "[run] role A case=${case_name} capacity=${capacity} perf_messages=${perf_messages}"
    "${BIN}" --role A \
        -s "${sender_shm}" \
        -r "${receiver_shm}" \
        --case "${case_name}" \
        --capacity "${capacity}" \
        --threshold "${THRESHOLD}" \
        --perf-messages "${perf_messages}" \
        --wait "${WAIT_SECONDS}" \
        --log-level "${LOG_LEVEL}"

    wait "${B_PID}"
    B_PID=""

    "${SHM_CREATOR}" delete "${SENDER_HOST}" "${sender_shm}" >/dev/null 2>&1 || true
    "${SHM_CREATOR}" delete "${RECEIVER_HOST}" "${receiver_shm}" >/dev/null 2>&1 || true
}

if [[ "${RUN_MATRIX}" == "1" ]]; then
    read -r -a capacities <<< "${CAPACITY_MATRIX}"
    read -r -a perf_messages_list <<< "${PERF_MESSAGES_MATRIX}"
    if [[ "${#capacities[@]}" -ne "${#perf_messages_list[@]}" ]]; then
        echo "[error] CAPACITY_MATRIX and PERF_MESSAGES_MATRIX must have the same item count" >&2
        exit 1
    fi

    for idx in "${!capacities[@]}"; do
        cap="${capacities[$idx]}"
        msgs="${perf_messages_list[$idx]}"
        if [[ "${cap}" -ge 2048 ]]; then
            run_once "hotupdate" "${cap}" "${msgs}" "matrix_${idx}"
        else
            run_once "all" "${cap}" "${msgs}" "matrix_${idx}"
        fi
    done
else
    run_once "${CASE_NAME}" "${CAPACITY}" "${PERF_MESSAGES}" "single"
fi

echo "[done] flow-control validation passed"
