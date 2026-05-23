/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Flow-control validation sample for ub_comm_queue.
 */

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

namespace {

constexpr uint64_t NODE_A = 0;
constexpr uint64_t NODE_B = 1;
constexpr uint8_t MAX_NODES = 2;

constexpr unsigned long SHM_MAP_MB = 1024;
constexpr size_t INIT_REGION_SIZE = 1024 * 1024;
constexpr size_t RING_REGION_SIZE = 64 * 1024 * 1024;
constexpr uint32_t DEFAULT_RING_CAPACITY = 64;
constexpr uint32_t MAX_MSG_SIZE = 256;
constexpr uint32_t FLOW_CONFIG_REFRESH_INTERVAL = 1024;

constexpr uint8_t PRIORITY_CTRL = 1;
constexpr uint8_t PRIORITY_ACK = 2;
constexpr uint8_t PRIORITY_DATA = 3;
constexpr uint8_t PRIORITY_NOT_CONFIGURED = 7;

constexpr int UB_COMM_OK = 0;
constexpr int UB_COMM_ERR_PRIVATE_BASE = 4096;
constexpr int UB_COMM_ERR_RING_FULL = -UB_COMM_ERR_PRIVATE_BASE;
constexpr int UB_COMM_ERR_RING_BUSY = -(UB_COMM_ERR_PRIVATE_BASE + 1);
constexpr int UB_COMM_ERR_PEER_NOT_READY = -(UB_COMM_ERR_PRIVATE_BASE + 3);
constexpr int UB_COMM_ERR_RING_NOT_FOUND = -(UB_COMM_ERR_PRIVATE_BASE + 4);

enum MsgType : uint8_t {
    TYPE_CTRL = 100,
    TYPE_CTRL_ACK = 101,
    TYPE_FLOW_DATA = 102,
};

enum CtrlCmd : uint32_t {
    CTRL_SET_THRESHOLD = 1,
    CTRL_SET_DELAY_US = 2,
    CTRL_STOP = 3,
};

#pragma pack(push, 1)
struct CtrlMessage {
    uint32_t cmd;
    uint32_t seq;
    uint32_t value;
    uint32_t reserved;
};

struct CtrlAck {
    uint32_t cmd;
    uint32_t seq;
    int32_t ret;
    uint64_t observed;
};

struct FlowMessage {
    uint32_t seq;
    uint32_t reserved;
    uint64_t send_ns;
    char padding[32];
};
#pragma pack(pop)

static_assert(sizeof(CtrlMessage) <= MAX_MSG_SIZE - sizeof(message_header_t), "CtrlMessage body too large");
static_assert(sizeof(CtrlAck) <= MAX_MSG_SIZE - sizeof(message_header_t), "CtrlAck body too large");
static_assert(sizeof(FlowMessage) <= MAX_MSG_SIZE - sizeof(message_header_t), "FlowMessage body too large");

struct TestContext {
    ub_shm_comm_t *handle = nullptr;
    uint8_t self_node = NODE_A;
    uint8_t peer_node = NODE_B;

    std::atomic<uint32_t> next_ctrl_seq{1};
    std::atomic<uint32_t> last_ack_seq{0};
    std::atomic<int32_t> last_ack_ret{0};
    std::atomic<uint64_t> last_ack_observed{0};
    std::atomic<uint32_t> data_delay_us{0};
    std::atomic<uint64_t> flow_received{0};
    std::atomic<bool> stop{false};
};

struct SendStats {
    uint64_t ok = 0;
    uint64_t congested = 0;
    uint64_t full = 0;
    uint64_t busy = 0;
    uint64_t other = 0;
    uint32_t first_congested_seq = UINT32_MAX;
};

struct PerfStats {
    uint64_t messages = 0;
    uint64_t status_queries = 0;
    double seconds = 0.0;
};

char g_role = 'A';
char g_sender_shm_name[64] = "flow_node0_export";
char g_receiver_shm_name[64] = "flow_node1_export";
char g_case_name[32] = "all";
int g_wait_peer_timeout_s = 15;
int g_log_level = LOG_LEVEL_ERROR;
uint32_t g_ring_capacity = DEFAULT_RING_CAPACITY;
uint32_t g_threshold_percent = 50;
uint32_t g_perf_messages = 20000;
TestContext g_ctx;

int stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    (void)func;
    const char *level_str = "UNKNOWN";
    switch (level) {
        case LOG_LEVEL_DEBUG:
            level_str = "DEBUG";
            break;
        case LOG_LEVEL_INFO:
            level_str = "INFO";
            break;
        case LOG_LEVEL_WARN:
            level_str = "WARN";
            break;
        case LOG_LEVEL_ERROR:
            level_str = "ERROR";
            break;
        case LOG_LEVEL_CRITICAL:
            level_str = "CRITICAL";
            break;
        default:
            break;
    }
    fprintf(stdout, "[%s:%u] [%s] %s\n", file, line, level_str, message);
    return 0;
}

int64_t now_ns()
{
    return (int64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void cpu_relax()
{
#if defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    asm volatile("" ::: "memory");
#endif
}

uint64_t threshold_count(uint32_t capacity, uint32_t percent)
{
    uint64_t threshold = ((uint64_t)capacity * percent + 99) / 100;
    if (percent != 0 && threshold == 0) {
        threshold = 1;
    }
    if (threshold > capacity) {
        threshold = capacity;
    }
    return threshold;
}

const char *state_name(ub_comm_queue_state_t state)
{
    switch (state) {
        case UB_COMM_QUEUE_IDLE:
            return "IDLE";
        case UB_COMM_QUEUE_NORMAL:
            return "NORMAL";
        case UB_COMM_QUEUE_CONGESTED:
            return "CONGESTED";
        case UB_COMM_QUEUE_FULL:
            return "FULL";
        default:
            return "UNKNOWN";
    }
}

void print_status(const char *label, const ub_comm_queue_status_t &status)
{
    printf("%s: state=%s used=%" PRIu64 " total=%" PRIu64 " free=%" PRIu64
           " threshold=%" PRIu64 " max_depth=%" PRIu64,
           label, state_name(status.state), status.used, status.total, status.free,
           status.congestion_threshold, status.max_depth);
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
    printf(" full_fail=%" PRIu64 " cas_fail=%" PRIu64 " enter_us=%" PRIu64 " exit_us=%" PRIu64,
           status.full_fail_count, status.cas_fail_count, status.congestion_enter_ts_us,
           status.congestion_exit_ts_us);
#endif
    printf("\n");
}

bool expect_true(bool cond, const char *label)
{
    if (!cond) {
        fprintf(stderr, "[FAIL] %s\n", label);
        return false;
    }
    printf("[PASS] %s\n", label);
    return true;
}

bool expect_eq_i64(int64_t actual, int64_t expected, const char *label)
{
    if (actual != expected) {
        fprintf(stderr, "[FAIL] %s: actual=%" PRId64 " expected=%" PRId64 "\n", label, actual, expected);
        return false;
    }
    printf("[PASS] %s: %" PRId64 "\n", label, actual);
    return true;
}

bool send_message(uint8_t dest_node, uint8_t priority, uint8_t msg_type, const void *body, uint32_t body_len,
                  int *ret_out = nullptr)
{
    message_t msg{};
    msg.header.dest_node_id = dest_node;
    msg.header.src_node_id = g_ctx.self_node;
    msg.header.msg_type = msg_type;
    msg.header.priority = priority;
    msg.header.body_length = body_len;
    msg.body = (char *)body;

    const int ret = ub_comm_queue_send(g_ctx.handle, &msg);
    if (ret_out != nullptr) {
        *ret_out = ret;
    }
    return ret == UB_COMM_OK || ret == UB_COMM_SEND_CONGESTED;
}

bool send_with_retry(uint8_t dest_node, uint8_t priority, uint8_t msg_type, const void *body, uint32_t body_len,
                     int timeout_ms = 3000)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        int ret = 0;
        if (send_message(dest_node, priority, msg_type, body, body_len, &ret)) {
            return true;
        }
        if (ret != UB_COMM_ERR_RING_FULL && ret != UB_COMM_ERR_RING_BUSY && ret != UB_COMM_ERR_PEER_NOT_READY) {
            fprintf(stderr, "send_with_retry unexpected ret=%d\n", ret);
            return false;
        }
        cpu_relax();
        std::this_thread::sleep_for(50us);
    }
    return false;
}

bool wait_peer_ready(ub_shm_comm_t *handle, uint8_t peer_node, int timeout_s)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        if (ub_comm_queue_check_ready(handle, peer_node)) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

int init_ub_shm()
{
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_init_attributes failed ret=%d\n", ret);
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_initialize failed ret=%d\n", ret);
        return -1;
    }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_lookup_regions failed ret=%d\n", ret);
        return -1;
    }
    return 0;
}

int map_ub_shm(const char *shm_name, void *&addr)
{
    const unsigned long length = SHM_MAP_MB * 1024UL * 1024UL;
    int ret = ubsmem_shmem_map(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) {
        fprintf(stderr, "Failed to map shared memory '%s', ret=%d\n", shm_name, ret);
        return -1;
    }
    return 0;
}

int init_handle(ub_shm_comm_t &handle)
{
    if (init_ub_shm() != 0) {
        return -1;
    }

    void *sender_shm_base = nullptr;
    if (map_ub_shm(g_sender_shm_name, sender_shm_base) != 0) {
        return -1;
    }

    void *receiver_shm_base = nullptr;
    if (map_ub_shm(g_receiver_shm_name, receiver_shm_base) != 0) {
        return -1;
    }

    void *init_area = sender_shm_base;
    void *ring_area_a = static_cast<char *>(sender_shm_base) + INIT_REGION_SIZE;
    void *ring_area_b = receiver_shm_base;

    ub_ring_desc_t ring_descs[3];
    ring_descs[0] = {g_ring_capacity, MAX_MSG_SIZE, PRIORITY_CTRL};
    ring_descs[1] = {g_ring_capacity, MAX_MSG_SIZE, PRIORITY_ACK};
    ring_descs[2] = {g_ring_capacity, MAX_MSG_SIZE, PRIORITY_DATA};

    ub_comm_conf_t conf{};
    conf.cpu_id = -1;
    conf.max_nodes = MAX_NODES;
    conf.current_node_id = (g_role == 'A') ? NODE_A : NODE_B;
    conf.num_rings = 3;
    conf.ring_descs = ring_descs;

    ub_shm_area_t init_shm_area{};
    init_shm_area.ptr = init_area;
    init_shm_area.size = INIT_REGION_SIZE;

    ub_ring_region_info_t ring_info[2];
    ring_info[0].node_id = NODE_A;
    ring_info[0].region.ptr = ring_area_a;
    ring_info[0].region.size = RING_REGION_SIZE;
    ring_info[1].node_id = NODE_B;
    ring_info[1].region.ptr = ring_area_b;
    ring_info[1].region.size = RING_REGION_SIZE;

    ub_ring_region_map_t ring_map{ring_info, 2};
    if (ub_comm_queue_init(&handle, &init_shm_area, &ring_map, &conf) != 0) {
        fprintf(stderr, "ub_comm_queue_init failed\n");
        return -1;
    }

    g_ctx.handle = &handle;
    g_ctx.self_node = (g_role == 'A') ? NODE_A : NODE_B;
    g_ctx.peer_node = (g_role == 'A') ? NODE_B : NODE_A;
    return 0;
}

void send_ctrl_ack(uint32_t cmd, uint32_t seq, int32_t ret, uint64_t observed)
{
    CtrlAck ack{};
    ack.cmd = cmd;
    ack.seq = seq;
    ack.ret = ret;
    ack.observed = observed;
    (void)send_with_retry(g_ctx.peer_node, PRIORITY_ACK, TYPE_CTRL_ACK, &ack, sizeof(ack));
}

void on_ctrl_ack(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg == nullptr || msg->body == nullptr || msg->header.body_length < sizeof(CtrlAck)) {
        return;
    }
    const auto *ack = reinterpret_cast<const CtrlAck *>(msg->body);
    g_ctx.last_ack_ret.store(ack->ret, std::memory_order_release);
    g_ctx.last_ack_observed.store(ack->observed, std::memory_order_release);
    g_ctx.last_ack_seq.store(ack->seq, std::memory_order_release);
}

void on_ctrl(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg == nullptr || msg->body == nullptr || msg->header.body_length < sizeof(CtrlMessage)) {
        return;
    }
    const auto *ctrl = reinterpret_cast<const CtrlMessage *>(msg->body);
    int ret = 0;
    uint64_t observed = 0;

    switch (ctrl->cmd) {
        case CTRL_SET_THRESHOLD: {
            ret = ub_comm_queue_set_congestion_threshold(g_ctx.handle, PRIORITY_DATA, ctrl->value);
            ub_comm_queue_status_t status{};
            if (ub_comm_queue_get_status(g_ctx.handle, g_ctx.self_node, PRIORITY_DATA, &status) == UB_COMM_OK) {
                observed = status.congestion_threshold;
            }
            break;
        }
        case CTRL_SET_DELAY_US:
            g_ctx.data_delay_us.store(ctrl->value, std::memory_order_release);
            observed = ctrl->value;
            break;
        case CTRL_STOP:
            g_ctx.stop.store(true, std::memory_order_release);
            observed = 1;
            break;
        default:
            ret = -EINVAL;
            break;
    }

    send_ctrl_ack(ctrl->cmd, ctrl->seq, ret, observed);
}

void on_flow_data(const message_t *msg, void *ctx)
{
    (void)ctx;
    (void)msg;
    const uint32_t delay_us = g_ctx.data_delay_us.load(std::memory_order_acquire);
    if (delay_us > 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }
    g_ctx.flow_received.fetch_add(1, std::memory_order_relaxed);
}

bool register_callbacks()
{
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_CTRL_ACK, UB_FUNC_SYNC, on_ctrl_ack, &g_ctx) !=
        UB_COMM_OK) {
        return false;
    }
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_CTRL, UB_FUNC_SYNC, on_ctrl, &g_ctx) != UB_COMM_OK) {
        return false;
    }
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_FLOW_DATA, UB_FUNC_SYNC, on_flow_data, &g_ctx) !=
        UB_COMM_OK) {
        return false;
    }
    return true;
}

bool send_ctrl_and_wait(uint32_t cmd, uint32_t value, int32_t *ret_out = nullptr, uint64_t *observed_out = nullptr,
                        int timeout_ms = 5000)
{
    CtrlMessage ctrl{};
    ctrl.cmd = cmd;
    ctrl.value = value;
    ctrl.seq = g_ctx.next_ctrl_seq.fetch_add(1, std::memory_order_relaxed);

    if (!send_with_retry(g_ctx.peer_node, PRIORITY_CTRL, TYPE_CTRL, &ctrl, sizeof(ctrl), timeout_ms)) {
        fprintf(stderr, "failed to send ctrl cmd=%u\n", cmd);
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (g_ctx.last_ack_seq.load(std::memory_order_acquire) >= ctrl.seq) {
            if (ret_out != nullptr) {
                *ret_out = g_ctx.last_ack_ret.load(std::memory_order_acquire);
            }
            if (observed_out != nullptr) {
                *observed_out = g_ctx.last_ack_observed.load(std::memory_order_acquire);
            }
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    fprintf(stderr, "timeout waiting ctrl ack cmd=%u seq=%u\n", cmd, ctrl.seq);
    return false;
}

bool get_peer_data_status(ub_comm_queue_status_t *status)
{
    return ub_comm_queue_get_status(g_ctx.handle, g_ctx.peer_node, PRIORITY_DATA, status) == UB_COMM_OK;
}

bool wait_peer_used_le(uint64_t expected, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    ub_comm_queue_status_t status{};
    while (std::chrono::steady_clock::now() < deadline) {
        if (get_peer_data_status(&status) && status.used <= expected) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    if (get_peer_data_status(&status)) {
        print_status("wait_peer_used_le timeout status", status);
    }
    return false;
}

bool should_run_case(const char *name)
{
    return strcmp(g_case_name, "all") == 0 || strcmp(g_case_name, name) == 0;
}

bool case_param_validation()
{
    printf("\n[CASE param] API parameter validation\n");
    bool ok = true;

    ub_shm_comm_t null_handle = nullptr;
    ub_comm_queue_status_t status{};
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(nullptr, PRIORITY_DATA, 50), -EINVAL,
                        "set threshold rejects null handle pointer");
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(&null_handle, PRIORITY_DATA, 50), -EINVAL,
                        "set threshold rejects null handle value");
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(g_ctx.handle, 8, 50), -EINVAL,
                        "set threshold rejects invalid priority");
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(g_ctx.handle, 0, 50), -EINVAL,
                        "set threshold rejects reserved priority 0");
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(g_ctx.handle, PRIORITY_DATA, 101), -EINVAL,
                        "set threshold rejects percent > 100");
    ok &= expect_eq_i64(
        ub_comm_queue_set_congestion_threshold(g_ctx.handle, PRIORITY_NOT_CONFIGURED, g_threshold_percent),
        UB_COMM_ERR_RING_NOT_FOUND, "set threshold rejects unconfigured priority");

    ok &= expect_eq_i64(ub_comm_queue_get_status(nullptr, g_ctx.self_node, PRIORITY_DATA, &status), -EINVAL,
                        "status rejects null handle pointer");
    ok &= expect_eq_i64(ub_comm_queue_get_status(&null_handle, g_ctx.self_node, PRIORITY_DATA, &status), -EINVAL,
                        "status rejects null handle value");
    ok &= expect_eq_i64(ub_comm_queue_get_status(g_ctx.handle, g_ctx.self_node, PRIORITY_DATA, nullptr), -EINVAL,
                        "status rejects null output");
    ok &= expect_eq_i64(ub_comm_queue_get_status(g_ctx.handle, g_ctx.self_node, 8, &status), -EINVAL,
                        "status rejects invalid priority");
    ok &= expect_eq_i64(ub_comm_queue_get_status(g_ctx.handle, g_ctx.self_node, PRIORITY_NOT_CONFIGURED, &status),
                        UB_COMM_ERR_RING_NOT_FOUND, "status rejects unconfigured priority");
    return ok;
}

bool case_status_and_threshold()
{
    printf("\n[CASE status] threshold configuration and status snapshot\n");
    bool ok = true;
    int32_t ack_ret = 0;
    uint64_t observed = 0;

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, g_threshold_percent, &ack_ret, &observed),
                      "remote threshold control message acknowledged");
    ok &= expect_eq_i64(ack_ret, UB_COMM_OK, "remote threshold setter returns OK");
    ok &= expect_eq_i64((int64_t)observed, (int64_t)threshold_count(g_ring_capacity, g_threshold_percent),
                        "remote threshold observed in target ring");

    ub_comm_queue_status_t status{};
    ok &= expect_true(get_peer_data_status(&status), "query peer data ring status");
    if (ok) {
        print_status("peer data ring", status);
        ok &= expect_eq_i64((int64_t)status.total, g_ring_capacity, "status total equals ring capacity");
        ok &= expect_eq_i64((int64_t)status.congestion_threshold,
                            (int64_t)threshold_count(g_ring_capacity, g_threshold_percent),
                            "status threshold equals configured percent");
        ok &= expect_eq_i64((int64_t)status.state, UB_COMM_QUEUE_IDLE, "empty ring reports IDLE");
    }

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, 0, &ack_ret, &observed),
                      "remote threshold 0 percent acknowledged");
    ok &= expect_eq_i64(ack_ret, UB_COMM_OK, "remote 0 percent threshold setter returns OK");
    ok &= expect_eq_i64((int64_t)observed, 0, "percent 0 maps to threshold 0");
    ok &= expect_true(get_peer_data_status(&status), "query peer data ring after threshold 0");
    if (ok) {
        print_status("peer data ring threshold 0", status);
        ok &= expect_eq_i64((int64_t)status.used, 0, "empty ring remains empty with threshold 0");
        ok &= expect_eq_i64((int64_t)status.state, UB_COMM_QUEUE_CONGESTED,
                            "empty non-full ring reports CONGESTED with threshold 0");
    }

    FlowMessage body{};
    body.seq = 0;
    body.send_ns = (uint64_t)now_ns();
    int send_ret = 0;
    (void)send_message(g_ctx.peer_node, PRIORITY_DATA, TYPE_FLOW_DATA, &body, sizeof(body), &send_ret);
    ok &= expect_eq_i64(send_ret, UB_COMM_SEND_CONGESTED,
                        "threshold 0 makes successful send return congestion hint");
    ok &= expect_true(wait_peer_used_le(0, 3000), "peer ring drains after threshold 0 send");

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, g_threshold_percent, &ack_ret, &observed),
                      "restore validation threshold");
    ok &= expect_eq_i64(ack_ret, UB_COMM_OK, "restore threshold returns OK");
    return ok;
}

SendStats burst_send_until_full(uint32_t messages)
{
    SendStats stats{};
    for (uint32_t i = 0; i < messages; ++i) {
        FlowMessage body{};
        body.seq = i;
        body.send_ns = (uint64_t)now_ns();
        int ret = 0;
        (void)send_message(g_ctx.peer_node, PRIORITY_DATA, TYPE_FLOW_DATA, &body, sizeof(body), &ret);
        if (ret == UB_COMM_OK) {
            ++stats.ok;
        } else if (ret == UB_COMM_SEND_CONGESTED) {
            ++stats.congested;
            if (stats.first_congested_seq == UINT32_MAX) {
                stats.first_congested_seq = i;
            }
        } else if (ret == UB_COMM_ERR_RING_FULL) {
            ++stats.full;
        } else if (ret == UB_COMM_ERR_RING_BUSY) {
            ++stats.busy;
        } else {
            ++stats.other;
            fprintf(stderr, "unexpected send ret=%d at seq=%u\n", ret, i);
        }
    }
    return stats;
}

bool case_flow_return_codes()
{
    printf("\n[CASE flow] OK / CONGESTED / FULL return codes and max_depth\n");
    bool ok = true;
    int32_t ack_ret = 0;
    uint64_t observed = 0;

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, g_threshold_percent, &ack_ret, &observed),
                      "set low threshold before burst");
    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_DELAY_US, 200000, &ack_ret, &observed),
                      "slow peer consumer before burst");
    ok &= expect_true(wait_peer_used_le(0, 3000), "peer data ring is empty before burst");

    const SendStats stats = burst_send_until_full(g_ring_capacity + 32);
    printf("burst send result: ok=%" PRIu64 " congested=%" PRIu64 " full=%" PRIu64 " busy=%" PRIu64
           " other=%" PRIu64 "\n",
           stats.ok, stats.congested, stats.full, stats.busy, stats.other);

    ub_comm_queue_status_t status{};
    ok &= expect_true(get_peer_data_status(&status), "query peer data ring after burst");
    if (ok) {
        print_status("peer data ring after burst", status);
        ok &= expect_true(stats.ok > 0, "normal success return observed");
        ok &= expect_true(stats.congested > 0, "congested success return observed");
        ok &= expect_true(stats.full > 0, "full failure return observed");
        ok &= expect_true(stats.other == 0, "no unexpected send return observed");
        ok &= expect_true(status.max_depth >= status.congestion_threshold, "max_depth records congestion depth");
        ok &= expect_true(status.max_depth <= status.total, "max_depth never exceeds ring capacity");
    }
    return ok;
}

bool case_recovery()
{
    printf("\n[CASE recover] congestion exit and status recovery\n");
    bool ok = true;
    int32_t ack_ret = 0;
    uint64_t observed = 0;

    ub_comm_queue_status_t before{};
    if (get_peer_data_status(&before) && before.max_depth < before.congestion_threshold) {
        printf("recovery case has no previous congestion peak, preparing a local burst first.\n");
        ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, g_threshold_percent, &ack_ret, &observed),
                          "set low threshold before recovery preparation");
        ok &= expect_true(send_ctrl_and_wait(CTRL_SET_DELAY_US, 200000, &ack_ret, &observed),
                          "slow peer consumer before recovery preparation");
        ok &= expect_true(wait_peer_used_le(0, 3000), "peer data ring is empty before recovery preparation");
        const SendStats stats = burst_send_until_full(g_ring_capacity + 32);
        ok &= expect_true(stats.congested > 0, "recovery preparation observes congestion");
        ok &= expect_true(stats.full > 0, "recovery preparation observes full ring");
    }

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_DELAY_US, 0, &ack_ret, &observed, 10000),
                      "restore peer consumer speed");
    ok &= expect_eq_i64(ack_ret, UB_COMM_OK, "restore consumer speed returns OK");
    ok &= expect_true(wait_peer_used_le(0, 15000), "peer ring drains to zero");

    ub_comm_queue_status_t status{};
    ok &= expect_true(get_peer_data_status(&status), "query peer data ring after drain");
    if (ok) {
        print_status("peer data ring after drain", status);
        ok &= expect_eq_i64((int64_t)status.used, 0, "used returns to zero after drain");
        ok &= expect_eq_i64((int64_t)status.state, UB_COMM_QUEUE_IDLE, "state returns to IDLE");
        ok &= expect_true(status.max_depth >= status.congestion_threshold, "max_depth remains as historical peak");
    }
    return ok;
}

bool case_threshold_hot_update()
{
    printf("\n[CASE hotupdate] remote threshold cache refresh interval\n");
    if (g_ring_capacity < FLOW_CONFIG_REFRESH_INTERVAL + 128) {
        printf("[SKIP] capacity=%u is too small for interval test; use capacity >= %u\n", g_ring_capacity,
               FLOW_CONFIG_REFRESH_INTERVAL + 128);
        return true;
    }

    bool ok = true;
    int32_t ack_ret = 0;
    uint64_t observed = 0;

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_DELAY_US, 200000, &ack_ret, &observed),
                      "slow peer consumer before hot-update interval test");
    ok &= expect_true(wait_peer_used_le(0, 3000), "peer data ring is empty before hot-update interval test");

    /*
     * The sender cache starts with the default 80% threshold. Set the remote threshold to 1% after
     * cache population, then send just beyond FLOW_CONFIG_REFRESH_INTERVAL but far below the old
     * 80% watermark. Congestion can only be observed if the periodic version refresh works.
     */
    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, 1, &ack_ret, &observed),
                      "hot-update remote threshold to 1 percent");
    ok &= expect_eq_i64(ack_ret, UB_COMM_OK, "hot-update threshold setter returns OK");
    ok &= expect_eq_i64((int64_t)observed, (int64_t)threshold_count(g_ring_capacity, 1),
                        "remote threshold changed to 1 percent");

    const uint32_t sends = FLOW_CONFIG_REFRESH_INTERVAL + 64;
    const SendStats stats = burst_send_until_full(sends);
    printf("hot-update burst result: messages=%u ok=%" PRIu64 " congested=%" PRIu64 " full=%" PRIu64
           " first_congested_seq=%u\n",
           sends, stats.ok, stats.congested, stats.full, stats.first_congested_seq);

    ub_comm_queue_status_t status{};
    ok &= expect_true(get_peer_data_status(&status), "query peer data ring after hot-update burst");
    if (ok) {
        print_status("peer data ring after hot-update burst", status);
        ok &= expect_true(stats.congested > 0, "periodic cache refresh observes lowered threshold");
        ok &= expect_true(stats.full == 0, "hot-update interval test does not rely on full ring");
        ok &= expect_true(stats.first_congested_seq + 1 >= FLOW_CONFIG_REFRESH_INTERVAL,
                          "congestion starts after refresh interval, not before cached threshold refresh");
        ok &= expect_true(stats.first_congested_seq < sends, "congestion appears within bounded refresh window");
        ok &= expect_eq_i64((int64_t)status.congestion_threshold, (int64_t)threshold_count(g_ring_capacity, 1),
                            "status reports lowered threshold after hot update");
    }

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_DELAY_US, 0, &ack_ret, &observed, 10000),
                      "restore peer consumer speed after hot-update interval test");
    ok &= expect_true(wait_peer_used_le(0, 15000), "peer ring drains after hot-update interval test");
    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, g_threshold_percent, &ack_ret, &observed),
                      "restore validation threshold after hot-update interval test");
    return ok;
}

bool send_flow_with_retry(uint32_t seq, int timeout_ms = 5000)
{
    FlowMessage body{};
    body.seq = seq;
    body.send_ns = (uint64_t)now_ns();
    return send_with_retry(g_ctx.peer_node, PRIORITY_DATA, TYPE_FLOW_DATA, &body, sizeof(body), timeout_ms);
}

PerfStats run_perf_round(bool query_status)
{
    std::atomic<bool> done{false};
    std::atomic<uint64_t> status_queries{0};
    std::thread query_thread;

    if (query_status) {
        query_thread = std::thread([&]() {
            ub_comm_queue_status_t status{};
            while (!done.load(std::memory_order_acquire)) {
                if (get_peer_data_status(&status)) {
                    status_queries.fetch_add(1, std::memory_order_relaxed);
                }
                std::this_thread::sleep_for(1ms);
            }
        });
    }

    const auto begin = std::chrono::steady_clock::now();
    uint64_t sent = 0;
    for (uint32_t i = 0; i < g_perf_messages; ++i) {
        if (!send_flow_with_retry(i, 10000)) {
            fprintf(stderr, "perf send timeout at seq=%u\n", i);
            break;
        }
        ++sent;
    }
    const auto end = std::chrono::steady_clock::now();
    done.store(true, std::memory_order_release);
    if (query_thread.joinable()) {
        query_thread.join();
    }

    PerfStats stats{};
    stats.messages = sent;
    stats.status_queries = status_queries.load(std::memory_order_relaxed);
    stats.seconds = std::chrono::duration<double>(end - begin).count();
    return stats;
}

bool case_perf()
{
    printf("\n[CASE perf] send throughput with and without status query\n");
    bool ok = true;
    int32_t ack_ret = 0;
    uint64_t observed = 0;

    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_THRESHOLD, 80, &ack_ret, &observed), "set perf threshold to 80");
    ok &= expect_true(send_ctrl_and_wait(CTRL_SET_DELAY_US, 0, &ack_ret, &observed, 10000),
                      "ensure peer consumer has no artificial delay");
    ok &= expect_true(wait_peer_used_le(0, 15000), "peer ring is empty before perf");

    const PerfStats base = run_perf_round(false);
    ok &= expect_true(wait_peer_used_le(0, 15000), "peer ring drains after base perf");
    const PerfStats queried = run_perf_round(true);
    ok &= expect_true(wait_peer_used_le(0, 15000), "peer ring drains after query perf");

    const double base_mps = (base.seconds > 0.0) ? (double)base.messages / base.seconds : 0.0;
    const double query_mps = (queried.seconds > 0.0) ? (double)queried.messages / queried.seconds : 0.0;
    const double ratio = (base_mps > 0.0) ? query_mps / base_mps : 0.0;
    printf("perf base: messages=%" PRIu64 " seconds=%.6f throughput=%.2f msg/s\n", base.messages, base.seconds,
           base_mps);
    printf("perf with status query: messages=%" PRIu64 " status_queries=%" PRIu64
           " seconds=%.6f throughput=%.2f msg/s ratio=%.3f\n",
           queried.messages, queried.status_queries, queried.seconds, query_mps, ratio);
    ok &= expect_true(base.messages == g_perf_messages, "base perf sends requested messages");
    ok &= expect_true(queried.messages == g_perf_messages, "query perf sends requested messages");
    ok &= expect_true(queried.status_queries > 0, "status query thread sampled during perf");
    return ok;
}

int run_role_a()
{
    if (!wait_peer_ready(g_ctx.handle, g_ctx.peer_node, g_wait_peer_timeout_s)) {
        fprintf(stderr, "Peer node is not ready within %d seconds\n", g_wait_peer_timeout_s);
        return 1;
    }

    bool ok = true;
    if (should_run_case("param")) {
        ok &= case_param_validation();
    }
    if (should_run_case("status")) {
        ok &= case_status_and_threshold();
    }
    if (should_run_case("flow")) {
        ok &= case_flow_return_codes();
    }
    if (should_run_case("recover")) {
        ok &= case_recovery();
    }
    if (should_run_case("hotupdate")) {
        ok &= case_threshold_hot_update();
    }
    if (should_run_case("perf")) {
        ok &= case_perf();
    }

    int32_t ack_ret = 0;
    uint64_t observed = 0;
    (void)send_ctrl_and_wait(CTRL_STOP, 0, &ack_ret, &observed, 5000);
    printf("\nFlow-control validation %s.\n", ok ? "passed" : "failed");
    return ok ? 0 : 2;
}

int run_role_b()
{
    printf("Role B ready. Waiting for flow-control validation commands...\n");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(g_wait_peer_timeout_s * 8);
    while (!g_ctx.stop.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(50ms);
    }
    if (!g_ctx.stop.load(std::memory_order_acquire)) {
        fprintf(stderr, "Role B timeout waiting for stop command\n");
        return 1;
    }
    printf("Role B exits. flow_received=%" PRIu64 "\n", g_ctx.flow_received.load(std::memory_order_relaxed));
    return 0;
}

void print_usage(const char *prog)
{
    printf("Usage: %s --role A|B -s <node0_shm> -r <node1_shm> [options]\n", prog);
    printf("Options:\n");
    printf("  --case <all|param|status|flow|recover|hotupdate|perf>  case to run on role A, default all\n");
    printf("  --capacity <n>                               ring capacity, power of 2, default %u\n",
           DEFAULT_RING_CAPACITY);
    printf("  --threshold <percent>                        validation threshold percent, default 50\n");
    printf("  --perf-messages <n>                          perf message count, default %u\n", g_perf_messages);
    printf("  --wait <seconds>                             peer wait timeout, default %d\n", g_wait_peer_timeout_s);
    printf("  --log-level <0-4>                            ub_comm_queue log level, default ERROR\n");
}

bool parse_args(int argc, char **argv)
{
    static option long_options[] = {
        {"role", required_argument, nullptr, 'R'},
        {"sender-shm", required_argument, nullptr, 's'},
        {"receiver-shm", required_argument, nullptr, 'r'},
        {"case", required_argument, nullptr, 'c'},
        {"capacity", required_argument, nullptr, 'n'},
        {"threshold", required_argument, nullptr, 't'},
        {"perf-messages", required_argument, nullptr, 'm'},
        {"wait", required_argument, nullptr, 'w'},
        {"log-level", required_argument, nullptr, 'l'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "R:s:r:c:n:t:m:w:l:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'R':
                g_role = optarg[0];
                break;
            case 's':
                snprintf(g_sender_shm_name, sizeof(g_sender_shm_name), "%s", optarg);
                break;
            case 'r':
                snprintf(g_receiver_shm_name, sizeof(g_receiver_shm_name), "%s", optarg);
                break;
            case 'c':
                snprintf(g_case_name, sizeof(g_case_name), "%s", optarg);
                break;
            case 'n':
                g_ring_capacity = (uint32_t)strtoul(optarg, nullptr, 10);
                break;
            case 't':
                g_threshold_percent = (uint32_t)strtoul(optarg, nullptr, 10);
                break;
            case 'm':
                g_perf_messages = (uint32_t)strtoul(optarg, nullptr, 10);
                break;
            case 'w':
                g_wait_peer_timeout_s = atoi(optarg);
                break;
            case 'l':
                g_log_level = atoi(optarg);
                break;
            case 'h':
                print_usage(argv[0]);
                return false;
            default:
                print_usage(argv[0]);
                return false;
        }
    }

    if (g_role != 'A' && g_role != 'B') {
        fprintf(stderr, "Invalid role: %c\n", g_role);
        return false;
    }
    if (g_ring_capacity == 0 || (g_ring_capacity & (g_ring_capacity - 1)) != 0) {
        fprintf(stderr, "capacity must be a power of 2\n");
        return false;
    }
    if (g_threshold_percent > 100) {
        fprintf(stderr, "threshold must be in [0, 100]\n");
        return false;
    }
    if (strcmp(g_case_name, "all") != 0 && strcmp(g_case_name, "param") != 0 &&
        strcmp(g_case_name, "status") != 0 && strcmp(g_case_name, "flow") != 0 &&
        strcmp(g_case_name, "recover") != 0 && strcmp(g_case_name, "hotupdate") != 0 &&
        strcmp(g_case_name, "perf") != 0) {
        fprintf(stderr, "unknown case: %s\n", g_case_name);
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char **argv)
{
    if (!parse_args(argc, argv)) {
        return 1;
    }

    ub_atomic_register_log_func(stdout_logger);
    ub_atomic_set_log_level(g_log_level);

    ub_shm_comm_t handle = nullptr;
    if (init_handle(handle) != 0) {
        return 1;
    }
    if (!register_callbacks()) {
        fprintf(stderr, "register_callbacks failed\n");
        ub_comm_queue_deinit(&handle);
        return 1;
    }

    int ret = (g_role == 'A') ? run_role_a() : run_role_b();
    ub_comm_queue_deinit(&handle);
    return ret;
}
