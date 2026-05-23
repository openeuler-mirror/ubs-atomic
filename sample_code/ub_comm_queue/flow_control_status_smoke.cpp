/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Lightweight smoke tests for ub_comm_queue flow-control status APIs.
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
#include <vector>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

namespace {

constexpr uint8_t NODE_A = 0;
constexpr uint8_t NODE_B = 1;
constexpr uint8_t MAX_NODES = 2;
constexpr unsigned long SHM_MAP_MB = 1024;
constexpr size_t INIT_REGION_SIZE = 1024 * 1024;
constexpr size_t RING_REGION_SIZE = 64 * 1024 * 1024;
constexpr uint32_t DEFAULT_CAPACITY = 1024;
constexpr uint32_t MAX_MSG_SIZE = 256;
constexpr uint8_t CTRL_PRIORITY = 1;
constexpr uint8_t MSG_CTRL = 210;
constexpr uint8_t MSG_ACK = 211;

enum class CaseId : uint32_t {
    CASE1_LOCAL_DEINIT = 1,
    CASE2_REMOTE_DEINIT = 2,
    CASE3_TWO_RINGS = 3,
    CASE4_SEVEN_RINGS = 4,
};

enum CtrlCmd : uint32_t {
    CMD_SET_THRESHOLD = 1,
    CMD_QUERY_A_TWO_RINGS = 2,
    CMD_STOP = 3,
};

#pragma pack(push, 1)
struct CtrlMessage {
    uint32_t cmd;
    uint32_t seq;
    uint32_t priority;
    uint32_t value;
};

struct AckMessage {
    uint32_t cmd;
    uint32_t seq;
    int32_t ret;
    uint64_t observed;
};
#pragma pack(pop)

static_assert(sizeof(CtrlMessage) <= MAX_MSG_SIZE - sizeof(message_header_t), "CtrlMessage body too large");
static_assert(sizeof(AckMessage) <= MAX_MSG_SIZE - sizeof(message_header_t), "AckMessage body too large");

struct Context {
    ub_shm_comm_t *handle = nullptr;
    uint8_t self = NODE_A;
    uint8_t peer = NODE_B;
    std::atomic<bool> stop{false};
    std::atomic<uint32_t> next_seq{1};
    std::atomic<uint32_t> ack_seq{0};
    std::atomic<int32_t> ack_ret{0};
    std::atomic<uint64_t> ack_observed{0};
};

char g_role = 'A';
CaseId g_case = CaseId::CASE1_LOCAL_DEINIT;
char g_sender_shm_name[64] = "smoke_sender";
char g_receiver_shm_name[64] = "smoke_receiver";
uint32_t g_capacity = DEFAULT_CAPACITY;
int g_wait_peer_timeout_s = 20;
int g_log_level = LOG_LEVEL_ERROR;
Context g_ctx;

int stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    (void)func;
    fprintf(stdout, "[%s:%u] [L%d] %s\n", file, line, level, message);
    return 0;
}

uint64_t threshold_count(uint32_t capacity, uint32_t percent)
{
    uint64_t threshold = (static_cast<uint64_t>(capacity) * percent + 99) / 100;
    if (percent != 0 && threshold == 0) {
        threshold = 1;
    }
    if (threshold > capacity) {
        threshold = capacity;
    }
    return threshold;
}

uint8_t ring_count_for_case(CaseId id)
{
    switch (id) {
        case CaseId::CASE1_LOCAL_DEINIT:
        case CaseId::CASE2_REMOTE_DEINIT:
            return 1;
        case CaseId::CASE3_TWO_RINGS:
            return 2;
        case CaseId::CASE4_SEVEN_RINGS:
            return 7;
        default:
            return 1;
    }
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

void print_status(const char *label, uint8_t node, uint8_t prio, const ub_comm_queue_status_t &status)
{
    printf("%s node=%u prio=%u state=%s used=%" PRIu64 " total=%" PRIu64 " free=%" PRIu64
           " threshold=%" PRIu64 " max_depth=%" PRIu64 "\n",
           label, node, prio, state_name(status.state), status.used, status.total, status.free,
           status.congestion_threshold, status.max_depth);
}

bool expect_status(uint8_t node, uint8_t prio, uint64_t threshold, ub_comm_queue_state_t state, const char *label)
{
    ub_comm_queue_status_t status{};
    const int ret = ub_comm_queue_get_status(g_ctx.handle, node, prio, &status);
    if (!expect_eq_i64(ret, 0, label)) {
        return false;
    }
    print_status(label, node, prio, status);
    bool ok = true;
    ok &= expect_eq_i64((int64_t)status.total, g_capacity, "status total");
    ok &= expect_eq_i64((int64_t)status.congestion_threshold, (int64_t)threshold, "status threshold");
    ok &= expect_eq_i64((int64_t)status.state, state, "status state");
    return ok;
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

    const uint8_t ring_count = ring_count_for_case(g_case);
    std::vector<ub_ring_desc_t> ring_descs(ring_count);
    for (uint8_t i = 0; i < ring_count; ++i) {
        ring_descs[i] = {g_capacity, MAX_MSG_SIZE, static_cast<uint8_t>(i + 1)};
    }

    ub_comm_conf_t conf{};
    conf.cpu_id = -1;
    conf.max_nodes = MAX_NODES;
    conf.current_node_id = (g_role == 'A') ? NODE_A : NODE_B;
    conf.num_rings = ring_count;
    conf.ring_descs = ring_descs.data();

    ub_shm_area_t init_shm_area{};
    init_shm_area.ptr = sender_shm_base;
    init_shm_area.size = INIT_REGION_SIZE;

    ub_ring_region_info_t ring_info[2];
    ring_info[0].node_id = NODE_A;
    ring_info[0].region.ptr = static_cast<char *>(sender_shm_base) + INIT_REGION_SIZE;
    ring_info[0].region.size = RING_REGION_SIZE;
    ring_info[1].node_id = NODE_B;
    ring_info[1].region.ptr = receiver_shm_base;
    ring_info[1].region.size = RING_REGION_SIZE;

    ub_ring_region_map_t ring_map{ring_info, 2};
    int ret = ub_comm_queue_init(&handle, &init_shm_area, &ring_map, &conf);
    if (ret != 0) {
        fprintf(stderr, "ub_comm_queue_init failed ret=%d\n", ret);
        return -1;
    }

    g_ctx.handle = &handle;
    g_ctx.self = (g_role == 'A') ? NODE_A : NODE_B;
    g_ctx.peer = (g_role == 'A') ? NODE_B : NODE_A;
    return 0;
}

bool wait_peer_ready(uint8_t peer)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(g_wait_peer_timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        if (ub_comm_queue_check_ready(g_ctx.handle, peer)) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

bool send_message(uint8_t dest, uint8_t type, uint8_t prio, const void *body, uint32_t body_len)
{
    message_t msg{};
    msg.header.dest_node_id = dest;
    msg.header.src_node_id = g_ctx.self;
    msg.header.msg_type = type;
    msg.header.priority = prio;
    msg.header.body_length = body_len;
    msg.body = const_cast<char *>(static_cast<const char *>(body));
    const int ret = ub_comm_queue_send(g_ctx.handle, &msg);
    return ret >= 0;
}

bool send_ack(uint32_t cmd, uint32_t seq, int32_t ret, uint64_t observed)
{
    AckMessage ack{};
    ack.cmd = cmd;
    ack.seq = seq;
    ack.ret = ret;
    ack.observed = observed;
    return send_message(g_ctx.peer, MSG_ACK, CTRL_PRIORITY, &ack, sizeof(ack));
}

void on_ack(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg == nullptr || msg->body == nullptr || msg->header.body_length < sizeof(AckMessage)) {
        return;
    }
    const auto *ack = reinterpret_cast<const AckMessage *>(msg->body);
    g_ctx.ack_ret.store(ack->ret, std::memory_order_release);
    g_ctx.ack_observed.store(ack->observed, std::memory_order_release);
    g_ctx.ack_seq.store(ack->seq, std::memory_order_release);
}

void on_ctrl(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg == nullptr || msg->body == nullptr || msg->header.body_length < sizeof(CtrlMessage)) {
        return;
    }
    const auto *ctrl = reinterpret_cast<const CtrlMessage *>(msg->body);
    int32_t ret = 0;
    uint64_t observed = 0;

    if (ctrl->cmd == CMD_SET_THRESHOLD) {
        ret = ub_comm_queue_set_congestion_threshold(g_ctx.handle, static_cast<uint8_t>(ctrl->priority), ctrl->value);
        ub_comm_queue_status_t status{};
        if (ret == 0 &&
            ub_comm_queue_get_status(g_ctx.handle, g_ctx.self, static_cast<uint8_t>(ctrl->priority), &status) == 0) {
            observed = status.congestion_threshold;
        }
    } else if (ctrl->cmd == CMD_QUERY_A_TWO_RINGS) {
        ub_comm_queue_status_t s1{};
        ub_comm_queue_status_t s2{};
        int r1 = ub_comm_queue_get_status(g_ctx.handle, NODE_A, 1, &s1);
        int r2 = ub_comm_queue_get_status(g_ctx.handle, NODE_A, 2, &s2);
        ret = (r1 == 0 && r2 == 0) ? 0 : -EINVAL;
        observed = (static_cast<uint64_t>(s1.congestion_threshold) << 32) |
                   (static_cast<uint64_t>(s2.congestion_threshold) & 0xffffffffULL);
        if (ret == 0) {
            print_status("B queried A1", NODE_A, 1, s1);
            print_status("B queried A2", NODE_A, 2, s2);
        }
    } else if (ctrl->cmd == CMD_STOP) {
        g_ctx.stop.store(true, std::memory_order_release);
    } else {
        ret = -EINVAL;
    }
    (void)send_ack(ctrl->cmd, ctrl->seq, ret, observed);
}

bool register_callbacks()
{
    if (ub_comm_queue_register_process_func(g_ctx.handle, MSG_ACK, UB_FUNC_SYNC, on_ack, nullptr) != 0) {
        return false;
    }
    if (ub_comm_queue_register_process_func(g_ctx.handle, MSG_CTRL, UB_FUNC_SYNC, on_ctrl, nullptr) != 0) {
        return false;
    }
    return true;
}

bool send_ctrl_and_wait(uint32_t cmd, uint8_t priority, uint32_t value, int32_t *ret_out = nullptr,
                        uint64_t *observed_out = nullptr)
{
    CtrlMessage ctrl{};
    ctrl.cmd = cmd;
    ctrl.seq = g_ctx.next_seq.fetch_add(1, std::memory_order_relaxed);
    ctrl.priority = priority;
    ctrl.value = value;
    if (!send_message(g_ctx.peer, MSG_CTRL, CTRL_PRIORITY, &ctrl, sizeof(ctrl))) {
        fprintf(stderr, "send ctrl failed cmd=%u\n", cmd);
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (g_ctx.ack_seq.load(std::memory_order_acquire) >= ctrl.seq) {
            if (ret_out != nullptr) {
                *ret_out = g_ctx.ack_ret.load(std::memory_order_acquire);
            }
            if (observed_out != nullptr) {
                *observed_out = g_ctx.ack_observed.load(std::memory_order_acquire);
            }
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    fprintf(stderr, "timeout waiting ack cmd=%u seq=%u\n", cmd, ctrl.seq);
    return false;
}

bool case1_local_deinit(ub_shm_comm_t &handle)
{
    printf("\n[CASE1] A local status, threshold, deinit\n");
    bool ok = true;
    ok &= expect_status(NODE_A, 1, threshold_count(g_capacity, 80), UB_COMM_QUEUE_IDLE, "A query A ring default");
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(g_ctx.handle, 1, 65), 0, "A set A1 threshold 65");
    ok &= expect_status(NODE_A, 1, threshold_count(g_capacity, 65), UB_COMM_QUEUE_IDLE, "A query A ring threshold 65");
    ok &= expect_eq_i64(ub_comm_queue_deinit(&handle), 0, "A deinit");
    ub_comm_queue_status_t status{};
    ok &= expect_eq_i64(ub_comm_queue_get_status(&handle, NODE_A, 1, &status), -EINVAL,
                        "A query after deinit rejects null handle");
    return ok;
}

bool case2_remote_deinit()
{
    printf("\n[CASE2] A query B status, B threshold, B deinit\n");
    bool ok = true;
    ok &= expect_true(wait_peer_ready(NODE_B), "A waits B ready");
    ok &= expect_status(NODE_B, 1, threshold_count(g_capacity, 80), UB_COMM_QUEUE_IDLE, "A query B default");

    int32_t ack_ret = 0;
    uint64_t observed = 0;
    ok &= expect_true(send_ctrl_and_wait(CMD_SET_THRESHOLD, 1, 65, &ack_ret, &observed), "A asks B set B1 65");
    ok &= expect_eq_i64(ack_ret, 0, "B set threshold ret");
    ok &= expect_eq_i64((int64_t)observed, (int64_t)threshold_count(g_capacity, 65), "B observed threshold 65");
    ok &= expect_status(NODE_B, 1, threshold_count(g_capacity, 65), UB_COMM_QUEUE_IDLE, "A query B threshold 65");

    ok &= expect_true(send_ctrl_and_wait(CMD_STOP, 1, 0, &ack_ret, &observed), "A asks B stop");
    std::this_thread::sleep_for(500ms);
    ub_comm_queue_status_t status{};
    int ret = ub_comm_queue_get_status(g_ctx.handle, NODE_B, 1, &status);
    printf("A query B after deinit ret=%d\n", ret);
    ok &= expect_true(ret < 0 || status.state == UB_COMM_QUEUE_FULL, "A query B after deinit is unavailable/full");
    return ok;
}

bool case3_two_rings()
{
    printf("\n[CASE3] A two rings, A1 threshold 65, A/B query A1/A2\n");
    bool ok = true;
    ok &= expect_true(wait_peer_ready(NODE_B), "A waits B ready");
    ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(g_ctx.handle, 1, 65), 0, "A set A1 threshold 65");
    ok &= expect_status(NODE_A, 1, threshold_count(g_capacity, 65), UB_COMM_QUEUE_IDLE, "A query A1 threshold 65");
    ok &= expect_status(NODE_A, 2, threshold_count(g_capacity, 80), UB_COMM_QUEUE_IDLE, "A query A2 default");

    int32_t ack_ret = 0;
    uint64_t observed = 0;
    ok &= expect_true(send_ctrl_and_wait(CMD_QUERY_A_TWO_RINGS, 1, 0, &ack_ret, &observed), "B query A1/A2");
    ok &= expect_eq_i64(ack_ret, 0, "B query A1/A2 ret");
    uint32_t b_seen_a1 = static_cast<uint32_t>(observed >> 32);
    uint32_t b_seen_a2 = static_cast<uint32_t>(observed & 0xffffffffULL);
    ok &= expect_eq_i64(b_seen_a1, threshold_count(g_capacity, 65), "B observed A1 threshold 65");
    ok &= expect_eq_i64(b_seen_a2, threshold_count(g_capacity, 80), "B observed A2 default threshold");
    (void)send_ctrl_and_wait(CMD_STOP, 1, 0, &ack_ret, &observed);
    return ok;
}

bool set_local_thresholds_for_case4()
{
    bool ok = true;
    const uint32_t percents[4] = {0, 65, 80, 100};
    for (uint8_t prio = 1; prio <= 4; ++prio) {
        ok &= expect_eq_i64(ub_comm_queue_set_congestion_threshold(g_ctx.handle, prio, percents[prio - 1]), 0,
                            "set local threshold case4");
    }
    return ok;
}

uint32_t expected_percent_for_case4(uint8_t prio)
{
    const uint32_t percents[7] = {0, 65, 80, 100, 80, 80, 80};
    return percents[prio - 1];
}

bool expect_case4_status(uint8_t node, uint8_t prio, const char *label)
{
    uint32_t percent = expected_percent_for_case4(prio);
    ub_comm_queue_state_t state = (percent == 0) ? UB_COMM_QUEUE_CONGESTED : UB_COMM_QUEUE_IDLE;
    return expect_status(node, prio, threshold_count(g_capacity, percent), state, label);
}

bool case4_seven_rings()
{
    printf("\n[CASE4] A/B seven rings, thresholds by priority, A queries all\n");
    bool ok = true;
    ok &= expect_true(wait_peer_ready(NODE_B), "A waits B ready");
    ok &= set_local_thresholds_for_case4();

    int32_t ack_ret = 0;
    uint64_t observed = 0;
    const uint32_t percents[4] = {0, 65, 80, 100};
    for (uint8_t prio = 1; prio <= 4; ++prio) {
        ok &= expect_true(send_ctrl_and_wait(CMD_SET_THRESHOLD, prio, percents[prio - 1], &ack_ret, &observed),
                          "A asks B set threshold case4");
        ok &= expect_eq_i64(ack_ret, 0, "B set threshold ret case4");
        ok &= expect_eq_i64((int64_t)observed, (int64_t)threshold_count(g_capacity, percents[prio - 1]),
                            "B observed threshold case4");
    }

    for (uint8_t prio = 1; prio <= 7; ++prio) {
        ok &= expect_case4_status(NODE_A, prio, "A query A ring case4");
        ok &= expect_case4_status(NODE_B, prio, "A query B ring case4");
    }
    (void)send_ctrl_and_wait(CMD_STOP, 1, 0, &ack_ret, &observed);
    return ok;
}

int run_role_a(ub_shm_comm_t &handle)
{
    switch (g_case) {
        case CaseId::CASE1_LOCAL_DEINIT:
            return case1_local_deinit(handle) ? 0 : 2;
        case CaseId::CASE2_REMOTE_DEINIT:
            return case2_remote_deinit() ? 0 : 2;
        case CaseId::CASE3_TWO_RINGS:
            return case3_two_rings() ? 0 : 2;
        case CaseId::CASE4_SEVEN_RINGS:
            return case4_seven_rings() ? 0 : 2;
        default:
            return 2;
    }
}

int run_role_b()
{
    if (g_case == CaseId::CASE1_LOCAL_DEINIT) {
        fprintf(stderr, "case1 does not need role B\n");
        return 1;
    }
    printf("Role B ready for smoke case.\n");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(g_wait_peer_timeout_s * 4);
    while (!g_ctx.stop.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(50ms);
    }
    if (!g_ctx.stop.load(std::memory_order_acquire)) {
        fprintf(stderr, "Role B timeout waiting stop\n");
        return 1;
    }
    return 0;
}

bool parse_case(const char *arg)
{
    if (strcmp(arg, "1") == 0 || strcmp(arg, "case1") == 0) {
        g_case = CaseId::CASE1_LOCAL_DEINIT;
    } else if (strcmp(arg, "2") == 0 || strcmp(arg, "case2") == 0) {
        g_case = CaseId::CASE2_REMOTE_DEINIT;
    } else if (strcmp(arg, "3") == 0 || strcmp(arg, "case3") == 0) {
        g_case = CaseId::CASE3_TWO_RINGS;
    } else if (strcmp(arg, "4") == 0 || strcmp(arg, "case4") == 0) {
        g_case = CaseId::CASE4_SEVEN_RINGS;
    } else {
        return false;
    }
    return true;
}

void print_usage(const char *prog)
{
    printf("Usage: %s --role A|B --case 1|2|3|4 -s <node0_shm> -r <node1_shm> [options]\n", prog);
    printf("Options:\n");
    printf("  --capacity <n>     ring capacity, power of 2, default %u\n", DEFAULT_CAPACITY);
    printf("  --wait <seconds>   peer wait timeout, default %d\n", g_wait_peer_timeout_s);
    printf("  --log-level <0-4>  log level, default ERROR\n");
}

bool parse_args(int argc, char **argv)
{
    static option long_options[] = {
        {"role", required_argument, nullptr, 'R'},
        {"case", required_argument, nullptr, 'c'},
        {"sender-shm", required_argument, nullptr, 's'},
        {"receiver-shm", required_argument, nullptr, 'r'},
        {"capacity", required_argument, nullptr, 'n'},
        {"wait", required_argument, nullptr, 'w'},
        {"log-level", required_argument, nullptr, 'l'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "R:c:s:r:n:w:l:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'R':
                g_role = optarg[0];
                break;
            case 'c':
                if (!parse_case(optarg)) {
                    fprintf(stderr, "invalid case: %s\n", optarg);
                    return false;
                }
                break;
            case 's':
                snprintf(g_sender_shm_name, sizeof(g_sender_shm_name), "%s", optarg);
                break;
            case 'r':
                snprintf(g_receiver_shm_name, sizeof(g_receiver_shm_name), "%s", optarg);
                break;
            case 'n':
                g_capacity = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
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
        fprintf(stderr, "role must be A or B\n");
        return false;
    }
    if (g_capacity == 0 || (g_capacity & (g_capacity - 1)) != 0) {
        fprintf(stderr, "capacity must be power of 2\n");
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

    int ret = (g_role == 'A') ? run_role_a(handle) : run_role_b();
    if (handle != nullptr) {
        ub_comm_queue_deinit(&handle);
    }
    printf("Smoke case result: %s\n", ret == 0 ? "PASS" : "FAIL");
    return ret;
}
