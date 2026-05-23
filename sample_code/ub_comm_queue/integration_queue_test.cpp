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
#include <mutex>
#include <thread>
#include <vector>

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
constexpr uint32_t RING_CAPACITY = 1024;
constexpr uint32_t MAX_MSG_SIZE = 256;
constexpr uint8_t PRIORITY_LOW = 1;
constexpr uint8_t PRIORITY_MID = 2;
constexpr uint8_t PRIORITY_HIGH = 3;

enum MsgType : uint8_t {
    TYPE_SELF_SYNC = 100,
    TYPE_SELF_ASYNC = 101,
    TYPE_CROSS_SYNC = 110,
    TYPE_CROSS_ASYNC = 111,
    TYPE_ACK_SINGLE = 112,
    TYPE_MIX_LOW = 120,
    TYPE_MIX_MID = 121,
    TYPE_MIX_HIGH = 122,
    TYPE_MIX_HIGH_ALT = 123,
    TYPE_MIX_RESULT = 124,
};

enum CaseId : uint32_t {
    CASE_SELF_SYNC = 1,
    CASE_SELF_ASYNC = 2,
    CASE_CROSS_SYNC = 3,
    CASE_CROSS_ASYNC = 4,
    CASE_MIXED_PRIORITY = 5,
};

#pragma pack(push, 1)
struct TestMessage {
    uint32_t case_id;
    uint32_t seq;
    uint32_t expected_total;
    uint32_t sender_thread;
    uint8_t sender_priority;
    uint8_t sender_type;
    uint16_t reserved0;
    uint64_t send_ns;
    char padding[32];
};

struct MixedSummary {
    uint32_t case_id;
    uint32_t expected_total;
    uint32_t total_received;
    uint32_t low_received;
    uint32_t mid_received;
    uint32_t high_received;
    uint32_t high_alt_received;
    uint32_t high_isolation_violations;
    uint32_t mid_isolation_violations;
    char padding[32];
};
#pragma pack(pop)

static_assert(sizeof(TestMessage) <= MAX_MSG_SIZE - sizeof(message_header_t), "TestMessage body too large");
static_assert(sizeof(MixedSummary) <= MAX_MSG_SIZE - sizeof(message_header_t), "MixedSummary body too large");

struct MixTracker {
    uint32_t expected_low = 0;
    uint32_t expected_mid = 0;
    uint32_t expected_high_total = 0;
    uint32_t low_received = 0;
    uint32_t mid_received = 0;
    uint32_t high_received = 0;
    uint32_t high_alt_received = 0;
    uint32_t total_received = 0;
    uint32_t high_isolation_violations = 0;
    uint32_t mid_isolation_violations = 0;
    bool first_mid_seen = false;
    bool first_high_seen = false;
    bool result_sent = false;
};

struct TestContext {
    ub_shm_comm_t *handle = nullptr;
    uint8_t self_node = NODE_A;
    uint8_t peer_node = NODE_B;
    uint32_t basic_count = 64;
    uint32_t mix_threads = 4;
    uint32_t mix_msgs_per_thread = 64;

    std::atomic<uint32_t> self_sync_received{0};
    std::atomic<uint32_t> self_async_received{0};
    std::atomic<uint32_t> cross_sync_acks{0};
    std::atomic<uint32_t> cross_async_acks{0};
    std::atomic<bool> mix_result_ready{false};
    std::atomic<uint32_t> b_cross_sync_received{0};
    std::atomic<uint32_t> b_cross_async_received{0};

    std::mutex mix_mu;
    MixTracker mix_tracker{};
    MixedSummary mix_summary{};
};

char g_role = 'A';
char g_sender_shm_name[64] = "shm_node0_export";
char g_receiver_shm_name[64] = "shm_node1_export";
int g_wait_peer_timeout_s = 15;
int g_log_level = LOG_LEVEL_ERROR;
TestContext g_ctx;

int my_stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    (void)func;
    const char *level_str = "UNKNOWN";
    switch (level) {
        case 0:
            level_str = "DEBUG";
            break;
        case 1:
            level_str = "INFO";
            break;
        case 2:
            level_str = "WARN";
            break;
        case 3:
            level_str = "ERROR";
            break;
        case 4:
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

bool send_with_retry(ub_shm_comm_t *handle, const message_t *msg, int timeout_ms = 3000)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (ub_comm_queue_send(handle, msg) >= 0) {
            return true;
        }
        cpu_relax();
        std::this_thread::sleep_for(50us);
    }
    return false;
}

bool wait_until_true(const std::atomic<uint32_t> &counter, uint32_t expected, int timeout_ms, const char *label)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    fprintf(stderr, "Timeout waiting for %s. expect=%u actual=%u\n", label, expected,
            counter.load(std::memory_order_acquire));
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
        fprintf(stderr, "ubsmem_init_attributes failed\n");
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_initialize failed\n");
        return -1;
    }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_lookup_regions failed\n");
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

void send_ack(uint32_t case_id, uint32_t seq)
{
    TestMessage ack{};
    ack.case_id = case_id;
    ack.seq = seq;
    ack.send_ns = static_cast<uint64_t>(now_ns());

    message_t msg{};
    msg.header.dest_node_id = g_ctx.peer_node;
    msg.header.src_node_id = g_ctx.self_node;
    msg.header.msg_type = TYPE_ACK_SINGLE;
    msg.header.priority = PRIORITY_HIGH;
    msg.header.body_length = sizeof(ack);
    msg.body = reinterpret_cast<char *>(&ack);

    if (!send_with_retry(g_ctx.handle, &msg)) {
        fprintf(stderr, "Failed to send ACK for case=%u seq=%u\n", case_id, seq);
    }
}

void send_mix_result_locked()
{
    if (g_ctx.mix_tracker.result_sent) {
        return;
    }

    MixedSummary summary{};
    summary.case_id = CASE_MIXED_PRIORITY;
    summary.expected_total =
        g_ctx.mix_tracker.expected_low + g_ctx.mix_tracker.expected_mid + g_ctx.mix_tracker.expected_high_total;
    summary.total_received = g_ctx.mix_tracker.total_received;
    summary.low_received = g_ctx.mix_tracker.low_received;
    summary.mid_received = g_ctx.mix_tracker.mid_received;
    summary.high_received = g_ctx.mix_tracker.high_received;
    summary.high_alt_received = g_ctx.mix_tracker.high_alt_received;
    summary.high_isolation_violations = g_ctx.mix_tracker.high_isolation_violations;
    summary.mid_isolation_violations = g_ctx.mix_tracker.mid_isolation_violations;

    message_t msg{};
    msg.header.dest_node_id = g_ctx.peer_node;
    msg.header.src_node_id = g_ctx.self_node;
    msg.header.msg_type = TYPE_MIX_RESULT;
    msg.header.priority = PRIORITY_HIGH;
    msg.header.body_length = sizeof(summary);
    msg.body = reinterpret_cast<char *>(&summary);

    if (!send_with_retry(g_ctx.handle, &msg)) {
        fprintf(stderr, "Failed to send mix result summary\n");
        return;
    }
    g_ctx.mix_tracker.result_sent = true;
}

void on_self_sync(const message_t *msg, void *ctx)
{
    (void)msg;
    auto *test_ctx = reinterpret_cast<TestContext *>(ctx);
    test_ctx->self_sync_received.fetch_add(1, std::memory_order_release);
}

void on_self_async(const message_t *msg, void *ctx)
{
    (void)msg;
    auto *test_ctx = reinterpret_cast<TestContext *>(ctx);
    test_ctx->self_async_received.fetch_add(1, std::memory_order_release);
}

void on_ack_single(const message_t *msg, void *ctx)
{
    auto *test_ctx = reinterpret_cast<TestContext *>(ctx);
    if (msg->header.body_length != sizeof(TestMessage)) {
        return;
    }
    const auto *body = reinterpret_cast<const TestMessage *>(msg->body);
    if (body->case_id == CASE_CROSS_SYNC) {
        test_ctx->cross_sync_acks.fetch_add(1, std::memory_order_release);
    } else if (body->case_id == CASE_CROSS_ASYNC) {
        test_ctx->cross_async_acks.fetch_add(1, std::memory_order_release);
    }
}

void on_mix_result(const message_t *msg, void *ctx)
{
    auto *test_ctx = reinterpret_cast<TestContext *>(ctx);
    if (msg->header.body_length != sizeof(MixedSummary)) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(test_ctx->mix_mu);
        memcpy(&test_ctx->mix_summary, msg->body, sizeof(MixedSummary));
    }
    test_ctx->mix_result_ready.store(true, std::memory_order_release);
}

void on_cross_sync(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg->header.body_length != sizeof(TestMessage)) {
        return;
    }
    const auto *body = reinterpret_cast<const TestMessage *>(msg->body);
    g_ctx.b_cross_sync_received.fetch_add(1, std::memory_order_relaxed);
    send_ack(body->case_id, body->seq);
}

void on_cross_async(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg->header.body_length != sizeof(TestMessage)) {
        return;
    }
    const auto *body = reinterpret_cast<const TestMessage *>(msg->body);
    g_ctx.b_cross_async_received.fetch_add(1, std::memory_order_relaxed);
    send_ack(body->case_id, body->seq);
}

void on_mix_message(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg->header.body_length != sizeof(TestMessage)) {
        return;
    }

    const auto *body = reinterpret_cast<const TestMessage *>(msg->body);
    std::lock_guard<std::mutex> lock(g_ctx.mix_mu);

    if (msg->header.priority == PRIORITY_HIGH) {
        g_ctx.mix_tracker.first_high_seen = true;
        if (msg->header.msg_type == TYPE_MIX_HIGH) {
            g_ctx.mix_tracker.high_received++;
        } else {
            g_ctx.mix_tracker.high_alt_received++;
        }
        g_ctx.mix_tracker.total_received++;
    } else if (msg->header.priority == PRIORITY_MID) {
        if (g_ctx.mix_tracker.first_high_seen &&
            (g_ctx.mix_tracker.high_received + g_ctx.mix_tracker.high_alt_received) <
                g_ctx.mix_tracker.expected_high_total) {
            g_ctx.mix_tracker.high_isolation_violations++;
        }
        g_ctx.mix_tracker.first_mid_seen = true;
        g_ctx.mix_tracker.mid_received++;
        g_ctx.mix_tracker.total_received++;
    } else if (msg->header.priority == PRIORITY_LOW) {
        if (g_ctx.mix_tracker.first_high_seen &&
            (g_ctx.mix_tracker.high_received + g_ctx.mix_tracker.high_alt_received) <
                g_ctx.mix_tracker.expected_high_total) {
            g_ctx.mix_tracker.high_isolation_violations++;
        }
        if (g_ctx.mix_tracker.first_mid_seen && g_ctx.mix_tracker.mid_received < g_ctx.mix_tracker.expected_mid) {
            g_ctx.mix_tracker.mid_isolation_violations++;
        }
        g_ctx.mix_tracker.low_received++;
        g_ctx.mix_tracker.total_received++;
        std::this_thread::sleep_for(1ms);
    }

    const uint32_t expected_total = body->expected_total;
    if (g_ctx.mix_tracker.total_received == expected_total) {
        send_mix_result_locked();
    }
}

void fill_test_message(TestMessage &body, uint32_t case_id, uint32_t seq, uint32_t expected_total, uint32_t sender_thread,
                       uint8_t priority, uint8_t type)
{
    memset(&body, 0, sizeof(body));
    body.case_id = case_id;
    body.seq = seq;
    body.expected_total = expected_total;
    body.sender_thread = sender_thread;
    body.sender_priority = priority;
    body.sender_type = type;
    body.send_ns = static_cast<uint64_t>(now_ns());
}

bool send_one(uint8_t dest_node, uint8_t msg_type, uint8_t priority, const TestMessage &body)
{
    message_t msg{};
    msg.header.dest_node_id = dest_node;
    msg.header.src_node_id = g_ctx.self_node;
    msg.header.msg_type = msg_type;
    msg.header.priority = priority;
    msg.header.body_length = sizeof(body);
    msg.body = const_cast<char *>(reinterpret_cast<const char *>(&body));
    return send_with_retry(g_ctx.handle, &msg);
}

bool run_self_case(uint32_t case_id, uint8_t msg_type, std::atomic<uint32_t> &counter)
{
    const uint32_t before = counter.load(std::memory_order_acquire);
    for (uint32_t i = 0; i < g_ctx.basic_count; ++i) {
        TestMessage body{};
        fill_test_message(body, case_id, i, g_ctx.basic_count, 0, PRIORITY_LOW, msg_type);
        if (!send_one(g_ctx.self_node, msg_type, PRIORITY_LOW, body)) {
            fprintf(stderr, "Self case send failed. case=%u seq=%u\n", case_id, i);
            return false;
        }
    }
    return wait_until_true(counter, before + g_ctx.basic_count, 5000,
                           case_id == CASE_SELF_SYNC ? "self_sync" : "self_async");
}

bool run_cross_case(uint32_t case_id, uint8_t msg_type, std::atomic<uint32_t> &ack_counter)
{
    const uint32_t before = ack_counter.load(std::memory_order_acquire);
    for (uint32_t i = 0; i < g_ctx.basic_count; ++i) {
        TestMessage body{};
        fill_test_message(body, case_id, i, g_ctx.basic_count, 0, PRIORITY_MID, msg_type);
        if (!send_one(g_ctx.peer_node, msg_type, PRIORITY_MID, body)) {
            fprintf(stderr, "Cross case send failed. case=%u seq=%u\n", case_id, i);
            return false;
        }
    }
    return wait_until_true(ack_counter, before + g_ctx.basic_count, 8000,
                           case_id == CASE_CROSS_SYNC ? "cross_sync_ack" : "cross_async_ack");
}

void mixed_sender_worker(uint32_t thread_idx, uint8_t msg_type, uint8_t priority, uint32_t per_thread_count,
                         uint32_t expected_total)
{
    for (uint32_t i = 0; i < per_thread_count; ++i) {
        const uint32_t seq = thread_idx * per_thread_count + i;
        TestMessage body{};
        fill_test_message(body, CASE_MIXED_PRIORITY, seq, expected_total, thread_idx, priority, msg_type);
        if (!send_one(g_ctx.peer_node, msg_type, priority, body)) {
            fprintf(stderr, "Mixed send failed. thread=%u seq=%u prio=%u type=%u\n", thread_idx, seq, priority, msg_type);
        }
    }
}

bool run_mixed_case()
{
    g_ctx.mix_result_ready.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(g_ctx.mix_mu);
        g_ctx.mix_summary = MixedSummary{};
    }

    const uint32_t low_total = g_ctx.mix_threads * g_ctx.mix_msgs_per_thread;
    const uint32_t mid_total = g_ctx.mix_threads * g_ctx.mix_msgs_per_thread;
    const uint32_t high_total = g_ctx.mix_threads * g_ctx.mix_msgs_per_thread;
    const uint32_t expected_total = low_total + mid_total + high_total;

    std::vector<std::thread> workers;
    workers.reserve(g_ctx.mix_threads * 3);

    for (uint32_t i = 0; i < g_ctx.mix_threads; ++i) {
        workers.emplace_back(mixed_sender_worker, i, TYPE_MIX_LOW, PRIORITY_LOW, g_ctx.mix_msgs_per_thread,
                             expected_total);
    }

    std::this_thread::sleep_for(15ms);

    for (uint32_t i = 0; i < g_ctx.mix_threads; ++i) {
        workers.emplace_back(mixed_sender_worker, i, TYPE_MIX_MID, PRIORITY_MID, g_ctx.mix_msgs_per_thread,
                             expected_total);
    }

    std::this_thread::sleep_for(15ms);

    for (uint32_t i = 0; i < g_ctx.mix_threads; ++i) {
        const uint8_t type = (i % 2 == 0) ? TYPE_MIX_HIGH : TYPE_MIX_HIGH_ALT;
        workers.emplace_back(mixed_sender_worker, i, type, PRIORITY_HIGH, g_ctx.mix_msgs_per_thread, expected_total);
    }

    for (auto &worker : workers) {
        worker.join();
    }

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (g_ctx.mix_result_ready.load(std::memory_order_acquire)) {
            break;
        }
        std::this_thread::sleep_for(5ms);
    }

    if (!g_ctx.mix_result_ready.load(std::memory_order_acquire)) {
        fprintf(stderr, "Timeout waiting mix result\n");
        return false;
    }

    std::lock_guard<std::mutex> lock(g_ctx.mix_mu);
    const MixedSummary &summary = g_ctx.mix_summary;
    const uint32_t got_high_total = summary.high_received + summary.high_alt_received;

    if (summary.total_received != expected_total || summary.low_received != low_total || summary.mid_received != mid_total ||
        got_high_total != high_total || summary.high_isolation_violations != 0 ||
        summary.mid_isolation_violations != 0) {
        fprintf(stderr,
                "Mixed case validation failed. total=%u/%u low=%u/%u mid=%u/%u high=%u/%u high_violation=%u "
                "mid_violation=%u\n",
                summary.total_received, expected_total, summary.low_received, low_total, summary.mid_received, mid_total,
                got_high_total, high_total, summary.high_isolation_violations, summary.mid_isolation_violations);
        return false;
    }
    return true;
}

void print_help(const char *prog)
{
    printf("Usage: %s --role A|B [-s sender_shm] [-r receiver_shm] [-n basic_count] [-t mix_threads] "
           "[-m mix_msgs_per_thread] [-w wait_peer_timeout_s] [-l log_level]\n",
           prog);
    printf("  --role/-o : process role, A runs integrated checks, B runs receiver/ack service\n");
    printf("  -s        : sender shared memory name, default shm_node0_export\n");
    printf("  -r        : receiver shared memory name, default shm_node1_export\n");
    printf("  -n        : message count of each simple case, default 64\n");
    printf("  -t        : sender threads of mixed case per priority, default 4\n");
    printf("  -m        : messages per thread of mixed case per priority, default 64\n");
    printf("  -w        : wait peer timeout seconds, default 15\n");
    printf("  -l        : log level, default 3(ERROR)\n");
}

bool parse_args(int argc, char **argv)
{
    static struct option long_options[] = {
        {"role", required_argument, nullptr, 'o'},
        {nullptr, 0, nullptr, 0},
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "o:s:r:n:t:m:w:l:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'o':
                g_role = optarg[0];
                break;
            case 's':
                strncpy(g_sender_shm_name, optarg, sizeof(g_sender_shm_name) - 1);
                break;
            case 'r':
                strncpy(g_receiver_shm_name, optarg, sizeof(g_receiver_shm_name) - 1);
                break;
            case 'n':
                g_ctx.basic_count = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case 't':
                g_ctx.mix_threads = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case 'm':
                g_ctx.mix_msgs_per_thread = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case 'w':
                g_wait_peer_timeout_s = atoi(optarg);
                break;
            case 'l':
                g_log_level = atoi(optarg);
                break;
            case 'h':
                print_help(argv[0]);
                return false;
            default:
                print_help(argv[0]);
                return false;
        }
    }

    if (g_role != 'A' && g_role != 'B') {
        fprintf(stderr, "Invalid role '%c', expected A or B\n", g_role);
        return false;
    }
    if (g_ctx.basic_count == 0 || g_ctx.mix_threads == 0 || g_ctx.mix_msgs_per_thread == 0) {
        fprintf(stderr, "basic_count/mix_threads/mix_msgs_per_thread must be greater than 0\n");
        return false;
    }
    return true;
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
    ring_descs[0] = {RING_CAPACITY, MAX_MSG_SIZE, PRIORITY_LOW};
    ring_descs[1] = {RING_CAPACITY, MAX_MSG_SIZE, PRIORITY_MID};
    ring_descs[2] = {RING_CAPACITY, MAX_MSG_SIZE, PRIORITY_HIGH};

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
    g_ctx.mix_tracker.expected_low = g_ctx.mix_threads * g_ctx.mix_msgs_per_thread;
    g_ctx.mix_tracker.expected_mid = g_ctx.mix_threads * g_ctx.mix_msgs_per_thread;
    g_ctx.mix_tracker.expected_high_total = g_ctx.mix_threads * g_ctx.mix_msgs_per_thread;
    return 0;
}

bool register_callbacks()
{
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_ACK_SINGLE, UB_FUNC_SYNC, on_ack_single, &g_ctx) != 0) {
        return false;
    }
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_MIX_RESULT, UB_FUNC_SYNC, on_mix_result, &g_ctx) != 0) {
        return false;
    }

    if (g_role == 'A') {
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_SELF_SYNC, UB_FUNC_SYNC, on_self_sync, &g_ctx) != 0) {
            return false;
        }
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_SELF_ASYNC, UB_FUNC_ASYNC, on_self_async, &g_ctx) != 0) {
            return false;
        }
    } else {
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_CROSS_SYNC, UB_FUNC_SYNC, on_cross_sync, &g_ctx) != 0) {
            return false;
        }
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_CROSS_ASYNC, UB_FUNC_ASYNC, on_cross_async, &g_ctx) != 0) {
            return false;
        }
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_MIX_LOW, UB_FUNC_SYNC, on_mix_message, &g_ctx) != 0) {
            return false;
        }
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_MIX_MID, UB_FUNC_SYNC, on_mix_message, &g_ctx) != 0) {
            return false;
        }
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_MIX_HIGH, UB_FUNC_SYNC, on_mix_message, &g_ctx) != 0) {
            return false;
        }
        if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_MIX_HIGH_ALT, UB_FUNC_SYNC, on_mix_message, &g_ctx) != 0) {
            return false;
        }
    }
    return true;
}

int run_role_a()
{
    if (!wait_peer_ready(g_ctx.handle, g_ctx.peer_node, g_wait_peer_timeout_s)) {
        fprintf(stderr, "Peer node is not ready within %d seconds\n", g_wait_peer_timeout_s);
        return 1;
    }

    printf("[CASE 1] A send A recv, SYNC callback\n");
    if (!run_self_case(CASE_SELF_SYNC, TYPE_SELF_SYNC, g_ctx.self_sync_received)) {
        return 2;
    }

    printf("[CASE 1] A send A recv, ASYNC callback\n");
    if (!run_self_case(CASE_SELF_ASYNC, TYPE_SELF_ASYNC, g_ctx.self_async_received)) {
        return 3;
    }

    printf("[CASE 2] A send B recv, SYNC callback\n");
    if (!run_cross_case(CASE_CROSS_SYNC, TYPE_CROSS_SYNC, g_ctx.cross_sync_acks)) {
        return 4;
    }

    printf("[CASE 2] A send B recv, ASYNC callback\n");
    if (!run_cross_case(CASE_CROSS_ASYNC, TYPE_CROSS_ASYNC, g_ctx.cross_async_acks)) {
        return 5;
    }

    printf("[CASE 3] concurrent mixed types/priorities with isolation check\n");
    if (!run_mixed_case()) {
        return 6;
    }

    {
        std::lock_guard<std::mutex> lock(g_ctx.mix_mu);
        const auto &summary = g_ctx.mix_summary;
        printf("All cases passed.\n");
        printf("  self_sync=%u self_async=%u cross_sync_ack=%u cross_async_ack=%u\n",
               g_ctx.self_sync_received.load(), g_ctx.self_async_received.load(), g_ctx.cross_sync_acks.load(),
               g_ctx.cross_async_acks.load());
        printf("  mix_total=%u low=%u mid=%u high=%u high_alt=%u high_violation=%u mid_violation=%u\n",
               summary.total_received, summary.low_received, summary.mid_received, summary.high_received,
               summary.high_alt_received, summary.high_isolation_violations, summary.mid_isolation_violations);
    }

    return 0;
}

int run_role_b()
{
    printf("Role B is ready. basic_count=%u mix_threads=%u mix_msgs_per_thread=%u\n", g_ctx.basic_count, g_ctx.mix_threads,
           g_ctx.mix_msgs_per_thread);

    const uint32_t expected_mix_total = (g_ctx.mix_threads * g_ctx.mix_msgs_per_thread) * 3;
    const auto deadline = std::chrono::steady_clock::now() + 60s;

    while (std::chrono::steady_clock::now() < deadline) {
        const uint32_t sync_cnt = g_ctx.b_cross_sync_received.load(std::memory_order_relaxed);
        const uint32_t async_cnt = g_ctx.b_cross_async_received.load(std::memory_order_relaxed);

        uint32_t mix_total = 0;
        {
            std::lock_guard<std::mutex> lock(g_ctx.mix_mu);
            mix_total = g_ctx.mix_tracker.total_received;
        }

        if (sync_cnt >= g_ctx.basic_count && async_cnt >= g_ctx.basic_count && mix_total >= expected_mix_total) {
            printf("Role B completed all expected traffic.\n");
            return 0;
        }
        std::this_thread::sleep_for(200ms);
    }

    fprintf(stderr, "Role B timed out. cross_sync=%u cross_async=%u\n", g_ctx.b_cross_sync_received.load(),
            g_ctx.b_cross_async_received.load());
    return 7;
}

} // namespace

int main(int argc, char **argv)
{
    if (!parse_args(argc, argv)) {
        return 1;
    }

    ub_atomic_set_log_level(g_log_level);
    ub_atomic_register_log_func(my_stdout_logger);

    ub_shm_comm_t handle = nullptr;
    if (init_handle(handle) != 0) {
        return 1;
    }

    if (!register_callbacks()) {
        fprintf(stderr, "register_callbacks failed\n");
        ub_comm_queue_deinit(&handle);
        return 1;
    }

    const int ret = (g_role == 'A') ? run_role_a() : run_role_b();
    ub_comm_queue_deinit(&handle);
    return ret;
}
