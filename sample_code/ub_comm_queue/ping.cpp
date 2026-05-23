#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <getopt.h>
#include <algorithm>
#include <numeric>
#include <mutex>
#include <cinttypes>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstddef>
#include <future>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

int my_stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
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

    time_t now = time(0);
    char *time_str = ctime(&now);
    time_str[strlen(time_str) - 1] = '\0';

    fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", time_str, file, line, level_str, message);

    return 0;
}

enum MsgType : uint8_t {
    TYPE_PING = 100,
    TYPE_PONG = 101
};

static const size_t   RING_CAPACITY   = 1024;
static const size_t   SLOT_SIZE       = 1024;
static const uint32_t MIN_BODY_LENGTH = 48;

static unsigned long g_shm_size_mb = 1024;
static uint64_t NodeA = 0;
static uint64_t NodeB = 1;
// 改为可修改的全局变量
static char kSenderShmName[64]   = "shm_node0_export";
static char kReceiverShmName[64] = "shm_node1_export";

static uint64_t g_msg_count        = 10000;
static int      g_thread_num       = 1;
static int      g_print_detail_num = 20;
static int      g_warmup_drop      = 1;
static int      g_interval_us      = 0;
static int      g_wait_b_ready_s   = 3;

#pragma pack(push, 1)
struct PingPongMsg {
    uint32_t msg_id;
    int64_t  A_send_time;
    int64_t  B_recv_time;
    int64_t  B_send_time;
    int64_t  A_recv_time;
    char     padding[12];
};
#pragma pack(pop)
static_assert(sizeof(PingPongMsg) == MIN_BODY_LENGTH, "PingPongMsg must be 48 bytes");

static inline int64_t get_ns() {
    return (int64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

#if defined(__aarch64__)
static inline void cpu_relax() { asm volatile("yield" ::: "memory"); }
#else
static inline void cpu_relax() { asm volatile("" ::: "memory"); }
#endif

static std::atomic<uint64_t> g_pong_count{0};
static std::atomic<uint32_t> g_next_id{0};
static std::atomic<uint8_t>* g_done = nullptr;

static std::vector<int64_t>     g_rtt_ns;
static std::vector<int64_t>     g_send_cost_ns;
static std::vector<int64_t>     g_recv_cb_cost_ns;
static std::vector<int64_t>     g_b_process_ns;
static std::vector<PingPongMsg> g_details;

static void on_pong_msg(const message_t *msg, void *ctx) {
    const int64_t t_recv = get_ns();

    if (msg->header.msg_type != TYPE_PONG || msg->header.body_length != MIN_BODY_LENGTH) {
        return;
    }

    PingPongMsg local;
    memcpy(&local, msg->body, sizeof(local));
    local.A_recv_time = t_recv;

    const uint32_t id = local.msg_id;
    if (id < g_rtt_ns.size()) {
        g_rtt_ns[id] = local.A_recv_time - local.A_send_time;
        g_b_process_ns[id] = local.B_send_time - local.B_recv_time;
        g_recv_cb_cost_ns[id] = get_ns() - t_recv;
        if ((int)id < (int)g_details.size()) {
            g_details[id] = local;
        }
        g_done[id].store(1, std::memory_order_release);
        g_pong_count.fetch_add(1, std::memory_order_relaxed);
    }
}

static int init_ub_shm() {
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "Failed to initialize ubsmem attributes!\n");
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "Failed to initialize ubsmem!\n");
        return -1;
    }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "Failed to look up node information!\n");
        return -1;
    }
    return 0;
}

static int map_ub_shm(const char *shm_name, void *&addr) {
    const unsigned long length = g_shm_size_mb * 1024UL * 1024UL;
    int ret = ubsmem_shmem_map(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) {
        fprintf(stderr, "Failed to map shared memory '%s'! ret=%d\n", shm_name, ret);
        return -1;
    }
    fprintf(stdout, "Mapped '%s' at %p\n", shm_name, addr);
    return 0;
}

// 新增：打印帮助信息（包含shm参数）
static void print_help(const char* prog) {
    printf("Usage: %s [-n msg_count] [-t threads] [-d detailN] [-w warmup_drop] [-i interval_us] [-s sender_shm] [-r receiver_shm] [-h]\n", prog);
    printf("  -n: total messages (default 10000)\n");
    printf("  -t: sender threads (default 1)\n");
    printf("  -d: print first N details (default 20)\n");
    printf("  -w: drop first N samples in stats (default 1)\n");
    printf("  -i: per-message interval in us (default 0)\n");
    printf("  -s: sender shared memory name (default: shm_node0_export)\n");
    printf("  -r: receiver shared memory name (default: shm_node1_export)\n");
    printf("  -h: show help\n");
}

static void print_stats() {
    const uint64_t total = g_msg_count;
    const uint64_t ok = g_pong_count.load(std::memory_order_relaxed);
    printf("\n===== A Node Stats =====\n");
    printf("Expected: %" PRIu64 " | Received PONG: %" PRIu64 "\n", total, ok);

    if (ok < (uint64_t)g_warmup_drop + 1) {
        printf("Not enough samples.\n");
        return;
    }

    std::vector<int64_t> rtts;
    rtts.reserve(total - g_warmup_drop);
    for (uint64_t i = (uint64_t)g_warmup_drop; i < total; ++i) {
        rtts.push_back(g_rtt_ns[i]);
    }
    std::sort(rtts.begin(), rtts.end());

    auto percentile = [&](double p) -> int64_t {
        size_t idx = (size_t)((rtts.size() - 1) * p);
        return rtts[idx];
    };

    double sum = 0.0;
    for (auto v : rtts) sum += (double)v;

    printf("\n[RTT ns] avg=%.2f min=%" PRId64 " p50=%" PRId64 " p99=%" PRId64 " max=%" PRId64 "\n",
           sum / rtts.size(),
           rtts.front(),
           percentile(0.50),
           percentile(0.99),
           rtts.back());

    const int N = std::min<int>(g_print_detail_num, (int)g_msg_count);
    if (N > 0) {
        printf("\n===== First %d message details (ID 0..%d) =====\n", N, N - 1);
        for (int i = 0; i < N; ++i) {
            const auto &m = g_details[i];
            printf("\n[ID=%u]\n", m.msg_id);
            printf("  A_send_time : %" PRId64 "\n", m.A_send_time);
            printf("  B_recv_time : %" PRId64 "\n", m.B_recv_time);
            printf("  B_send_time : %" PRId64 "\n", m.B_send_time);
            printf("  A_recv_time : %" PRId64 "\n", m.A_recv_time);
            printf("  RTT(ns)     : %" PRId64 "\n", g_rtt_ns[i]);
            printf("  B_process(ns): %" PRId64 "\n", g_b_process_ns[i]);
            printf("  send_cost(ns): %" PRId64 "\n", g_send_cost_ns[i]);
            printf("  recv_cb_cost(ns): %" PRId64 "\n", g_recv_cb_cost_ns[i]);
        }
    }
}

static void ping_worker(ub_shm_comm_t* handle, int thread_id) {
    message_t msg{};
    msg.header.dest_node_id = NodeB;
    msg.header.src_node_id  = NodeA;
    msg.header.msg_type     = TYPE_PING;
    msg.header.priority     = 1;
    msg.header.body_length  = MIN_BODY_LENGTH;

    PingPongMsg body{};
    msg.body = (char*)&body;

    while (true) {
        const uint32_t id = g_next_id.fetch_add(1, std::memory_order_relaxed);
        if (id >= g_msg_count) break;

        g_done[id].store(0, std::memory_order_relaxed);

        body.msg_id      = id;
        body.A_send_time = 0;
        body.B_recv_time = 0;
        body.B_send_time = 0;
        body.A_recv_time = 0;

        const int64_t t0 = get_ns();
        body.A_send_time = t0;

        for (;;) {
            const int ret = ub_comm_queue_send(handle, &msg);
            if (ret >= 0) break;
            cpu_relax();
        }
        const int64_t t1 = get_ns();
        g_send_cost_ns[id] = t1 - t0;

        while (g_done[id].load(std::memory_order_acquire) == 0) {
            cpu_relax();
        }

        if (g_interval_us > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(g_interval_us));
        }
    }
}

int main(int argc, char* argv[]) {
    int opt;
    ub_atomic_set_log_level(LOG_LEVEL_ERROR); // 设置日志级别为 DEBUG
    ub_atomic_register_log_func(my_stdout_logger);
    // 新增：解析 -s -r 参数
    while ((opt = getopt(argc, argv, "n:t:d:w:i:s:r:h")) != -1) {
        switch (opt) {
            case 'n': g_msg_count = strtoull(optarg, nullptr, 10); break;
            case 't': g_thread_num = atoi(optarg); break;
            case 'd': g_print_detail_num = atoi(optarg); break;
            case 'w': g_warmup_drop = atoi(optarg); break;
            case 'i': g_interval_us = atoi(optarg); break;
            case 's': strncpy(kSenderShmName, optarg, sizeof(kSenderShmName)-1); break;
            case 'r': strncpy(kReceiverShmName, optarg, sizeof(kReceiverShmName)-1); break;
            case 'h': print_help(argv[0]); return 0;
            default: print_help(argv[0]); return -1;
        }
    }
    if (g_thread_num <= 0) g_thread_num = 1;
    if (g_print_detail_num < 0) g_print_detail_num = 0;
    if (g_warmup_drop < 0) g_warmup_drop = 0;

    printf("===== A Node (Pinger) =====\n");
    printf("msg_count=%" PRIu64 " threads=%d detailN=%d warmup_drop=%d interval_us=%d\n",
           g_msg_count, g_thread_num, g_print_detail_num, g_warmup_drop, g_interval_us);
    printf("sender_shm=%s  receiver_shm=%s\n", kSenderShmName, kReceiverShmName);

    g_rtt_ns.assign(g_msg_count, 0);
    g_send_cost_ns.assign(g_msg_count, 0);
    g_recv_cb_cost_ns.assign(g_msg_count, 0);
    g_b_process_ns.assign(g_msg_count, 0);
    g_details.assign(std::min<uint64_t>(g_msg_count, (uint64_t)g_print_detail_num), PingPongMsg{});

    g_done = new std::atomic<uint8_t>[g_msg_count];
    for (uint64_t i = 0; i < g_msg_count; ++i) g_done[i].store(0, std::memory_order_relaxed);

    if (init_ub_shm() != 0) return -1;

    void *sender_shm_base = nullptr;
    if (map_ub_shm(kSenderShmName, sender_shm_base) != 0) return -1;

    void *receiver_shm_base = nullptr;
    if (map_ub_shm(kReceiverShmName, receiver_shm_base) != 0) return -1;

    ub_shm_comm_t handle = nullptr;
    const size_t init_size = 1024 * 1024;
    const size_t ring_size = 1900800;

    void* init_area   = sender_shm_base;
    void* ring_area_A = (char*)sender_shm_base + init_size;
    void* ring_area_B = (char*)receiver_shm_base;

    ub_ring_desc_t ring_desc = {RING_CAPACITY, SLOT_SIZE, 1};
    ub_comm_conf_t conf = {
        .cpu_id = 4,
        .max_nodes = 2,
        .current_node_id = NodeA,
        .num_rings = 1,
        .ring_descs = &ring_desc
    };

    ub_shm_area_t init_shm_area;
    init_shm_area.ptr = init_area;
    init_shm_area.size = init_size;

    ub_ring_region_info_t ring_info[2];
    ring_info[0].node_id = NodeA;
    ring_info[0].region.ptr = ring_area_A;
    ring_info[0].region.size = ring_size;
    ring_info[1].node_id = NodeB;
    ring_info[1].region.ptr = ring_area_B;
    ring_info[1].region.size = ring_size;

    ub_ring_region_map_t ring_map{ ring_info, 2 };

    if (ub_comm_queue_init(&handle, &init_shm_area, &ring_map, &conf) != 0) {
        fprintf(stderr, "ub_comm_queue_init failed\n");
        return -1;
    }

    ub_shm_comm_t* handlep = &handle;
    ub_comm_queue_register_process_func(handlep, TYPE_PONG, UB_FUNC_SYNC, on_pong_msg, nullptr);

    printf("Wait B ready... %ds\n", g_wait_b_ready_s);
    std::this_thread::sleep_for(std::chrono::seconds(g_wait_b_ready_s));

    std::vector<std::thread> threads;
    threads.reserve(g_thread_num);
    for (int i = 0; i < g_thread_num; ++i) {
        threads.emplace_back(ping_worker, handlep, i);
    }

    while (g_pong_count.load(std::memory_order_relaxed) < g_msg_count) {
        std::this_thread::sleep_for(50ms);
    }

    for (auto& t : threads) t.join();

    print_stats();

    ub_comm_queue_deinit(&handle);

    delete[] g_done;
    g_done = nullptr;
    return 0;
}
