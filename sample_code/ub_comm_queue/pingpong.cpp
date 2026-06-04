/*
 * pingpong.cpp - 合并的 ping/pong 测试 demo
 *
 * Usage:
 *   ./pingpong --role A [options]    (相当于原来的 ping，发送端)
 *   ./pingpong --role B [options]    (相当于原来的 pong，接收端)
 *
 * Options:
 *   --role A|B         运行角色 (必选)
 *   --cpu-id <N>       绑定 CPU ID (A 默认 4, B 默认 200)
 *   --msg-size <bytes> 消息总长度，含消息头 (支持: 64, 4096, 8192，默认 64)
 *   -n <count>         消息总数 (A 角色，默认 10000)
 *   -t <threads>       发送线程数 (A 角色，默认 1)
 *   -d <N>             打印前 N 条详情 (A 角色，默认 20)
 *   -w <N>             丢弃前 N 条预热样本 (A 角色，默认 1)
 *   -i <us>            每条消息间隔微秒 (A 角色，默认 0)
 *   -s <shm_name>      发送端共享内存名 (默认 shm_node0_export)
 *   -r <shm_name>      接收端共享内存名 (默认 shm_node1_export)
 *   -h                 显示帮助
 */

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
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstddef>
#include <future>
#include <iostream>
#include <set>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

// ===================== 日志 =====================

static int my_stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    const char *level_str = "UNKNOWN";
    switch (level) {
        case 0: level_str = "DEBUG"; break;
        case 1: level_str = "INFO"; break;
        case 2: level_str = "WARN"; break;
        case 3: level_str = "ERROR"; break;
        case 4: level_str = "CRITICAL"; break;
        default: break;
    }

    time_t now = time(0);
    char *time_str = ctime(&now);
    time_str[strlen(time_str) - 1] = '\0';

    fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", time_str, file, line, level_str, message);
    return 0;
}

// ===================== 常量与类型 =====================

enum Role { ROLE_A, ROLE_B };

enum MsgType : uint8_t {
    TYPE_PING = 100,
    TYPE_PONG = 101
};

static const size_t   RING_CAPACITY = 1024;
static const size_t   HEADER_SIZE   = sizeof(message_header_t); // 16 bytes

// PingPongMsg: 固定 48 字节的消息体头部，包含时间戳信息
static const uint32_t PINGPONG_MSG_SIZE = 48;

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
static_assert(sizeof(PingPongMsg) == PINGPONG_MSG_SIZE, "PingPongMsg must be 48 bytes");

// ===================== 全局配置 =====================

static Role     g_role             = ROLE_A;
static bool     g_role_set         = false; // 是否通过 --role 显式设置
static int      g_cpu_id           = -1;   // -1 表示使用角色默认值
static size_t   g_msg_size         = 64;   // 消息总长度 (含消息头)
static uint64_t NodeA              = 0;
static uint64_t NodeB              = 1;

static char     kSenderShmName[64]   = "shm_node0_export";
static char     kReceiverShmName[64] = "shm_node1_export";

// A 角色参数
static uint64_t g_msg_count        = 10000;
static int      g_thread_num       = 1;
static int      g_print_detail_num = 20;
static int      g_warmup_drop      = 1;
static int      g_interval_us      = 0;
static int      g_wait_b_ready_s   = 3;
static unsigned long g_shm_size_mb = 1024;

// B 角色参数
static uint64_t g_expect           = 0;    // 0=永远运行
static bool     g_expect_set       = false; // 是否通过 -n 显式设置

// ===================== 工具函数 =====================

static inline int64_t get_ns() {
    return (int64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

#if defined(__aarch64__)
static inline void cpu_relax() { asm volatile("yield" ::: "memory"); }
#else
static inline void cpu_relax() { asm volatile("" ::: "memory"); }
#endif

static inline uint32_t body_length_from_msg_size(size_t msg_size) {
    return static_cast<uint32_t>(msg_size - HEADER_SIZE);
}

// ===================== A 角色 (Ping) 状态 =====================

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
    const uint32_t expected_body = body_length_from_msg_size(g_msg_size);

    if (msg->header.msg_type != TYPE_PONG || msg->header.body_length != expected_body) {
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

static void ping_worker(ub_shm_comm_t* handle, int thread_id) {
    const uint32_t body_len = body_length_from_msg_size(g_msg_size);

    message_t msg{};
    msg.header.dest_node_id = NodeB;
    msg.header.src_node_id  = NodeA;
    msg.header.msg_type     = TYPE_PING;
    msg.header.priority     = 1;
    msg.header.body_length  = body_len;

    // 分配消息体缓冲区 (PingPongMsg 头部 + 填充数据)
    std::vector<char> body_buf(body_len, 0);
    msg.body = body_buf.data();

    PingPongMsg* pp = reinterpret_cast<PingPongMsg*>(body_buf.data());

    while (true) {
        const uint32_t id = g_next_id.fetch_add(1, std::memory_order_relaxed);
        if (id >= g_msg_count) break;

        g_done[id].store(0, std::memory_order_relaxed);

        pp->msg_id      = id;
        pp->A_send_time = 0;
        pp->B_recv_time = 0;
        pp->B_send_time = 0;
        pp->A_recv_time = 0;

        const int64_t t0 = get_ns();
        pp->A_send_time = t0;

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

static void print_stats() {
    const uint64_t total = g_msg_count;
    const uint64_t ok = g_pong_count.load(std::memory_order_relaxed);
    printf("\n===== A Node Stats (msg_size=%zu) =====\n", g_msg_size);
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

// ===================== B 角色 (Pong) 状态 =====================

static std::atomic<uint64_t> g_ping_recv{0};
static std::atomic<uint64_t> g_pong_send{0};
static std::atomic<int64_t>  g_b_process_sum{0};

static void on_ping_msg(const message_t *msg, void *ctx) {
    const int64_t t_recv = get_ns();
    const uint32_t expected_body = body_length_from_msg_size(g_msg_size);

    if (msg->header.msg_type != TYPE_PING || msg->header.body_length != expected_body) {
        return;
    }

    // 分配回复消息体缓冲区
    std::vector<char> body_buf(expected_body, 0);
    memcpy(body_buf.data(), msg->body, sizeof(PingPongMsg));

    PingPongMsg* pp = reinterpret_cast<PingPongMsg*>(body_buf.data());
    pp->B_recv_time = t_recv;
    pp->B_send_time = get_ns();

    message_t pong{};
    pong.header.dest_node_id = NodeA;
    pong.header.src_node_id  = NodeB;
    pong.header.msg_type     = TYPE_PONG;
    pong.header.priority     = 1;
    pong.header.body_length  = expected_body;
    pong.body = body_buf.data();

    ub_shm_comm_t* handlep = (ub_shm_comm_t*)ctx;

    for (;;) {
        int ret = ub_comm_queue_send(handlep, &pong);
        if (ret >= 0) break;
        cpu_relax();
    }

    g_ping_recv.fetch_add(1, std::memory_order_relaxed);
    g_pong_send.fetch_add(1, std::memory_order_relaxed);
    g_b_process_sum.fetch_add(pp->B_send_time - pp->B_recv_time, std::memory_order_relaxed);
}

// ===================== 公共函数 =====================

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

static void print_help(const char* prog) {
    printf("Usage: %s --role A|B [options]\n", prog);
    printf("\n");
    printf("Required:\n");
    printf("  --role A|B         运行角色 (A=ping发送端, B=pong接收端)\n");
    printf("\n");
    printf("Common options:\n");
    printf("  --cpu-id <N>       绑定 CPU ID (A 默认 4, B 默认 200)\n");
    printf("  --msg-size <bytes> 消息总长度含消息头 (支持: 64, 4096, 8192，默认 64)\n");
    printf("  -s <shm_name>      发送端共享内存名 (默认 shm_node0_export)\n");
    printf("  -r <shm_name>      接收端共享内存名 (默认 shm_node1_export)\n");
    printf("  -h                 显示帮助\n");
    printf("\n");
    printf("Role A (ping) options:\n");
    printf("  -n <count>         消息总数 (默认 10000)\n");
    printf("  -t <threads>       发送线程数 (默认 1)\n");
    printf("  -d <N>             打印前 N 条详情 (默认 20)\n");
    printf("  -w <N>             丢弃前 N 条预热样本 (默认 1)\n");
    printf("  -i <us>            每条消息间隔微秒 (默认 0)\n");
    printf("\n");
    printf("Role B (pong) options:\n");
    printf("  -n <count>         期望消息数 (0=永远运行，默认 0)\n");
}

// ===================== main =====================

int main(int argc, char* argv[]) {
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);
    ub_atomic_register_log_func(my_stdout_logger);

    static struct option long_options[] = {
        {"role",     required_argument, 0, 'R'},
        {"cpu-id",   required_argument, 0, 'C'},
        {"msg-size", required_argument, 0, 'M'},
        {"help",     no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    int long_index = 0;
    while ((opt = getopt_long(argc, argv, "n:t:d:w:i:s:r:h", long_options, &long_index)) != -1) {
        switch (opt) {
            case 'R': // --role
                if (strcmp(optarg, "A") == 0) {
                    g_role = ROLE_A;
                } else if (strcmp(optarg, "B") == 0) {
                    g_role = ROLE_B;
                } else {
                    fprintf(stderr, "Invalid role '%s'. Must be A or B.\n", optarg);
                    return -1;
                }
                g_role_set = true;
                break;
            case 'C': // --cpu-id
                g_cpu_id = atoi(optarg);
                break;
            case 'M': // --msg-size
                g_msg_size = strtoull(optarg, nullptr, 10);
                break;
            case 'n':
                g_msg_count = strtoull(optarg, nullptr, 10);
                g_expect_set = true;
                break;
            case 't': g_thread_num = atoi(optarg); break;
            case 'd': g_print_detail_num = atoi(optarg); break;
            case 'w': g_warmup_drop = atoi(optarg); break;
            case 'i': g_interval_us = atoi(optarg); break;
            case 's': strncpy(kSenderShmName, optarg, sizeof(kSenderShmName) - 1); break;
            case 'r': strncpy(kReceiverShmName, optarg, sizeof(kReceiverShmName) - 1); break;
            case 'h': print_help(argv[0]); return 0;
            default:  print_help(argv[0]); return -1;
        }
    }

    // 校验 --role 必须显式指定
    if (!g_role_set) {
        fprintf(stderr, "Error: --role A|B is required.\n");
        print_help(argv[0]);
        return -1;
    }

    // 校验 msg_size
    if (g_msg_size != 64 && g_msg_size != 4096 && g_msg_size != 8192) {
        fprintf(stderr, "Invalid --msg-size %zu. Supported values: 64, 4096, 8192\n", g_msg_size);
        return -1;
    }

    // 校验 msg_size 至少能容纳 header + PingPongMsg
    if (g_msg_size < HEADER_SIZE + PINGPONG_MSG_SIZE) {
        fprintf(stderr, "msg_size %zu too small, must be >= %zu (header + PingPongMsg)\n",
                g_msg_size, HEADER_SIZE + PINGPONG_MSG_SIZE);
        return -1;
    }

    // 根据角色设置默认 cpu_id
    if (g_cpu_id < 0) {
        g_cpu_id = (g_role == ROLE_A) ? 4 : 200;
    }

    // B 角色特殊参数：-n 含义不同（期望接收数），默认 0 表示永远运行
    if (g_role == ROLE_B) {
        g_expect = g_expect_set ? g_msg_count : 0;
        g_shm_size_mb = 128;
    }

    // 根据角色校验必要参数
    if (g_role == ROLE_A) {
        if (g_thread_num <= 0) g_thread_num = 1;
        if (g_print_detail_num < 0) g_print_detail_num = 0;
        if (g_warmup_drop < 0) g_warmup_drop = 0;
    }

    // 打印配置
    const char* role_str = (g_role == ROLE_A) ? "A (Pinger)" : "B (Ponger)";
    printf("===== Role %s =====\n", role_str);
    printf("cpu_id=%d msg_size=%zu body_length=%u\n",
           g_cpu_id, g_msg_size, body_length_from_msg_size(g_msg_size));
    printf("sender_shm=%s  receiver_shm=%s\n", kSenderShmName, kReceiverShmName);

    if (g_role == ROLE_A) {
        printf("msg_count=%" PRIu64 " threads=%d detailN=%d warmup_drop=%d interval_us=%d\n",
               g_msg_count, g_thread_num, g_print_detail_num, g_warmup_drop, g_interval_us);
    } else {
        printf("expect=%" PRIu64 " (0=forever)\n", g_expect);
    }

    // ===================== 初始化共享内存 =====================

    if (init_ub_shm() != 0) return -1;

    void *sender_shm_base = nullptr;
    if (map_ub_shm(kSenderShmName, sender_shm_base) != 0) return -1;

    void *receiver_shm_base = nullptr;
    if (map_ub_shm(kReceiverShmName, receiver_shm_base) != 0) return -1;

    // ===================== 初始化通信队列 =====================

    ub_shm_comm_t handle = nullptr;
    const size_t init_size = 1024 * 1024;

    // 根据 msg_size 动态计算 ring_size
    // 每个 entry 大小 ≈ entry_header(8B) + max_msg_size，向上对齐到 64B cache line
    // ring 总大小 ≈ GetDataOffset + capacity * entry_stride
    // 为安全起见，使用 capacity * msg_size * 2 并确保不小于当前默认值
    const size_t min_ring_size = 1900800;
    const size_t ring_size = std::max(min_ring_size, RING_CAPACITY * g_msg_size * 2);

    void* init_area   = sender_shm_base;
    void* ring_area_A = (char*)sender_shm_base + init_size;
    void* ring_area_B = (char*)receiver_shm_base;

    // max_msg_size 设置为 g_msg_size，确保能容纳完整的消息 (header + body)
    ub_ring_desc_t ring_desc = {static_cast<uint32_t>(RING_CAPACITY),
                                static_cast<uint32_t>(g_msg_size),
                                1};
    ub_comm_conf_t conf = {
        .cpu_id = g_cpu_id,
        .max_nodes = 2,
        .current_node_id = (g_role == ROLE_A) ? NodeA : NodeB,
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

    // ===================== 角色分支 =====================

    if (g_role == ROLE_A) {
        // ---- A 角色: Pinger ----
        ub_comm_queue_register_process_func(handlep, TYPE_PONG, UB_FUNC_SYNC, on_pong_msg, nullptr);

        g_rtt_ns.assign(g_msg_count, 0);
        g_send_cost_ns.assign(g_msg_count, 0);
        g_recv_cb_cost_ns.assign(g_msg_count, 0);
        g_b_process_ns.assign(g_msg_count, 0);
        g_details.assign(std::min<uint64_t>(g_msg_count, (uint64_t)g_print_detail_num), PingPongMsg{});

        g_done = new std::atomic<uint8_t>[g_msg_count];
        for (uint64_t i = 0; i < g_msg_count; ++i) g_done[i].store(0, std::memory_order_relaxed);

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

        delete[] g_done;
        g_done = nullptr;

    } else {
        // ---- B 角色: Ponger ----
        ub_comm_queue_register_process_func(handlep, TYPE_PING, UB_FUNC_SYNC, on_ping_msg, handlep);

        printf("B ready.\n");

        while (true) {
            if (g_expect > 0 && g_pong_send.load(std::memory_order_relaxed) >= g_expect) break;
            std::this_thread::sleep_for(1s);
            const auto cnt = g_pong_send.load(std::memory_order_relaxed);
            if (cnt > 0) {
                double avg = (double)g_b_process_sum.load(std::memory_order_relaxed) / (double)cnt;
                printf("[B] pong_sent=%" PRIu64 " avg_b_process(ns)=%.2f\n", cnt, avg);
            }
        }

        printf("\n===== B Summary (msg_size=%zu) =====\n", g_msg_size);
        printf("ping_recv=%" PRIu64 " pong_sent=%" PRIu64 "\n",
               g_ping_recv.load(), g_pong_send.load());
        if (g_pong_send.load() > 0) {
            double avg = (double)g_b_process_sum.load() / (double)g_pong_send.load();
            printf("avg_b_process(ns)=%.2f\n", avg);
        }
    }

    ub_comm_queue_deinit(&handle);
    return 0;
}
