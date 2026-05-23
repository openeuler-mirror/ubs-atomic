
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

enum MsgType : uint8_t {
    TYPE_PING = 100,
    TYPE_PONG = 101
};

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

static const size_t   RING_CAPACITY   = 1024;
static const size_t   SLOT_SIZE       = 1024;
static const uint32_t MIN_BODY_LENGTH = 48;

static unsigned long g_shm_size_mb = 128;
static uint64_t NodeA = 0;
static uint64_t NodeB = 1;
// 改为可修改的全局变量
static char kSenderShmName[64]   = "shm_node0_export";
static char kReceiverShmName[64] = "shm_node1_export";

static uint64_t g_expect = 0;

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

static std::atomic<uint64_t> g_ping_recv{0};
static std::atomic<uint64_t> g_pong_send{0};
static std::atomic<int64_t>  g_b_process_sum{0};

static void on_ping_msg(const message_t *msg, void *ctx) {
    const int64_t t_recv = get_ns();
    if (msg->header.msg_type != TYPE_PING || msg->header.body_length != MIN_BODY_LENGTH) {
        return;
    }

    PingPongMsg local;
    memcpy(&local, msg->body, sizeof(local));

    local.B_recv_time = t_recv;
    local.B_send_time = get_ns();

    message_t pong{};
    pong.header.dest_node_id = NodeA;
    pong.header.src_node_id  = NodeB;
    pong.header.msg_type     = TYPE_PONG;
    pong.header.priority     = 1;
    pong.header.body_length  = MIN_BODY_LENGTH;
    pong.body = (char*)&local;

    ub_shm_comm_t* handlep = (ub_shm_comm_t*)ctx;

    for (;;) {
        int ret = ub_comm_queue_send(handlep, &pong);
        if (ret >= 0) break;
#if defined(__aarch64__)
        asm volatile("yield" ::: "memory");
#endif
    }

    g_ping_recv.fetch_add(1, std::memory_order_relaxed);
    g_pong_send.fetch_add(1, std::memory_order_relaxed);
    g_b_process_sum.fetch_add(local.B_send_time - local.B_recv_time, std::memory_order_relaxed);
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
    printf("Usage: %s [-n expect_count(0=run forever)] [-s sender_shm] [-r receiver_shm] [-h]\n", prog);
    printf("  -n: expect pong count (0=forever, default: 0)\n");
    printf("  -s: sender shared memory name (default: shm_node0_export)\n");
    printf("  -r: receiver shared memory name (default: shm_node1_export)\n");
    printf("  -h: show help\n");
}

int main(int argc, char** argv) {
    int opt;
    // 新增：解析 -s -r 参数
    while ((opt = getopt(argc, argv, "n:s:r:h")) != -1) {
        switch (opt) {
            case 'n': g_expect = strtoull(optarg, nullptr, 10); break;
            case 's': strncpy(kSenderShmName, optarg, sizeof(kSenderShmName)-1); break;
            case 'r': strncpy(kReceiverShmName, optarg, sizeof(kReceiverShmName)-1); break;
            case 'h': print_help(argv[0]); return 0;
            default:  print_help(argv[0]); return -1;
        }
    }
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);
    ub_atomic_register_log_func(my_stdout_logger);

    printf("===== B Node (Ponger) ===== expect=%" PRIu64 " (0=forever)\n", g_expect);
    printf("sender_shm=%s  receiver_shm=%s\n", kSenderShmName, kReceiverShmName);

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
        .cpu_id = 200,
        .max_nodes = 2,
        .current_node_id = NodeB,
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

    printf("\n===== B Summary =====\n");
    printf("ping_recv=%" PRIu64 " pong_sent=%" PRIu64 "\n",
           g_ping_recv.load(), g_pong_send.load());
    if (g_pong_send.load() > 0) {
        double avg = (double)g_b_process_sum.load() / (double)g_pong_send.load();
        printf("avg_b_process(ns)=%.2f\n", avg);
    }

    ub_comm_queue_deinit(&handle);
    return 0;
}
