#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include <sys/mman.h>
#include <time.h>

#include "ub_dist_comm_queue.h"
#include "ub_dist_lock.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

static constexpr const char *SHM_NAME_QUEUEA = "shm_sender_128";
static constexpr const char *SHM_NAME_QUEUEB = "shm_receiver_128";
static constexpr size_t SHM_TOTAL_SIZE = (size_t)1024 * 1024 * 1024;

static int g_node_id = -1;
static void *g_shm_base_ptr = nullptr;
static ub_rw_lock_t *g_ub_lock = nullptr;

static ub_shm_comm_t g_handle_send = nullptr;
static ub_shm_comm_t g_handle_recv = nullptr;

static uint64_t ub_get_tid_u64()
{
    static thread_local const uint64_t tid =
        static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return tid;
}

static int ubsmem_init_once()
{
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        std::fprintf(stderr, "ubsmem_init_attributes ret=%d\n", ret);
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        std::fprintf(stderr, "ubsmem_initialize ret=%d\n", ret);
        return -1;
    }
    return 0;
}

static int ubsmem_map(const char *name, size_t size, void *&out)
{
    void *addr = nullptr;
    int ret = ubsmem_shmem_map(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, name, 0, &addr);
    if (ret != 0) {
        std::fprintf(stderr, "ubsmem_shmem_map(%s) ret=%d\n", name, ret);
        return -1;
    }
    out = addr;
    return 0;
}

static int init_comm_queue(bool is_master)
{
    void *shmA = nullptr;
    void *shmB = nullptr;

    if (ubsmem_map(SHM_NAME_QUEUEA, SHM_TOTAL_SIZE, shmA) != 0)
        return -1;
    if (ubsmem_map(SHM_NAME_QUEUEB, SHM_TOTAL_SIZE, shmB) != 0)
        return -1;

    constexpr size_t kInitSize = 1024;
    constexpr size_t kRingSize = 1376640;

    const uint8_t nodeA = 0;
    const uint8_t nodeB = 1;
    const uint8_t cur = is_master ? nodeA : nodeB;
    const uint8_t peer = is_master ? nodeB : nodeA;

    void *init_region_cur = (is_master ? (char *)shmA : (char *)shmB);
    void *ring_region_cur = (is_master ? (char *)shmA : (char *)shmB) + kInitSize;
    void *ring_region_peer = (is_master ? (char *)shmB : (char *)shmA) + kInitSize;

    ub_ring_desc_t ring_descs[1]{};
    ring_descs[0].ring_capacity = 1024;
    ring_descs[0].max_msg_size = 512;
    ring_descs[0].priority = 1;

    ub_comm_conf_t conf{};
    conf.max_nodes = 2;
    conf.current_node_id = cur;
    conf.num_rings = 1;
    conf.ring_descs = ring_descs;

    ub_shm_area_t init_area{.size=kInitSize, .ptr=init_region_cur};

    ub_ring_region_info_t infos[2]{};
    infos[0].node_id = cur;
    infos[0].region = {kRingSize, ring_region_cur};
    infos[1].node_id = peer;
    infos[1].region = {kRingSize, ring_region_peer};

    ub_ring_region_map_t ring_map{infos, 2};

    ub_shm_comm_t *handle = is_master ? &g_handle_send : &g_handle_recv;
    int ret = ub_comm_queue_init(handle, &init_area, &ring_map, &conf);
    if (ret != 0) {
        std::cerr << "ub_comm_queue_init failed ret=" << ret << "\n";
        return -1;
    }
    return 0;
}

static void dump_lock(const ub_rw_lock_t *lock)
{
    if (!lock)
        return;
    std::cout << "\n==== Lock Dump ====\n";
    std::cout << "lock_word=" << lock->lock_word.load(std::memory_order_acquire)
              << " waiting_count=" << lock->waiting_count.load(std::memory_order_acquire)
              << " head=" << lock->queue_head.load(std::memory_order_acquire)
              << " tail=" << lock->queue_tail.load(std::memory_order_acquire)
              << " reserve_owner=" << lock->reserve_lock_owner.load(std::memory_order_acquire) << "\n";

    for (int i = 0; i < UB_MAX_NODES; ++i) {
        auto addr = lock->node_registry[i];
        if (addr) {
            std::cout << "local[" << i << "] addr=" << addr << " node=" << int(nid) << "\n";
        }
    }
}

static ub_lock_result_t do_op(int tid, const std::string &op, bool local, bool delay)
{
    ub_location_t loc{.tid = (uint64_t)tid, .node_id = (uint8_t)g_node_id};
    ub_lock_policy_t policy{};
    policy.timeout_ts = 10000;
    policy.allow_delay_release = delay;
    policy.recursive = false;

    if (op == "s+")
        return ub_rw_lock_s_lock(g_ub_lock, &policy, &loc);
    if (op == "s-")
        return ub_rw_lock_s_unlock(g_ub_lock, &policy, &loc);
    if (op == "sx+")
        return ub_rw_lock_sx_lock(g_ub_lock, &policy, &loc);
    if (op == "sx-")
        return ub_rw_lock_sx_unlock(g_ub_lock, &policy, &loc);
    if (op == "x+")
        return ub_rw_lock_x_lock(g_ub_lock, &policy, &loc);
    if (op == "x-")
        return ub_rw_lock_x_unlock(g_ub_lock, &policy, &loc);
    return UB_LOCK_ERROR;
}

int main(int argc, char *argv[])
{
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <master|slave> <path>\n";
        std::cerr << "  <master|slave>: node identity";
        std::cerr << "  <path>: ubsmd shared memory name";
        return -1;
    }

    const std::string role = argv[1];
    const char *path = argv[2];

    const bool is_master = (role == "master");
    g_node_id = is_master ? 0 : 1;

    if (ubsmem_init_once() != 0)
        return -1;

    if (init_comm_queue(is_master) != 0)
        return -1;

    if (ubsmem_map(path, SHM_TOTAL_SIZE, g_shm_base_ptr) != 0)
        return -1;

    g_ub_lock = reinterpret_cast<ub_rw_lock_t *>(g_shm_base_ptr);

    ub_lock_config_t config{};
    config.lease_time = 60000;
    config.heartbeat_timeout = 500;

    ub_location_t creator{.tid = ub_get_tid_u64(), .node_id = (uint8_t)g_node_id};
    if (!is_master)
        std::this_thread::sleep_for(2s);
    ub_rw_lock_create(g_ub_lock, &config, &creator);

    std::cout << "Commands: <tid> s+/s-/sx+/sx-/x+/x- <local 0|1> <delay 0|1>\n";
    std::cout << "list / quit\n";

    std::string line;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line))
            break;
        if (line == "quit")
            break;
        if (line == "list") {
            dump_lock(g_ub_lock);
            continue;
        }

        std::istringstream iss(line);
        int tid = 0, local = 0, delay = 0;
        std::string op;
        if (!(iss >> tid >> op >> local >> delay)) {
            std::cout << "bad cmd\n";
            continue;
        }

        auto r = do_op(tid, op, local != 0, delay != 0);
        std::cout << "ret=" << int(r) << "\n";
    }

    ub_rw_lock_free(g_ub_lock, &creator);

    if (g_shm_base_ptr) {
        ubsmem_shmem_unmap(g_shm_base_ptr, SHM_TOTAL_SIZE);
        g_shm_base_ptr = nullptr;
    }
    ub_comm_queue_deinit(&g_handle_send);
    ub_comm_queue_deinit(&g_handle_recv);
    ubsmem_finalize();
    return 0;
}
