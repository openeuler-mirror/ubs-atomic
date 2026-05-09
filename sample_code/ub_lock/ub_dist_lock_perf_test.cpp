#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "ub_dist_comm_queue.h"
#include "ub_dist_lock.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;
using namespace ublock;

// ===================== 常量 & 全局 =====================
static constexpr size_t SHM_TOTAL_SIZE = (size_t)1024 * 1024 * 1024;
static constexpr int CPUS_PER_SOCKET = 96;

static constexpr const char *SHM_NAME_QUEUEA = "shm_sender_128";
static constexpr const char *SHM_NAME_QUEUEB = "shm_receiver_128";

static int g_node_id = -1;
static void *g_shm_base_ptr = nullptr;
static uint64_t *g_shm_data_ptr = nullptr; 

static ub_rw_lock_t *g_lock = nullptr;

static ub_shm_comm_t g_handle = nullptr; // 每进程只需要一个 handle
static ubsmem_options_t g_ubsm_opts{};

// 统计数据（最后汇总写入，不在并发时 push_back）
static std::mutex g_stat_mu;
static std::vector<uint64_t> g_all_lock_ns;
static std::vector<uint64_t> g_r_lock_ns;
static std::vector<uint64_t> g_w_lock_ns;

// ===================== 工具函数 =====================
static inline uint64_t now_ns()
{
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t ub_get_tid_u64()
{
    static thread_local const uint64_t tid =
        static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return tid;
}

static inline bool pin_this_thread_to_cpu(int cpu_id)
{
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_id, &set);
    return sched_setaffinity(0, sizeof(set), &set) == 0;
#else
    (void)cpu_id;
    return false;
#endif
}

static ub_location_t make_location(uint8_t node_id)
{
    ub_location_t loc{};
    loc.node_id = node_id;
    loc.tid = static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return loc;
}

// ===================== Stats =====================
struct Stats {
    double mean_ns = 0;
    uint64_t min_ns = 0;
    uint64_t p50_ns = 0;
    uint64_t p95_ns = 0;
    uint64_t p99_ns = 0;
    size_t n = 0;
};

static uint64_t percentile_nearest_rank(const std::vector<uint64_t> &sorted, double p)
{
    if (sorted.empty())
        return 0;
    if (p <= 0.0)
        return sorted.front();
    if (p >= 1.0)
        return sorted.back();
    double pos = p * (double)sorted.size();
    size_t idx = (size_t)std::ceil(pos) - 1;
    if (idx >= sorted.size())
        idx = sorted.size() - 1;
    return sorted[idx];
}

static Stats compute_stats(std::vector<uint64_t> samples)
{
    Stats s;
    s.n = samples.size();
    if (samples.empty())
        return s;

    s.min_ns = *std::min_element(samples.begin(), samples.end());
    long double sum = 0;
    for (auto v : samples)
        sum += (long double)v;

    std::sort(samples.begin(), samples.end());
    s.mean_ns = (double)(sum / (long double)s.n);
    s.p50_ns = percentile_nearest_rank(samples, 0.50);
    s.p95_ns = percentile_nearest_rank(samples, 0.95);
    s.p99_ns = percentile_nearest_rank(samples, 0.99);
    return s;
}

static void print_stats(const char *title, const std::vector<uint64_t> &v)
{
    auto st = compute_stats(v);
    std::cout << "================== " << title << " ==================\n";
    std::cout << "samples : " << st.n << "\n";
    std::cout << "mean    : " << (int64_t)std::llround(st.mean_ns) << " ns\n";
    std::cout << "p50     : " << st.p50_ns << " ns\n";
    std::cout << "p95     : " << st.p95_ns << " ns\n";
    std::cout << "p99     : " << st.p99_ns << " ns\n";
}

// ===================== ubsmem 初始化/映射 =====================
static int ubsmem_init_once()
{
    int ret = ubsmem_init_attributes(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        std::fprintf(stderr, "ubsmem_init_attributes ret=%d\n", ret);
        return -1;
    }
    ret = ubsmem_initialize(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        std::fprintf(stderr, "ubsmem_initialize ret=%d\n", ret);
        return -1;
    }
    return 0;
}

static int ubsmem_map(const char *name, void *&out_addr)
{
    void *addr = nullptr;
    int ret = ubsmem_shmem_map(nullptr, SHM_TOTAL_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, name, 0, &addr);
    if (ret != 0) {
        std::fprintf(stderr, "ubsmem_shmem_map(%s) ret=%d\n", name, ret);
        return -1;
    }
    out_addr = addr;
    return 0;
}

// 映射 lock shm
static int map_lock_shm_ubsm(const char *path)
{
    void *shm_addr = nullptr;
    if (ubsmem_map(path, shm_addr) != 0)
        return -1;

    // 8 字节对齐（保险起见）
    uintptr_t p = (uintptr_t)shm_addr;
    if (p % alignof(uint64_t) != 0) {
        p += (alignof(uint64_t) - (p % alignof(uint64_t)));
    }
    g_shm_base_ptr = shm_addr;
    g_lock = reinterpret_cast<ub_rw_lock_t *>(g_shm_base_ptr);
    return 0;
}

// ===================== Comm Queue init =====================

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

    ub_shm_area_t init_area{.size = kInitSize, .ptr = init_region_cur};

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

// ===================== Dump（保留，但加保护） =====================
static void dump_ub_rw_lock_state(const ub_rw_lock_t *lock)
{
    if (!lock) {
        std::cout << "Lock pointer is NULL!\n";
        return;
    }

    std::cout << "\n>>>>>> ub_rw_lock_t Dump <<<<<<\n";
    std::cout << "lock_word     : 0x" << std::hex << lock->lock_word.load(std::memory_order_acquire) << std::dec
              << "\n";
    std::cout << "is_inited     : " << lock->is_inited.load(std::memory_order_acquire) << "\n";
    std::cout << "waiting_count : " << lock->waiting_count.load(std::memory_order_acquire) << "\n";
    std::cout << "head          : " << lock->queue_head.load(std::memory_order_acquire) << "\n";
    std::cout << "tail          : " << lock->queue_tail.load(std::memory_order_acquire) << "\n";

    for (int i = 0; i < UB_MAX_NODES; ++i) {
        void *addr = lock->node_registry[i];
        if (!addr)
            continue;
        std::cout << "local[" << i << "] addr=" << addr << " node_id=" << int(nid) << "\n";
    }
    std::cout << "-------------------------------------------\n";
}

// ===================== 测试线程=====================
static int g_shared_var = 0;

static void reader_thread(ub_rw_lock_t *lock, int idx, bool enable_local, bool enable_delay, int setaffinity)
{
    if (setaffinity >= 0) {
        int local_cpu_index = idx % CPUS_PER_SOCKET;
        int global_cpu_id = setaffinity * CPUS_PER_SOCKET + local_cpu_index;
        pin_this_thread_to_cpu(global_cpu_id);
    }

    ub_location_t loc = make_location((uint8_t)g_node_id);
    ub_lock_policy_t policy{};
    policy.timeout_ts = 20000;
    policy.allow_delay_release = enable_delay;
    policy.recursive = false;

    // 每线程本地收集（避免 data race）
    std::vector<uint64_t> local_samples;
    local_samples.reserve(1);

    uint64_t t0 = now_ns();
    int ret = ub_rw_lock_s_lock(lock, &policy, &loc);
    uint64_t t1 = now_ns();

    if (ret == UB_LOCK_SUCCESS) {
        (void)g_shared_var;
        ub_rw_lock_s_unlock(lock, &policy, &loc);
        local_samples.push_back(t1 - t0);
    }

    // 汇总
    {
        std::lock_guard<std::mutex> g(g_stat_mu);
        g_r_lock_ns.insert(g_r_lock_ns.end(), local_samples.begin(), local_samples.end());
        g_all_lock_ns.insert(g_all_lock_ns.end(), local_samples.begin(), local_samples.end());
    }
}

static void writer_thread(ub_rw_lock_t *lock, int idx, bool enable_local, bool enable_delay, int setaffinity)
{
    if (setaffinity >= 0) {
        int local_cpu_index = idx % CPUS_PER_SOCKET;
        int global_cpu_id = setaffinity * CPUS_PER_SOCKET + local_cpu_index;
        pin_this_thread_to_cpu(global_cpu_id);
    }

    ub_location_t loc = make_location((uint8_t)g_node_id);
    ub_lock_policy_t policy{};
    policy.timeout_ts = 20000;
    policy.allow_delay_release = enable_delay;
    policy.recursive = false;

    std::vector<uint64_t> local_samples;
    local_samples.reserve(1);

    uint64_t t0 = now_ns();
    int ret = ub_rw_lock_x_lock(lock, &policy, &loc);
    uint64_t t1 = now_ns();

    if (ret == UB_LOCK_SUCCESS) {
        ++g_shared_var;
        ub_rw_lock_x_unlock(lock, &policy, &loc);
        local_samples.push_back(t1 - t0);
    }

    {
        std::lock_guard<std::mutex> g(g_stat_mu);
        g_w_lock_ns.insert(g_w_lock_ns.end(), local_samples.begin(), local_samples.end());
        g_all_lock_ns.insert(g_all_lock_ns.end(), local_samples.begin(), local_samples.end());
    }
}

// ===================== main =====================
int main(int argc, char *argv[])
{
    /*
      Usage:
      ./test_ub_lock <master|slave> <path> <count> <Tpercent> <RWpercent> <local> <delay> <setaffinity>
    */

    if (argc != 9) {
        std::fprintf(stderr,
                     "Usage: %s <master|slave> <path> <count> <Tpercent> <RWpercent> <local> <delay> "
                     "<setaffinity>\n",
                     argv[0]);
        return -1;
    }

    const std::string role = argv[1];
    const char *lock_path = argv[2];

    const bool is_master = (role == "master");
    if (!is_master && role != "slave") {
        std::fprintf(stderr, "role must be master|slave\n");
        return -1;
    }

    int total_node_threads = std::stoi(argv[3]);
    char *endp = nullptr;
    float Tpercent = std::strtof(argv[4], &endp);
    float RWpercent = std::strtof(argv[5], &endp);
    bool enable_local = std::stoi(argv[6]) != 0;
    bool enable_delay = std::stoi(argv[7]) != 0;
    int setaffinity = std::stoi(argv[8]);

    g_node_id = is_master ? 0 : 1;
    total_node_threads = (int)std::llround(total_node_threads * (is_master ? Tpercent : (1.0f - Tpercent)));

    // ===== ubsmem init=====
    if (ubsmem_init_once() != 0)
        return -1;

    // ===== comm queue init=====
    if (init_comm_queue(is_master) != 0) {
        std::cerr << "init_comm_queue failed\n";
        return -1;
    }
    std::cout << "[" << role << "] comm queue ready\n";

    // ===== lock shm map =====
    if (map_lock_shm_ubsm(lock_path) != 0)
        return -1;

    if (setaffinity >= 0) {
        // 主线程也绑定（避免漂移）
        int cpu = CPUS_PER_SOCKET * setaffinity + 1;
        pin_this_thread_to_cpu(cpu);
    }

    // ===== init lock =====
    ub_lock_config_t config{};
    config.lease_time = 60000;
    config.heartbeat_timeout = 500;

    ub_location_t creator{};
    creator.node_id = (uint8_t)g_node_id;
    creator.tid = ub_get_tid_u64();

    if (!is_master)
        std::this_thread::sleep_for(10s);
    ub_rw_lock_create(g_lock, &config, &creator);
    std::cout << "[" << role << "] lock created\n";

    int read_count = (int)std::llround(total_node_threads * RWpercent);
    int write_count = total_node_threads - read_count;

    std::cout << "[" << role << "] Node " << g_node_id << " starting: " << read_count << " readers, " << write_count
              << " writers.\n";

    // 等待手动开始
    std::cout << "Type 'c' to continue...\n";
    for (std::string cmd; std::cin >> cmd;) {
        if (cmd == "c")
            break;
    }

    // ===== launch threads =====
    g_all_lock_ns.clear();
    g_r_lock_ns.clear();
    g_w_lock_ns.clear();
    g_all_lock_ns.reserve((size_t)read_count + (size_t)write_count);
    g_r_lock_ns.reserve((size_t)read_count);
    g_w_lock_ns.reserve((size_t)write_count);

    std::vector<std::thread> threads;
    threads.reserve((size_t)read_count + (size_t)write_count);

    for (int i = 0; i < read_count; ++i) {
        threads.emplace_back(reader_thread, g_lock, i, enable_local, enable_delay, setaffinity);
    }
    std::this_thread::sleep_for(5us);
    for (int i = 0; i < write_count; ++i) {
        threads.emplace_back(writer_thread, g_lock, i + read_count, enable_local, enable_delay, setaffinity);
    }

    for (auto &t : threads)
        t.join();

    // ===== stats =====
    print_stats("READ lock", g_r_lock_ns);
    print_stats("WRITE lock", g_w_lock_ns);
    print_stats("ALL lock", g_all_lock_ns);

    // ===== interactive dump/quit =====
    std::cout << "Type 's' to dump lock, 'q' to quit...\n";
    for (std::string cmd; std::cin >> cmd;) {
        if (cmd == "q" || cmd == "quit")
            break;
        if (cmd == "s")
            dump_ub_rw_lock_state(g_lock);
    }

    // ===== free =====
    ub_rw_lock_free(g_lock, &creator);

    if (g_shm_base_ptr)
        ubsmem_shmem_unmap(g_shm_base_ptr, SHM_TOTAL_SIZE);
    ub_comm_queue_deinit(&g_handle);
    ubsmem_finalize();

    return 0;
}
