/*
 * ub_dist_tx_res_conc_2n_test.cpp - 并发测试（两节点）
 *
 * 对应用例修复版.md 中的用例 5~8：
 *   TC-ADD-2N  : add 两节点均128线程并发 ADD/GET，预期终值 2^16+1
 *   TC-XOR-2N  : xor 两节点均128线程并发 XOR/GET，预期终值 1
 *   TC-CAS-1N  : cas 单节点并发128线程 cas，预期恰1个线程成功，终值 2
 *   TC-CAS-2N  : cas 两节点均128线程并发 while-cas/GET，预期终值 2^16
 *
 * 说明：
 *   - 不包含前置步骤 P1/P2（组网、共享内存创建），仅实现测试步骤与预期校验
 *   - 通过 --shm <name> 指定共享内存名，参考 nc_test 的 ubsmem 映射流程
 *   - --role writer (Node0=导出方) / reader (Node1=导入方)
 *   - 支持 --case <id> 单用例执行，不指定则批量执行
 *
 * 编译：
 *   g++ -O2 -g -std=c++17 -o ub_dist_tx_res_conc_2n_test \
 *       ub_dist_tx_res_conc_2n_test.cpp \
 *       -I../../include \
 *       -I/usr/local/ubs_mem/include \
 *       -L../../build/lib -lubs-atomic \
 *       -L/usr/local/ubs_mem/lib -lubsm_sdk \
 *       -lpthread \
 *       -Wl,-rpath,/usr/local/ubs_mem/lib \
 *       -Wl,-rpath,'$ORIGIN/../../build/lib'
 *
 * 运行：
 *   # Node0 (导出方) 执行全部用例
 *   ./ub_dist_tx_res_conc_2n_test --shm shm_conc_2n --role writer
 *   # Node1 (导入方) 执行全部用例
 *   ./ub_dist_tx_res_conc_2n_test --shm shm_conc_2n --role reader
 *   # 仅执行 TC-ADD-2N
 *   ./ub_dist_tx_res_conc_2n_test --shm shm_conc_2n --role writer --case TC-ADD-2N
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cinttypes>
#include <ctime>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <string>
#include <getopt.h>
#include <sys/mman.h>

#include "ubs_mem.h"
#include "ubs_mem_def.h"
#include "ub_dist_tx_res.h"

/* ===================== 常量 ===================== */

#define SEPARATOR  "------------------------------------------------------------"
#define HEADER_SEP "============================================================"

static const size_t SHM_SIZE_DEFAULT = 1UL * 1024 * 1024 * 1024; // 1GB
static const char *SHM_NAME_DEFAULT = "shm_conc_2n";

/* 并发规模参数（128线程 × 2^8 次） */
static constexpr int THREADS_PER_NODE = 128;
static constexpr uint64_t OPS_PER_THREAD = 1ULL << 8; // 2^8

/* 握手同步用控制字段 */
struct ConcLayout {
    uint64_t ready_flag;   // offset 0  : writer→reader: 开始并发
    uint64_t writer_done;  // offset 8  : writer→reader: writer 并发完成，reader 可读最终值
    uint64_t done_flag;    // offset 16 : reader→writer: reader 已读最终值并完成
    uint64_t target;       // offset 24 : 各用例操作的共享变量
    uint64_t cas_counter;  // offset 32 : TC-CAS-1N 用计数器（统计成功线程数）
    uint64_t result_val;   // offset 40 : reader 读 target 后回传（兼容双端校验）
};

/* ===================== 全局 ===================== */

static void *g_shm_base = nullptr;
static size_t g_shm_size = SHM_SIZE_DEFAULT;
static ConcLayout *g_layout = nullptr;
static bool g_is_writer = true; // true=Node0(writer/导出方), false=Node1(reader/导入方)

static int g_pass_cnt = 0;
static int g_fail_cnt = 0;

/* ===================== 日志 ===================== */

static int my_stdout_logger(int level, const char *file, const char *func,
                            uint32_t line, const char *message)
{
    (void)func;
    const char *level_str = "UNKNOWN";
    switch (level) {
        case LOG_LEVEL_DEBUG:    level_str = "DEBUG";    break;
        case LOG_LEVEL_INFO:     level_str = "INFO";     break;
        case LOG_LEVEL_WARN:     level_str = "WARN";     break;
        case LOG_LEVEL_ERROR:    level_str = "ERROR";    break;
        case LOG_LEVEL_CRITICAL: level_str = "CRITICAL"; break;
        default: break;
    }
    time_t now = time(nullptr);
    char *ts = ctime(&now);
    ts[strlen(ts) - 1] = '\0';
    fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", ts, file, line, level_str, message);
    return 0;
}

/* ===================== 断言宏 ===================== */

#define CHECK_RES(desc, actual, expect)                                       \
    do {                                                                     \
        int _a = (actual), _e = (expect);                                    \
        if (_a == _e) {                                                      \
            printf("  [PASS] %-50s (ret=%d)\n", (desc), _a);                 \
            g_pass_cnt++;                                                    \
        } else {                                                             \
            printf("  [FAIL] %-50s (实际ret=%d, 期望ret=%d)\n",              \
                   (desc), _a, _e);                                          \
            g_fail_cnt++;                                                    \
        }                                                                    \
    } while (0)

#define CHECK_VAL(desc, actual, expect)                                      \
    do {                                                                     \
        uint64_t _a = (actual), _e = (expect);                               \
        if (_a == _e) {                                                      \
            printf("  [PASS] %-50s (值=%" PRIu64 ")\n", (desc), _a);         \
            g_pass_cnt++;                                                    \
        } else {                                                             \
            printf("  [FAIL] %-50s (实际=%" PRIu64 ", 期望=%" PRIu64 ")\n",  \
                   (desc), _a, _e);                                          \
            g_fail_cnt++;                                                    \
        }                                                                    \
    } while (0)

static void case_begin(const char *name)
{
    printf("\n" HEADER_SEP "\n");
    printf("  %s [%s]\n", name, g_is_writer ? "Node0/writer" : "Node1/reader");
    printf(HEADER_SEP "\n");
    g_pass_cnt = 0;
    g_fail_cnt = 0;
}

static void case_end(const char *name)
{
    printf(SEPARATOR "\n");
    if (g_fail_cnt == 0)
        printf("[PASS] %s 全部通过 (%d/%d)\n", name, g_pass_cnt, g_pass_cnt + g_fail_cnt);
    else
        printf("[FAIL] %s 有 %d 项失败 (%d/%d 通过)\n", name, g_fail_cnt,
               g_pass_cnt, g_pass_cnt + g_fail_cnt);
    printf(HEADER_SEP "\n\n");
}

/* ===================== 共享内存映射（参考 nc_test） ===================== */

static int init_ubsmem_shm(const char *shm_name, size_t shm_size, void **base_addr)
{
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_init_attributes failed: %d\n", ret);
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_initialize failed: %d\n", ret);
        return -1;
    }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_lookup_regions failed: %d\n", ret);
        return -1;
    }
    ret = ubsmem_shmem_map(nullptr, shm_size,
                           PROT_READ | PROT_WRITE, MAP_SHARED,
                           shm_name, 0, base_addr);
    if (ret != 0) {
        fprintf(stderr, "[Error] ubsmem_shmem_map(%s) failed: %d\n", shm_name, ret);
        return -1;
    }
    printf("[Info] 共享内存映射成功: name=%s, addr=%p, size=%zu\n",
           shm_name, *base_addr, shm_size);
    return 0;
}

/* ===================== 双向握手同步 ===================== */
/* 流程：
 *   writer: set(ready_flag)      → reader 开始并发
 *   writer: 并发完成 → set(writer_done)  → reader 可读最终值
 *   reader: 等 writer_done → 读 target → set(done_flag) → writer 可读最终值
 * 这样保证 reader 读 target 时 writer 已完成所有写入。 */

static void writer_signal_ready(uint64_t scenario_num)
{
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    ub_dist_tx_res_set(&g_layout->ready_flag, scenario_num);
}

static void writer_signal_done(uint64_t scenario_num)
{
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    ub_dist_tx_res_set(&g_layout->writer_done, scenario_num);
}

static void reader_wait_ready(uint64_t scenario_num)
{
    uint64_t r = 0;
    while (r != scenario_num) {
        ub_dist_tx_res_get(&g_layout->ready_flag, &r);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
}

/* reader 等 writer 完成并发，返回后可安全读 target */
static void reader_wait_writer_done(uint64_t scenario_num)
{
    uint64_t d = 0;
    while (d != scenario_num) {
        ub_dist_tx_res_get(&g_layout->writer_done, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
}

static void reader_signal_done(uint64_t scenario_num)
{
    ub_dist_tx_res_set(&g_layout->done_flag, scenario_num);
}

static void reset_control_flags(void)
{
    ub_dist_tx_res_set(&g_layout->ready_flag, 0);
    ub_dist_tx_res_set(&g_layout->writer_done, 0);
    ub_dist_tx_res_set(&g_layout->done_flag, 0);
    ub_dist_tx_res_set(&g_layout->target, 0);
    ub_dist_tx_res_set(&g_layout->cas_counter, 0);
    ub_dist_tx_res_set(&g_layout->result_val, 0);
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

/* writer 端读最终值：等 reader 完成（done_flag），fence(ACQUIRE) 后读 target。
 * reader 读 target 时 writer 已完成写入，writer 读 target 时 reader 已确认读完，
 * 两端都通过 writer_done / done_flag 的 release-acquire 配对保证可见性。 */
static uint64_t writer_read_final(uint64_t scenario)
{
    uint64_t d = 0;
    while (d != scenario) {
        ub_dist_tx_res_get(&g_layout->done_flag, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
    uint64_t val = 0;
    ub_dist_tx_res_get(&g_layout->target, &val);
    return val;
}

/* ===================== 用例 TC-ADD-2N ===================== */
/* 两节点均128线程并发执行 2^8 次 ADD(1)，2^8 次 GET，初值1，预期终值 2^16+1 */

static void case_tc_add_2n_writer(void)
{
    case_begin("TC-ADD-2N : add 两节点128线程并发 ADD/GET");
    reset_control_flags();

    // 初始化：init + set(1)
    int ret = ub_dist_tx_res_init(&g_layout->target);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_set(&g_layout->target, 1);
    CHECK_RES("S1 SET(1) 返回 OK", ret, UB_RES_OK);

    // 通知 reader 开始并发
    writer_signal_ready(5);

    printf("  [writer] 启动 %d 线程, 每线程 ADD(1) × %llu + GET × %llu\n",
           THREADS_PER_NODE,
           (unsigned long long)OPS_PER_THREAD,
           (unsigned long long)OPS_PER_THREAD);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([]() {
            uint64_t out = 0;
            for (uint64_t k = 0; k < OPS_PER_THREAD; k++) {
                ub_dist_tx_res_add(&g_layout->target, 1);
                ub_dist_tx_res_get(&g_layout->target, &out);
            }
            (void)out;
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("  [writer] 并发完成, 耗时 %.2f ms\n", ms);

    // 通知 reader writer 已完成，等 reader 读完后读最终值
    writer_signal_done(5);
    uint64_t expect = (2ULL * THREADS_PER_NODE * OPS_PER_THREAD) + 1;
    uint64_t final_val = writer_read_final(5);
    CHECK_VAL("E2 最终值 == 2^16+1", final_val, expect);

    case_end("TC-ADD-2N");
}

static void case_tc_add_2n_reader(void)
{
    case_begin("TC-ADD-2N : add 两节点128线程并发 ADD/GET");
    reader_wait_ready(5);

    printf("  [reader] 启动 %d 线程, 每线程 ADD(1) × %llu + GET × %llu\n",
           THREADS_PER_NODE,
           (unsigned long long)OPS_PER_THREAD,
           (unsigned long long)OPS_PER_THREAD);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([]() {
            uint64_t out = 0;
            for (uint64_t k = 0; k < OPS_PER_THREAD; k++) {
                ub_dist_tx_res_add(&g_layout->target, 1);
                ub_dist_tx_res_get(&g_layout->target, &out);
            }
            (void)out;
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("  [reader] 并发完成, 耗时 %.2f ms\n", ms);

    /* 等 writer 完成并发，保证 writer 的写入全部落地后再读最终值 */
    reader_wait_writer_done(5);
    uint64_t final_val = 0;
    ub_dist_tx_res_get(&g_layout->target, &final_val);

    /* 通知 writer reader 已读完，writer 可读最终值 */
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    reader_signal_done(5);

    uint64_t expect = (2ULL * THREADS_PER_NODE * OPS_PER_THREAD) + 1;
    CHECK_VAL("E2 最终值 == 2^16+1", final_val, expect);

    case_end("TC-ADD-2N");
}

/* ===================== 用例 TC-XOR-2N ===================== */
/* 两节点均128线程并发执行 2^8 次 XOR；Node0 value=2，Node1 value=9；初值1，预期终值 1 */

static void case_tc_xor_2n_writer(void)
{
    case_begin("TC-XOR-2N : xor 两节点128线程并发 XOR/GET");
    reset_control_flags();

    int ret = ub_dist_tx_res_init(&g_layout->target);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_set(&g_layout->target, 1);
    CHECK_RES("S1 SET(1) 返回 OK", ret, UB_RES_OK);

    writer_signal_ready(6);

    printf("  [writer] 启动 %d 线程, 每线程 XOR(2) × %llu + GET × %llu\n",
           THREADS_PER_NODE,
           (unsigned long long)OPS_PER_THREAD,
           (unsigned long long)OPS_PER_THREAD);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([]() {
            uint64_t out = 0;
            for (uint64_t k = 0; k < OPS_PER_THREAD; k++) {
                ub_dist_tx_res_fetch_xor(&g_layout->target, 2, &out);
                ub_dist_tx_res_get(&g_layout->target, &out);
            }
            (void)out;
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("  [writer] 并发完成, 耗时 %.2f ms\n", ms);

    writer_signal_done(6);
    uint64_t final_val = writer_read_final(6);
    // Node0 XOR(2) × 2^8 次, Node1 XOR(9) × 2^8 次，2 和 9 各偶数次抵消，1 xor 0 xor 0 = 1
    CHECK_VAL("E2 最终值 == 1", final_val, 1ULL);

    case_end("TC-XOR-2N");
}

static void case_tc_xor_2n_reader(void)
{
    case_begin("TC-XOR-2N : xor 两节点128线程并发 XOR/GET");
    reader_wait_ready(6);

    printf("  [reader] 启动 %d 线程, 每线程 XOR(9) × %llu + GET × %llu\n",
           THREADS_PER_NODE,
           (unsigned long long)OPS_PER_THREAD,
           (unsigned long long)OPS_PER_THREAD);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([]() {
            uint64_t out = 0;
            for (uint64_t k = 0; k < OPS_PER_THREAD; k++) {
                ub_dist_tx_res_fetch_xor(&g_layout->target, 9, &out);
                ub_dist_tx_res_get(&g_layout->target, &out);
            }
            (void)out;
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("  [reader] 并发完成, 耗时 %.2f ms\n", ms);

    /* 等 writer 完成并发，保证 writer 的写入全部落地后再读最终值 */
    reader_wait_writer_done(6);
    uint64_t final_val = 0;
    ub_dist_tx_res_get(&g_layout->target, &final_val);

    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    reader_signal_done(6);

    CHECK_VAL("E2 最终值 == 1", final_val, 1ULL);

    case_end("TC-XOR-2N");
}

/* ===================== 用例 TC-CAS-1N ===================== */
/* 单节点(Node1)128线程并发 CAS(expected=0, desired=2)，预期恰1线程成功，终值2 */
/* Node0 负责 INIT，Node1 执行并发 cas */

static void case_tc_cas_1n_writer(void)
{
    case_begin("TC-CAS-1N : cas 单节点128线程并发（Node0 init）");
    reset_control_flags();

    int ret = ub_dist_tx_res_init(&g_layout->target);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    uint64_t start = 0;
    ub_dist_tx_res_get(&g_layout->target, &start);
    CHECK_VAL("S1 start_val == 0", start, 0);

    // 通知 reader (Node1) 开始并发。CAS-1N writer 不参与并发，直接 signal_done
    writer_signal_ready(7);
    writer_signal_done(7);

    // 等 reader 完成后读最终值
    uint64_t final_val = writer_read_final(7);
    CHECK_VAL("E3 最终值 == 2", final_val, 2ULL);

    case_end("TC-CAS-1N");
}

static void case_tc_cas_1n_reader(void)
{
    case_begin("TC-CAS-1N : cas 单节点128线程并发（Node1 执行）");
    reader_wait_ready(7);

    // 128 线程各执行 1 次 CAS(expected=0, desired=2)
    printf("  [reader] 启动 %d 线程, 每线程 1 次 CAS(exp=0, desired=2)\n",
           THREADS_PER_NODE);

    auto counter = std::atomic<int>(0);
    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([&counter]() {
            uint64_t expected = 0;
            int success = 0;
            int ret = ub_dist_tx_res_compare_exchange(
                &g_layout->target, &expected, 2, &success);
            if (ret == UB_RES_OK && success == 1) {
                counter.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    int succ_cnt = counter.load();

    printf("  [reader] 并发完成, 耗时 %.2f ms\n", ms);
    CHECK_VAL("E2 恰1个线程 success=1", (uint64_t)succ_cnt, 1ULL);

    /* CAS-1N writer 不参与并发，writer_done 已在 ready 后立即设置。
     * 等 writer_done 后读最终值（保证 writer 的 init 写入可见）。 */
    reader_wait_writer_done(7);
    uint64_t final_val = 0;
    ub_dist_tx_res_get(&g_layout->target, &final_val);
    CHECK_VAL("E3 最终值 == 2", final_val, 2ULL);

    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    reader_signal_done(7);

    case_end("TC-CAS-1N");
}

/* ===================== 用例 TC-CAS-2N ===================== */
/* 两节点均128线程并发 while-cas 自增，预期终值 2^16 */
/* 每线程执行 2^8 次成功 CAS（success=0 时 desired=*expected+1 重试同一目标值） */

static void case_tc_cas_2n_writer(void)
{
    case_begin("TC-CAS-2N : cas 两节点128线程并发 while-cas/GET");
    reset_control_flags();

    int ret = ub_dist_tx_res_init(&g_layout->target);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_set(&g_layout->target, 0);

    writer_signal_ready(8);

    printf("  [writer] 启动 %d 线程, 每线程 while-CAS 成功 %llu 次 + GET %llu 次\n",
           THREADS_PER_NODE,
           (unsigned long long)OPS_PER_THREAD,
           (unsigned long long)OPS_PER_THREAD);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([]() {
            uint64_t out = 0;
            for (uint64_t k = 0; k < OPS_PER_THREAD; k++) {
                /* while-cas：success=0 时用更新后的 expected 重算 desired 并重试，直到成功 */
                while (1) {
                    uint64_t expected = 0;
                    ub_dist_tx_res_get(&g_layout->target, &expected);
                    uint64_t desired = expected + 1;
                    int success = 0;
                    int r = ub_dist_tx_res_compare_exchange(
                        &g_layout->target, &expected, desired, &success);
                    (void)r;
                    if (success == 1) break;
                }
                ub_dist_tx_res_get(&g_layout->target, &out);
            }
            (void)out;
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("  [writer] 并发完成, 耗时 %.2f ms\n", ms);

    writer_signal_done(8);
    uint64_t expect = 2ULL * THREADS_PER_NODE * OPS_PER_THREAD; // 2^16
    uint64_t final_val = writer_read_final(8);
    CHECK_VAL("E2 最终值 == 2^16", final_val, expect);

    case_end("TC-CAS-2N");
}

static void case_tc_cas_2n_reader(void)
{
    case_begin("TC-CAS-2N : cas 两节点128线程并发 while-cas/GET");
    reader_wait_ready(8);

    printf("  [reader] 启动 %d 线程, 每线程 while-CAS 成功 %llu 次 + GET %llu 次\n",
           THREADS_PER_NODE,
           (unsigned long long)OPS_PER_THREAD,
           (unsigned long long)OPS_PER_THREAD);

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(THREADS_PER_NODE);
    for (int i = 0; i < THREADS_PER_NODE; i++) {
        workers.emplace_back([]() {
            uint64_t out = 0;
            for (uint64_t k = 0; k < OPS_PER_THREAD; k++) {
                /* while-cas：success=0 时用更新后的 expected 重算 desired 并重试，直到成功 */
                while (1) {
                    uint64_t expected = 0;
                    ub_dist_tx_res_get(&g_layout->target, &expected);
                    uint64_t desired = expected + 1;
                    int success = 0;
                    int r = ub_dist_tx_res_compare_exchange(
                        &g_layout->target, &expected, desired, &success);
                    (void)r;
                    if (success == 1) break;
                }
                ub_dist_tx_res_get(&g_layout->target, &out);
            }
            (void)out;
        });
    }
    for (auto &t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("  [reader] 并发完成, 耗时 %.2f ms\n", ms);

    /* 等 writer 完成并发，保证 writer 的写入全部落地后再读最终值 */
    reader_wait_writer_done(8);
    uint64_t final_val = 0;
    ub_dist_tx_res_get(&g_layout->target, &final_val);

    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    reader_signal_done(8);

    uint64_t expect = 2ULL * THREADS_PER_NODE * OPS_PER_THREAD; // 2^16
    CHECK_VAL("E2 最终值 == 2^16", final_val, expect);

    case_end("TC-CAS-2N");
}

/* ===================== 用例调度 ===================== */

struct CaseEntry {
    const char *id;
    void (*writer_fn)(void);
    void (*reader_fn)(void);
};

static CaseEntry g_cases[] = {
    {"TC-ADD-2N",  case_tc_add_2n_writer,  case_tc_add_2n_reader},
    {"TC-XOR-2N",  case_tc_xor_2n_writer,  case_tc_xor_2n_reader},
    {"TC-CAS-1N",  case_tc_cas_1n_writer,  case_tc_cas_1n_reader},
    {"TC-CAS-2N",  case_tc_cas_2n_writer,  case_tc_cas_2n_reader},
};

static void run_case(const std::string &id)
{
    for (const auto &c : g_cases) {
        if (id == c.id) {
            if (g_is_writer) c.writer_fn();
            else            c.reader_fn();
            return;
        }
    }
    fprintf(stderr, "[Error] 未知用例编号: %s\n", id.c_str());
}

static void run_all(void)
{
    for (const auto &c : g_cases) {
        if (g_is_writer) c.writer_fn();
        else            c.reader_fn();
    }
}

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s --shm <name> --role <writer|reader> [--shm-size <MB>] [--case <id>]\n"
            "\n"
            "  --shm <name>           共享内存名 (必需)\n"
            "  --role writer|reader   角色: writer=Node0/导出方, reader=Node1/导入方 (必需)\n"
            "  --shm-size <MB>        共享内存大小, 默认 %zu MB\n"
            "  --case <id>            单用例执行; 不指定则批量执行\n"
            "\n"
            "可用用例编号:\n",
            prog, SHM_SIZE_DEFAULT / (1024 * 1024));
    for (const auto &c : g_cases) {
        fprintf(stderr, "  %s\n", c.id);
    }
    fprintf(stderr, "\n示例:\n"
                    "  # Node0\n"
                    "  %s --shm shm_conc_2n --role writer\n"
                    "  # Node1\n"
                    "  %s --shm shm_conc_2n --role reader\n"
                    "  # 单用例\n"
                    "  %s --shm shm_conc_2n --role writer --case TC-ADD-2N\n",
            prog, prog, prog);
}

static struct option long_options[] = {
    {"shm",      required_argument, nullptr, 's'},
    {"role",     required_argument, nullptr, 'r'},
    {"shm-size", required_argument, nullptr, 'z'},
    {"case",     required_argument, nullptr, 'c'},
    {"help",     no_argument,       nullptr, 'h'},
    {nullptr,    0,                 nullptr,  0 }
};

/* ===================== 主函数 ===================== */

int main(int argc, char *argv[])
{
    std::string shm_name;
    std::string role_str;
    std::string case_id;
    size_t shm_size_mb = SHM_SIZE_DEFAULT / (1024 * 1024);

    int opt;
    while ((opt = getopt_long(argc, argv, "s:r:z:c:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 's': shm_name = optarg; break;
            case 'r': role_str = optarg; break;
            case 'z': shm_size_mb = std::stoull(optarg); break;
            case 'c': case_id = optarg; break;
            case 'h': print_usage(argv[0]); return 0;
            default:  print_usage(argv[0]); return 1;
        }
    }

    if (shm_name.empty()) {
        fprintf(stderr, "[Error] --shm 参数必需\n");
        print_usage(argv[0]);
        return 1;
    }
    if (role_str.empty()) {
        fprintf(stderr, "[Error] --role 参数必需\n");
        print_usage(argv[0]);
        return 1;
    }
    if (role_str == "writer")      g_is_writer = true;
    else if (role_str == "reader") g_is_writer = false;
    else {
        fprintf(stderr, "[Error] --role 必须是 writer 或 reader, 当前: %s\n", role_str.c_str());
        return 1;
    }

    ub_atomic_register_log_func(my_stdout_logger);
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);

    g_shm_size = shm_size_mb * 1024UL * 1024UL;

    printf(HEADER_SEP "\n");
    printf("  ub_dist_tx_res 并发测试（两节点）\n");
    printf("  角色: %s (%s)\n",
           g_is_writer ? "writer" : "reader",
           g_is_writer ? "Node0/导出方" : "Node1/导入方");
    printf("  共享内存: %s (%zu MB)\n", shm_name.c_str(), shm_size_mb);
    if (!case_id.empty())
        printf("  执行: 单用例 %s\n", case_id.c_str());
    else
        printf("  执行: 批量全部\n");
    printf(HEADER_SEP "\n\n");

    if (init_ubsmem_shm(shm_name.c_str(), g_shm_size, &g_shm_base) != 0) {
        fprintf(stderr, "[Error] 共享内存初始化失败\n");
        return 1;
    }

    uintptr_t base_addr = reinterpret_cast<uintptr_t>(g_shm_base);
    if (base_addr % alignof(uint64_t) != 0) {
        base_addr += alignof(uint64_t) - (base_addr % alignof(uint64_t));
        printf("[Info] 基地址对齐调整: %p -> 0x%" PRIxPTR "\n", g_shm_base, base_addr);
    }
    g_layout = reinterpret_cast<ConcLayout *>(base_addr);
    printf("[Info] ConcLayout 地址: %p, 大小: %zu\n",
           (void *)g_layout, sizeof(ConcLayout));

    // writer 端初始化控制字段
    if (g_is_writer) {
        ub_dist_tx_res_init(&g_layout->ready_flag);
        ub_dist_tx_res_init(&g_layout->done_flag);
        ub_dist_tx_res_init(&g_layout->target);
        ub_dist_tx_res_init(&g_layout->cas_counter);
    }
    // 等待 writer 初始化完成（仅 reader 等待）
    if (!g_is_writer) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    int total_pass = 0, total_fail = 0;
    int saved_pass = 0, saved_fail = 0;

    if (!case_id.empty()) {
        run_case(case_id);
        total_pass = g_pass_cnt;
        total_fail = g_fail_cnt;
    } else {
        for (const auto &c : g_cases) {
            saved_pass = g_pass_cnt; saved_fail = g_fail_cnt;
            if (g_is_writer) c.writer_fn();
            else            c.reader_fn();
            total_pass += (g_pass_cnt - saved_pass);
            total_fail += (g_fail_cnt - saved_fail);
            // 用例间隔
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
    }

    printf(HEADER_SEP "\n");
    printf("  总计: PASS=%d, FAIL=%d\n", total_pass, total_fail);
    printf(HEADER_SEP "\n");

    if (g_shm_base) {
        ubsmem_shmem_unmap(g_shm_base, g_shm_size);
        printf("[Info] 共享内存已卸载\n");
    }
    ubsmem_finalize();
    return total_fail == 0 ? 0 : 1;
}
