/*
 * ub_dist_tx_res_fence_semantic_test.cpp - fence 接口语义验证测试（单节点多线程）
 *
 * 验证 ub_dist_tx_res_fence 不同 order 的实际内存序语义：
 *   TC-FENCE-MSGPASS  : StoreStore 屏障（RELEASE）—— 消息传递模型
 *   TC-FENCE-STORELOAD: StoreLoad 屏障（SEQ_CST）—— Store-Load 重排验证
 *
 * 设计参考：newtest.txt 中的两个经典内存序用例，改为基于 ubs-atomic 接口实现：
 *   - 共享变量用 ub_dist_tx_res 接口操作（atomic + 内存序）
 *   - 屏障用 ub_dist_tx_res_fence(order)
 *   - 多线程用 std::thread + std::atomic 统计
 *
 * 说明：
 *   - 不包含前置步骤 P1/P2（组网、共享内存创建），仅实现测试步骤与预期校验
 *   - 通过 --shm <name> 指定共享内存名
 *   - 支持 --case <id> 单用例执行，不指定则批量执行
 *
 * 编译：
 *   g++ -O2 -g -std=c++17 -o ub_dist_tx_res_fence_semantic_test \
 *       ub_dist_tx_res_fence_semantic_test.cpp \
 *       -I../../include \
 *       -I/usr/local/ubs_mem/include \
 *       -L../../build/lib -lubs-atomic \
 *       -L/usr/local/ubs_mem/lib -lubsm_sdk \
 *       -lpthread \
 *       -Wl,-rpath,/usr/local/ubs_mem/lib \
 *       -Wl,-rpath,'$ORIGIN/../../build/lib'
 *
 * 运行：
 *   ./ub_dist_tx_res_fence_semantic_test --shm shm_fence_sem
 *   ./ub_dist_tx_res_fence_semantic_test --shm shm_fence_sem --case TC-FENCE-MSGPASS
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cinttypes>
#include <ctime>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <getopt.h>
#include <sys/mman.h>

#include "ubs_mem.h"
#include "ubs_mem_def.h"
#include "ub_dist_tx_res.h"

/* ===================== 常量 ===================== */

#define SEPARATOR  "------------------------------------------------------------"
#define HEADER_SEP "============================================================"

static const size_t SHM_SIZE_DEFAULT = 1UL * 1024 * 1024 * 1024; // 1GB
static const char *SHM_NAME_DEFAULT = "shm_fence_sem";

/* 内存序语义验证用例的共享内存布局：
 * 所有字段强制 cache line 对齐（64B），避免伪共享干扰重排观测。
 *
 * TC-FENCE-MSGPASS（消息传递模型）:
 *   data_msg[0..63]   : 生产者写入的数据（cache line 0）
 *   ready_msg[64..127]: 就绪标志位（cache line 1）
 *
 * TC-FENCE-STORELOAD（Store-Load 重排）:
 *   x_val[0..63]      : 线程1 写 / 线程2 读（cache line 2）
 *   y_val[64..127]    : 线程2 写 / 线程1 读（cache line 3）
 *   r1_val[128..191]  : 线程1 读到的 r1（cache line 4）
 *   r2_val[192..255]  : 线程2 读到的 r2（cache line 5）
 *
 * 每 64B 为一个独立 cache line，确保屏障验证不受伪共享影响。
 */
struct FenceSemLayout {
    /* cache line 0: TC-FENCE-MSGPASS 的 data */
    uint64_t data_msg;        char _pad0[56];
    /* cache line 1: TC-FENCE-MSGPASS 的 ready */
    uint64_t ready_msg;       char _pad1[56];
    /* cache line 2: TC-FENCE-STORELOAD 的 x */
    uint64_t x_val;           char _pad2[56];
    /* cache line 3: TC-FENCE-STORELOAD 的 y */
    uint64_t y_val;           char _pad3[56];
    /* cache line 4: TC-FENCE-STORELOAD 的 r1（线程1 读 Y 的结果） */
    uint64_t r1_val;          char _pad4[56];
    /* cache line 5: TC-FENCE-STORELOAD 的 r2（线程2 读 X 的结果） */
    uint64_t r2_val;          char _pad5[56];
};
static_assert(sizeof(FenceSemLayout) == 384, "layout must be 6 cache lines");

/* ===================== 全局 ===================== */

static void *g_shm_base = nullptr;
static size_t g_shm_size = SHM_SIZE_DEFAULT;
static FenceSemLayout *g_layout = nullptr;

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
            printf("  [PASS] %-50s (值=%" PRIu64 ")\n", (desc), _a);          \
            g_pass_cnt++;                                                    \
        } else {                                                             \
            printf("  [FAIL] %-50s (实际=%" PRIu64 ", 期望=%" PRIu64 ")\n",  \
                   (desc), _a, _e);                                          \
            g_fail_cnt++;                                                    \
        }                                                                    \
    } while (0)

#define CHECK_COND(desc, cond)                                              \
    do {                                                                     \
        if (cond) {                                                          \
            printf("  [PASS] %-50s\n", (desc));                              \
            g_pass_cnt++;                                                    \
        } else {                                                             \
            printf("  [FAIL] %-50s\n", (desc));                              \
            g_fail_cnt++;                                                    \
        }                                                                    \
    } while (0)

static void case_begin(const char *name)
{
    printf("\n" HEADER_SEP "\n");
    printf("  %s\n", name);
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

/* ===================== 用例间清理 ===================== */

static void case_cleanup(void)
{
    ub_dist_tx_res_set(&g_layout->data_msg, 0);
    ub_dist_tx_res_set(&g_layout->ready_msg, 0);
    ub_dist_tx_res_set(&g_layout->x_val, 0);
    ub_dist_tx_res_set(&g_layout->y_val, 0);
    ub_dist_tx_res_set(&g_layout->r1_val, 0);
    ub_dist_tx_res_set(&g_layout->r2_val, 0);
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
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

/* ===================== 用例 TC-FENCE-MSGPASS ===================== */
/* StoreStore 屏障（RELEASE）—— 消息传递模型
 *
 * 验证 UB_FENCE_RELEASE 保证"先写数据，后写标志位"的顺序：
 *   生产者：set(data, i) → fence(RELEASE) → set(ready, 1)
 *   消费者：等 ready==1 → fence(ACQUIRE) → get(data)
 *
 * 如果 RELEASE 屏障有效，消费者读到 ready==1 时必然能看到 data==i。
 * 如果屏障无效，ready 可能先于 data 可见，消费者读到旧 data。
 *
 * 对照原 newtest.txt 用例1 的改进：
 *   - 消费者侧增加 fence(ACQUIRE)，形成 release-acquire 配对，语义严谨
 *   - data/ready 用 ub_dist_tx_res 接口（atomic + 内存序），而非裸 volatile
 *   - fail_count 用 std::atomic，避免统计本身的重排
 *   - ready 重置时也走 fence(RELEASE)，保证消费者读 data 先于写 ready=0
 */

static constexpr uint64_t MSGPASS_ITERS = 50000000ULL; // 5000万次

static void case_tc_fence_msgpass(void)
{
    case_begin("TC-FENCE-MSGPASS : StoreStore(RELEASE) 消息传递模型");
    case_cleanup();

    printf("  [Info] 迭代次数: %llu\n", (unsigned long long)MSGPASS_ITERS);
    printf("  [Info] 验证点: fence(RELEASE) 保证 data 先于 ready 可见\n");

    /* 重置 ready=0（消费者先等待 ready==1） */
    ub_dist_tx_res_set(&g_layout->ready_msg, 0);
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);

    std::atomic<uint64_t> fail_count{0};
    std::atomic<bool> producer_done{false};

    /* 生产者：写 data → fence(RELEASE) → 写 ready=1；等消费者 reset ready=0 */
    auto producer = [&]() {
        for (uint64_t i = 1; i <= MSGPASS_ITERS; i++) {
            /* 等消费者消费完上一轮（ready 变回 0） */
            uint64_t r = 1;
            while (r == 1) {
                ub_dist_tx_res_get(&g_layout->ready_msg, &r);
            }
            /* 写数据 */
            ub_dist_tx_res_set(&g_layout->data_msg, i);
            /* StoreStore 屏障：保证 data 先于 ready 可见 */
            ub_dist_tx_res_fence(UB_FENCE_RELEASE);
            /* 写就绪标志 */
            ub_dist_tx_res_set(&g_layout->ready_msg, 1);
        }
        producer_done.store(true, std::memory_order_release);
    };

    /* 消费者：等 ready==1 → fence(ACQUIRE) → 读 data → reset ready=0 */
    auto consumer = [&]() {
        for (uint64_t i = 1; i <= MSGPASS_ITERS; i++) {
            /* 等 ready==1 */
            uint64_t r = 0;
            while (r != 1) {
                ub_dist_tx_res_get(&g_layout->ready_msg, &r);
            }
            /* Acquire 屏障：与生产者的 release 配对，保证读到 ready==1 时 data 也可见 */
            ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
            /* 读数据 */
            uint64_t d = 0;
            ub_dist_tx_res_get(&g_layout->data_msg, &d);
            if (d != i) {
                fail_count.fetch_add(1, std::memory_order_relaxed);
            }
            /* 重置 ready=0 前先 release，保证读 data 先于写 ready=0 */
            ub_dist_tx_res_fence(UB_FENCE_RELEASE);
            ub_dist_tx_res_set(&g_layout->ready_msg, 0);
        }
    };

    auto t0 = std::chrono::steady_clock::now();
    std::thread tc(consumer);
    std::thread tp(producer);
    tp.join();
    tc.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    uint64_t fails = fail_count.load(std::memory_order_acquire);
    printf("  [Info] 完成 %llu 次迭代, 耗时 %.2f ms\n",
           (unsigned long long)MSGPASS_ITERS, ms);
    printf("  [Info] 检测到 StoreStore 重排次数: %llu\n",
           (unsigned long long)fails);

    /* E1: 屏障有效时，重排次数必须为 0 */
    CHECK_COND("E1 StoreStore 重排次数 == 0 (RELEASE 屏障有效)", fails == 0);

    case_end("TC-FENCE-MSGPASS");
}

/* ===================== 用例 TC-FENCE-STORELOAD ===================== */
/* StoreLoad 屏障（SEQ_CST）—— Store-Load 重排验证
 *
 * 验证 UB_FENCE_SEQ_CST 防止 Store-Load 重排：
 *   线程1: set(x,1) → fence(SEQ_CST) → get(y, &r1)
 *   线程2: set(y,1) → fence(SEQ_CST) → get(x, &r2)
 *
 * 程序顺序下 (r1,r2) 不可能为 (0,0)。若屏障无效发生 Store-Load 重排，
 * 两个 load 可能在两个 store 之前执行，导致 (r1,r2)=(0,0)。
 *
 * 对照原 newtest.txt 用例2 的改进：
 *   - x/y 用 ub_dist_tx_res 接口（atomic + 内存序）
 *   - 屏障用 ub_dist_tx_res_fence(SEQ_CST)，对应 dmb ish 全屏障
 *   - 加迭代次数上限（避免死循环），支持 --iters 参数
 *   - 同步信号 start1/start2 用 std::atomic，语义严谨
 */

static uint64_t g_storeload_iters = 1000000ULL; // 默认 100 万次

static void case_tc_fence_storeload(void)
{
    case_begin("TC-FENCE-STORELOAD : StoreLoad(SEQ_CST) 重排验证");
    case_cleanup();

    printf("  [Info] 迭代次数: %llu\n", (unsigned long long)g_storeload_iters);
    printf("  [Info] 验证点: fence(SEQ_CST) 防止 (r1,r2)=(0,0)\n");

    std::atomic<bool> running{true};
    std::atomic<uint64_t> reorder_count{0};

    /* 同步信号：主线程置 1 通知子线程开始本轮，子线程完成后置 0 */
    std::atomic<int> start1{0};
    std::atomic<int> start2{0};

    /* 线程1: X=1 → fence(SEQ_CST) → r1=Y → 写 r1_val → 通知完成 */
    auto thread1 = [&]() {
        while (running.load(std::memory_order_acquire)) {
            while (start1.load(std::memory_order_acquire) == 0) {
                if (!running.load(std::memory_order_acquire)) return;
            }
            ub_dist_tx_res_set(&g_layout->x_val, 1);
            /* StoreLoad 屏障：防止 X=1 与 r1=Y 之间发生 Store-Load 重排 */
            ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
            uint64_t r1 = 0;
            ub_dist_tx_res_get(&g_layout->y_val, &r1);
            /* 把 r1 写到共享变量，供主线程汇总判断 (r1,r2) */
            ub_dist_tx_res_fence(UB_FENCE_RELEASE);
            ub_dist_tx_res_set(&g_layout->r1_val, r1);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            start1.store(0, std::memory_order_release);
        }
    };

    /* 线程2: Y=1 → fence(SEQ_CST) → r2=X → 写 r2_val → 通知完成 */
    auto thread2 = [&]() {
        while (running.load(std::memory_order_acquire)) {
            while (start2.load(std::memory_order_acquire) == 0) {
                if (!running.load(std::memory_order_acquire)) return;
            }
            ub_dist_tx_res_set(&g_layout->y_val, 1);
            ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
            uint64_t r2 = 0;
            ub_dist_tx_res_get(&g_layout->x_val, &r2);
            ub_dist_tx_res_fence(UB_FENCE_RELEASE);
            ub_dist_tx_res_set(&g_layout->r2_val, r2);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            start2.store(0, std::memory_order_release);
        }
    };

    std::thread t1(thread1);
    std::thread t2(thread2);

    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < g_storeload_iters; i++) {
        /* 重置 X/Y/r1/r2 为 0 */
        ub_dist_tx_res_set(&g_layout->x_val, 0);
        ub_dist_tx_res_set(&g_layout->y_val, 0);
        ub_dist_tx_res_set(&g_layout->r1_val, 0);
        ub_dist_tx_res_set(&g_layout->r2_val, 0);
        ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);

        /* 通知两个线程同时开始 */
        start1.store(1, std::memory_order_release);
        start2.store(1, std::memory_order_release);

        /* 等两个线程都完成（start 变回 0） */
        while (start1.load(std::memory_order_acquire) != 0) {
        }
        while (start2.load(std::memory_order_acquire) != 0) {
        }

        /* 汇总本轮 r1/r2，判断是否 (0,0) */
        std::atomic_thread_fence(std::memory_order_acquire);
        uint64_t r1 = 0, r2 = 0;
        ub_dist_tx_res_get(&g_layout->r1_val, &r1);
        ub_dist_tx_res_get(&g_layout->r2_val, &r2);
        if (r1 == 0 && r2 == 0) {
            reorder_count.fetch_add(1, std::memory_order_relaxed);
        }

        if ((i + 1) % 100000 == 0) {
            printf("  [Info] 进度: %llu / %llu\n",
                   (unsigned long long)(i + 1),
                   (unsigned long long)g_storeload_iters);
        }
    }
    running.store(false, std::memory_order_release);
    /* 唤醒可能阻塞在 start 等待的线程 */
    start1.store(1, std::memory_order_release);
    start2.store(1, std::memory_order_release);
    t1.join();
    t2.join();
    auto t1end = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1end - t0).count();

    uint64_t reorders = reorder_count.load(std::memory_order_acquire);
    printf("  [Info] 完成 %llu 次迭代, 耗时 %.2f ms\n",
           (unsigned long long)g_storeload_iters, ms);
    printf("  [Info] 检测到 (r1,r2)=(0,0) 次数: %llu\n",
           (unsigned long long)reorders);

    /* E1: SEQ_CST 屏障有效时，不应出现 (r1,r2)=(0,0) */
    CHECK_COND("E1 StoreLoad 重排次数 == 0 (SEQ_CST 屏障有效)", reorders == 0);

    case_end("TC-FENCE-STORELOAD");
}

/* ===================== 用例调度 ===================== */

struct CaseEntry {
    const char *id;
    void (*fn)(void);
};

static CaseEntry g_cases[] = {
    {"TC-FENCE-MSGPASS",   case_tc_fence_msgpass},
    {"TC-FENCE-STORELOAD", case_tc_fence_storeload},
};

static void run_case(const std::string &id)
{
    for (const auto &c : g_cases) {
        if (id == c.id) {
            c.fn();
            return;
        }
    }
    fprintf(stderr, "[Error] 未知用例编号: %s\n", id.c_str());
}

static void run_all(void)
{
    for (const auto &c : g_cases) {
        c.fn();
    }
}

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s --shm <name> [--shm-size <MB>] [--case <id>] [--iters <n>]\n"
            "\n"
            "  --shm <name>       共享内存名 (必需)\n"
            "  --shm-size <MB>    共享内存大小, 默认 %zu MB\n"
            "  --case <id>        单用例执行; 不指定则批量执行\n"
            "  --iters <n>        TC-FENCE-STORELOAD 迭代次数, 默认 %llu\n"
            "\n"
            "可用用例编号:\n",
            prog, SHM_SIZE_DEFAULT / (1024 * 1024),
            (unsigned long long)g_storeload_iters);
    for (const auto &c : g_cases) {
        fprintf(stderr, "  %s\n", c.id);
    }
    fprintf(stderr, "\n示例:\n"
                    "  %s --shm shm_fence_sem\n"
                    "  %s --shm shm_fence_sem --case TC-FENCE-MSGPASS\n"
                    "  %s --shm shm_fence_sem --case TC-FENCE-STORELOAD --iters 5000000\n",
            prog, prog, prog);
}

static struct option long_options[] = {
    {"shm",      required_argument, nullptr, 's'},
    {"shm-size", required_argument, nullptr, 'z'},
    {"case",     required_argument, nullptr, 'c'},
    {"iters",    required_argument, nullptr, 'n'},
    {"help",     no_argument,       nullptr, 'h'},
    {nullptr,    0,                 nullptr,  0 }
};

/* ===================== 主函数 ===================== */

int main(int argc, char *argv[])
{
    std::string shm_name;
    std::string case_id;
    size_t shm_size_mb = SHM_SIZE_DEFAULT / (1024 * 1024);

    int opt;
    while ((opt = getopt_long(argc, argv, "s:z:c:n:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 's': shm_name = optarg; break;
            case 'z': shm_size_mb = std::stoull(optarg); break;
            case 'c': case_id = optarg; break;
            case 'n': g_storeload_iters = std::stoull(optarg); break;
            case 'h': print_usage(argv[0]); return 0;
            default:  print_usage(argv[0]); return 1;
        }
    }

    if (shm_name.empty()) {
        fprintf(stderr, "[Error] 必须指定 --shm <name>\n");
        print_usage(argv[0]);
        return 1;
    }

    /* 注册日志 */
    ub_atomic_register_log_func(my_stdout_logger);
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);

    /* 映射共享内存 */
    int ret = init_ubsmem_shm(shm_name.c_str(), shm_size_mb * 1024 * 1024, &g_shm_base);
    if (ret != 0) {
        fprintf(stderr, "[Error] 共享内存映射失败: ret=%d\n", ret);
        return 1;
    }
    // 8 字节对齐
    uintptr_t base_addr = reinterpret_cast<uintptr_t>(g_shm_base);
    if (base_addr % alignof(uint64_t) != 0) {
        base_addr += alignof(uint64_t) - (base_addr % alignof(uint64_t));
        printf("[Info] 基地址对齐调整: %p -> 0x%" PRIxPTR "\n", g_shm_base, base_addr);
    }
    g_layout = reinterpret_cast<FenceSemLayout *>(base_addr);
    printf("[Info] FenceSemLayout 地址: %p, 大小: %zu\n",
           (void *)g_layout, sizeof(FenceSemLayout));

    /* 执行用例 */
    if (case_id.empty()) {
        run_all();
    } else {
        run_case(case_id);
    }

    /* 汇总 */
    printf(HEADER_SEP "\n");
    printf("  总计: PASS=%d, FAIL=%d\n", g_pass_cnt, g_fail_cnt);
    printf(HEADER_SEP "\n");

    if (g_shm_base) {
        ubsmem_shmem_unmap(g_shm_base, g_shm_size);
        printf("[Info] 共享内存已卸载\n");
    }
    ubsmem_finalize();
    return g_fail_cnt == 0 ? 0 : 1;
}
