/*
 * ub_dist_tx_res_func_test.cpp - 分布式事务资源全场景功能验证 Demo
 *
 * 交互式菜单驱动，覆盖 init/set/get/add/fetch_add/fence 全部 API，
 * 以及 NULL 指针、未对齐地址、UINT64_MAX 溢出回绕等边界场景。
 *
 * 编译：
 *   g++ -O2 -g -std=c++17 -o ub_dist_tx_res_func_test ub_dist_tx_res_func_test.cpp \
 *       -I../../include -L../../dist/release/lib -lubs-atomic -lpthread \
 *       -Wl,-rpath,'$ORIGIN/../../dist/release/lib'
 *
 * 运行：
 *   ./ub_dist_tx_res_func_test
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cstdarg>
#include <cinttypes>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include "ub_dist_tx_res.h"

/* ===================== 日志注册 ===================== */

static int my_stdout_logger(int level, const char *file, const char *func,
                            uint32_t line, const char *message)
{
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
    ts[strlen(ts) - 1] = '\0'; // 去掉末尾换行
    fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", ts, file, line, level_str, message);
    return 0;
}

/* ===================== 辅助宏与函数 ===================== */

#define SEPARATOR  "------------------------------------------------------------"
#define HEADER_SEP "============================================================"

#define CHECK_EQ(desc, actual, expect)                                       \
    do {                                                                     \
        uint64_t _a = (actual), _e = (expect);                              \
        if (_a == _e) {                                                      \
            printf("  [PASS] %-40s  (值=%" PRIu64 ")\n", (desc), _a);       \
            pass_cnt++;                                                      \
        } else {                                                             \
            printf("  [FAIL] %-40s  (实际=%" PRIu64 ", 期望=%" PRIu64 ")\n",\
                   (desc), _a, _e);                                          \
            fail_cnt++;                                                      \
        }                                                                    \
    } while (0)

#define CHECK_RES(desc, actual, expect)                                      \
    do {                                                                     \
        int _a = (actual), _e = (expect);                                   \
        if (_a == _e) {                                                      \
            printf("  [PASS] %-40s  (ret=%d)\n", (desc), _a);               \
            pass_cnt++;                                                      \
        } else {                                                             \
            printf("  [FAIL] %-40s  (实际ret=%d, 期望ret=%d)\n",            \
                   (desc), _a, _e);                                          \
            fail_cnt++;                                                      \
        }                                                                    \
    } while (0)

static void print_summary(const char *name, int pass_cnt, int fail_cnt)
{
    printf(SEPARATOR "\n");
    if (fail_cnt == 0)
        printf("[PASS] %s 全部通过 (%d/%d)\n", name, pass_cnt, pass_cnt + fail_cnt);
    else
        printf("[FAIL] %s 有 %d 项失败 (%d/%d 通过)\n", name, fail_cnt,
               pass_cnt, pass_cnt + fail_cnt);
    printf(HEADER_SEP "\n\n");
}

/* ===================== 场景1：基础操作验证 ===================== */

static void scenario_basic(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景1: 基础操作验证 (init / set / get)\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t val = 0;
    uint64_t out = 0;

    // init → get → 应为0
    int ret = ub_dist_tx_res_init(&val);
    CHECK_RES("init 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("init 后 get == 0", out, 0);

    // set(100) → get
    ret = ub_dist_tx_res_set(&val, 100);
    CHECK_RES("set(100) 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("set(100) 后 get == 100", out, 100);

    // set(UINT64_MAX) → get
    ret = ub_dist_tx_res_set(&val, UINT64_MAX);
    CHECK_RES("set(UINT64_MAX) 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("set(UINT64_MAX) 后 get == UINT64_MAX", out, UINT64_MAX);

    // fetch_add 基础：set(99) → fetch_add(1) 旧值99, 新值100
    ub_dist_tx_res_set(&val, 99);
    uint64_t old = 0;
    ret = ub_dist_tx_res_fetch_add(&val, 1, &old);
    CHECK_RES("fetch_add(1) 返回 OK", ret, UB_RES_OK);
    CHECK_EQ("fetch_add 旧值 == 99", old, 99);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("fetch_add 后 get == 100", out, 100);

    print_summary("场景1: 基础操作验证", pass_cnt, fail_cnt);
}

/* ===================== 场景2：add 原子加法验证 ===================== */

static void scenario_add(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景2: add 原子加法验证\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t val = 0;
    uint64_t out = 0;

    ub_dist_tx_res_init(&val);

    // add(10) → get == 10
    int ret = ub_dist_tx_res_add(&val, 10);
    CHECK_RES("add(10) 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("add(10) 后 get == 10", out, 10);

    // add(20) → get == 30
    ret = ub_dist_tx_res_add(&val, 20);
    CHECK_RES("add(20) 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("add(20) 后 get == 30", out, 30);

    // add(0) → get 不变
    ret = ub_dist_tx_res_add(&val, 0);
    CHECK_RES("add(0) 返回 OK", ret, UB_RES_OK);
    ub_dist_tx_res_get(&val, &out);
    CHECK_EQ("add(0) 后 get 不变 == 30", out, 30);

    print_summary("场景2: add 原子加法验证", pass_cnt, fail_cnt);
}

/* ===================== 场景3：fetch_add 与 add 对比 ===================== */

static void scenario_fetch_add_vs_add(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景3: fetch_add 与 add 对比\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t val1 = 0, val2 = 0;

    ub_dist_tx_res_init(&val1);
    ub_dist_tx_res_init(&val2);
    ub_dist_tx_res_set(&val1, 100);
    ub_dist_tx_res_set(&val2, 100);

    // val1: fetch_add(42) 获取旧值
    uint64_t old_val = 0;
    ub_dist_tx_res_fetch_add(&val1, 42, &old_val);
    CHECK_EQ("fetch_add 返回旧值 == 100", old_val, 100);

    // val2: add(42)
    ub_dist_tx_res_add(&val2, 42);

    // 两者最终值应相同
    uint64_t out1 = 0, out2 = 0;
    ub_dist_tx_res_get(&val1, &out1);
    ub_dist_tx_res_get(&val2, &out2);
    CHECK_EQ("fetch_add 最终值 == 142", out1, 142);
    CHECK_EQ("add 最终值 == 142", out2, 142);
    CHECK_EQ("两者最终值一致", out1, out2);

    print_summary("场景3: fetch_add 与 add 对比", pass_cnt, fail_cnt);
}

/* ===================== 场景4：多线程并发 add 原子性验证 ===================== */

static void scenario_concurrent_add(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景4: 多线程并发 add 原子性验证\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;

    const int NUM_THREADS = 8;
    const int ADDS_PER_THREAD = 10000;
    const uint64_t EXPECTED = (uint64_t)NUM_THREADS * ADDS_PER_THREAD;

    uint64_t val = 0;
    ub_dist_tx_res_init(&val);

    printf("  线程数: %d, 每线程 add 次数: %d\n", NUM_THREADS, ADDS_PER_THREAD);
    printf("  期望最终值: %" PRIu64 "\n", EXPECTED);

    std::vector<std::thread> threads;
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&val]() {
            for (int j = 0; j < ADDS_PER_THREAD; j++) {
                ub_dist_tx_res_add(&val, 1);
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }

    auto end = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    uint64_t out = 0;
    ub_dist_tx_res_get(&val, &out);
    printf("  实际最终值: %" PRIu64 "  (耗时 %.2f ms)\n", out, elapsed_ms);
    CHECK_EQ("并发 add 最终值 == 80000", out, EXPECTED);

    print_summary("场景4: 多线程并发 add 原子性验证", pass_cnt, fail_cnt);
}

/* ===================== 场景5：fence release + acquire 可见性验证 ===================== */

static void scenario_fence_release_acquire(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景5: fence release + acquire 可见性验证 (本端写-本端读)\n");
    printf(HEADER_SEP "\n");
    printf("  原理: writer 先写 data=42，fence_release 后写 flag=1；\n");
    printf("        reader 自旋等 flag!=0，fence_acquire 后读 data，应得 42。\n\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t data = 0, flag = 0;
    ub_dist_tx_res_init(&data);
    ub_dist_tx_res_init(&flag);

    std::thread writer([&]() {
        ub_dist_tx_res_set(&data, 42);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);
        ub_dist_tx_res_set(&flag, 1);
    });

    std::thread reader([&]() {
        uint64_t f = 0;
        while (f == 0) {
            ub_dist_tx_res_get(&flag, &f);
        }
        ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
        uint64_t d = 0;
        ub_dist_tx_res_get(&data, &d);
        CHECK_EQ("reader 读到 data == 42", d, 42);
    });

    writer.join();
    reader.join();

    print_summary("场景5: fence release+acquire 可见性验证", pass_cnt, fail_cnt);
}

/* ===================== 场景6：fence seq_cst 全局序验证 ===================== */

static void scenario_fence_seq_cst(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景6: fence seq_cst 全局序验证\n");
    printf(HEADER_SEP "\n");
    printf("  原理: 线程A set(x,1)→fence_seq_cst→get(y)；\n");
    printf("        线程B set(y,1)→fence_seq_cst→get(x)；\n");
    printf("        seq_cst 保证不会出现 x_read==0 && y_read==0。\n\n");

    int pass_cnt = 0, fail_cnt = 0;
    const int ITERATIONS = 100;
    int violation_count = 0;

    printf("  迭代次数: %d\n", ITERATIONS);

    for (int iter = 0; iter < ITERATIONS; iter++) {
        uint64_t x = 0, y = 0;
        ub_dist_tx_res_init(&x);
        ub_dist_tx_res_init(&y);

        std::atomic<int> x_read{-1}, y_read{-1};

        std::thread a([&]() {
            ub_dist_tx_res_set(&x, 1);
            ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
            uint64_t v = 0;
            ub_dist_tx_res_get(&y, &v);
            x_read.store((int)v, std::memory_order_relaxed);
        });

        std::thread b([&]() {
            ub_dist_tx_res_set(&y, 1);
            ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
            uint64_t v = 0;
            ub_dist_tx_res_get(&x, &v);
            y_read.store((int)v, std::memory_order_relaxed);
        });

        a.join();
        b.join();

        int xr = x_read.load(std::memory_order_relaxed);
        int yr = y_read.load(std::memory_order_relaxed);

        if (xr == 0 && yr == 0) {
            violation_count++;
        }
    }

    printf("  违反全局序次数 (两者均读到0): %d / %d\n", violation_count, ITERATIONS);
    CHECK_EQ("seq_cst 全局序违反次数 == 0", (uint64_t)violation_count, 0);

    print_summary("场景6: fence seq_cst 全局序验证", pass_cnt, fail_cnt);
}

/* ===================== 场景7：fence acq_rel 双向同步验证 ===================== */

static void scenario_fence_acq_rel(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景7: fence acq_rel 双向同步验证\n");
    printf(HEADER_SEP "\n");
    printf("  原理: writer 写 payload1=0xDEADBEEF, payload2=0xCAFEBABE，\n");
    printf("        fence_acq_rel 后写 ready_flag=1；reader 自旋等 flag，\n");
    printf("        fence_acq_rel 后读 payload，验证双向同步可见性。\n\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t payload1 = 0, payload2 = 0, ready_flag = 0;
    ub_dist_tx_res_init(&payload1);
    ub_dist_tx_res_init(&payload2);
    ub_dist_tx_res_init(&ready_flag);

    const uint64_t EXPECTED_P1 = 0xDEADBEEF;
    const uint64_t EXPECTED_P2 = 0xCAFEBABE;

    std::thread writer([&]() {
        ub_dist_tx_res_set(&payload1, EXPECTED_P1);
        ub_dist_tx_res_set(&payload2, EXPECTED_P2);
        ub_dist_tx_res_fence(UB_FENCE_ACQ_REL);
        ub_dist_tx_res_set(&ready_flag, 1);
    });

    std::thread reader([&]() {
        uint64_t f = 0;
        while (f == 0) {
            ub_dist_tx_res_get(&ready_flag, &f);
        }
        ub_dist_tx_res_fence(UB_FENCE_ACQ_REL);
        uint64_t p1 = 0, p2 = 0;
        ub_dist_tx_res_get(&payload1, &p1);
        ub_dist_tx_res_get(&payload2, &p2);
        CHECK_EQ("reader 读到 payload1 == 0xDEADBEEF", p1, EXPECTED_P1);
        CHECK_EQ("reader 读到 payload2 == 0xCAFEBABE", p2, EXPECTED_P2);
    });

    writer.join();
    reader.join();

    print_summary("场景7: fence acq_rel 双向同步验证", pass_cnt, fail_cnt);
}

/* ===================== 场景8：add + fence 组合（模拟本端写远端读） ===================== */

static void scenario_add_fence_combo(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景8: add + fence 组合场景 (模拟本端写远端读)\n");
    printf(HEADER_SEP "\n");
    printf("  原理: writer 执行 5 次 add(10)，fence_release 确保所有加法\n");
    printf("        对远端可见；reader 自旋等 ready 后 fence_acquire 读 counter。\n\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t counter = 0, ready = 0;
    ub_dist_tx_res_init(&counter);
    ub_dist_tx_res_init(&ready);

    const int ADD_COUNT = 5;
    const uint64_t ADD_VAL = 10;
    const uint64_t EXPECTED = (uint64_t)ADD_COUNT * ADD_VAL;

    std::thread writer([&]() {
        for (int i = 0; i < ADD_COUNT; i++) {
            ub_dist_tx_res_add(&counter, ADD_VAL);
        }
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);
        ub_dist_tx_res_set(&ready, 1);
    });

    std::thread reader([&]() {
        uint64_t r = 0;
        while (r == 0) {
            ub_dist_tx_res_get(&ready, &r);
        }
        ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
        uint64_t c = 0;
        ub_dist_tx_res_get(&counter, &c);
        CHECK_EQ("远端读到 counter == 50", c, EXPECTED);
    });

    writer.join();
    reader.join();

    print_summary("场景8: add + fence 组合场景", pass_cnt, fail_cnt);
}

/* ===================== 场景9：溢出回绕验证 ===================== */

static void scenario_overflow_wrap(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景9: 溢出回绕验证 (UINT64_MAX + 1)\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t val = 0;
    uint64_t out = 0;

    // 测试1: set(UINT64_MAX) → add(1) → 回绕为0
    ub_dist_tx_res_init(&val);
    ub_dist_tx_res_set(&val, UINT64_MAX);
    ub_dist_tx_res_add(&val, 1);
    ub_dist_tx_res_get(&val, &out);
    printf("  UINT64_MAX + 1 => 0x%016" PRIx64 "\n", out);
    CHECK_EQ("UINT64_MAX + 1 回绕 == 0", out, 0);

    // 测试2: set(UINT64_MAX - 1) → add(2) → 回绕为0
    ub_dist_tx_res_set(&val, UINT64_MAX - 1);
    ub_dist_tx_res_add(&val, 2);
    ub_dist_tx_res_get(&val, &out);
    printf("  (UINT64_MAX - 1) + 2 => 0x%016" PRIx64 "\n", out);
    CHECK_EQ("(UINT64_MAX - 1) + 2 回绕 == 0", out, 0);

    // 测试3: set(UINT64_MAX) → add(UINT64_MAX) → UINT64_MAX - 1
    ub_dist_tx_res_set(&val, UINT64_MAX);
    ub_dist_tx_res_add(&val, UINT64_MAX);
    ub_dist_tx_res_get(&val, &out);
    printf("  UINT64_MAX + UINT64_MAX => 0x%016" PRIx64 "\n", out);
    CHECK_EQ("UINT64_MAX + UINT64_MAX == UINT64_MAX-1", out, UINT64_MAX - 1);

    // 测试4: fetch_add 溢出
    ub_dist_tx_res_set(&val, UINT64_MAX);
    uint64_t old = 0;
    ub_dist_tx_res_fetch_add(&val, 1, &old);
    ub_dist_tx_res_get(&val, &out);
    printf("  fetch_add(UINT64_MAX, 1): old=0x%016" PRIx64 ", new=0x%016" PRIx64 "\n",
           old, out);
    CHECK_EQ("fetch_add 旧值 == UINT64_MAX", old, UINT64_MAX);
    CHECK_EQ("fetch_add 溢出后 == 0", out, 0);

    print_summary("场景9: 溢出回绕验证", pass_cnt, fail_cnt);
}

/* ===================== 场景10：错误处理验证 ===================== */

static void scenario_error_handling(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景10: 错误处理验证 (NULL / 未对齐地址)\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;
    uint64_t out = 0;

    printf("  ---- NULL 指针测试 ----\n");
    CHECK_RES("init(NULL) 返回 ERROR",
              ub_dist_tx_res_init(nullptr), UB_RES_ERROR);
    CHECK_RES("set(NULL, 1) 返回 ERROR",
              ub_dist_tx_res_set(nullptr, 1), UB_RES_ERROR);
    CHECK_RES("get(NULL, &out) 返回 ERROR",
              ub_dist_tx_res_get(nullptr, &out), UB_RES_ERROR);
    CHECK_RES("fetch_add(NULL, 1, &out) 返回 ERROR",
              ub_dist_tx_res_fetch_add(nullptr, 1, &out), UB_RES_ERROR);
    CHECK_RES("add(NULL, 1) 返回 ERROR",
              ub_dist_tx_res_add(nullptr, 1), UB_RES_ERROR);
    // get(handle, NULL) 也应返回错误
    uint64_t valid_val = 0;
    ub_dist_tx_res_init(&valid_val);
    CHECK_RES("get(valid, NULL) 返回 ERROR",
              ub_dist_tx_res_get(&valid_val, nullptr), UB_RES_ERROR);
    CHECK_RES("fetch_add(valid, 1, NULL) 返回 ERROR",
              ub_dist_tx_res_fetch_add(&valid_val, 1, nullptr), UB_RES_ERROR);

    printf("\n  ---- 未对齐地址测试 ----\n");
    // 构造一个未8字节对齐的地址
    uint64_t *unaligned = reinterpret_cast<uint64_t *>(0x7ffee3b5a001);
    CHECK_RES("init(未对齐) 返回 ERROR",
              ub_dist_tx_res_init(unaligned), UB_RES_ERROR);
    CHECK_RES("set(未对齐, 1) 返回 ERROR",
              ub_dist_tx_res_set(unaligned, 1), UB_RES_ERROR);
    CHECK_RES("get(未对齐, &out) 返回 ERROR",
              ub_dist_tx_res_get(unaligned, &out), UB_RES_ERROR);
    CHECK_RES("fetch_add(未对齐, 1, &out) 返回 ERROR",
              ub_dist_tx_res_fetch_add(unaligned, 1, &out), UB_RES_ERROR);
    CHECK_RES("add(未对齐, 1) 返回 ERROR",
              ub_dist_tx_res_add(unaligned, 1), UB_RES_ERROR);

    printf("\n  ---- fence 各变体返回值测试 ----\n");
    CHECK_RES("fence_acquire() 返回 OK",
              ub_dist_tx_res_fence(UB_FENCE_ACQUIRE), UB_RES_OK);
    CHECK_RES("fence_release() 返回 OK",
              ub_dist_tx_res_fence(UB_FENCE_RELEASE), UB_RES_OK);
    CHECK_RES("fence_acq_rel() 返回 OK",
              ub_dist_tx_res_fence(UB_FENCE_ACQ_REL), UB_RES_OK);
    CHECK_RES("fence_seq_cst() 返回 OK",
              ub_dist_tx_res_fence(UB_FENCE_SEQ_CST), UB_RES_OK);

    print_summary("场景10: 错误处理验证", pass_cnt, fail_cnt);
}

/* ===================== 场景11：多线程并发 fetch_add 原子性验证 ===================== */

static void scenario_concurrent_fetch_add(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景11: 多线程并发 fetch_add 原子性验证\n");
    printf(HEADER_SEP "\n");

    int pass_cnt = 0, fail_cnt = 0;

    const int NUM_THREADS = 8;
    const int OPS_PER_THREAD = 10000;
    const uint64_t INCREMENT = 1;
    const uint64_t EXPECTED = (uint64_t)NUM_THREADS * OPS_PER_THREAD * INCREMENT;

    uint64_t val = 0;
    ub_dist_tx_res_init(&val);

    printf("  线程数: %d, 每线程 fetch_add 次数: %d\n", NUM_THREADS, OPS_PER_THREAD);
    printf("  期望最终值: %" PRIu64 "\n", EXPECTED);

    // 每个线程收集自己 fetch_add 返回的旧值之和
    std::vector<uint64_t> old_sums(NUM_THREADS, 0);
    std::vector<std::thread> threads;

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&val, &old_sums, i, OPS_PER_THREAD]() {
            uint64_t local_sum = 0;
            uint64_t old = 0;
            for (int j = 0; j < OPS_PER_THREAD; j++) {
                ub_dist_tx_res_fetch_add(&val, INCREMENT, &old);
                local_sum += old;
            }
            old_sums[i] = local_sum;
        });
    }
    for (auto &t : threads) {
        t.join();
    }

    auto end = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    uint64_t out = 0;
    ub_dist_tx_res_get(&val, &out);
    printf("  实际最终值: %" PRIu64 "  (耗时 %.2f ms)\n", out, elapsed_ms);
    CHECK_EQ("并发 fetch_add 最终值正确", out, EXPECTED);

    // 所有旧值之和应等于 0+1+2+...+(N-1)，其中 N = 总操作数
    // 因为每次 fetch_add(1) 返回的是加之前的值
    uint64_t total_old_sum = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_old_sum += old_sums[i];
    }
    // 总操作数 = NUM_THREADS * OPS_PER_THREAD
    // 每次 fetch_add(1) 返回加之前的值，所有旧值之和 = 0+1+2+...+(N-1) = N*(N-1)/2
    uint64_t N = (uint64_t)NUM_THREADS * OPS_PER_THREAD;
    uint64_t expected_old_sum = N * (N - 1) / 2;
    printf("  所有旧值之和: %" PRIu64 "  (期望: %" PRIu64 ")\n",
           total_old_sum, expected_old_sum);
    CHECK_EQ("旧值之和 == N*(N-1)/2", total_old_sum, expected_old_sum);

    print_summary("场景11: 多线程并发 fetch_add 原子性验证", pass_cnt, fail_cnt);
}

/* ===================== 场景12：多线程 fence 可见性压测 ===================== */

static void scenario_fence_stress(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  场景12: 多线程 fence release+acquire 可见性压测\n");
    printf(HEADER_SEP "\n");
    printf("  原理: 多轮重复 writer→fence_release→reader→fence_acquire，\n");
    printf("        验证在高频调度下 fence 始终保证可见性。\n\n");

    int pass_cnt = 0, fail_cnt = 0;
    const int ROUNDS = 50;
    int fail_rounds = 0;

    printf("  压测轮数: %d\n", ROUNDS);

    for (int r = 0; r < ROUNDS; r++) {
        uint64_t data = 0, flag = 0;
        ub_dist_tx_res_init(&data);
        ub_dist_tx_res_init(&flag);

        const uint64_t magic = (uint64_t)r * 1000 + 42;
        std::atomic<uint64_t> reader_result{0};

        std::thread writer([&]() {
            ub_dist_tx_res_set(&data, magic);
            ub_dist_tx_res_fence(UB_FENCE_RELEASE);
            ub_dist_tx_res_set(&flag, 1);
        });

        std::thread reader([&]() {
            uint64_t f = 0;
            while (f == 0) {
                ub_dist_tx_res_get(&flag, &f);
            }
            ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
            uint64_t d = 0;
            ub_dist_tx_res_get(&data, &d);
            reader_result.store(d, std::memory_order_relaxed);
        });

        writer.join();
        reader.join();

        if (reader_result.load(std::memory_order_relaxed) != magic) {
            fail_rounds++;
        }
    }

    printf("  失败轮数: %d / %d\n", fail_rounds, ROUNDS);
    CHECK_EQ("fence 压测失败轮数 == 0", (uint64_t)fail_rounds, 0);

    print_summary("场景12: 多线程 fence 可见性压测", pass_cnt, fail_cnt);
}

/* ===================== 交互菜单 ===================== */

static void print_menu(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  ub_dist_tx_res 全场景功能验证 Demo\n");
    printf(HEADER_SEP "\n");
    printf("   1. 基础操作验证 (init / set / get / fetch_add)\n");
    printf("   2. add 原子加法验证\n");
    printf("   3. fetch_add 与 add 对比\n");
    printf("   4. 多线程并发 add 原子性验证\n");
    printf("   5. fence release + acquire 可见性验证 (本端写-本端读)\n");
    printf("   6. fence seq_cst 全局序验证\n");
    printf("   7. fence acq_rel 双向同步验证\n");
    printf("   8. add + fence 组合场景 (模拟本端写远端读)\n");
    printf("   9. 溢出回绕验证 (UINT64_MAX + 1)\n");
    printf("  10. 错误处理验证 (NULL / 未对齐地址)\n");
    printf("  11. 多线程并发 fetch_add 原子性验证\n");
    printf("  12. 多线程 fence release+acquire 可见性压测\n");
    printf("  99. 运行全部场景\n");
    printf("   0. 退出\n");
    printf(HEADER_SEP "\n");
    printf("请输入场景编号: ");
}

static void run_all_scenarios(void)
{
    scenario_basic();
    scenario_add();
    scenario_fetch_add_vs_add();
    scenario_concurrent_add();
    scenario_fence_release_acquire();
    scenario_fence_seq_cst();
    scenario_fence_acq_rel();
    scenario_add_fence_combo();
    scenario_overflow_wrap();
    scenario_error_handling();
    scenario_concurrent_fetch_add();
    scenario_fence_stress();
}

/* ===================== 主函数 ===================== */

int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;

    // 注册日志回调
    ub_atomic_register_log_func(my_stdout_logger);
    ub_atomic_set_log_level(LOG_LEVEL_WARN);

    printf("\n" HEADER_SEP "\n");
    printf("  欢迎使用 ub_dist_tx_res 全场景功能验证 Demo\n");
    printf("  覆盖 API: init / set / get / add / fetch_add / fence_*\n");
    printf("  边界场景: NULL指针 / 未对齐地址 / UINT64_MAX溢出回绕\n");
    printf(HEADER_SEP "\n");

    int choice = -1;
    while (true) {
        print_menu();
        if (scanf("%d", &choice) != 1) {
            // 清空无效输入
            int c;
            while ((c = getchar()) != '\n' && c != EOF)
                ;
            printf("输入无效，请输入数字。\n");
            continue;
        }

        switch (choice) {
            case 1:  scenario_basic();                  break;
            case 2:  scenario_add();                    break;
            case 3:  scenario_fetch_add_vs_add();       break;
            case 4:  scenario_concurrent_add();         break;
            case 5:  scenario_fence_release_acquire();  break;
            case 6:  scenario_fence_seq_cst();          break;
            case 7:  scenario_fence_acq_rel();          break;
            case 8:  scenario_add_fence_combo();        break;
            case 9:  scenario_overflow_wrap();          break;
            case 10: scenario_error_handling();         break;
            case 11: scenario_concurrent_fetch_add();   break;
            case 12: scenario_fence_stress();           break;
            case 99: run_all_scenarios();               break;
            case 0:
                printf("退出验证 Demo，再见。\n");
                return 0;
            default:
                printf("未知选项: %d，请重新输入。\n", choice);
                break;
        }
    }

    return 0;
}
