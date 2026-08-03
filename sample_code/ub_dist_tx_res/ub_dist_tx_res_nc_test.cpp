/*
 * ub_dist_tx_res_nc_test.cpp - 分布式事务资源跨节点 NC 环境验证 Demo
 *
 * 基于 ubsmem 真实共享内存映射，验证 writer（本端CC）与 reader（远端NC）
 * 之间通过 fence 保证数据可见性的各场景。
 *
 * 角色模型：
 *   --role writer  : 本端CC映射，执行写操作和fence
 *   --role reader  : 远端NC映射，执行读操作验证可见性
 *
 * 编译：
 *   g++ -O2 -g -std=c++17 -o ub_dist_tx_res_nc_test ub_dist_tx_res_nc_test.cpp \
 *       -I../../include \
 *       -I/usr/local/ubs_mem/include \
 *       -L../../build/lib -lubs-atomic \
 *       -L/usr/local/ubs_mem/lib -lubsm_sdk \
 *       -lpthread \
 *       -Wl,-rpath,/usr/local/ubs_mem/lib \
 *       -Wl,-rpath,'$ORIGIN/../../build/lib'
 *
 * 运行：
 *   # NodeA（writer）
 *   ./ub_dist_tx_res_nc_test --role writer --config tx_res_nc.conf
 *   # NodeB（reader）
 *   ./ub_dist_tx_res_nc_test --role reader --config tx_res_nc.conf
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cstdarg>
#include <cinttypes>
#include <chrono>
#include <thread>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <getopt.h>
#include <sys/mman.h>

#include "ubs_mem.h"
#include "ubs_mem_def.h"
#include "ub_dist_tx_res.h"

/* ===================== 共享内存布局 ===================== */

#pragma pack(push, 1)
struct NcTestLayout {
    // Control Area (64B, cache line aligned)
    uint64_t ready_flag;     // offset 0
    uint64_t done_flag;      // offset 8
    uint64_t scenario_id;    // offset 16
    uint8_t  padding[40];    // offset 24, 对齐到64B

    // Data Area
    uint64_t data;           // offset 64  - 场景1: 本端写远端读
    uint64_t counter;        // offset 72  - 场景2: 本端add远端读
    uint64_t x;              // offset 80  - 场景3: seq_cst全局序
    uint64_t y;              // offset 88  - 场景3: seq_cst全局序
    uint64_t payload[2];     // offset 96  - 场景4: acq_rel双向同步
    uint64_t fetch_counter;  // offset 112 - 场景5: 并发fetch_add
    uint64_t result;         // offset 120 - 结果回写
};
#pragma pack(pop)

static_assert(sizeof(NcTestLayout) <= 128, "Layout exceeds expected size");

/* ===================== 常量定义 ===================== */

#define SEPARATOR  "------------------------------------------------------------"
#define HEADER_SEP "============================================================"

static const size_t SHM_SIZE_DEFAULT = 64UL * 1024 * 1024; // 64MB
static const char *SHM_NAME_DEFAULT = "shm_tx_res_nc";

/* ===================== 全局变量 ===================== */

static void *g_shm_base = nullptr;
static size_t g_shm_size = SHM_SIZE_DEFAULT;
static NcTestLayout *g_layout = nullptr;
static bool g_is_writer = true; // true=writer(CC), false=reader(NC)

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
    if (fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", ts, file, line, level_str, message) < 0) {
        clearerr(stdout);
    }
    return 0;
}

/* ===================== 辅助函数 ===================== */

static std::string trim(const std::string &s)
{
    auto not_space = [](unsigned char c) { return !std::isspace(c); };
    auto begin = std::find_if(s.begin(), s.end(), not_space);
    auto end = std::find_if(s.rbegin(), s.rend(), not_space).base();
    if (begin >= end)
        return {};
    return std::string(begin, end);
}

/* ===================== 配置文件解析 ===================== */

struct NcConfig {
    std::string self;           // NodeA 或 NodeB
    std::string shm_name;       // 共享内存名称
    size_t shm_size_mb = 64;    // 共享内存大小(MB)
};

static bool load_nc_config(const std::string &path, NcConfig &cfg, std::string &err)
{
    std::ifstream in(path);
    if (!in) {
        err = "cannot open config: " + path;
        return false;
    }

    std::string line;
    int lineno = 0;
    while (std::getline(in, line)) {
        lineno++;
        auto pos_hash = line.find('#');
        if (pos_hash != std::string::npos)
            line = line.substr(0, pos_hash);

        line = trim(line);
        if (line.empty())
            continue;

        auto pos_eq = line.find('=');
        if (pos_eq == std::string::npos) {
            err = "bad line (no '=') at " + std::to_string(lineno) + ": " + line;
            return false;
        }

        std::string key = trim(line.substr(0, pos_eq));
        std::string val = trim(line.substr(pos_eq + 1));

        if (key == "self")
            cfg.self = val;
        else if (key == "shm_name")
            cfg.shm_name = val;
        else if (key == "shm_size_mb")
            cfg.shm_size_mb = std::stoull(val);
    }

    if (cfg.self.empty()) {
        err = "missing key: self";
        return false;
    }
    if (cfg.shm_name.empty()) {
        err = "missing key: shm_name";
        return false;
    }
    return true;
}

/* ===================== ubsmem 初始化与映射 ===================== */

static int init_ubsmem_shm(const char *shm_name, size_t shm_size, void **base_addr)
{
    // 1. 初始化属性
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_init_attributes failed: %d\n", ret);
        return -1;
    }

    // 2. 初始化运行时
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_initialize failed: %d\n", ret);
        return -1;
    }

    // 3. 查询regions
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_lookup_regions failed: %d\n", ret);
        return -1;
    }

    // 4. 映射共享内存
    ret = ubsmem_shmem_map(nullptr, shm_size,
                           PROT_READ | PROT_WRITE, MAP_SHARED,
                           shm_name, 0, base_addr);
    if (ret != 0) {
        fprintf(stderr, "[Error] ubsmem_shmem_map(%s) failed: %d\n", shm_name, ret);
        return -1;
    }

    fprintf(stdout, "[Info] 共享内存映射成功: name=%s, addr=%p, size=%zu\n",
            shm_name, *base_addr, shm_size);
    return 0;
}

/* ===================== 握手同步辅助 ===================== */

/*
 * writer 端：写入数据后调用
 *   fence → set(ready_flag, scenario_num) → 等 done_flag == scenario_num
 */
static void writer_signal_ready(uint64_t scenario_num)
{
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    ub_dist_tx_res_set(&g_layout->ready_flag, scenario_num);
    // 等待 reader 完成
    uint64_t d = 0;
    while (d != scenario_num) {
        ub_dist_tx_res_get(&g_layout->done_flag, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

/*
 * reader 端：等 ready_flag == scenario_num → fence → 读数据 → set(done_flag, scenario_num)
 */
static void reader_wait_ready(uint64_t scenario_num)
{
    uint64_t r = 0;
    while (r != scenario_num) {
        ub_dist_tx_res_get(&g_layout->ready_flag, &r);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
}

static void reader_signal_done(uint64_t scenario_num)
{
    ub_dist_tx_res_set(&g_layout->done_flag, scenario_num);
}

/* 重置控制标志（每个场景开始前由 writer 调用） */
static void reset_control_flags(void)
{
    ub_dist_tx_res_set(&g_layout->ready_flag, 0);
    ub_dist_tx_res_set(&g_layout->done_flag, 0);
    ub_dist_tx_res_set(&g_layout->scenario_id, 0);
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    // 等待双方都看到0
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

/* ===================== CHECK 宏 ===================== */

static int g_pass_cnt = 0;
static int g_fail_cnt = 0;

#define CHECK_EQ(desc, actual, expect)                                       \
    do {                                                                     \
        uint64_t _a = (actual), _e = (expect);                              \
        if (_a == _e) {                                                      \
            printf("  [PASS] %-40s  (值=0x%" PRIx64 ")\n", (desc), _a);     \
            g_pass_cnt++;                                                    \
        } else {                                                             \
            printf("  [FAIL] %-40s  (实际=0x%" PRIx64 ", 期望=0x%" PRIx64 ")\n", \
                   (desc), _a, _e);                                          \
            g_fail_cnt++;                                                    \
        }                                                                    \
    } while (0)

static void print_scenario_summary(const char *name)
{
    printf(SEPARATOR "\n");
    if (g_fail_cnt == 0)
        printf("[PASS] %s 全部通过 (%d/%d)\n", name, g_pass_cnt, g_pass_cnt + g_fail_cnt);
    else
        printf("[FAIL] %s 有 %d 项失败 (%d/%d 通过)\n", name, g_fail_cnt,
               g_pass_cnt, g_pass_cnt + g_fail_cnt);
    printf(HEADER_SEP "\n\n");
    g_pass_cnt = 0;
    g_fail_cnt = 0;
}

/* ===================== 场景1：本端写-远端读 ===================== */

static void scenario1_writer(void)
{
    printf("\n[writer] 场景1: 本端写-远端读 (set + fence_release + fence_acquire)\n");
    reset_control_flags();

    const uint64_t MAGIC = 0xDEADBEEF;
    ub_dist_tx_res_set(&g_layout->data, MAGIC);
    printf("  [writer] 写入 data = 0x%" PRIx64 "\n", MAGIC);

    writer_signal_ready(1);
    printf("  [writer] reader 已确认读取完成\n");
}

static void scenario1_reader(void)
{
    printf("\n[reader] 场景1: 本端写-远端读 (set + fence_release + fence_acquire)\n");
    printf("  [reader] 等待 writer 就绪...\n");

    reader_wait_ready(1);

    uint64_t val = 0;
    ub_dist_tx_res_get(&g_layout->data, &val);
    printf("  [reader] 读到 data = 0x%" PRIx64 "\n", val);
    CHECK_EQ("reader 读到 data == 0xDEADBEEF", val, 0xDEADBEEF);

    reader_signal_done(1);
    print_scenario_summary("场景1: 本端写-远端读");
}

/* ===================== 场景2：本端add-远端读 ===================== */

static void scenario2_writer(void)
{
    printf("\n[writer] 场景2: 本端add-远端读 (add + fence_release + fence_acquire)\n");
    reset_control_flags();

    ub_dist_tx_res_set(&g_layout->counter, 0);
    const int ADD_COUNT = 5;
    const uint64_t ADD_VAL = 100;

    for (int i = 0; i < ADD_COUNT; i++) {
        ub_dist_tx_res_add(&g_layout->counter, ADD_VAL);
        uint64_t cur = 0;
        ub_dist_tx_res_get(&g_layout->counter, &cur);
        printf("  [writer] add(%" PRIu64 ") 第%d次, counter=%" PRIu64 "\n",
               ADD_VAL, i + 1, cur);
    }

    writer_signal_ready(2);
    printf("  [writer] reader 已确认读取完成\n");
}

static void scenario2_reader(void)
{
    printf("\n[reader] 场景2: 本端add-远端读 (add + fence_release + fence_acquire)\n");
    printf("  [reader] 等待 writer 就绪...\n");

    reader_wait_ready(2);

    uint64_t val = 0;
    ub_dist_tx_res_get(&g_layout->counter, &val);
    printf("  [reader] 读到 counter = %" PRIu64 "\n", val);
    CHECK_EQ("reader 读到 counter == 500", val, 500);

    reader_signal_done(2);
    print_scenario_summary("场景2: 本端add-远端读");
}

/* ===================== 场景3：全局序验证 ===================== */

static void scenario3_writer(void)
{
    printf("\n[writer] 场景3: 全局序验证 (fence_seq_cst 双向)\n");
    reset_control_flags();

    const int ROUNDS = 100;
    int violation_count = 0;

    for (int r = 0; r < ROUNDS; r++) {
        // 重置 x, y
        ub_dist_tx_res_set(&g_layout->x, 0);
        ub_dist_tx_res_set(&g_layout->y, 0);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);

        // 通知 reader 新一轮开始：ready_flag = 1000 + r
        uint64_t round_signal = 1000 + (uint64_t)r;
        ub_dist_tx_res_set(&g_layout->ready_flag, round_signal);

        // writer: set(x, 1) → fence_seq_cst → get(y)
        ub_dist_tx_res_set(&g_layout->x, 1);
        ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
        uint64_t y_read = 0;
        ub_dist_tx_res_get(&g_layout->y, &y_read);

        // 等 reader 完成本轮
        uint64_t d = 0;
        while (d != round_signal) {
            ub_dist_tx_res_get(&g_layout->done_flag, &d);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        // reader 把它的读取结果存在 result 中
        uint64_t x_read = 0;
        ub_dist_tx_res_get(&g_layout->result, &x_read);

        if (y_read == 0 && x_read == 0) {
            violation_count++;
        }
    }

    printf("  [writer] %d 轮完成, 违反全局序次数: %d\n", ROUNDS, violation_count);
    // 把违反次数写入 result 供 reader 端打印
    ub_dist_tx_res_set(&g_layout->result, (uint64_t)violation_count);
    // 发最终信号
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    ub_dist_tx_res_set(&g_layout->ready_flag, 9999);
    uint64_t d = 0;
    while (d != 9999) {
        ub_dist_tx_res_get(&g_layout->done_flag, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

static void scenario3_reader(void)
{
    printf("\n[reader] 场景3: 全局序验证 (fence_seq_cst 双向)\n");
    printf("  [reader] 等待 writer 开始...\n");

    const int ROUNDS = 100;
    uint64_t last_round = 0;

    for (int r = 0; r < ROUNDS; r++) {
        uint64_t round_signal = 1000 + (uint64_t)r;

        // 等待本轮开始
        uint64_t rf = 0;
        while (rf != round_signal) {
            ub_dist_tx_res_get(&g_layout->ready_flag, &rf);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        // reader: set(y, 1) → fence_seq_cst → get(x)
        ub_dist_tx_res_set(&g_layout->y, 1);
        ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
        uint64_t x_read = 0;
        ub_dist_tx_res_get(&g_layout->x, &x_read);

        // 把读取结果写入 result
        ub_dist_tx_res_set(&g_layout->result, x_read);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);
        ub_dist_tx_res_set(&g_layout->done_flag, round_signal);
    }

    // 等待最终信号
    uint64_t rf = 0;
    while (rf != 9999) {
        ub_dist_tx_res_get(&g_layout->ready_flag, &rf);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
    uint64_t violations = 0;
    ub_dist_tx_res_get(&g_layout->result, &violations);
    printf("  [reader] %d 轮完成, 全局序违反次数: %" PRIu64 "\n", ROUNDS, violations);
    CHECK_EQ("seq_cst 全局序违反次数 == 0", violations, 0);
    print_scenario_summary("场景3: 全局序验证");

    ub_dist_tx_res_set(&g_layout->done_flag, 9999);
}

/* ===================== 场景4：双向同步 ===================== */

static void scenario4_writer(void)
{
    printf("\n[writer] 场景4: 双向同步 (fence_acq_rel + payload验证)\n");
    reset_control_flags();

    const uint64_t P0 = 0xAAAA;
    const uint64_t P1 = 0xBBBB;

    ub_dist_tx_res_set(&g_layout->payload[0], P0);
    ub_dist_tx_res_set(&g_layout->payload[1], P1);
    printf("  [writer] 写入 payload[0]=0x%" PRIx64 ", payload[1]=0x%" PRIx64 "\n", P0, P1);

    // acq_rel fence: writer 端用 acq_rel 而不是 release
    ub_dist_tx_res_fence(UB_FENCE_ACQ_REL);
    ub_dist_tx_res_set(&g_layout->ready_flag, 4);

    // 等待 reader 完成
    uint64_t d = 0;
    while (d != 4) {
        ub_dist_tx_res_get(&g_layout->done_flag, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    printf("  [writer] reader 已确认读取完成\n");
}

static void scenario4_reader(void)
{
    printf("\n[reader] 场景4: 双向同步 (fence_acq_rel + payload验证)\n");
    printf("  [reader] 等待 writer 就绪...\n");

    uint64_t r = 0;
    while (r != 4) {
        ub_dist_tx_res_get(&g_layout->ready_flag, &r);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_fence(UB_FENCE_ACQ_REL);

    uint64_t p0 = 0, p1 = 0;
    ub_dist_tx_res_get(&g_layout->payload[0], &p0);
    ub_dist_tx_res_get(&g_layout->payload[1], &p1);
    printf("  [reader] 读到 payload[0]=0x%" PRIx64 ", payload[1]=0x%" PRIx64 "\n", p0, p1);

    CHECK_EQ("reader 读到 payload[0] == 0xAAAA", p0, 0xAAAA);
    CHECK_EQ("reader 读到 payload[1] == 0xBBBB", p1, 0xBBBB);

    reader_signal_done(4);
    print_scenario_summary("场景4: 双向同步");
}

/* ===================== 场景5：并发fetch_add ===================== */

static void scenario5_writer(void)
{
    printf("\n[writer] 场景5: 并发fetch_add竞争 (双端各执行N次)\n");
    reset_control_flags();

    ub_dist_tx_res_set(&g_layout->fetch_counter, 0);
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);

    // 通知 reader 开始并发
    ub_dist_tx_res_set(&g_layout->ready_flag, 5);

    const int OPS = 10000;
    printf("  [writer] 开始执行 %d 次 fetch_add(1)...\n", OPS);

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < OPS; i++) {
        uint64_t old = 0;
        ub_dist_tx_res_fetch_add(&g_layout->fetch_counter, 1, &old);
    }
    auto end = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    printf("  [writer] %d 次 fetch_add 完成, 耗时 %.2f ms\n", OPS, elapsed_ms);

    // 等待 reader 也完成
    uint64_t d = 0;
    while (d != 5) {
        ub_dist_tx_res_get(&g_layout->done_flag, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // reader 完成后做 fence_acquire 再读最终值
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
    uint64_t final_val = 0;
    ub_dist_tx_res_get(&g_layout->fetch_counter, &final_val);
    printf("  [writer] 最终 fetch_counter = %" PRIu64 " (期望 %d)\n",
           final_val, OPS * 2);
}

static void scenario5_reader(void)
{
    printf("\n[reader] 场景5: 并发fetch_add竞争 (双端各执行N次)\n");
    printf("  [reader] 等待 writer 就绪...\n");

    uint64_t r = 0;
    while (r != 5) {
        ub_dist_tx_res_get(&g_layout->ready_flag, &r);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    const int OPS = 10000;
    printf("  [reader] 开始执行 %d 次 fetch_add(1)...\n", OPS);

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < OPS; i++) {
        uint64_t old = 0;
        ub_dist_tx_res_fetch_add(&g_layout->fetch_counter, 1, &old);
    }
    auto end = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    printf("  [reader] %d 次 fetch_add 完成, 耗时 %.2f ms\n", OPS, elapsed_ms);

    // fence_release 确保 reader 的所有写对 writer 可见
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    ub_dist_tx_res_set(&g_layout->done_flag, 5);

    // 也读一下最终值
    ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
    uint64_t final_val = 0;
    ub_dist_tx_res_get(&g_layout->fetch_counter, &final_val);
    printf("  [reader] 最终 fetch_counter = %" PRIu64 " (期望 %d)\n",
           final_val, OPS * 2);
    CHECK_EQ("并发 fetch_add 最终值 == 20000", final_val, (uint64_t)(OPS * 2));
    print_scenario_summary("场景5: 并发fetch_add竞争");
}

/* ===================== 场景6：多轮连续fence可见性压测 ===================== */

static void scenario6_writer(void)
{
    printf("\n[writer] 场景6: 多轮连续fence可见性压测\n");
    reset_control_flags();

    const int ROUNDS = 50;
    printf("  [writer] 开始 %d 轮压测...\n", ROUNDS);

    for (int r = 0; r < ROUNDS; r++) {
        uint64_t round_signal = 2000 + (uint64_t)r;
        uint64_t magic = (uint64_t)r * 1000 + 42;

        // 重置 data 和 ready
        ub_dist_tx_res_set(&g_layout->data, 0);
        ub_dist_tx_res_set(&g_layout->ready_flag, 0);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);

        // 写入本轮 magic
        ub_dist_tx_res_set(&g_layout->data, magic);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);
        ub_dist_tx_res_set(&g_layout->ready_flag, round_signal);

        // 等 reader 确认
        uint64_t d = 0;
        while (d != round_signal) {
            ub_dist_tx_res_get(&g_layout->done_flag, &d);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        // 检查 reader 回写的结果
        uint64_t reader_val = 0;
        ub_dist_tx_res_get(&g_layout->result, &reader_val);
        if (reader_val != magic) {
            printf("  [writer] 轮次 %d FAIL: reader 读到 0x%" PRIx64 ", 期望 0x%" PRIx64 "\n",
                   r, reader_val, magic);
        }
    }

    printf("  [writer] %d 轮压测完成\n", ROUNDS);
    // 发结束信号
    ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    ub_dist_tx_res_set(&g_layout->ready_flag, 8888);
    uint64_t d = 0;
    while (d != 8888) {
        ub_dist_tx_res_get(&g_layout->done_flag, &d);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

static void scenario6_reader(void)
{
    printf("\n[reader] 场景6: 多轮连续fence可见性压测\n");
    printf("  [reader] 等待 writer 开始...\n");

    const int ROUNDS = 50;
    int fail_rounds = 0;

    for (int r = 0; r < ROUNDS; r++) {
        uint64_t round_signal = 2000 + (uint64_t)r;
        uint64_t expected_magic = (uint64_t)r * 1000 + 42;

        // 等待本轮开始
        uint64_t rf = 0;
        while (rf != round_signal) {
            ub_dist_tx_res_get(&g_layout->ready_flag, &rf);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);

        uint64_t val = 0;
        ub_dist_tx_res_get(&g_layout->data, &val);

        if (val != expected_magic) {
            fail_rounds++;
            printf("  [reader] 轮次 %d FAIL: 读到 0x%" PRIx64 ", 期望 0x%" PRIx64 "\n",
                   r, val, expected_magic);
        }

        // 回写结果
        ub_dist_tx_res_set(&g_layout->result, val);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);
        ub_dist_tx_res_set(&g_layout->done_flag, round_signal);
    }

    printf("  [reader] %d 轮完成, 失败轮数: %d\n", ROUNDS, fail_rounds);
    CHECK_EQ("fence 压测失败轮数 == 0", (uint64_t)fail_rounds, 0);
    print_scenario_summary("场景6: 多轮连续fence可见性压测");

    // 等待结束信号
    uint64_t rf = 0;
    while (rf != 8888) {
        ub_dist_tx_res_get(&g_layout->ready_flag, &rf);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ub_dist_tx_res_set(&g_layout->done_flag, 8888);
}

/* ===================== 场景调度 ===================== */

typedef void (*ScenarioFunc)(void);

static void run_scenario(int id)
{
    ScenarioFunc writer_fn = nullptr;
    ScenarioFunc reader_fn = nullptr;

    switch (id) {
        case 1: writer_fn = scenario1_writer; reader_fn = scenario1_reader; break;
        case 2: writer_fn = scenario2_writer; reader_fn = scenario2_reader; break;
        case 3: writer_fn = scenario3_writer; reader_fn = scenario3_reader; break;
        case 4: writer_fn = scenario4_writer; reader_fn = scenario4_reader; break;
        case 5: writer_fn = scenario5_writer; reader_fn = scenario5_reader; break;
        case 6: writer_fn = scenario6_writer; reader_fn = scenario6_reader; break;
        default:
            printf("未知场景: %d\n", id);
            return;
    }

    if (g_is_writer) {
        writer_fn();
    } else {
        reader_fn();
    }
}

static void run_all_scenarios(void)
{
    for (int i = 1; i <= 6; i++) {
        run_scenario(i);
        // 场景间短暂间隔
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

/* ===================== 交互菜单 ===================== */

static void print_menu(void)
{
    printf("\n" HEADER_SEP "\n");
    printf("  ub_dist_tx_res 跨节点 NC 环境验证\n");
    printf(HEADER_SEP "\n");
    printf("  角色: %s (%s)\n",
           g_is_writer ? "writer" : "reader",
           g_is_writer ? "本端CC" : "远端NC");
    printf(HEADER_SEP "\n");
    printf("  1. 本端写-远端读 (set + fence_release + fence_acquire)\n");
    printf("  2. 本端add-远端读 (add + fence_release + fence_acquire)\n");
    printf("  3. 全局序验证 (fence_seq_cst 双向)\n");
    printf("  4. 双向同步 (fence_acq_rel + payload验证)\n");
    printf("  5. 并发fetch_add竞争 (双端各执行N次)\n");
    printf("  6. 多轮连续fence可见性压测\n");
    printf("  7. 运行全部场景\n");
    printf("  0. 退出\n");
    printf(HEADER_SEP "\n");
    printf("请输入场景编号: ");
}

/* ===================== 命令行参数解析 ===================== */

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s --role <writer|reader> [--config <path>]\n"
            "\n"
            "  --role writer|reader  角色: writer=本端CC, reader=远端NC (必需)\n"
            "  --config <path>       配置文件路径 (默认: tx_res_nc.conf)\n"
            "\n"
            "示例:\n"
            "  # NodeA 上运行 writer:\n"
            "  %s --role writer --config tx_res_nc.conf\n"
            "  # NodeB 上运行 reader:\n"
            "  %s --role reader --config tx_res_nc.conf\n",
            prog, prog, prog);
}

static struct option long_options[] = {
    {"role",   required_argument, nullptr, 'r'},
    {"config", required_argument, nullptr, 'c'},
    {"help",   no_argument,       nullptr, 'h'},
    {nullptr,  0,                 nullptr,  0 }
};

/* ===================== 主函数 ===================== */

int main(int argc, char *argv[])
{
    std::string role_str;
    std::string config_path = "tx_res_nc.conf";

    int opt;
    while ((opt = getopt_long(argc, argv, "r:c:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'r':
                role_str = optarg;
                break;
            case 'c':
                config_path = optarg;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (role_str.empty()) {
        fprintf(stderr, "[Error] --role 参数必需\n");
        print_usage(argv[0]);
        return 1;
    }

    if (role_str == "writer") {
        g_is_writer = true;
    } else if (role_str == "reader") {
        g_is_writer = false;
    } else {
        fprintf(stderr, "[Error] --role 必须是 writer 或 reader, 当前: %s\n", role_str.c_str());
        return 1;
    }

    // 注册日志
    ub_atomic_register_log_func(my_stdout_logger);
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);

    // 解析配置
    NcConfig cfg;
    cfg.shm_name = SHM_NAME_DEFAULT;
    std::string err;
    if (!load_nc_config(config_path, cfg, err)) {
        fprintf(stderr, "[Warn] 加载配置文件失败: %s, 使用默认值\n", err.c_str());
        cfg.shm_name = SHM_NAME_DEFAULT;
        cfg.shm_size_mb = 64;
    }
    g_shm_size = cfg.shm_size_mb * 1024UL * 1024UL;

    printf(HEADER_SEP "\n");
    printf("  ub_dist_tx_res 跨节点 NC 环境验证\n");
    printf("  角色: %s (%s)\n", g_is_writer ? "writer" : "reader",
           g_is_writer ? "本端CC/export端" : "远端NC/import端");
    printf("  节点: %s\n", cfg.self.c_str());
    printf("  共享内存: %s (%zu MB)\n", cfg.shm_name.c_str(), cfg.shm_size_mb);
    printf(HEADER_SEP "\n\n");

    // 初始化 ubsmem 并映射共享内存
    if (init_ubsmem_shm(cfg.shm_name.c_str(), g_shm_size, &g_shm_base) != 0) {
        fprintf(stderr, "[Error] 共享内存初始化失败\n");
        return 1;
    }

    // 对齐基地址到 8 字节
    uintptr_t base_addr = reinterpret_cast<uintptr_t>(g_shm_base);
    if (base_addr % alignof(uint64_t) != 0) {
        base_addr += alignof(uint64_t) - (base_addr % alignof(uint64_t));
        printf("[Info] 基地址对齐调整: %p -> 0x%" PRIxPTR "\n", g_shm_base, base_addr);
    }

    g_layout = reinterpret_cast<NcTestLayout *>(base_addr);
    printf("[Info] NcTestLayout 地址: %p, 大小: %zu\n",
           g_layout, sizeof(NcTestLayout));

    // 初始化 tx_res 对象
    ub_dist_tx_res_init(&g_layout->ready_flag);
    ub_dist_tx_res_init(&g_layout->done_flag);
    ub_dist_tx_res_init(&g_layout->scenario_id);
    ub_dist_tx_res_init(&g_layout->data);
    ub_dist_tx_res_init(&g_layout->counter);
    ub_dist_tx_res_init(&g_layout->x);
    ub_dist_tx_res_init(&g_layout->y);
    ub_dist_tx_res_init(&g_layout->payload[0]);
    ub_dist_tx_res_init(&g_layout->payload[1]);
    ub_dist_tx_res_init(&g_layout->fetch_counter);
    ub_dist_tx_res_init(&g_layout->result);

    printf("[Info] tx_res 对象初始化完成\n\n");

    // 交互菜单
    int choice = -1;
    while (true) {
        print_menu();
        if (scanf("%d", &choice) != 1) {
            int c;
            while ((c = getchar()) != '\n' && c != EOF)
                ;
            printf("输入无效，请输入数字。\n");
            continue;
        }

        switch (choice) {
            case 1: case 2: case 3: case 4: case 5: case 6:
                run_scenario(choice);
                break;
            case 7:
                run_all_scenarios();
                break;
            case 0:
                printf("退出 NC 验证 Demo，再见。\n");
                goto cleanup;
            default:
                printf("未知选项: %d，请重新输入。\n", choice);
                break;
        }
    }

cleanup:
    // 卸载共享内存
    if (g_shm_base) {
        ubsmem_shmem_unmap(g_shm_base, g_shm_size);
        printf("[Info] 共享内存已卸载\n");
    }
    ubsmem_finalize();
    return 0;
}
