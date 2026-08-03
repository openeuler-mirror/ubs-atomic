/*
 * ub_dist_tx_res_if_test.cpp - 接口功能测试（单节点）
 *
 * 对应用例修复版.md 中的用例 1~4：
 *   TC-ADD-IF   : add 接口测试（含回绕验证）
 *   TC-XOR-IF   : xor 接口测试
 *   TC-CAS-IF   : cas 接口测试
 *   TC-FENCE-IF : fence 接口测试
 *
 * 说明：
 *   - 不包含前置步骤 P1/P2（组网、共享内存创建），仅实现测试步骤与预期校验
 *   - 通过 --shm <name> 指定共享内存名，参考 nc_test 的 ubsmem 映射流程
 *   - 支持 --case <id> 单用例执行，不指定则批量执行全部
 *
 * 编译：
 *   g++ -O2 -g -std=c++17 -o ub_dist_tx_res_if_test ub_dist_tx_res_if_test.cpp \
 *       -I../../include \
 *       -I/usr/local/ubs_mem/include \
 *       -L../../build/lib -lubs-atomic \
 *       -L/usr/local/ubs_mem/lib -lubsm_sdk \
 *       -lpthread \
 *       -Wl,-rpath,/usr/local/ubs_mem/lib \
 *       -Wl,-rpath,'$ORIGIN/../../build/lib'
 *
 * 运行：
 *   # 执行全部用例
 *   ./ub_dist_tx_res_if_test --shm shm_tx_res_if
 *   # 仅执行 TC-ADD-IF
 *   ./ub_dist_tx_res_if_test --shm shm_tx_res_if --case TC-ADD-IF
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cinttypes>
#include <ctime>
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
static const char *SHM_NAME_DEFAULT = "shm_tx_res_if";

/* 单节点用例共享内存布局：
 *   offset 0  : target (8B, 8字节对齐) —— 接口操作目标
 *   offset 8  : aux    (8B)            —— 辅助字段（如未对齐测试基准）
 *   offset 16 : reserved
 */
struct IfTestLayout {
    uint64_t target;
    uint64_t aux;
};

/* ===================== 全局 ===================== */

static void *g_shm_base = nullptr;
static size_t g_shm_size = SHM_SIZE_DEFAULT;
static IfTestLayout *g_layout = nullptr;

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
            printf("  [PASS] %-50s (值=0x%" PRIx64 ")\n", (desc), _a);        \
            g_pass_cnt++;                                                    \
        } else {                                                             \
            printf("  [FAIL] %-50s (实际=0x%" PRIx64 ", 期望=0x%" PRIx64 ")\n",\
                   (desc), _a, _e);                                          \
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
/* 批跑时每个用例开始前重置共享内存数据字段，避免用例间相互污染。
 * 注意：target/aux 重置为 0，避免上一用例残留值影响当前用例的 S1 INIT 后状态。 */
static void case_cleanup(void)
{
    ub_dist_tx_res_set(&g_layout->target, 0);
    ub_dist_tx_res_set(&g_layout->aux, 0);
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

/* ===================== 用例 TC-ADD-IF : add 接口测试 ===================== */
/* 对应用例修复版 用例1，已按当前实现修订回绕预期 */

static void case_tc_add_if(void)
{
    case_begin("TC-ADD-IF : add 接口测试（含回绕）");

    uint64_t *tid = &g_layout->target;
    uint64_t out = 0;
    int ret = 0;

    /* S1: INIT 后 GET，预期 start_val=0 */
    ret = ub_dist_tx_res_init(tid);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S1 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S1 *out_value == 0 (E1)", out, 0);

    /* S2: GET 检查初值 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S2 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S2 *out_value == 0 (E2)", out, 0);

    /* S3: add(2^64-2) */
    ret = ub_dist_tx_res_add(tid, (uint64_t)(UINT64_MAX - 1));
    CHECK_RES("S3 add(2^64-2) 返回 OK (E3)", ret, UB_RES_OK);

    /* S4: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S4 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S4 *out_value == 2^64-2 (E4)", out, (uint64_t)(UINT64_MAX - 1));

    /* S5: add(1) -> 2^64-1 */
    ret = ub_dist_tx_res_add(tid, 1);
    CHECK_RES("S5 add(1) 返回 OK (E5)", ret, UB_RES_OK);

    /* S6: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S6 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S6 *out_value == 2^64-1 (E6)", out, UINT64_MAX);

    /* S7: add(1) -> 回绕为 0（无符号模 2^64 回绕，实现无溢出检测） */
    ret = ub_dist_tx_res_add(tid, 1);
    CHECK_RES("S7 add(1) 回绕 返回 OK (E7)", ret, UB_RES_OK);

    /* S8: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S8 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S8 *out_value == 0 (回绕, E8)", out, 0);

    /* S9: set(0) */
    ret = ub_dist_tx_res_set(tid, 0);
    CHECK_RES("S9 set(0) 返回 OK (E9)", ret, UB_RES_OK);

    /* S10: add(0) */
    ret = ub_dist_tx_res_add(tid, 0);
    CHECK_RES("S10 add(0) 返回 OK (E10)", ret, UB_RES_OK);

    /* S11: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S11 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S11 *out_value == 0 (E11)", out, 0);

    /* S12: 未对齐地址 add(1) —— 基地址 +2 字节 */
    {
        uintptr_t base = reinterpret_cast<uintptr_t>(&g_layout->aux);
        uint64_t *misaligned = reinterpret_cast<uint64_t *>(base + 2);
        ret = ub_dist_tx_res_add(misaligned, 1);
        CHECK_RES("S12 未对齐 add(1) 返回 ERROR (E12)", ret, UB_RES_ERROR);
    }

    /* S13: add(-1) -> 隐式转 uint64_t = 2^64-1，0 + (2^64-1) 回绕为 2^64-1 */
    ret = ub_dist_tx_res_add(tid, (uint64_t)(-1));
    CHECK_RES("S13 add(-1) 即 add(2^64-1) 返回 OK (E13)", ret, UB_RES_OK);

    /* S14: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S14 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S14 *out_value == 2^64-1 (回绕后值, E14)", out, UINT64_MAX);

    /* S15: set(0) */
    ret = ub_dist_tx_res_set(tid, 0);
    CHECK_RES("S15 set(0) 返回 OK (E15)", ret, UB_RES_OK);

    /* S16: add(1) */
    ret = ub_dist_tx_res_add(tid, 1);
    CHECK_RES("S16 add(1) 返回 OK (E16)", ret, UB_RES_OK);

    /* S17: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S17 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S17 *out_value == 1 (E17)", out, 1);

    /* S18: tid=NULL add(1) */
    ret = ub_dist_tx_res_add(nullptr, 1);
    CHECK_RES("S18 NULL tid add(1) 返回 ERROR (E18)", ret, UB_RES_ERROR);

    /* S19: GET 验证值未变 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S19 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S19 *out_value == 1 (E19)", out, 1);

    case_end("TC-ADD-IF");
}

/* ===================== 用例 TC-XOR-IF : xor 接口测试 ===================== */

static void case_tc_xor_if(void)
{
    case_begin("TC-XOR-IF : xor 接口测试");

    uint64_t *tid = &g_layout->target;
    uint64_t out = 0;
    int ret = 0;

    /* S1: INIT + GET */
    ret = ub_dist_tx_res_init(tid);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S1 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S1 start_val == 0 (E1)", out, 0);

    /* S2: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S2 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S2 *out_value == 0 (E2)", out, 0);

    /* S3: xor(2^64-1)，返回旧值 0 */
    ret = ub_dist_tx_res_fetch_xor(tid, UINT64_MAX, &out);
    CHECK_RES("S3 xor(2^64-1) 返回 OK (E3)", ret, UB_RES_OK);
    CHECK_VAL("S3 *out_value(旧值) == 0 (E3)", out, 0);

    /* S4: GET，应得 2^64-1 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S4 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S4 *out_value == 2^64-1 (E4)", out, UINT64_MAX);

    /* S5: xor(2^64-1)，返回旧值 2^64-1 */
    ret = ub_dist_tx_res_fetch_xor(tid, UINT64_MAX, &out);
    CHECK_RES("S5 xor(2^64-1) 返回 OK (E5)", ret, UB_RES_OK);
    CHECK_VAL("S5 *out_value(旧值) == 2^64-1 (E5)", out, UINT64_MAX);

    /* S6: GET，应得 0 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S6 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S6 *out_value == 0 (E6)", out, 0);

    /* S7: 未对齐地址 xor(2^64-1) */
    {
        uintptr_t base = reinterpret_cast<uintptr_t>(&g_layout->aux);
        uint64_t *misaligned = reinterpret_cast<uint64_t *>(base + 2);
        ret = ub_dist_tx_res_fetch_xor(misaligned, UINT64_MAX, &out);
        CHECK_RES("S7 未对齐 xor 返回 ERROR (E7)", ret, UB_RES_ERROR);
    }

    /* S8: GET 验证未变 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S8 GET 返回 OK (E8)", ret, UB_RES_OK);
    CHECK_VAL("S8 *out_value == 0 (E8)", out, 0);

    /* S9: tid=NULL xor */
    ret = ub_dist_tx_res_fetch_xor(nullptr, UINT64_MAX, &out);
    CHECK_RES("S9 NULL tid xor 返回 ERROR (E9)", ret, UB_RES_ERROR);

    /* S10: GET 验证未变 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S10 GET 返回 OK (E10)", ret, UB_RES_OK);
    CHECK_VAL("S10 *out_value == 0 (E10)", out, 0);

    /* S11: xor(-1) -> 即 xor(2^64-1)，旧值 0 */
    ret = ub_dist_tx_res_fetch_xor(tid, (uint64_t)(-1), &out);
    CHECK_RES("S11 xor(-1)=xor(2^64-1) 返回 OK (E11)", ret, UB_RES_OK);
    CHECK_VAL("S11 *out_value(旧值) == 0 (E11)", out, 0);

    /* S12: GET 应得 2^64-1 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S12 GET 返回 OK (E12)", ret, UB_RES_OK);
    CHECK_VAL("S12 *out_value == 2^64-1 (E12)", out, UINT64_MAX);

    /* S13: xor(0)，旧值 2^64-1 */
    ret = ub_dist_tx_res_fetch_xor(tid, 0, &out);
    CHECK_RES("S13 xor(0) 返回 OK (E13)", ret, UB_RES_OK);
    CHECK_VAL("S13 *out_value(旧值) == 2^64-1 (E13)", out, UINT64_MAX);

    /* S14: GET 应仍为 2^64-1 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S14 GET 返回 OK (E14)", ret, UB_RES_OK);
    CHECK_VAL("S14 *out_value == 2^64-1 (E14)", out, UINT64_MAX);

    case_end("TC-XOR-IF");
}

/* ===================== 用例 TC-CAS-IF : cas 接口测试 ===================== */

static void case_tc_cas_if(void)
{
    case_begin("TC-CAS-IF : cas 接口测试");

    uint64_t *tid = &g_layout->target;
    uint64_t out = 0;
    uint64_t expected = 0;
    int success = 0;
    int ret = 0;

    /* S1: INIT + GET */
    ret = ub_dist_tx_res_init(tid);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S1 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S1 start_val == 0 (E1)", out, 0);

    /* S2: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S2 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S2 *out_value == 0 (E2)", out, 0);

    /* S3: CAS(*expected=0, desired=-1=2^64-1)，应成功 */
    expected = 0;
    success = 0;
    ret = ub_dist_tx_res_compare_exchange(tid, &expected, (uint64_t)(-1), &success);
    CHECK_RES("S3 CAS(*exp=0, desired=-1) 返回 OK (E3)", ret, UB_RES_OK);
    CHECK_VAL("S3 *expect == 0 (E3)", expected, 0);
    CHECK_VAL("S3 *success == 1 (E3)", (uint64_t)success, 1);

    /* S4: GET 应为 2^64-1 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S4 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S4 *out_value == 2^64-1 (E4)", out, UINT64_MAX);

    /* S5: CAS(*expected=2^64-1, desired=0)，应成功 */
    expected = UINT64_MAX;
    success = 0;
    ret = ub_dist_tx_res_compare_exchange(tid, &expected, 0, &success);
    CHECK_RES("S5 CAS(*exp=2^64-1, desired=0) 返回 OK (E5)", ret, UB_RES_OK);
    CHECK_VAL("S5 *expect == 2^64-1 (E5)", expected, UINT64_MAX);
    CHECK_VAL("S5 *success == 1 (E5)", (uint64_t)success, 1);

    /* S6: GET 应为 0 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S6 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S6 *out_value == 0 (E6)", out, 0);

    /* S7: CAS(*expected=0, desired=2^64-1)，应成功 */
    expected = 0;
    success = 0;
    ret = ub_dist_tx_res_compare_exchange(tid, &expected, UINT64_MAX, &success);
    CHECK_RES("S7 CAS(*exp=0, desired=2^64-1) 返回 OK (E7)", ret, UB_RES_OK);
    CHECK_VAL("S7 *expect == 0 (E7)", expected, 0);
    CHECK_VAL("S7 *success == 1 (E7)", (uint64_t)success, 1);

    /* S8: GET 应为 2^64-1 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S8 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S8 *out_value == 2^64-1 (E8)", out, UINT64_MAX);

    /* S9: CAS(*expected=0, desired=0)，应失败；*expect 输出当前值 2^64-1 */
    expected = 0;
    success = 1; // 故意初值非0
    ret = ub_dist_tx_res_compare_exchange(tid, &expected, 0, &success);
    CHECK_RES("S9 CAS(*exp=0, desired=0) 返回 OK (E9)", ret, UB_RES_OK);
    CHECK_VAL("S9 *expect == 2^64-1 (E9)", expected, UINT64_MAX);
    CHECK_VAL("S9 *success == 0 (E9)", (uint64_t)success, 0);

    /* S10: GET 应仍为 2^64-1 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S10 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S10 *out_value == 2^64-1 (E10)", out, UINT64_MAX);

    /* S11: expected=NULL，应返回 ERROR */
    ret = ub_dist_tx_res_compare_exchange(tid, nullptr, 0, &success);
    CHECK_RES("S11 expected=NULL 返回 ERROR (E11)", ret, UB_RES_ERROR);

    /* S12: GET 验证未变 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S12 GET 返回 OK (E12)", ret, UB_RES_OK);
    CHECK_VAL("S12 *out_value == 2^64-1 (E12)", out, UINT64_MAX);

    /* S13: 未对齐地址 CAS */
    {
        uintptr_t base = reinterpret_cast<uintptr_t>(&g_layout->aux);
        uint64_t *misaligned = reinterpret_cast<uint64_t *>(base + 2);
        expected = 0;
        success = 0;
        ret = ub_dist_tx_res_compare_exchange(misaligned, &expected, 1, &success);
        CHECK_RES("S13 未对齐 CAS 返回 ERROR (E13)", ret, UB_RES_ERROR);
    }

    /* S14: GET 验证未变 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S14 GET 返回 OK (E14)", ret, UB_RES_OK);
    CHECK_VAL("S14 *out_value == 2^64-1 (E14)", out, UINT64_MAX);

    /* S15: tid=NULL CAS，应返回 ERROR */
    expected = 0;
    success = 0;
    ret = ub_dist_tx_res_compare_exchange(nullptr, &expected, 1, &success);
    CHECK_RES("S15 tid=NULL CAS 返回 ERROR (E15)", ret, UB_RES_ERROR);

    /* S16: GET 验证未变 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S16 GET 返回 OK (E16)", ret, UB_RES_OK);
    CHECK_VAL("S16 *out_value == 2^64-1 (E16)", out, UINT64_MAX);

    /* S17: CAS(*expected=2^64-1, desired=0)，应成功 */
    expected = UINT64_MAX;
    success = 0;
    ret = ub_dist_tx_res_compare_exchange(tid, &expected, 0, &success);
    CHECK_RES("S17 CAS(*exp=2^64-1, desired=0) 返回 OK (E17)", ret, UB_RES_OK);
    CHECK_VAL("S17 *expect == 2^64-1 (E17)", expected, UINT64_MAX);
    CHECK_VAL("S17 *success == 1 (E17)", (uint64_t)success, 1);

    /* S18: GET 应为 0 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S18 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S18 *out_value == 0 (E18)", out, 0);

    /* S19: success=NULL，应返回 ERROR */
    expected = UINT64_MAX;
    ret = ub_dist_tx_res_compare_exchange(tid, &expected, 0, nullptr);
    CHECK_RES("S19 success=NULL 返回 ERROR (E19)", ret, UB_RES_ERROR);

    /* S20: GET 应仍为 0 */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S20 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S20 *out_value == 0 (E20)", out, 0);

    case_end("TC-CAS-IF");
}

/* ===================== 用例 TC-FENCE-IF : fence 接口测试 ===================== */

static void case_tc_fence_if(void)
{
    case_begin("TC-FENCE-IF : fence 接口测试");
    case_cleanup();

    uint64_t *tid = &g_layout->target;
    uint64_t out = 0;
    int ret = 0;

    /* S1: INIT + GET */
    ret = ub_dist_tx_res_init(tid);
    CHECK_RES("S1 INIT 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S1 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S1 start_val == 0 (E1)", out, 0);

    /* S2: GET */
    ret = ub_dist_tx_res_get(tid, &out);
    CHECK_RES("S2 GET 返回 OK", ret, UB_RES_OK);
    CHECK_VAL("S2 *out_value == 0 (E2)", out, 0);

    /* S3: fence(5) 非法入参，应返回 ERROR */
    ret = ub_dist_tx_res_fence((ub_fence_order_t)5);
    CHECK_RES("S3 fence(5) 非法入参 返回 ERROR (E3)", ret, UB_RES_ERROR);

    /* S4: fence(0=UB_FENCE_RELAXED) 正常分支，应返回 OK */
    ret = ub_dist_tx_res_fence(UB_FENCE_RELAXED);
    CHECK_RES("S4 fence(0=RELAXED) 返回 OK (E4)", ret, UB_RES_OK);

    /* 顺带覆盖所有合法分支，确保正常路径可执行 */
    ret = ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
    CHECK_RES("S4 fence(ACQUIRE) 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_fence(UB_FENCE_RELEASE);
    CHECK_RES("S4 fence(RELEASE) 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_fence(UB_FENCE_ACQ_REL);
    CHECK_RES("S4 fence(ACQ_REL) 返回 OK", ret, UB_RES_OK);
    ret = ub_dist_tx_res_fence(UB_FENCE_SEQ_CST);
    CHECK_RES("S4 fence(SEQ_CST) 返回 OK", ret, UB_RES_OK);

    case_end("TC-FENCE-IF");
}

/* ===================== 用例调度 ===================== */

struct CaseEntry {
    const char *id;
    void (*fn)(void);
};

static CaseEntry g_cases[] = {
    {"TC-ADD-IF",   case_tc_add_if},
    {"TC-XOR-IF",   case_tc_xor_if},
    {"TC-CAS-IF",   case_tc_cas_if},
    {"TC-FENCE-IF", case_tc_fence_if},
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
            "Usage: %s --shm <name> [--shm-size <MB>] [--case <id>]\n"
            "\n"
            "  --shm <name>       共享内存名 (必需)\n"
            "  --shm-size <MB>    共享内存大小, 默认 %zu MB\n"
            "  --case <id>        单用例执行; 不指定则批量执行\n"
            "\n"
            "可用用例编号:\n",
            prog, SHM_SIZE_DEFAULT / (1024 * 1024));
    for (const auto &c : g_cases) {
        fprintf(stderr, "  %s\n", c.id);
    }
    fprintf(stderr, "\n示例:\n"
                    "  %s --shm shm_tx_res_if\n"
                    "  %s --shm shm_tx_res_if --case TC-ADD-IF\n",
            prog, prog);
}

static struct option long_options[] = {
    {"shm",      required_argument, nullptr, 's'},
    {"shm-size", required_argument, nullptr, 'z'},
    {"case",     required_argument, nullptr, 'c'},
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
    while ((opt = getopt_long(argc, argv, "s:z:c:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 's': shm_name = optarg; break;
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

    ub_atomic_register_log_func(my_stdout_logger);
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);

    g_shm_size = shm_size_mb * 1024UL * 1024UL;

    printf(HEADER_SEP "\n");
    printf("  ub_dist_tx_res 接口功能测试（单节点）\n");
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

    // 8 字节对齐
    uintptr_t base_addr = reinterpret_cast<uintptr_t>(g_shm_base);
    if (base_addr % alignof(uint64_t) != 0) {
        base_addr += alignof(uint64_t) - (base_addr % alignof(uint64_t));
        printf("[Info] 基地址对齐调整: %p -> 0x%" PRIxPTR "\n", g_shm_base, base_addr);
    }
    g_layout = reinterpret_cast<IfTestLayout *>(base_addr);
    printf("[Info] IfTestLayout 地址: %p, 大小: %zu\n",
           (void *)g_layout, sizeof(IfTestLayout));

    int total_pass = 0, total_fail = 0;
    int saved_pass = 0, saved_fail = 0;

    if (!case_id.empty()) {
        run_case(case_id);
        total_pass = g_pass_cnt;
        total_fail = g_fail_cnt;
    } else {
        for (const auto &c : g_cases) {
            saved_pass = g_pass_cnt; saved_fail = g_fail_cnt;
            c.fn();
            total_pass += (g_pass_cnt - saved_pass);
            total_fail += (g_fail_cnt - saved_fail);
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
