#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdalign.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "ub_dist_tx_res.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

// ========== Core Configuration ==========
#define SHM_NAME "ub_dist_tx_res_shm"
#define SHM_TOTAL_SIZE (128 * 1024 * 1024)
#define UINT64_ALIGN (alignof(uint64_t))

// 默认测试配置（可通过命令行参数调整）
#define DEFAULT_THREAD_NUM 4
#define DEFAULT_OP_COUNT 1000000
#define MAX_THREAD_NUM 600

// 操作比例定义（3:7）
#define FETCH_ADD_RATIO 30  // 30%概率执行fetch_add
#define GET_RATIO 70        // 70%概率执行get
#define RAND_MAX_VAL 100    // 随机数范围上限

// 全局变量
static void *g_shm_base_ptr = NULL;
static uint64_t *g_shm_atomic_ptr = NULL;
static ubsmem_options_t g_ubsm_opts;
static uint64_t op_count_per_thread = DEFAULT_OP_COUNT;
static int thread_num = DEFAULT_THREAD_NUM;

// 线程统计结果结构体（扩展统计项）
typedef struct {
    double thread_op_time;      // 线程内原子操作总耗时（秒）
    uint64_t thread_op_cnt;     // 线程内完成的原子操作总数
    uint64_t thread_fetchadd_cnt; // fetch_add操作次数
    uint64_t thread_get_cnt;      // get操作次数
} thread_stat_t;

static thread_stat_t *g_thread_stats = NULL;

/**
 * @brief 共享内存指针安全校验
 */
static int check_shm_ptr_safe()
{
    if (g_shm_base_ptr == NULL || g_shm_atomic_ptr == NULL) {
        return -1;
    }

    uintptr_t atomic_ptr = (uintptr_t)g_shm_atomic_ptr;
    if (atomic_ptr % UINT64_ALIGN != 0) {
        return -1;
    }

    uintptr_t max_safe_addr = (uintptr_t)g_shm_base_ptr + SHM_TOTAL_SIZE;
    uintptr_t target_addr = (uintptr_t)(g_shm_atomic_ptr + 2);
    if (target_addr >= max_safe_addr) {
        return -1;
    }

    return 0;
}

/**
 * @brief 映射128MB共享内存（单模式，不分主从）
 */
static int map_ubsmem_shm()
{
    int ret = UBSM_OK;
    void *shm_addr = NULL;

    // 初始化ubsmem
    ret = ubsmem_init_attributes(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        return -1;
    }
    ret = ubsmem_initialize(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        return -1;
    }

    // 直接创建并映射共享内存（无需区分主从）
    ret = ubsmem_shmem_map(NULL, SHM_TOTAL_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, (char *)SHM_NAME, 0,
                           &shm_addr);
    if (ret != 0) {
        ubsmem_finalize();
        return -1;
    }

    // 设置共享内存权限为0666，允许多进程访问
    int shm_fd = open("/dev/shm/ub_dist_tx_res_shm", O_RDWR);
    if (shm_fd >= 0) {
        fchmod(shm_fd, 0666);
        close(shm_fd);
    }

    // 8字节对齐处理
    uintptr_t shm_uintptr = (uintptr_t)shm_addr;
    if (shm_uintptr % UINT64_ALIGN != 0) {
        shm_uintptr += (UINT64_ALIGN - (shm_uintptr % UINT64_ALIGN));
    }

    // 范围校验
    uintptr_t max_shm_addr = (uintptr_t)shm_addr + SHM_TOTAL_SIZE;
    uintptr_t atomic_ptr_plus_2 = shm_uintptr + 2 * UINT64_ALIGN;
    if (atomic_ptr_plus_2 >= max_shm_addr) {
        ubsmem_shmem_unmap(shm_addr, SHM_TOTAL_SIZE);
        ubsmem_finalize();
        return -1;
    }

    // 赋值原子值指针并初始化
    g_shm_atomic_ptr = (uint64_t *)shm_uintptr;
    g_shm_base_ptr = shm_addr;

    int init_ret = ub_dist_tx_res_init(g_shm_atomic_ptr);
    if (init_ret != UB_RES_OK) {
        ubsmem_shmem_unmap(shm_addr, SHM_TOTAL_SIZE);
        ubsmem_finalize();
        return -1;
    }

    return 0;
}

/**
 * @brief 单个线程的原子操作工作函数（内部精准计时）
 */
static void *atomic_operation_worker(void *arg)
{
    int thread_idx = *(int *)arg;
    free(arg);

    uint64_t i;
    uint64_t ret_val;
    int ret;
    struct timespec start_ns, end_ns;

    // 初始化线程统计数据（扩展统计项）
    g_thread_stats[thread_idx].thread_op_time = 0.0;
    g_thread_stats[thread_idx].thread_op_cnt = 0;
    g_thread_stats[thread_idx].thread_fetchadd_cnt = 0;
    g_thread_stats[thread_idx].thread_get_cnt = 0;

    if (check_shm_ptr_safe() != 0) {
        return (void *)-1;
    }

    // 每个线程独立初始化随机数种子（避免线程间随机数重复）
    unsigned int seed = (unsigned int)(time(NULL) ^ pthread_self() ^ getpid());
    srand(seed);

    // ========== 按3:7比例随机执行fetch_add/get操作 ==========
    for (i = 0; i < op_count_per_thread; i++) {

        // 生成0-99的随机数，按比例选择操作
        int rand_val = rand() % RAND_MAX_VAL;
        if (rand_val < FETCH_ADD_RATIO) {
            // 30%概率执行fetch_add
            clock_gettime(CLOCK_MONOTONIC_RAW, &start_ns);
            ret = ub_dist_tx_res_fetch_add(g_shm_atomic_ptr, 1, &ret_val);
            clock_gettime(CLOCK_MONOTONIC_RAW, &end_ns);
            if (ret == UB_RES_OK) {
                g_thread_stats[thread_idx].thread_fetchadd_cnt++;
            }
        } else {
            // 70%概率执行get
            clock_gettime(CLOCK_MONOTONIC_RAW, &start_ns);
            ret = ub_dist_tx_res_get(g_shm_atomic_ptr, &ret_val);
            clock_gettime(CLOCK_MONOTONIC_RAW, &end_ns);
            if (ret == UB_RES_OK) {
                g_thread_stats[thread_idx].thread_get_cnt++;
            }
        }
        
        // 计算单次操作耗时并累加
        uint64_t start = (uint64_t)start_ns.tv_sec * 1000000000ULL + start_ns.tv_nsec;
        uint64_t end = (uint64_t)end_ns.tv_sec * 1000000000ULL + end_ns.tv_nsec;
        g_thread_stats[thread_idx].thread_op_time += (double)(end - start);
        
        // 操作失败则退出
        if (ret != UB_RES_OK) {
            return (void *)-1;
        }
        g_thread_stats[thread_idx].thread_op_cnt++;
    }

    // 转换为秒级耗时
    g_thread_stats[thread_idx].thread_op_time = g_thread_stats[thread_idx].thread_op_time / 1000000000.0;

    return NULL;
}

/**
 * @brief 执行原子操作性能测试并输出结果
 */
static int run_atomic_perf_test()
{
    if (thread_num > MAX_THREAD_NUM) {
        fprintf(stderr, "Error: Thread num exceed max %d\n", MAX_THREAD_NUM);
        return -1;
    }

    // 分配线程统计数组内存
    g_thread_stats = (thread_stat_t *)calloc(thread_num, sizeof(thread_stat_t));
    if (g_thread_stats == NULL) {
        return -1;
    }

    // 重置共享内存原子值为0
    if (ub_dist_tx_res_set(g_shm_atomic_ptr, 0) != UB_RES_OK) {
        free(g_thread_stats);
        return -1;
    }

    // 创建测试线程
    pthread_t threads[thread_num];
    int create_fail = 0;
    for (int i = 0; i < thread_num && !create_fail; i++) {
        int *idx_ptr = (int *)malloc(sizeof(int));
        *idx_ptr = i;
        if (pthread_create(&threads[i], NULL, atomic_operation_worker, idx_ptr) != 0) {
            free(idx_ptr);
            create_fail = 1;
            break;
        }
    }

    // 等待所有线程结束（此过程耗时不计入原子操作耗时）
    if (!create_fail) {
        for (int i = 0; i < thread_num; i++) {
            void *thread_ret;
            pthread_join(threads[i], &thread_ret);
            if (thread_ret != NULL) {
                free(g_thread_stats);
                return -1;
            }
        }
    } else {
        free(g_thread_stats);
        return -1;
    }

    // ========== 汇总统计并输出性能结果 ==========
    double total_op_time = 0.0;
    uint64_t total_op_cnt = 0;
    uint64_t total_fetchadd_cnt = 0;
    uint64_t total_get_cnt = 0;
    
    for (int i = 0; i < thread_num; i++) {
        total_op_time += g_thread_stats[i].thread_op_time;
        total_op_cnt += g_thread_stats[i].thread_op_cnt;
        total_fetchadd_cnt += g_thread_stats[i].thread_fetchadd_cnt;
        total_get_cnt += g_thread_stats[i].thread_get_cnt;
    }

    double ops_per_second = total_op_cnt / total_op_time;
    double avg_ns_per_op = (total_op_time * 1000000000.0) / (double)total_op_cnt;

    // 输出扩展的性能指标（包含操作类型统计）
    printf("=====================================\n");
    printf("共享内存原子操作性能测试结果\n");
    printf("-------------------------------------\n");
    printf("测试配置: %d线程 x 每线程%lu次操作\n", thread_num, op_count_per_thread);
    printf("操作比例  : fetch_add(30%%) : get(70%%)\n");
    printf("原子操作总次数: %lu\n", total_op_cnt);
    printf("  - fetch_add操作次数: %lu (实际占比: %.2f%%)\n", 
           total_fetchadd_cnt, (double)total_fetchadd_cnt/total_op_cnt*100);
    printf("  - get操作次数      : %lu (实际占比: %.2f%%)\n", 
           total_get_cnt, (double)total_get_cnt/total_op_cnt*100);
    printf("原子操作总耗时: %.4f 秒\n", total_op_time);
    printf("每秒操作数    : %.2f OPS/s\n", ops_per_second);
    printf("单次操作平均时延: %.2f 纳秒\n", avg_ns_per_op);
    printf("共享内存原子值最终结果: %lu\n", g_shm_atomic_ptr[0]);
    printf("=====================================\n");

    free(g_thread_stats);
    g_thread_stats = NULL;

    return 0;
}

static void EmptyLog(int level, const char* msg){
    
}

// ========== 主函数（单模式，支持自定义线程数/操作数） ==========
int main(int argc, char *argv[])
{
    // 解析命令行参数：可选传入 线程数 每线程操作数
    if (argc >= 2) {
        thread_num = atoi(argv[1]);
        if (thread_num <= 0 || thread_num > MAX_THREAD_NUM) {
            fprintf(stderr, "Warn: Invalid thread num, use default %d\n", DEFAULT_THREAD_NUM);
            thread_num = DEFAULT_THREAD_NUM;
        }
    }
    if (argc >= 3) {
        op_count_per_thread = atoll(argv[2]);
        if (op_count_per_thread == 0) {
            fprintf(stderr, "Warn: Invalid op count, use default %lu\n", DEFAULT_OP_COUNT);
            op_count_per_thread = DEFAULT_OP_COUNT;
        }
    }

    ubsmem_set_extern_logger(EmptyLog);
    // 映射共享内存
    if (map_ubsmem_shm() != 0) {
        fprintf(stderr, "Error: Map shared memory failed\n");
        return -1;
    }

    // 执行性能测试
    int ret = run_atomic_perf_test();

    // 清理资源
    if (g_shm_base_ptr != NULL) {
        ubsmem_shmem_unmap(g_shm_base_ptr, SHM_TOTAL_SIZE);
    }
    ubsmem_finalize();

    return ret;
}