#include "ub_dist_tx_res.h"
#include "ubs_mem_def.h"
#include "ubs_mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <sys/stat.h>
#include <signal.h>
#include <stdalign.h>
#include <stdbool.h>  // 新增：用于布尔值判断

// ========== 全局模式标记 ==========
static char g_run_mode[16] = {0};

// ========== Core Configuration (核心：128MB共享内存) ==========
#define SHM_NAME "ub_dist_tx_res_shm"
#define SHM_TOTAL_SIZE (128 * 1024 * 1024)  // 134217728字节（128MB）
#define UINT64_ALIGN (alignof(uint64_t))    // 仅8字节对齐

// Master/slave配置
#define MASTER_THREAD_NUM     80    
#define MASTER_FETCH_PER_THREAD 1000
#define SLAVE_THREAD_NUM      20    
#define SLAVE_FETCH_PER_THREAD 4000

// Single模式新增并发配置
#define SINGLE_THREAD_NUM     4       // single模式并发线程数
#define SINGLE_FETCH_PER_THREAD 1000  // 每个线程fetch_add次数
#define SINGLE_EXPECT_TOTAL   (SINGLE_THREAD_NUM * SINGLE_FETCH_PER_THREAD)  // 预期总操作数

#define WAIT_TIMEOUT          60    
#define SINGLE_NODE_TEST_VAL  123456

// 全局变量
static void *g_shm_base_ptr = NULL;       // 128MB共享内存基地址
static uint64_t *g_shm_data_ptr = NULL;   // 8字节对齐的uint64_t起始地址
static uint64_t g_local_op_count = 0;
static pthread_t g_main_tid;
static ubsmem_options_t g_ubsm_opts;

// Single模式并发测试全局变量（线程安全）
static pthread_mutex_t g_single_mutex;    // 保护返回值集合的互斥锁
static uint64_t *g_single_fetch_vals = NULL;  // 存储所有fetch_add返回值
static uint64_t g_single_val_count = 0;   // 已存储的返回值数量

// 线程参数结构体
typedef struct {
    int thread_idx;
    int is_master;
} thread_arg_t;

// ========== 工具函数：打印最终TID ==========
#define PRINT_FINAL_TID() \
    do { \
        printf("[Final TID] Main thread TID: %lu, Process PID: %d\n", \
               (uint64_t)pthread_self(), getpid()); \
    } while (0)

static void EmptyLog(int level, const char* msg){
    
}

static int check_shm_ptr_safe() {
    printf("[DEBUG] check_shm_ptr_safe start: base=%p, data=%p\n", g_shm_base_ptr, g_shm_data_ptr);
    
    if (g_shm_base_ptr == NULL || g_shm_data_ptr == NULL) {
        fprintf(stderr, "[ERROR] Shm ptr is NULL (base: %p, data: %p)\n", g_shm_base_ptr, g_shm_data_ptr);
        return -1;
    }
    uintptr_t data_ptr = (uintptr_t)g_shm_data_ptr;
    if (data_ptr % UINT64_ALIGN != 0) {
        fprintf(stderr, "[ERROR] g_shm_data_ptr %p is NOT 8-byte aligned! (Atomic op risk)\n", g_shm_data_ptr);
        return -1;
    }
    printf("[DEBUG] check_shm_ptr_safe: g_shm_data_ptr %p is 8-byte aligned (UINT64_ALIGN=%lu)\n",
            g_shm_data_ptr, UINT64_ALIGN);

    uintptr_t max_safe_addr = (uintptr_t)g_shm_base_ptr + SHM_TOTAL_SIZE;
    uintptr_t target_addr = (uintptr_t)(g_shm_data_ptr + 2);
    if (target_addr >= max_safe_addr) {
        fprintf(stderr, "[ERROR] g_shm_data_ptr[2] %p out of 128MB shm range (max: %p)\n",
                (void*)target_addr, (void*)max_safe_addr);
        return -1;
    }
    printf("[DEBUG] check_shm_ptr_safe: g_shm_data_ptr[2] %p is within 128MB (max: %p)\n",
            (void*)target_addr, (void*)max_safe_addr);

    return 0;
}

static int get_comm_flag(int flag_idx, uint64_t *out_val) {
    printf("[DEBUG] get_comm_flag start: flag_idx=%d, out_val=%p\n", flag_idx, out_val);
    
    if (flag_idx != 1 && flag_idx != 2) {
        fprintf(stderr, "[ERROR] Invalid flag idx: %d (only 1/2 allowed)\n", flag_idx);
        return -1;
    }
    if (out_val == NULL) {
        fprintf(stderr, "[ERROR] out_val is NULL\n");
        return -1;
    }
    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] get_comm_flag: shm ptr unsafe, skip read flag %d\n", flag_idx);
        return -1;
    }

    int ret = ub_dist_tx_res_get(&g_shm_data_ptr[flag_idx], out_val);
    if (ret != 0) {
        fprintf(stderr, "[ERROR] get_comm_flag: ub_dist_tx_res_get failed (flag_idx=%d, ret=%d)\n", flag_idx, ret);
        return -1;
    }
    printf("[INFO] get_comm_flag SUCCESS (YOUR ATOMIC): flag_idx=%d, addr=%p, value=%lu\n",
            flag_idx, &g_shm_data_ptr[flag_idx], *out_val);
    return 0;
}

static int set_comm_flag(int flag_idx, uint64_t val) {
    printf("[DEBUG] set_comm_flag start: flag_idx=%d, target_val=%lu\n", flag_idx, val);
    
    if (flag_idx != 1 && flag_idx != 2) {
        fprintf(stderr, "[ERROR] Invalid flag idx: %d (only 1/2 allowed)\n", flag_idx);
        return -1;
    }
    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] set_comm_flag: shm ptr unsafe, skip set flag %d to %lu\n", flag_idx, val);
        return -1;
    }

    uint64_t old_val = 0;
    int ret_get = ub_dist_tx_res_get(&g_shm_data_ptr[flag_idx], &old_val);
    if (ret_get != 0) {
        fprintf(stderr, "[WARN] set_comm_flag: read old val failed (flag_idx=%d, ret=%d)\n", flag_idx, ret_get);
        old_val = 0; // 日志兜底
    }
    printf("[DEBUG] set_comm_flag: flag_idx=%d, addr=%p, old_val=%lu, will set to %lu (YOUR ATOMIC)\n",
            flag_idx, &g_shm_data_ptr[flag_idx], old_val, val);
    
    int ret_set = ub_dist_tx_res_set(&g_shm_data_ptr[flag_idx], val);
    if (ret_set != 0) {
        fprintf(stderr, "[ERROR] set_comm_flag: ub_dist_tx_res_set failed (flag_idx=%d, val=%lu, ret=%d)\n",
                flag_idx, val, ret_set);
        return -1;
    }

    uint64_t verify_val = 0;
    int ret_verify = ub_dist_tx_res_get(&g_shm_data_ptr[flag_idx], &verify_val);
    if (ret_verify != 0) {
        fprintf(stderr, "[WARN] set_comm_flag: verify val failed (flag_idx=%d, ret=%d)\n", flag_idx, ret_verify);
    } else if (verify_val != val) {
        fprintf(stderr, "[ERROR] set_comm_flag FAILED (YOUR ATOMIC): flag_idx=%d, addr=%p, set to %lu but verify got %lu\n",
                flag_idx, &g_shm_data_ptr[flag_idx], val, verify_val);
        return -1;
    }

    printf("[INFO] set_comm_flag SUCCESS (YOUR ATOMIC): flag_idx=%d, addr=%p, old_val=%lu → new_val=%lu\n",
            flag_idx, &g_shm_data_ptr[flag_idx], old_val, val);
    return 0;
}

static void reset_comm_flags() {
    printf("[DEBUG] reset_comm_flags start (YOUR ATOMIC)\n");
    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[WARN] reset_comm_flags: shm ptr unsafe, skip reset\n");
        return;
    }

    uint64_t master_flag_old = 0;
    get_comm_flag(1, &master_flag_old);
    if (master_flag_old != 0) {
        printf("[DEBUG] reset_comm_flags: master flag (1) old_val=%lu, will reset to 0 (YOUR ATOMIC)\n", master_flag_old);
        if (set_comm_flag(1, 0) == 0) {
            printf("[INFO] reset_comm_flags: master flag (1) reset SUCCESS (YOUR ATOMIC) (old=%lu → new=0)\n", master_flag_old);
        } else {
            fprintf(stderr, "[ERROR] reset_comm_flags: master flag (1) reset FAILED (YOUR ATOMIC)\n");
        }
    } else {
        printf("[INFO] reset_comm_flags: master flag (1) is already 0, skip reset (YOUR ATOMIC)\n");
    }

    uint64_t slave_flag_old = 0;
    get_comm_flag(2, &slave_flag_old);
    if (slave_flag_old != 0) {
        printf("[DEBUG] reset_comm_flags: slave flag (2) old_val=%lu, will reset to 0 (YOUR ATOMIC)\n", slave_flag_old);
        if (set_comm_flag(2, 0) == 0) {
            printf("[INFO] reset_comm_flags: slave flag (2) reset SUCCESS (YOUR ATOMIC) (old=%lu → new=0)\n", slave_flag_old);
        } else {
            fprintf(stderr, "[ERROR] reset_comm_flags: slave flag (2) reset FAILED (YOUR ATOMIC)\n");
        }
    } else {
        printf("[INFO] reset_comm_flags: slave flag (2) is already 0, skip reset (YOUR ATOMIC)\n");
    }

    uint64_t master_flag_final = 0, slave_flag_final = 0;
    get_comm_flag(1, &master_flag_final);
    get_comm_flag(2, &slave_flag_final);
    printf("[INFO] reset_comm_flags FINAL CHECK (YOUR ATOMIC): master_flag(1)=%lu, slave_flag(2)=%lu\n",
            master_flag_final, slave_flag_final);
}

static int map_ubsmem_shm() {
    int ret = UBSM_OK;
    int wait_sec = 0;
    void *shm_addr = NULL;

    ret = ubsmem_init_attributes(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[ERROR] ubsmem_init_attributes failed (ret: %d)\n", ret);
        return -1;
    }
    ret = ubsmem_initialize(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[ERROR] ubsmem_initialize failed (ret: %d)\n", ret);
        return -1;
    }

    if (strcmp(g_run_mode, "single") == 0) {
        // Single模式
        printf("[INFO] Single mode: Map 128MB ubsmem shm %s (size: %lu bytes)\n", 
               SHM_NAME, SHM_TOTAL_SIZE);
        ret = ubsmem_shmem_map(
            NULL,
            SHM_TOTAL_SIZE,          
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            (char*)SHM_NAME,
            0,
            &shm_addr
        );
        if (ret != 0) {
            fprintf(stderr, "[ERROR] Single mode map 128MB shm failed (ret: %d)\n", ret);
            ubsmem_finalize();
            return -1;
        }
        printf("[INFO] Single mode map 128MB shm success, base addr: %p\n", shm_addr);

    } else if (strcmp(g_run_mode, "master") == 0) {
        // Master模式：创建并设置0666权限
        printf("[INFO] Master mode: Create 128MB ubsmem shm %s\n", SHM_NAME);
        ret = ubsmem_shmem_map(
            NULL,
            SHM_TOTAL_SIZE,          
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            (char*)SHM_NAME,
            0,
            &shm_addr
        );
        if (ret != 0) {
            fprintf(stderr, "[ERROR] Master mode map 128MB shm failed (ret: %d)\n", ret);
            ubsmem_finalize();
            return -1;
        }
        // 设置共享内存权限为0666（所有用户可读可写）
        int shm_fd = open("/dev/shm/ub_dist_tx_res_shm", O_RDWR);
        if (shm_fd >= 0) {
            if (fchmod(shm_fd, 0666) == 0) {
                printf("[INFO] Master set shm /dev/shm/ub_dist_tx_res_shm perm to 0666\n");
            } else {
                perror("[WARN] Master fchmod shm failed");
            }
            close(shm_fd);
        } else {
            perror("[WARN] Master open shm file failed");
        }
        printf("[INFO] Master mode map 128MB shm success, base addr: %p\n", shm_addr);

    } else if (strcmp(g_run_mode, "slave") == 0) {
        // Slave模式：等待Master创建的共享内存
        printf("[INFO] Slave mode: Waiting for 128MB ubsmem shm %s\n", SHM_NAME);
        while (1) {
            ret = ubsmem_shmem_map(
                NULL,
                SHM_TOTAL_SIZE,          
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE,
                (char*)SHM_NAME,
                0,
                &shm_addr
            );
            if (ret == 0) break;
            
            wait_sec++;
            if (wait_sec >= WAIT_TIMEOUT) {
                fprintf(stderr, "[ERROR] Wait 128MB shm %s timeout (%d sec)\n", SHM_NAME, WAIT_TIMEOUT);
                ubsmem_finalize();
                return -1;
            }
            printf("[INFO] Wait 128MB shm %s... (%d/%d)\n", SHM_NAME, wait_sec, WAIT_TIMEOUT);
            sleep(1);
        }
        printf("[INFO] Slave mode map 128MB shm success: %s, base addr: %p\n", SHM_NAME, shm_addr);
    }

    uintptr_t shm_uintptr = (uintptr_t)shm_addr;
    if (shm_uintptr % UINT64_ALIGN != 0) {
        shm_uintptr += (UINT64_ALIGN - (shm_uintptr % UINT64_ALIGN));
        printf("[INFO] Adjust shm addr to 8-byte align: %p → %p (UINT64_ALIGN=%lu)\n",
                shm_addr, (void*)shm_uintptr, UINT64_ALIGN);
    }

    uintptr_t max_shm_addr = (uintptr_t)shm_addr + SHM_TOTAL_SIZE;
    uintptr_t data_ptr_plus_2 = shm_uintptr + 2 * UINT64_ALIGN;
    if (data_ptr_plus_2 >= max_shm_addr) {
        fprintf(stderr, "[ERROR] g_shm_data_ptr[2] %p out of 128MB shm range (max: %p)\n",
                (void*)data_ptr_plus_2, (void*)max_shm_addr);
        ubsmem_shmem_unmap(shm_addr, SHM_TOTAL_SIZE);
        ubsmem_finalize();
        return -1;
    }

    g_shm_data_ptr = (uint64_t*)shm_uintptr;
    g_shm_base_ptr = shm_addr;
    
    printf("[INFO] First aligned uint64_t addr: %p (offset: %lu bytes from base)\n",
           g_shm_data_ptr, (uint64_t)(g_shm_data_ptr - (uint64_t*)g_shm_base_ptr));
    printf("[INFO] g_shm_data_ptr[2] addr: %p (within 128MB shm: %s)\n",
           &g_shm_data_ptr[2], (data_ptr_plus_2 < max_shm_addr) ? "YES" : "NO");

    uint64_t init_flag1 = 0, init_flag2 = 0;
    get_comm_flag(1, &init_flag1);
    get_comm_flag(2, &init_flag2);
    printf("[INFO] After map shm, INIT flag values (YOUR ATOMIC): master_flag(1)=%lu, slave_flag(2)=%lu\n",
            init_flag1, init_flag2);

    return 0;
}

// ========== Single模式新增：并发fetch_add线程函数 ==========
static void* single_fetch_add_worker(void *arg) {
    thread_arg_t *t_arg = (thread_arg_t*)arg;
    int thread_idx = t_arg->thread_idx;
    uint64_t fetch_val = 0;
    int ret = UB_RES_OK;

    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] [Single Thread %d] Shm ptr unsafe, exit\n", thread_idx);
        free(t_arg);
        return (void*)-1;
    }

    printf("[INFO] [Single Thread %d] Start %d times fetch_add\n", thread_idx, SINGLE_FETCH_PER_THREAD);
    
    // 执行指定次数的原子fetch_add
    for (int i = 0; i < SINGLE_FETCH_PER_THREAD; i++) {
        // 调用原子fetch_add接口，返回自增前的值
        ret = ub_dist_tx_res_fetch_add(&g_shm_data_ptr[0], 1, &fetch_val);
        if (ret != UB_RES_OK) {
            fprintf(stderr, "[ERROR] [Single Thread %d] fetch_add failed (i:%d, ret:%d)\n", thread_idx, i, ret);
            free(t_arg);
            return (void*)-1;
        }

        // 线程安全存储返回值（加锁保护）
        pthread_mutex_lock(&g_single_mutex);
        g_single_fetch_vals[g_single_val_count++] = fetch_val;
        pthread_mutex_unlock(&g_single_mutex);

        // 每100次打印一次进度
        if (i % 100 == 0) {
            printf("[INFO] [Single Thread %d] fetch_add %d times, current return val: %lu\n", 
                   thread_idx, i+1, fetch_val);
        }
    }

    printf("[INFO] [Single Thread %d] Finish! Total ops: %d\n", thread_idx, SINGLE_FETCH_PER_THREAD);
    free(t_arg);
    return NULL;
}

// ========== Single模式新增：验证fetch_add返回值唯一性 ==========
static int verify_single_fetch_unique() {
    printf("\n===== Single Mode: Verify Fetch_Add Return Values =====\n");
    printf("Expected total values: %lu\n", SINGLE_EXPECT_TOTAL);
    printf("Actual stored values: %lu\n", g_single_val_count);

    if (g_single_val_count != SINGLE_EXPECT_TOTAL) {
        fprintf(stderr, "[ERROR] Single verify: total count mismatch (expect:%lu, actual:%lu)\n",
                SINGLE_EXPECT_TOTAL, g_single_val_count);
        return -1;
    }

    bool has_duplicate = false;
    for (uint64_t i = 0; i < g_single_val_count; i++) {
        for (uint64_t j = i+1; j < g_single_val_count; j++) {
            if (g_single_fetch_vals[i] == g_single_fetch_vals[j]) {
                fprintf(stderr, "[ERROR] Single verify: duplicate value found! val=%lu (index:%lu and %lu)\n",
                        g_single_fetch_vals[i], i, j);
                has_duplicate = true;
                break;
            }
        }
        if (has_duplicate) break;
    }

    if (has_duplicate) {
        fprintf(stderr, "[ERROR] Single verify: duplicate values exist!\n");
        return -1;
    }

    uint64_t min_val = g_single_fetch_vals[0];
    uint64_t max_val = g_single_fetch_vals[0];
    for (uint64_t i = 1; i < g_single_val_count; i++) {
        if (g_single_fetch_vals[i] < min_val) min_val = g_single_fetch_vals[i];
        if (g_single_fetch_vals[i] > max_val) max_val = g_single_fetch_vals[i];
    }
    printf("Min return val: %lu, Max return val: %lu\n", min_val, max_val);
    printf("Value range: %lu (should equal total ops: %lu)\n", max_val - min_val + 1, SINGLE_EXPECT_TOTAL);

    if ((max_val - min_val + 1) != SINGLE_EXPECT_TOTAL) {
        fprintf(stderr, "[WARN] Single verify: value range mismatch (range:%lu, total:%lu)\n",
                max_val - min_val + 1, SINGLE_EXPECT_TOTAL);
    }

    printf("[INFO] Single verify: all %lu values are UNIQUE!\n", SINGLE_EXPECT_TOTAL);
    return 0;
}

// ========== Single Node模式 ==========
static int run_single_node() {
    printf("\n===== Single Node Test (init/set/get) =====\n");
    int ret = UB_RES_OK;
    uint64_t out_val = 0;

    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] [Single] Shm ptr unsafe, exit\n");
        return -1;
    }

    // 原有init/set/get测试逻辑
    ret = ub_dist_tx_res_init(&g_shm_data_ptr[0]);
    if (ret != UB_RES_OK) {
        fprintf(stderr, "[ERROR] [Single] Init failed (ret: %d)\n", ret);
        return -1;
    }
    printf("[INFO] [Single] Init success (ret: %d)\n", ret);

    ret = ub_dist_tx_res_get(&g_shm_data_ptr[0], &out_val);
    if (ret != UB_RES_OK || out_val != 0) {
        fprintf(stderr, "[ERROR] [Single] Get init val failed (expect:0, actual:%lu, ret:%d)\n", out_val, ret);
        return -1;
    }
    printf("[INFO] [Single] Get init val success: %lu\n", out_val);

    ret = ub_dist_tx_res_set(&g_shm_data_ptr[0], SINGLE_NODE_TEST_VAL);
    if (ret != UB_RES_OK) {
        fprintf(stderr, "[ERROR] [Single] Set val failed (ret: %d)\n", ret);
        return -1;
    }
    printf("[INFO] [Single] Set val success: %lu\n", SINGLE_NODE_TEST_VAL);

    ret = ub_dist_tx_res_get(&g_shm_data_ptr[0], &out_val);
    if (ret != UB_RES_OK || out_val != SINGLE_NODE_TEST_VAL) {
        fprintf(stderr, "[ERROR] [Single] Get set val failed (expect:%lu, actual:%lu)\n", SINGLE_NODE_TEST_VAL, out_val);
        return -1;
    }
    printf("[INFO] [Single] Get set val success: %lu\n", out_val);

    // ========== 新增：Single模式并发fetch_add测试 ==========
    printf("\n===== Single Node Concurrent Fetch_Add Test (4 threads × 1000 ops) =====\n");
    
    // 1. 重新初始化原子值为0（确保并发测试从0开始）
    ret = ub_dist_tx_res_init(&g_shm_data_ptr[0]);
    if (ret != UB_RES_OK) {
        fprintf(stderr, "[ERROR] [Single] Re-init for concurrent test failed (ret: %d)\n", ret);
        return -1;
    }
    printf("[INFO] [Single] Re-init atomic val to 0 for concurrent test\n");

    // 2. 初始化并发测试资源
    pthread_mutex_init(&g_single_mutex, NULL);
    g_single_fetch_vals = (uint64_t*)malloc(SINGLE_EXPECT_TOTAL * sizeof(uint64_t));
    if (g_single_fetch_vals == NULL) {
        perror("[ERROR] [Single] malloc for fetch vals failed");
        pthread_mutex_destroy(&g_single_mutex);
        return -1;
    }
    memset(g_single_fetch_vals, 0, SINGLE_EXPECT_TOTAL * sizeof(uint64_t));
    g_single_val_count = 0;

    // 3. 创建4个并发线程
    pthread_t threads[SINGLE_THREAD_NUM];
    for (int i = 0; i < SINGLE_THREAD_NUM; i++) {
        thread_arg_t *arg = (thread_arg_t*)malloc(sizeof(thread_arg_t));
        arg->thread_idx = i;
        arg->is_master = 0;  // single模式无需区分master/slave
        if (pthread_create(&threads[i], NULL, single_fetch_add_worker, arg) != 0) {
            perror("[ERROR] [Single] pthread_create failed");
            free(arg);
            free(g_single_fetch_vals);
            pthread_mutex_destroy(&g_single_mutex);
            return -1;
        }
    }

    // 4. 等待所有线程完成
    for (int i = 0; i < SINGLE_THREAD_NUM; i++) {
        void *thread_ret;
        pthread_join(threads[i], &thread_ret);
        if (thread_ret != NULL) {
            fprintf(stderr, "[ERROR] [Single] Thread %d failed\n", i);
            free(g_single_fetch_vals);
            pthread_mutex_destroy(&g_single_mutex);
            return -1;
        }
    }
    printf("[INFO] [Single] All 4 concurrent threads finish\n");

    // 5. 验证所有fetch_add返回值的唯一性
    ret = verify_single_fetch_unique();
    if (ret != 0) {
        fprintf(stderr, "[ERROR] [Single] Concurrent fetch_add verify failed\n");
        free(g_single_fetch_vals);
        pthread_mutex_destroy(&g_single_mutex);
        return -1;
    }

    // 6. 清理并发测试资源
    free(g_single_fetch_vals);
    pthread_mutex_destroy(&g_single_mutex);

    printf("\n===== Single Node Concurrent Test SUCCESS! =====\n");
    printf("\n===== Single Node Test ALL SUCCESS! =====\n");
    return 0;
}

static void* fetch_add_worker(void *arg) {
    thread_arg_t *t_arg = (thread_arg_t*)arg;
    int thread_idx = t_arg->thread_idx;
    int is_master = t_arg->is_master;
    uint64_t out_val = 0;
    uint64_t get_val = 0;
    int ret = UB_RES_OK;

    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] [%s Thread %d] Shm ptr unsafe, exit\n", is_master ? "Master" : "Slave", thread_idx);
        free(t_arg);
        return (void*)-1;
    }

    int fetch_count = is_master ? MASTER_FETCH_PER_THREAD : SLAVE_FETCH_PER_THREAD;
    const char *node_type = is_master ? "Master" : "Slave";

    for (int i = 0; i < fetch_count; i++) {
        ret = ub_dist_tx_res_fetch_add(&g_shm_data_ptr[0], 1, &out_val);
        if (ret != UB_RES_OK) {
            fprintf(stderr, "[ERROR] [%s Thread %d] fetch_add failed (i:%d, ret:%d)\n", node_type, thread_idx, i, ret);
            free(t_arg);
            return (void*)-1;
        }
        __sync_fetch_and_add(&g_local_op_count, 1);

        if (i % 100 == 0) {
            ret = ub_dist_tx_res_get(&g_shm_data_ptr[0], &get_val);
            if (ret != UB_RES_OK) {
                fprintf(stderr, "[ERROR] [%s Thread %d] get failed (i:%d, ret:%d)\n", node_type, thread_idx, i, ret);
                free(t_arg);
                return (void*)-1;
            }
            if (i == 0 || i == fetch_count - 1) {
                printf("[INFO] [%s Thread %d] fetch_add %d times, current val: %lu\n", node_type, thread_idx, i+1, get_val);
            }
        }
    }

    printf("[INFO] [%s Thread %d] Finish! Total ops: %d, local total: %lu\n", node_type, thread_idx, fetch_count, g_local_op_count);
    free(t_arg);
    return NULL;
}

static int run_master() {
    printf("\n===== Master Node Start (80 threads×1000 ops) =====\n");
    int ret = UB_RES_OK;
    uint64_t out_val = 0;
    pthread_t threads[MASTER_THREAD_NUM];
    uint64_t master_flag = 0, slave_flag = 0;

    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] [Master] Shm ptr unsafe, exit\n");
        reset_comm_flags();
        return -1;
    }

    ret = ub_dist_tx_res_init(&g_shm_data_ptr[0]);
    if (ret != UB_RES_OK) {
        fprintf(stderr, "[ERROR] [Master] Init failed (ret: %d)\n", ret);
        reset_comm_flags();
        return -1;
    }
    printf("[INFO] [Master] Init success (ret: %d)\n", ret);

    ret = ub_dist_tx_res_get(&g_shm_data_ptr[0], &out_val);
    if (ret != UB_RES_OK || out_val != 0) {
        fprintf(stderr, "[ERROR] [Master] Init val verify failed (val:%lu, ret:%d)\n", out_val, ret);
        reset_comm_flags();
        return -1;
    }
    printf("[INFO] [Master] Init val verify success: %lu\n", out_val);

    printf("[DEBUG] [Master] Will init slave flag (2) to 0 (YOUR ATOMIC)\n");
    if (set_comm_flag(2, 0) != 0) {
        fprintf(stderr, "[ERROR] [Master] Failed to init slave flag (2) to 0\n");
        reset_comm_flags();
        return -1;
    }

    printf("[DEBUG] [Master] Will set master flag (1) to 1 (YOUR ATOMIC)\n");
    if (set_comm_flag(1, 1) != 0) {
        fprintf(stderr, "[ERROR] [Master] Failed to set master flag (1) to 1\n");
        reset_comm_flags();
        return -1;
    }
    printf("[INFO] [Master] Set master_state=1, start threads...\n");

    for (int i = 0; i < MASTER_THREAD_NUM; i++) {
        thread_arg_t *arg = (thread_arg_t*)malloc(sizeof(thread_arg_t));
        arg->thread_idx = i;
        arg->is_master = 1;
        if (pthread_create(&threads[i], NULL, fetch_add_worker, arg) != 0) {
            perror("[ERROR] [Master] pthread_create failed");
            free(arg);
            reset_comm_flags();
            return -1;
        }
    }

    for (int i = 0; i < MASTER_THREAD_NUM; i++) {
        void *thread_ret;
        pthread_join(threads[i], &thread_ret);
        if (thread_ret != NULL) {
            fprintf(stderr, "[ERROR] [Master] Thread %d failed\n", i);
            reset_comm_flags();
            return -1;
        }
    }
    printf("[INFO] [Master] All 80 threads finish, local total ops: %lu\n", g_local_op_count);

    int wait_sec = 0;
    printf("[DEBUG] [Master] Start waiting slave flag (2) to be 1 (YOUR ATOMIC)\n");
    while (1) {
        if (get_comm_flag(2, &slave_flag) != 0) {
            fprintf(stderr, "[ERROR] [Master] Failed to read slave flag (2)\n");
            reset_comm_flags();
            return -1;
        }
        printf("[DEBUG] [Master] Wait slave loop: wait_sec=%d, slave_flag(2)=%lu (expect 1)\n", wait_sec, slave_flag);
        if (slave_flag == 1) break;
        
        wait_sec++;
        if (wait_sec >= WAIT_TIMEOUT) {
            fprintf(stderr, "[ERROR] [Master] Wait slave timeout (%d sec)\n", WAIT_TIMEOUT);
            reset_comm_flags();
            return -1;
        }
        sleep(1);
    }
    printf("[INFO] [Master] Slave execute done (slave_state=1)\n");

    uint64_t master_total = (uint64_t)MASTER_THREAD_NUM * MASTER_FETCH_PER_THREAD;
    uint64_t slave_total = (uint64_t)SLAVE_THREAD_NUM * SLAVE_FETCH_PER_THREAD;
    uint64_t expect_val = master_total + slave_total;

    ret = ub_dist_tx_res_get(&g_shm_data_ptr[0], &out_val);
    if (ret != UB_RES_OK) {
        fprintf(stderr, "[ERROR] [Master] Get final val failed (ret: %d)\n", ret);
        reset_comm_flags();
        return -1;
    }

    printf("\n===== Master Verify Result =====\n");
    printf("Master total ops: %lu (80x1000)\n", master_total);
    printf("Slave total ops: %lu (20x4000)\n", slave_total);
    printf("Expect final val: %lu\n", expect_val);
    printf("Actual final val: %lu\n", out_val);

    if (out_val != expect_val) {
        fprintf(stderr, "[ERROR] [Master] Verify FAILED! Val mismatch\n");
        reset_comm_flags();
        return -1;
    } else {
        printf("[INFO] [Master] Verify SUCCESS! Val is correct\n");
    }

    printf("\n[DEBUG] [Master] All business done, start reset comm flags (YOUR ATOMIC)\n");
    reset_comm_flags();

    uint64_t final_flag1 = 0, final_flag2 = 0;
    get_comm_flag(1, &final_flag1);
    get_comm_flag(2, &final_flag2);
    printf("[INFO] [Master] After reset, FINAL flag values (YOUR ATOMIC): master_flag(1)=%lu, slave_flag(2)=%lu\n",
            final_flag1, final_flag2);

    return 0;
}

static int run_slave() {
    printf("\n===== Slave Node Start (20 threads×4000 ops) =====\n");
    int ret = UB_RES_OK;
    uint64_t out_val = 0;
    pthread_t threads[SLAVE_THREAD_NUM];
    uint64_t master_flag = 0, slave_flag = 0;

    if (check_shm_ptr_safe() != 0) {
        fprintf(stderr, "[ERROR] [Slave] Shm ptr unsafe, exit\n");
        return -1;
    }

    int wait_sec = 0;
    printf("[DEBUG] [Slave] Start waiting master flag (1) to be 1 (YOUR ATOMIC)\n");
    while (1) {
        if (get_comm_flag(1, &master_flag) != 0) {
            fprintf(stderr, "[ERROR] [Slave] Failed to read master flag (1)\n");
            set_comm_flag(2, 0);
            return -1;
        }
        printf("[DEBUG] [Slave] Wait master loop: wait_sec=%d, master_flag(1)=%lu (expect 1)\n", wait_sec, master_flag);
        if (master_flag == 1) break;
        
        wait_sec++;
        if (wait_sec >= WAIT_TIMEOUT) {
            fprintf(stderr, "[ERROR] [Slave] Wait master timeout (%d sec)\n", WAIT_TIMEOUT);
            set_comm_flag(2, 0);
            printf("[INFO] [Slave] Timeout, reset slave_state (flag 2) to 0 (YOUR ATOMIC)\n");
            return -1;
        }
        sleep(1);
    }
    printf("[INFO] [Slave] Master init done (master_state=1)\n");

    ret = ub_dist_tx_res_get(&g_shm_data_ptr[0], &out_val);
    if (ret != UB_RES_OK) {
        fprintf(stderr, "[ERROR] [Slave] Get init val failed (ret: %d)\n", ret);
        set_comm_flag(2, 0);
        return -1;
    }
    printf("[INFO] [Slave] Current atomic val: %lu\n", out_val);

    for (int i = 0; i < SLAVE_THREAD_NUM; i++) {
        thread_arg_t *arg = (thread_arg_t*)malloc(sizeof(thread_arg_t));
        arg->thread_idx = i;
        arg->is_master = 0;
        if (pthread_create(&threads[i], NULL, fetch_add_worker, arg) != 0) {
            perror("[ERROR] [Slave] pthread_create failed");
            free(arg);
            set_comm_flag(2, 0);
            return -1;
        }
    }

    for (int i = 0; i < SLAVE_THREAD_NUM; i++) {
        void *thread_ret;
        pthread_join(threads[i], &thread_ret);
        if (thread_ret != NULL) {
            fprintf(stderr, "[ERROR] [Slave] Thread %d failed\n", i);
            set_comm_flag(2, 0);
            return -1;
        }
    }
    printf("[INFO] [Slave] All 20 threads finish, local total ops: %lu\n", g_local_op_count);

    uint64_t slave_total = (uint64_t)SLAVE_THREAD_NUM * SLAVE_FETCH_PER_THREAD;
    if (g_local_op_count != slave_total) {
        fprintf(stderr, "[ERROR] [Slave] Local ops mismatch (expect:%lu, actual:%lu)\n", slave_total, g_local_op_count);
        set_comm_flag(2, 0);
        return -1;
    }
    printf("[INFO] [Slave] Local ops verify success: %lu\n", slave_total);

    printf("[DEBUG] [Slave] Will set slave flag (2) to 1 (YOUR ATOMIC)\n");
    if (set_comm_flag(2, 1) != 0) {
        fprintf(stderr, "[ERROR] [Slave] Failed to set slave flag (2) to 1\n");
        return -1;
    }
    printf("[INFO] [Slave] Set slave_state=1, exit\n");

    uint64_t slave_end_flag1 = 0, slave_end_flag2 = 0;
    get_comm_flag(1, &slave_end_flag1);
    get_comm_flag(2, &slave_end_flag2);
    printf("[INFO] [Slave] Before exit, flag values (YOUR ATOMIC): master_flag(1)=%lu, slave_flag(2)=%lu\n",
            slave_end_flag1, slave_end_flag2);

    return 0;
}

// ========== 主函数 ==========
int main(int argc, char *argv[]) {
    g_main_tid = pthread_self();
    int ret = 0;

    // 参数校验
    if (argc != 2 || (strcmp(argv[1], "single") != 0 && strcmp(argv[1], "master") != 0 && strcmp(argv[1], "slave") != 0)) {
        fprintf(stderr, "Usage: %s <single|master|slave>\n", argv[0]);
        fprintf(stderr, "  single: Single node (init/set/get + 4 threads×1000 fetch_add)\n");
        fprintf(stderr, "  master: Master node (80x1000 ops)\n");
        fprintf(stderr, "  slave:  Slave node (20x4000 ops)\n");
        PRINT_FINAL_TID();
        return -1;
    }

    // 设置运行模式
    strncpy(g_run_mode, argv[1], sizeof(g_run_mode)-1);
    printf("[INFO] Run mode set to: %s\n", g_run_mode);

    ubsmem_set_extern_logger(EmptyLog);
    if (map_ubsmem_shm() != 0) {
        fprintf(stderr, "[ERROR] [%s] Map 128MB ubsmem shm failed\n", argv[1]);
        PRINT_FINAL_TID();
        return -1;
    }

    if (strcmp(argv[1], "single") == 0) {
        ret = run_single_node();
    } else if (strcmp(argv[1], "master") == 0) {
        ret = run_master();
    } else if (strcmp(argv[1], "slave") == 0) {
        ret = run_slave();
    }

    if (g_shm_base_ptr != NULL) {
        if (ubsmem_shmem_unmap(g_shm_base_ptr, SHM_TOTAL_SIZE) != 0) {
            fprintf(stderr, "[ERROR] [%s] ubsmem_shmem_unmap 128MB failed\n", argv[1]);
        } else {
            printf("[INFO] [%s] 128MB ubsmem shm unmap success\n", argv[1]);
        }
        g_shm_base_ptr = NULL;
        g_shm_data_ptr = NULL;
    }

    if (ubsmem_finalize() != UBSM_OK) {
        fprintf(stderr, "[ERROR] [%s] ubsmem_finalize failed\n", argv[1]);
    }

    PRINT_FINAL_TID();

    if (ret == 0) {
        printf("\n===== [%s] All Test SUCCESS! =====\n", argv[1]);
    } else {
        fprintf(stderr, "\n===== [%s] Test FAILED! =====\n", argv[1]);
    }

    return ret;
}