#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

#pragma pack(push, 1)

// 消息类型定义
enum MsgType : uint8_t {
    TYPE_DATA_TEST = 100
};

// 测试用的 Payload 结构体 (固定为256字节)
struct TestPayload {
    int32_t id;
    char padding[248]; // 256B - sizeof(int32_t)
};

#pragma pack(pop)

// 全局原子计数器，用于接收消息计数
std::atomic<int> g_received_count{0};

// 对齐辅助函数
template <typename T>
constexpr T align_up(T value, std::size_t alignment)
{
    return (value + alignment - 1) / alignment * alignment;
}

// 全局变量：共享内存大小固定为 128MB
unsigned long g_request_size_in_mb = 1024;
char g_region_name[256] = "default";

// 测试上下文结构体
struct TestCtx {
    std::atomic<int> count{0};
    ub_shm_comm_t *handle;       // 单个句柄
    int expected_total_messages; // 存储预期计数
};

// 回调函数：打印接收到的消息 ID 并增加计数器
void cb_process_message_simple(const message_t *msg, void *ctx)
{
    if (!msg || msg->header.msg_type != TYPE_DATA_TEST)
        return;

    const TestPayload *p = (const TestPayload *)msg->body;
    printf("Received message with ID: %d\n", p->id);
    fflush(stdout); // 确保立即输出

    // 增加全局接收计数器
    g_received_count.fetch_add(1, std::memory_order_relaxed);
}

// 初始化 ub_shm 内存
int init_ub_shm()
{
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "初始化 ubsmem 属性失败!\n");
        return -1;
    }

    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "初始化 ubsmem 失败!\n");
        return -1;
    }

    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "查找节点信息失败!\n");
        return -1;
    }
    return 0;
}

// 映射 ub_shm 共享内存
int map_ub_shm(char *shm_name, void *&addr)
{
    unsigned long length = g_request_size_in_mb * 1024 * 1024;
    int ret = ubsmem_shmem_map(addr, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) {
        fprintf(stderr, "映射共享内存失败! ret=%d\n", ret);
        return -1;
    }
    fprintf(stdout, "共享内存映射到: %p\n", addr);
    return 0;
}

int main(int argc, char *argv[])
{
    // ====================== 0. 解析命令行参数 ======================
    if (argc != 7) { // 现在期望 7 个参数 (程序名 + 6 个参数)
        fprintf(
            stderr,
            "用法: %s <num_threads> <num_messages> <num_rings_flag (0 for 1 ring, 1 for 3 rings)> <callback_type_flag "
            "(0 for SYNC, 1 for ASYNC)> <sender_shm_name> <receiver_shm_name>\n",
            argv[0]);
        return -1;
    }

    int num_threads = atoi(argv[1]);
    int num_messages = atoi(argv[2]);
    int num_rings_flag = atoi(argv[3]);
    int callback_type_flag = atoi(argv[4]);

    // 新增参数：SHM 名称
    const char *sender_shm_name_input = argv[5];
    const char *receiver_shm_name_input = argv[6];

    if (num_threads <= 0 || num_messages <= 0) {
        fprintf(stderr, "错误: 参数必须大于 0。\n");
        return -1;
    }

    int num_rings = (num_rings_flag == 1) ? 3 : 1;
    ub_func_type_t callback_type = (callback_type_flag == 1) ? UB_FUNC_ASYNC : UB_FUNC_SYNC;

    long long expected_total_msgs_ll = (long long)num_threads * num_messages;
    if (expected_total_msgs_ll > INT_MAX) {
        fprintf(stderr, "错误: 预期的总消息数 (%lld) 超出整数范围。\n", expected_total_msgs_ll);
        return -1;
    }
    int expected_total_msgs = static_cast<int>(expected_total_msgs_ll);

    printf("线程数: %d, 每线程消息数: %d, Ring数量: %d, Callback Type: %s, Sender SHM Name: %s, Receiver SHM "
           "Name: %s\n",
           num_threads, num_messages, num_rings, (callback_type == UB_FUNC_ASYNC ? "ASYNC" : "SYNC"),
           sender_shm_name_input, receiver_shm_name_input);
    printf("预期总消息数: %d\n", expected_total_msgs);

    // ====================== 1. 共享内存配置 ======================
    char kSenderShmName[64];
    snprintf(kSenderShmName, sizeof(kSenderShmName), "%s", sender_shm_name_input); // 将输入复制到本地缓冲区

    char kReceiverShmName[64];
    snprintf(kReceiverShmName, sizeof(kReceiverShmName), "%s", receiver_shm_name_input); // 将输入复制到本地缓冲区

    const uint8_t kNodes = 2;
    const uint8_t nodeA = 0; // 发送方节点
    const uint8_t nodeB = 1; // 接收方节点

    // 发送端共享内存（用于接收方读取发送方的环区A）
    const size_t kSenderShmSize = g_request_size_in_mb * 1024 * 1024;
    // 接收端共享内存（用于接收方自身的环区B）
    const size_t kReceiverShmSize = g_request_size_in_mb * 1024 * 1024;

    // ====================== 2. 固定大小 ======================
    const size_t kInitRegionSize = 1024 * 1024; // 1KB
    const size_t kRingSizePerType = 2556672;    // 每种环的大小

    // ====================== 3. 计算对齐后的每个 handler 的大小 ======================
    const size_t unaligned_handler_size = kInitRegionSize + (kRingSizePerType * num_rings);
    const size_t HANDLER_SIZE = align_up(unaligned_handler_size, 64);

    // 检查总大小要求
    size_t total_required_size = HANDLER_SIZE;
    if (total_required_size > g_request_size_in_mb * 1024 * 1024) {
        std::cerr << "错误: 需要的大小 (" << total_required_size << ") 超出了分配的大小 ("
                  << g_request_size_in_mb * 1024 * 1024 << ")。\n";
        return -1;
    }

    // ====================== 4. 初始化并映射共享内存 ======================
    if (init_ub_shm() != 0) {
        std::cout << "初始化失败\n";
        return -1;
    }
    std::cout << "初始化成功\n";

    // 4.1 映射发送端共享内存（获取环区A地址，接收方读取）
    void *sender_shm_base = nullptr;
    if (map_ub_shm(kSenderShmName, sender_shm_base) != 0) {
        std::cout << "接收端映射发送端共享内存失败\n";
        return -1;
    }
    std::cout << "接收端映射发送端共享内存成功\n";

    // 4.2 映射接收端共享内存（自身环区B）
    void *receiver_shm_base = nullptr;
    if (map_ub_shm(kReceiverShmName, receiver_shm_base) != 0) {
        std::cout << "接收端共享内存分配失败\n";
        return -1;
    }
    std::cout << "接收端共享内存分配成功\n";

    // ====================== 5. 计算各 handler 区域地址 (接收端视角) ======================
    // B的初始化区域地址 (从A的内存空间获取)
    void *init_region_B = (char *)sender_shm_base;
    std::cout << "接收端 Handler 引导区地址：" << init_region_B << "\n";

    // B的环区A地址 (从A的内存空间获取，用于读取A发出的消息)
    void *ring_region_A_ptrs = (char *)sender_shm_base + kInitRegionSize;
    // B的环区B地址 (从B的内存空间获取，用于A发送给B的消息)
    void *ring_region_B_ptrs = (char *)receiver_shm_base;
    std::cout << "接收端 Handler 环区A_地址：" << ring_region_A_ptrs << "\n";
    std::cout << "接收端 Handler 环区B_地址：" << ring_region_B_ptrs << "\n";

    // ====================== 6. 通信模块配置 (1个实例, 根据flag配置1个或3个ring) ======================
    std::vector<ub_ring_desc_t> ring_descs(num_rings);
    for (int i = 0; i < num_rings; ++i) {
        ring_descs[i].ring_capacity = 64;
        ring_descs[i].max_msg_size = 256; // 固定最大消息大小为256字节
        ring_descs[i].priority = i + 1;   // 根据环索引分配优先级
    }

    ub_comm_conf_t conf = {0};
    conf.max_nodes = kNodes;
    conf.current_node_id = nodeB; // 当前是接收方节点B
    conf.num_rings = num_rings;
    conf.ring_descs = ring_descs.data();

    // 初始化单个句柄
    ub_shm_comm_t handle = nullptr;
    TestCtx g_ctx = {0};
    g_ctx.handle = &handle;
    g_ctx.expected_total_messages = expected_total_msgs; // 在上下文中设置预期计数

    // 初始化区域B (接收方视角)
    ub_shm_area_t init_area_B = {0};
    init_area_B.ptr = init_region_B;
    init_area_B.size = kInitRegionSize;

    // 环区域映射信息
    ub_ring_region_info_t ring_region_infos[2];
    // Node A 的环 (接收方读取A的环)
    ring_region_infos[0].node_id = nodeA;
    ring_region_infos[0].region.ptr = ring_region_A_ptrs;
    ring_region_infos[0].region.size = kRingSizePerType;
    // Node B 的环 (接收方写入B的环)
    ring_region_infos[1].node_id = nodeB;
    ring_region_infos[1].region.ptr = ring_region_B_ptrs;
    ring_region_infos[1].region.size = kRingSizePerType;

    ub_ring_region_map_t ring_regions = {0};
    ring_regions.entries = ring_region_infos;
    ring_regions.count = 2;

    auto ret = ub_comm_queue_init(&handle, &init_area_B, &ring_regions, &conf);
    auto reg_ret =
        ub_comm_queue_register_process_func(&handle, TYPE_DATA_TEST, callback_type, cb_process_message_simple, &g_ctx);

    if (ret != 0 || reg_ret != 0) {
        std::cerr << "初始化/注册失败: init_ret=" << ret << ", reg_ret=" << reg_ret << "\n";
        if (handle)
            ub_comm_queue_deinit(&handle);
        return -1;
    }

    std::cout << "初始化完成。等待消息...\n";
    std::cout << "将为每个消息打印 'Received message with ID: X'。\n";
    std::cout << "将在接收到 " << expected_total_msgs << " 条消息后自动退出。\n";

    // 主循环：等待接收预期数量的消息
    while (g_received_count.load(std::memory_order_relaxed) < expected_total_msgs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 短暂休眠以避免忙等待
    }

    std::cout << "\n已接收到预期的消息数量 (" << g_received_count.load() << ")。正在退出...\n";

    // ====================== 7. 清理 ======================
    ub_comm_queue_deinit(&handle);
    std::cout << "接收端程序正常退出。\n";
    return 0;
}