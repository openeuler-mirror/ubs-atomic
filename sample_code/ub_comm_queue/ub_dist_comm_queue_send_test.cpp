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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

#pragma pack(push, 1)

// 消息类型定义
enum MsgType : uint8_t {
    TYPE_DATA_TEST = 100 // A -> B: 正式压测数据
};

// 测试用的 Payload 结构体
struct TestPayload64 {
    int32_t id;
    char padding[56]; // 64 - 4 - 4 = 56 (假设4字节对齐头部)
};

#pragma pack(pop)

// 全局变量：共享内存大小固定为 128MB
unsigned long g_request_size_in_mb = 1024;
char g_region_name[256] = "default";
char g_shm_name[256]; // 用户输入的共享内存名

// 测试上下文结构体
struct TestCtx {
    std::atomic<int> count{0};
    ub_shm_comm_t *handle;       // 单个句柄
    int expected_total_messages; // 存储预期计数
};

// 对齐辅助函数
template <typename T>
constexpr T align_up(T value, std::size_t alignment)
{
    return (value + alignment - 1) / alignment * alignment;
}

// 初始化 ub_shm 内存
int init_ub_shm()
{
    ubsmem_options_t ubsm_shmem_opts;
    unsigned long length = g_request_size_in_mb * 1024 * 1024;
    int ret;

    ret = ubsmem_init_attributes(&ubsm_shmem_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "初始化 ubsmem 属性失败!\n");
        return -1;
    }

    ret = ubsmem_initialize(&ubsm_shmem_opts);
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
    int ret;

    ret = ubsmem_shmem_map(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) {
        fprintf(stderr, "映射共享内存失败! ret=%d\n", ret);
        return -1;
    }
    fprintf(stdout, "addr的内存地址值: %p\n", addr);

    return 0;
}

// 自发自收的回调函数
void cb_sync_self(const message_t *msg, void *ctx)
{
    TestCtx *tc = reinterpret_cast<TestCtx *>(ctx);
    ub_shm_comm_t *handle = tc->handle;

    if (!msg) {
        printf("[ERROR] A send failed.\n");
        return;
    }

    printf("[INFO] A send success.\n");
}

int main(int argc, char *argv[])
{
    // ====================== 0. 解析命令行参数 ======================
    if (argc != 9) {
        fprintf(
            stderr,
            "用法: %s <num_threads> <num_messages> <num_rings_flag (0 for 1 ring, 1 for 3 rings)> <callback_type_flag "
            "(0 for SYNC, 1 for ASYNC)> <self_send_flag (0 "
            "to enable self-send, 1 to disable)> <message_body_length (1-1024)> <sender_shm_name> "
            "<receiver_shm_name>\n",
            argv[0]);
        return -1;
    }
    int num_threads_per_round = atoi(argv[1]);
    int num_messages_per_thread = atoi(argv[2]);
    int num_rings_flag = atoi(argv[3]);
    int callback_type_flag = atoi(argv[4]);
    int self_send_flag = atoi(argv[5]);
    int message_body_length = atoi(argv[6]);

    const char *sender_shm_name_input = argv[7];
    const char *receiver_shm_name_input = argv[8];

    if (num_threads_per_round <= 0 || num_messages_per_thread <= 0 || message_body_length <= 0 ||
        message_body_length > 1024) {
        fprintf(stderr, "错误: 所有参数必须大于 0, 且 message_body_length 必须在 1 到 1024 之间.\n");
        return -1;
    }

    int num_rings = (num_rings_flag == 1) ? 3 : 1;
    bool enable_self_send = (self_send_flag == 0);
    ub_func_type_t callback_type = (callback_type_flag == 1) ? UB_FUNC_ASYNC : UB_FUNC_SYNC;
    printf("每轮线程数: %d, 每线程消息数: %d, Ring数量: %d, Self-Send Enabled: %s, Callback Type: %s, Message Body "
           "Length: %d, Sender SHM Name: %s, Receiver SHM Name: %s\n",
           num_threads_per_round, num_messages_per_thread, num_rings, (enable_self_send ? "Yes" : "No"),
           (callback_type == UB_FUNC_ASYNC ? "ASYNC" : "SYNC"), message_body_length, sender_shm_name_input,
           receiver_shm_name_input);

    // ====================== 1. 共享内存配置 ======================
    char kSenderShmName[64];
    snprintf(kSenderShmName, sizeof(kSenderShmName), "%s", sender_shm_name_input);

    const size_t kSenderShmSize = g_request_size_in_mb * 1024 * 1024;

    char kReceiverShmName[64];
    snprintf(kReceiverShmName, sizeof(kReceiverShmName), "%s", receiver_shm_name_input);

    const size_t kReceiverShmSize = g_request_size_in_mb * 1024 * 1024;

    const uint8_t kNodes = 2;
    const uint8_t nodeA = 0;
    const uint8_t nodeB = 1;

    // ====================== 2. 固定大小 ======================
    const size_t kRingSizePerType = 1024 * 1024; // 1KB
    const size_t RING_SIZE_PER_TYPE = 2556672;   // 每种消息类型需要的环大小

    // ====================== 3. 计算对齐后的每个 handler 的大小 ======================
    const size_t unaligned_handler_size = kRingSizePerType + (RING_SIZE_PER_TYPE * num_rings);
    const size_t HANDLER_SIZE = align_up(unaligned_handler_size, 64); // 向上对齐到 64 字节

    printf("未对齐的 handler 大小: %zu, 对齐后的 handler 大小: %zu\n", unaligned_handler_size, HANDLER_SIZE);

    // ====================== 4. 计算对齐后的偏移 (发送端) ======================
    const size_t HANDLER_OFFSET = 0;                                     // 0
    const size_t INIT_OFFSET = HANDLER_OFFSET;                           // 0
    const size_t RING_BASE_OFFSET_A = HANDLER_OFFSET + kRingSizePerType; // 1KB

    // ====================== 5. 计算对齐后的偏移 (接收端) ======================
    const size_t RECV_HANDLER_OFFSET = 0;
    const size_t RECV_INIT_OFFSET = RECV_HANDLER_OFFSET;
    const size_t RECV_RING_BASE_OFFSET_B = RECV_HANDLER_OFFSET + kRingSizePerType; // 1KB

    // 检查总大小是否超出分配的空间
    size_t total_required_size = HANDLER_SIZE; // 现在只有 1 个 handler
    if (total_required_size > g_request_size_in_mb * 1024 * 1024) {
        std::cerr << "错误: 需要的共享内存大小 (" << total_required_size << " 字节) 超出了分配的大小 ("
                  << g_request_size_in_mb * 1024 * 1024 << " 字节).\n";
        return -1;
    }
    std::cout << "计算出的总所需大小: " << total_required_size << " 字节 (" << total_required_size / 1024.0 / 1024.0
              << " MB)\n";

    // ====================== 5. 创建+映射两块共享内存 ======================
    if (init_ub_shm() != 0) {
        std::cout << "初始化失败\n";
        return -1;
    }
    std::cout << "初始化成功\n";
    void *sender_shm_base = nullptr;
    if (map_ub_shm(kSenderShmName, sender_shm_base) != 0) {
        std::cout << "发送端共享内存分配失败\n";
        return -1;
    }
    std::cout << "发送端共享内存分配成功\n";

    void *receiver_shm_base = nullptr;
    if (map_ub_shm(kReceiverShmName, receiver_shm_base) != 0) {
        std::cout << "发送端映射接收端共享内存失败\n";
        return -1;
    }
    std::cout << "发送端映射接收端共享内存成功\n";

    // ====================== 6. 计算各 handler 区域地址 (发送端视角) ======================
    void *init_region_A = (char *)sender_shm_base;
    std::cout << "发送端 Handler 引导区地址：" << init_region_A << "\n";

    void *ring_region_A_ptrs = (char *)sender_shm_base + kRingSizePerType;
    void *ring_region_B_ptrs = (char *)receiver_shm_base;
    std::cout << "发送端 Handler 环区A_地址：" << ring_region_A_ptrs << "\n";
    std::cout << "接收端 Handler 环区B_地址：" << ring_region_B_ptrs << "\n";

    // 7. 通信模块配置 (1个实例, 根据flag配置1个或3个ring)
    std::vector<ub_ring_desc_t> ring_descs(num_rings);
    for (int i = 0; i < num_rings; ++i) {
        ring_descs[i].ring_capacity = 64;  // 固定容量
        ring_descs[i].max_msg_size = 1024; // 固定最大消息大小, 与新参数范围匹配
        ring_descs[i].priority = i + 1;    // 根据环索引分配优先级
    }

    ub_comm_conf_t conf;
    conf.max_nodes = kNodes;
    conf.current_node_id = nodeA;
    conf.num_rings = num_rings;
    conf.ring_descs = ring_descs.data();

    // 初始化单个句柄
    ub_shm_comm_t handle = nullptr;
    TestCtx g_ctx = {0};
    g_ctx.handle = &handle;

    // 8. 初始化：传入对应的引导区和环区地址 (1个实例)
    ub_shm_area_t init_area;
    init_area.ptr = init_region_A;
    init_area.size = kRingSizePerType;

    ub_ring_region_info_t ring_region_infos[2]; // 2 个节点

    // Ring i for Node A (reading from A's ring)
    ring_region_infos[0].node_id = nodeA;
    ring_region_infos[0].region.ptr = ring_region_A_ptrs;
    ring_region_infos[0].region.size = kRingSizePerType;
    // Ring i for Node B (writing to B's ring)
    ring_region_infos[1].node_id = nodeB;
    ring_region_infos[1].region.ptr = ring_region_B_ptrs;
    ring_region_infos[1].region.size = kRingSizePerType;

    ub_ring_region_map_t ring_regions;
    ring_regions.entries = ring_region_infos;
    ring_regions.count = 2;

    auto ret = ub_comm_queue_init(&handle, &init_area, &ring_regions, &conf);

    // 根据新标志确定回调类型
    auto reg_ret = ub_comm_queue_register_process_func(&handle, TYPE_DATA_TEST, callback_type, cb_sync_self, &g_ctx);

    std::cout << "等待接收端启动并注册回调...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    if (ret != 0 || reg_ret != 0) {
        std::cerr << "初始化句柄失败: ret=" << ret << "\n";
        if (handle)
            ub_comm_queue_deinit(&handle);
        return -1;
    }
    std::cout << "发送端通信模块初始化成功（Handle），Ring数量: " << num_rings << "\n";

    // ====================== 9. 启动循环发送 ======================

    // 发送给对方节点 (Node B) 的函数
    auto sender_func = [&](int thread_id) {
        std::vector<char> buffer_dynamic(message_body_length, 0); // 根据新参数动态缓冲区

        for (int i = 0; i < num_messages_per_thread; ++i) {
            int base_id = thread_id * num_messages_per_thread * (num_rings == 3 ? 3 : 1) + i * (num_rings == 3 ? 3 : 1);

            if (num_rings == 3) {
                // 发送特定大小到特定环
                for (int j = 0; j < 3; ++j) {
                    int id = base_id + j;
                    TestPayload64 payload64;
                    payload64.id = id;

                    if (sizeof(TestPayload64) <= message_body_length) {
                        std::memcpy(buffer_dynamic.data(), &payload64, sizeof(TestPayload64));
                    } else {
                        fprintf(stderr, "警告: 缓冲区大小 (%d) 小于有效载荷大小。填充零。\n", message_body_length);
                        std::memset(buffer_dynamic.data(), 0, message_body_length);
                        std::memcpy(buffer_dynamic.data(), &payload64, sizeof(payload64.id));
                    }

                    message_t msg;
                    std::memset(&msg, 0, sizeof(msg));
                    msg.header.src_thread_id = std::hash<std::thread::id>()(std::this_thread::get_id());
                    msg.header.body_length = message_body_length; // 使用新参数
                    msg.header.dest_node_id = nodeB;              // 发送到 Node B
                    msg.header.src_node_id = nodeA;
                    msg.header.msg_type = TYPE_DATA_TEST;
                    msg.header.priority = j + 1;      // Ring 0, 1, 2
                    msg.body = buffer_dynamic.data(); // 使用动态缓冲区

                    int send_ret = ub_comm_queue_send(&handle, &msg);
                    if (send_ret < 0) {
                        std::cerr << " 线程 " << thread_id << " 消息 " << i << "-" << message_body_length
                                  << "B (ID:" << id << ", Ring:" << j << ", Priority:" << (j + 1)
                                  << ") 失败: ret=" << send_ret << "\n";
                    }
                }
            } else { // num_rings == 1
                // 发送所有消息到单个环
                int id = base_id + 0;
                TestPayload64 payload64;
                payload64.id = id;

                if (sizeof(TestPayload64) <= message_body_length) {
                    std::memcpy(buffer_dynamic.data(), &payload64, sizeof(TestPayload64));
                } else {
                    fprintf(stderr, "警告: 缓冲区大小 (%d) 小于有效载荷大小 (单环情况)。填充零。\n",
                            message_body_length);
                    std::memset(buffer_dynamic.data(), 0, message_body_length);
                    std::memcpy(buffer_dynamic.data(), &payload64, sizeof(payload64.id));
                }

                message_t msg;
                std::memset(&msg, 0, sizeof(msg));
                msg.header.src_thread_id = std::hash<std::thread::id>()(std::this_thread::get_id());
                msg.header.body_length = message_body_length; // 使用新参数
                msg.header.dest_node_id = nodeB;              // 发送到 Node B
                msg.header.src_node_id = nodeA;
                msg.header.msg_type = TYPE_DATA_TEST;
                msg.header.priority = 1;          // Ring 0 (或唯一环)
                msg.body = buffer_dynamic.data(); // 使用动态缓冲区

                int send_ret = ub_comm_queue_send(&handle, &msg);
                if (send_ret < 0) {
                    std::cerr << " 线程 " << thread_id << " 消息 " << i << "-" << message_body_length << "B (ID:" << id
                              << ", Ring:0, Priority:1) 失败: ret=" << send_ret << "\n";
                }
            }
        }
    };

    // 发送给自己 (Node A) 的函数
    auto sender_func_self = [&](int thread_id) {
        std::vector<char> buffer_dynamic(message_body_length, 0); // 根据新参数动态缓冲区

        for (int i = 0; i < num_messages_per_thread; ++i) {
            int base_id = thread_id * num_messages_per_thread * (num_rings == 3 ? 3 : 1) + i * (num_rings == 3 ? 3 : 1);

            if (num_rings == 3) {
                // 发送特定大小到特定环
                for (int j = 0; j < 3; ++j) {
                    int id = base_id + j;
                    TestPayload64 payload64;
                    payload64.id = id;

                    if (message_body_length >= sizeof(TestPayload64)) {
                        std::memcpy(buffer_dynamic.data(), &payload64, sizeof(TestPayload64));
                    } else {
                        fprintf(stderr, "警告: 缓冲区大小 (%d) 小于有效载荷大小 (自发送)。填充零。\n",
                                message_body_length);
                        std::memset(buffer_dynamic.data(), 0, message_body_length);
                        std::memcpy(buffer_dynamic.data(), &payload64, sizeof(payload64.id));
                    }

                    message_t msg;
                    std::memset(&msg, 0, sizeof(msg));
                    msg.header.src_thread_id = std::hash<std::thread::id>()(std::this_thread::get_id());
                    msg.header.body_length = message_body_length; // 使用新参数
                    msg.header.dest_node_id = nodeA;              // 发送到 Node A (自己)
                    msg.header.src_node_id = nodeA;
                    msg.header.msg_type = TYPE_DATA_TEST;
                    msg.header.priority = j + 1;      // Ring 0, 1, 2
                    msg.body = buffer_dynamic.data(); // 使用动态缓冲区

                    int send_ret = ub_comm_queue_send(&handle, &msg);
                    if (send_ret < 0) {
                        std::cerr << " 线程 " << thread_id << " 消息 " << i << "-" << message_body_length
                                  << "B (ID:" << id << ", Ring:" << j << ", Priority:" << (j + 1)
                                  << ") 失败: ret=" << send_ret << "\n";
                    }
                }
            } else { // num_rings == 1
                // 发送所有消息到单个环
                int id = base_id + 0;
                TestPayload64 payload64;
                payload64.id = id;

                if (sizeof(TestPayload64) <= message_body_length) {
                    std::memcpy(buffer_dynamic.data(), &payload64, sizeof(TestPayload64));
                } else {
                    fprintf(stderr, "警告: 缓冲区大小 (%d) 小于有效载荷大小 (单环情况, 自发送)。填充零。\n",
                            message_body_length);
                    std::memset(buffer_dynamic.data(), 0, message_body_length);
                    std::memcpy(buffer_dynamic.data(), &payload64, sizeof(payload64.id));
                }

                message_t msg;
                std::memset(&msg, 0, sizeof(msg));
                msg.header.src_thread_id = std::hash<std::thread::id>()(std::this_thread::get_id());
                msg.header.body_length = message_body_length; // 使用新参数
                msg.header.dest_node_id = nodeA;              // 发送到 Node A (自己)
                msg.header.src_node_id = nodeA;
                msg.header.msg_type = TYPE_DATA_TEST;
                msg.header.priority = 1;          // Ring 0 (或唯一环)
                msg.body = buffer_dynamic.data(); // 使用动态缓冲区

                int send_ret = ub_comm_queue_send(&handle, &msg);
                if (send_ret < 0) {
                    std::cerr << " 线程 " << thread_id << " 消息 " << i << "-" << message_body_length << "B (ID:" << id
                              << ", Ring:0, Priority:1) 失败: ret=" << send_ret << "\n";
                }
            }
        }
    };

    std::cout << "开始向对方节点 (Node B) 循环发送----------------\n";
    std::vector<std::thread> threads_to_other;
    threads_to_other.reserve(num_threads_per_round);

    for (int t = 0; t < num_threads_per_round; ++t) {
        threads_to_other.emplace_back(sender_func, t);
    }

    // 等待所有发送给对方的线程完成
    for (auto &th : threads_to_other) {
        if (th.joinable()) {
            th.join();
        }
    }
    std::cout << "向对方节点 (Node B) 发送完成----------------\n";

    // 根据标志条件执行自发送逻辑
    if (enable_self_send) {
        std::cout << "开始向本节点 (Node A) 循环发送----------------\n";
        std::vector<std::thread> threads_to_self;
        threads_to_self.reserve(num_threads_per_round);

        for (int t = 0; t < num_threads_per_round; ++t) {
            threads_to_self.emplace_back(sender_func_self, t);
        }

        // 等待所有自发送线程完成
        for (auto &th : threads_to_self) {
            if (th.joinable()) {
                th.join();
            }
        }
        std::cout << "向本节点 (Node A) 发送完成----------------\n";
    } else {
        std::cout << "跳过向本节点发送，因为 self-send 已禁用。\n";
    }

    // ====================== 10. 等待+清理 ======================
    std::this_thread::sleep_for(3s);

    ub_comm_queue_deinit(&handle);

    std::cout << "发送端程序正常退出\n";
    return 0;
}