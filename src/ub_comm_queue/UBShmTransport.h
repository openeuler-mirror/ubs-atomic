/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * rmrs is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef UBSHM_TRANSPORT_H
#define UBSHM_TRANSPORT_H

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "MPSCRingBuffer.h"
#include "ThreadPool.h"
#include "ub_atomic_log_print.h"
#include "ub_comm_errno.h"
#include "ub_dist_comm_queue.h"

namespace ub_comm_queue {

// --- 常量定义 ---
constexpr uint8_t MAX_PRIORITY_LEVELS = 8;
constexpr uint8_t MAX_NODES_LIMIT = 8;

// 内置分布式锁环配置
constexpr uint8_t LOCK_RING_PRIORITY = 0;
constexpr uint32_t LOCK_RING_CAPACITY = 4096;
constexpr uint32_t LOCK_RING_MSG_SIZE = 128;
constexpr uint8_t MSG_TYPE_DIST_LOCK = 0xFF;

// 遗言消息类型
constexpr uint8_t MSG_TYPE_SYS_PEER_EXIT = 0xFE;
// 流控配置更新消息类型
constexpr uint8_t MSG_TYPE_SYS_FLOW_CONFIG_UPDATE = 0xFD;

// 工作线程池占总cpu比例
constexpr double WORKER_POOL_RATIO = 0.2;
constexpr size_t WORKER_POOL_MIN_SIZE = 4;

// --- 内部共享内存结构 (公告牌) ---
struct alignas(CACHELINE_SIZE) NodeBoardInfo {
    std::atomic<bool> initialized;
    // 存储相对于 ring_region_ptr 的偏移量 (UINT64_MAX 表示无效)
    std::atomic<uint64_t> ring_offsets[MAX_PRIORITY_LEVELS];
    std::atomic<uint64_t> consumer_heartbeat_seq;
    std::atomic<uint32_t> heartbeat_interval_ms;
    // dummy 写cache_probe_pad， 用于强制更新结构体
    volatile uint64_t cache_probe_pad;
};

struct Billboard {
    NodeBoardInfo nodes[MAX_NODES_LIMIT];
};

class UBShmTransport {
public:
    struct CallbackInfo {
        ub_func_type_t type;
        ub_callback_t func;
        void *ctx;
    };

    struct cb_info_t {
        ub_callback_t func = nullptr; // 默认置空，用于判断是否已注册
        void *ctx = nullptr;
        ub_func_type_t type = UB_FUNC_SYNC;
        // 默认构造函数
        cb_info_t() : func(nullptr), ctx(nullptr), type(UB_FUNC_SYNC) {}

        // 带参构造
        cb_info_t(ub_callback_t f, void *c, ub_func_type_t t) : func(f), ctx(c), type(t) {}
    };

    struct RemoteRingCache {
        // 状态标记
        std::atomic<bool> initialized{false};

        // 缓存的常量 (从远端读一次后就不变了)
        MPSCRingBuffer *raw_ptr{nullptr}; // 远端对象的指针 (用于计算 tail 地址)
        uint64_t mask{0};
        uint64_t stride{0};
        uint64_t max_size{0};

        // 运行时影子变量
        std::atomic<uint64_t> shadow_head{0};
        std::atomic<uint32_t> cached_threshold{0};
        std::atomic<uint64_t> cached_threshold_version{0};

        // 填充至 CacheLine (64B) 避免伪共享 (False Sharing)
        char padding[64];
    };

    UBShmTransport();
    ~UBShmTransport();

    // 初始化: 起线程池，构建本地环，发布公告牌，准备路由表，起分发线程
    int init(const ub_shm_area_t *init_area, const ub_ring_region_map_t *ring_map, const ub_comm_conf_t *conf);

    // 注册回调
    int register_func(uint8_t msg_type, ub_func_type_t func_type, ub_callback_t func, void *ctx);
    int register_func_for_lock(uint8_t msg_type, ub_func_type_t func_type, ub_callback_t func, void *ctx);

    // 发送
    int send(const message_t *msg);

    // 接收
    int recv(void *buffer, size_t capacity);
    int get_status(uint8_t node_id, uint8_t priority, ub_comm_queue_status_t *status);
    int set_congestion_threshold(uint8_t priority, uint32_t congestion_threshold_percent);
    int config_heartbeat(const ub_comm_queue_heartbeat_config_t *request, ub_comm_queue_heartbeat_config_t *effective);

    void remove_node_cache(uint32_t node_id);
    void update_cached_congestion_threshold(uint32_t node_id, uint8_t priority, uint32_t threshold, uint64_t version);

    // 锁实例相关
    bool get_is_for_lock() const;
    int set_is_for_lock(bool is_for_lock);
    bool query_inited(const uint8_t node_id);

private:
    // --- 内部辅助流程函数 (Init Pipeline) ---

    // 1. 基础校验与配置拷贝
    int setup_config_and_validate(const ub_shm_area_t *init_area, const ub_ring_region_map_t *ring_map,
                                  const ub_comm_conf_t *conf);

    // 2. 构建节点 ID 映射 (稀疏 ID -> 紧凑 Index)
    int build_node_mapping(const ub_ring_region_map_t *ring_map);

    // 3. 内存容量预检 (Fail Fast)
    int check_memory_capacity(const ub_shm_area_t *init_area);

    // 4. 构建内存基址表
    int build_region_bases(const ub_ring_region_map_t *ring_map);

    // 5. 深拷贝 Ring 描述符
    int deep_copy_ring_descs();

    // 6. 初始化线程池
    void init_thread_pool();

    // 7. 创建本地环 (含锁环注入逻辑)
    int create_local_rings(std::vector<uint64_t> &out_offsets);

    // 8. 发布到公告牌
    int publish_to_billboard(const std::vector<uint64_t> &offsets);

    // 9. 等待集群就绪
    void wait_for_cluster_ready();

    // 10. 预热远程表
    void preload_remote_table();

    // 11. 启动分发线程
    void start_dispatcher();
    void start_reliability_threads();
    void stop_reliability_threads();
    void run_consumer_heartbeat_loop();
    void run_producer_heartbeat_monitor();
    void refresh_local_consumer_heartbeat();

    // 获取远程环 (包含 ID 映射查找)
    int get_remote_ring(uint32_t node_id, uint32_t priority, MPSCRingBuffer **out_ring);

    // ID 映射辅助
    int32_t get_compact_index(uint32_t node_id) const;

    // 分发线程逻辑
    void run_dispatcher_loop();
    int poll_and_dispatch_once(std::vector<char> &recv_buffer);
    int dispatch_internal(const void *data, uint32_t len);

    // 初始化远程环缓存
    int try_populate_cache(uint32_t node_id, uint32_t prio);

    // 去初始化：伪造满 -> 清公告牌 -> 广播删除消息 -> 删缓存
    void deinit_and_broadcast();
    void broadcast_flow_config_update(uint8_t priority, uint32_t threshold, uint64_t version);

private:
    uint64_t instance_id_;
    // 默认 false
    bool init_complete_ = false;
    // 读写锁：保护 caches 和 lookup_table 的并发访问
    std::shared_mutex cache_mutex_;
    // 配置与状态
    ub_comm_conf_t conf_;
    // Ring 描述符深拷贝存储
    std::vector<ub_ring_desc_t> ring_descs_storage_;
    // 引导区地址
    char *init_region_ptr_;
    // 本节点环区基址
    char *ring_region_ptr_;

    // 回调与线程池
    // 业务处理线程池大小 (不包含分发线程)
    uint32_t num_threads;
    // 异步任务最大积压数
    uint32_t max_async_queue_len;
    // 积压满时的超时等待 (微秒)
    uint32_t async_wait_timeout_us;
    std::array<cb_info_t, 256> callbacks_;
    mutable std::shared_mutex cb_mu_;
    ThreadPool *worker_pool_;
    std::atomic<uint32_t> active_async_tasks_;

    // ID 映射表
    std::unordered_map<uint32_t, uint32_t> node_id_to_idx_; // NodeID -> CompactIndex
    std::vector<uint32_t> idx_to_node_id_;                  // CompactIndex -> NodeID

    // 内存视图 (下标为 Compact Index)
    std::vector<char *> region_bases_;
    std::vector<size_t> region_sizes_;

    // 本地环引用 (Index = Physical Priority)
    // 0: Lock Ring, 1+: User Rings
    std::array<MPSCRingBuffer *, MAX_PRIORITY_LEVELS> local_rings_;
    // 记录已激活的优先级，用于轮询优化
    std::vector<uint32_t> local_active_priorities_;

    // 远程路由表 [CompactIndex][Priority]
    std::vector<std::vector<MPSCRingBuffer *>> remote_lookup_table_;
    // 远程环缓存
    RemoteRingCache ring_caches_[MAX_NODES_LIMIT][MAX_PRIORITY_LEVELS];
    // 分发线程绑定的CPU ID
    int cpu_id_;

    // 专用的分发线程
    std::thread dispatcher_thread_;
    std::thread consumer_heartbeat_thread_;
    std::thread producer_heartbeat_thread_;
    // 停止标志
    std::atomic<bool> stop_flag_;
    std::atomic<bool> reliability_stop_flag_;
    std::array<std::atomic<bool>, MAX_NODES_LIMIT> peer_alive_;
    std::array<std::atomic<uint64_t>, MAX_NODES_LIMIT> peer_heartbeat_seq_;
    std::array<std::atomic<uint64_t>, MAX_NODES_LIMIT> peer_heartbeat_seen_us_;
    std::array<std::atomic<uint64_t>, MAX_NODES_LIMIT> peer_heartbeat_timeout_us_;
    std::atomic<uint64_t> local_consumer_heartbeat_seq_;
    std::atomic<uint64_t> heartbeat_interval_us_;
    std::atomic<uint64_t> heartbeat_check_interval_us_;
    std::atomic<uint64_t> heartbeat_timeout_us_;
    uint32_t max_msg_size_global_;
    // 是否为锁实例
    bool is_for_lock;
};

} // namespace ub_comm_queue

#endif // UBSHM_TRANSPORT_H
