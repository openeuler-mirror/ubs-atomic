/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "UBShmTransport.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <new>
#include <set>

namespace ub_comm_queue {
static constexpr uint32_t DEFAULT_HEARTBEAT_INTERVAL_MS = 100;
static constexpr uint32_t DEFAULT_HEARTBEAT_CHECK_INTERVAL_MS = 100;
static constexpr uint32_t DEFAULT_HEARTBEAT_TIMEOUT_MS = 1000;
static constexpr uint64_t US_PER_MS = 1000ULL;

UBShmTransport::UBShmTransport()
    : conf_({}),
      init_region_ptr_(nullptr),
      ring_region_ptr_(nullptr),
      worker_pool_(nullptr),
      max_msg_size_global_(0),
      active_async_tasks_(0),
      async_wait_timeout_us(0),
      stop_flag_(false),
      reliability_stop_flag_(false),
      local_consumer_heartbeat_seq_(0),
      heartbeat_interval_us_(DEFAULT_HEARTBEAT_INTERVAL_MS * US_PER_MS),
      heartbeat_check_interval_us_(DEFAULT_HEARTBEAT_CHECK_INTERVAL_MS * US_PER_MS),
      heartbeat_timeout_us_(DEFAULT_HEARTBEAT_TIMEOUT_MS * US_PER_MS)
{
    num_threads =
        std::max(static_cast<size_t>(std::thread::hardware_concurrency() * WORKER_POOL_RATIO), WORKER_POOL_MIN_SIZE);
    max_async_queue_len = num_threads;
    local_rings_.fill(nullptr);
    for (auto &alive : peer_alive_) {
        alive.store(true, std::memory_order_relaxed);
    }
    for (auto &seq : peer_heartbeat_seq_) {
        seq.store(0, std::memory_order_relaxed);
    }
    for (auto &seen : peer_heartbeat_seen_us_) {
        seen.store(0, std::memory_order_relaxed);
    }
    for (auto &timeout : peer_heartbeat_timeout_us_) {
        timeout.store(DEFAULT_HEARTBEAT_TIMEOUT_MS * US_PER_MS, std::memory_order_relaxed);
    }
}

UBShmTransport::~UBShmTransport()
{
    deinit_and_broadcast();
}

// ===========================================================================
// 写侧专用 (Non-Temporal Store)
// 防止 NC 崩溃，强制 CC 落盘。
// ===========================================================================
inline void ub_nt_store64(volatile uint64_t *addr, uint64_t val)
{
#if defined(__aarch64__) || defined(__arm__)
    asm volatile("stlr %0, [%1]" ::"r"(val), "r"(addr) : "memory");
#else
    reinterpret_cast<std::atomic<uint64_t> *>(const_cast<uint64_t *>(addr))->store(val, std::memory_order_release);
#endif
}

inline void ub_nt_store8(volatile void *addr, uint8_t val)
{
#if defined(__aarch64__) || defined(__arm__)
    // 8-bit 使用 stlrb (Store-Release Byte)，配合 sfence 足够安全
    asm volatile("stlrb %w0, [%1]" ::"r"((uint32_t)val), "r"(addr) : "memory");
#else
    reinterpret_cast<std::atomic<uint8_t> *>(const_cast<void *>(addr))->store(val, std::memory_order_release);
#endif
}

inline void force_refresh_whole_struct(NodeBoardInfo *node)
{
    // -----------------------------------------------------------
    // 1. 刷新 Cache Line 0 (0-63 bytes)
    //    包含: initialized, ring_offsets[0]~[6]
    // -----------------------------------------------------------
    //   使用 CAS (Compare-And-Swap) 写入 true
    // - 如果当前内存已经是 true：写入 true -> 触发 Write-Allocate -> 刷新 Line 0
    // - 如果当前内存是 false：CAS 失败 -> 不写内存 -> 安全 (反正也没 Ready)
    bool expected = true;
    node->initialized.compare_exchange_strong(expected, true, std::memory_order_relaxed);

    // -----------------------------------------------------------
    // 2. 刷新 Cache Line 1 (64-127 bytes)
    //    包含: ring_offsets[7], cache_probe_pad
    // -----------------------------------------------------------
    //   直接写 Padding
    // - NC 内存：安全写入
    // - CC 内存：触发 Write-Allocate -> 刷新 Line 1
    node->cache_probe_pad = 0;

    // -----------------------------------------------------------
    // 3. 屏障
    // -----------------------------------------------------------
    // 确保上述两个伪写操作在读取之前执行完毕
    arm_sfence();
}

static uint64_t generate_instance_id()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    // 组合秒和纳秒，保证单调递增
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static uint64_t steady_time_us()
{
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

struct FlowConfigUpdateMessage {
    uint64_t version;
    uint32_t threshold;
    uint8_t priority;
};
static_assert(sizeof(FlowConfigUpdateMessage) <= LOCK_RING_MSG_SIZE - sizeof(message_header_t),
              "Flow config update body is too large for lock ring");

void on_flow_config_update(const message_t *msg, void *ctx)
{
    if (msg == nullptr || ctx == nullptr || msg->body == nullptr ||
        msg->header.body_length < sizeof(FlowConfigUpdateMessage)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid flow config update message.");
        return;
    }

    auto *self = static_cast<UBShmTransport *>(ctx);
    auto *body = reinterpret_cast<const FlowConfigUpdateMessage *>(msg->body);
    self->update_cached_congestion_threshold(msg->header.src_node_id, body->priority, body->threshold, body->version);
}

// 回调实现
void on_peer_exit(const message_t *msg, void *ctx)
{
    UBShmTransport *self = (UBShmTransport *)ctx;
    uint32_t dead_node = msg->header.src_node_id;
    ATOMIC_LOG(LOG_LEVEL_WARN, "Peer %u went down. Cleaning caches.", dead_node);
    self->remove_node_cache(dead_node);
}

void UBShmTransport::remove_node_cache(uint32_t node_id)
{
    // 加写锁 (保护 ring_caches_ 的并发访问)
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);

    for (int p = 0; p < MAX_PRIORITY_LEVELS; ++p) {
        // TODO: ring_caches_ 这里仍然直接使用逻辑 node_id 作为数组下标。
        // 如果后续要支持稀疏 node_id，需要统一收敛到 compact index，再整体调整这条访问链路。
        auto &cache = ring_caches_[node_id][p];
        int32_t idx = get_compact_index(node_id);
        cache.raw_ptr = nullptr;
        cache.shadow_head.store(0, std::memory_order_relaxed);
        cache.cached_threshold.store(0, std::memory_order_relaxed);
        cache.cached_threshold_version.store(0, std::memory_order_relaxed);
        cache.initialized.store(false, std::memory_order_release);
        remote_lookup_table_[idx][p] = nullptr;
    }
}

void UBShmTransport::update_cached_congestion_threshold(uint32_t node_id, uint8_t priority, uint32_t threshold,
                                                        uint64_t version)
{
    if (priority == LOCK_RING_PRIORITY || priority >= MAX_PRIORITY_LEVELS) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "Ignore invalid flow config update, node=%u priority=%u", node_id, priority);
        return;
    }
    if (get_compact_index(node_id) < 0) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "Ignore flow config update from unknown node=%u", node_id);
        return;
    }

    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    auto &cache = ring_caches_[node_id][priority];
    uint64_t cached_version = cache.cached_threshold_version.load(std::memory_order_relaxed);
    if (version <= cached_version) {
        return;
    }

    cache.cached_threshold.store(threshold, std::memory_order_relaxed);
    cache.cached_threshold_version.store(version, std::memory_order_release);
    ATOMIC_LOG(LOG_LEVEL_INFO,
               "Updated cached flow threshold from control message, node=%u priority=%u threshold=%u version=%llu",
               node_id, priority, threshold, version);
}

// =========================================================
// 主初始化流程 (Pipeline)
// =========================================================
int UBShmTransport::init(const ub_shm_area_t *init_area, const ub_ring_region_map_t *ring_map,
                         const ub_comm_conf_t *conf)
{
    instance_id_ = generate_instance_id();
    ATOMIC_LOG(LOG_LEVEL_INFO, "Initializing UBShmTransport...");
    int ret = UB_COMM_OK;

    // 1. 基础校验 & 配置拷贝
    if ((ret = setup_config_and_validate(init_area, ring_map, conf)) != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to setup config and validate: %d", ret);
        return ret;
    }
    // 2. 构建 NodeID 映射 (处理稀疏 ID)
    if ((ret = build_node_mapping(ring_map)) != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to build node mapping: %d", ret);
        return ret;
    }

    // 3. 构建内存基址表
    if ((ret = build_region_bases(ring_map)) != 0)
        return ret;

    // 4. 内存容量预检 (Fail Fast)
    if ((ret = check_memory_capacity(init_area)) != 0)
        return ret;

    // 5. 深拷贝配置
    if ((ret = deep_copy_ring_descs()) != 0)
        return ret;

    // 6. 初始化线程池
    init_thread_pool();

    // 7. 创建本地环 (含锁环逻辑)
    std::vector<uint64_t> offsets(MAX_PRIORITY_LEVELS, UINT64_MAX);
    if ((ret = create_local_rings(offsets)) != 0)
        return ret;

    // 8. 发布到公告牌
    if ((ret = publish_to_billboard(offsets)) != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to publish to billboard");
        return ret;
    }
    register_func(MSG_TYPE_SYS_PEER_EXIT, UB_FUNC_SYNC, on_peer_exit, this);
    register_func(MSG_TYPE_SYS_FLOW_CONFIG_UPDATE, UB_FUNC_SYNC, on_flow_config_update, this);
    start_reliability_threads();

    // 9. 等待集群就绪
    wait_for_cluster_ready();

    // 10. 预热远程表
    preload_remote_table();

    // 11. 启动分发线程
    start_dispatcher();
    init_complete_ = true;

    ATOMIC_LOG(LOG_LEVEL_INFO, "UBShmTransport initialized successfully. Instance ID: %llu", instance_id_);
    return 0;
}

// =========================================================
// 内部辅助函数实现
// =========================================================

// 1. 基础校验
int UBShmTransport::setup_config_and_validate(const ub_shm_area_t *init_area, const ub_ring_region_map_t *ring_map,
                                              const ub_comm_conf_t *conf)
{
    // -------------------------------------------------------------------------
    // Level 1: 基础指针检查 (防止直接 Core Dump)
    // -------------------------------------------------------------------------
    if (!init_area || !ring_map || !conf) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Validation Failed: Top-level arguments cannot be NULL.");
        return -EINVAL;
    }
    if (!init_area->ptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Validation Failed: Init area pointer (Billboard) is NULL.");
        return -EINVAL;
    }
    if (!ring_map->entries) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Validation Failed: Ring map entries array is NULL.");
        return -EINVAL;
    }
    // -------------------------------------------------------------------------
    // Level 2: 配置参数合规性 (防止逻辑错误)
    // -------------------------------------------------------------------------
    if (conf->max_nodes == 0 || conf->max_nodes > MAX_NODES_LIMIT) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Validation Failed: Invalid max_nodes: %u (Limit: %u)", conf->max_nodes,
                   MAX_NODES_LIMIT);
        return -EINVAL;
    }
    if (init_area->size < sizeof(Billboard)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Init region too small: %u < %zu", init_area->size, sizeof(Billboard));
        return -EINVAL;
    }
    if (!init_area->ptr || !ring_map->entries) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: pointers inside structs are NULL.");
        return -EINVAL;
    }
    if (ring_map->count == 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: Ring map is empty.");
        return -EINVAL;
    }
    if (ring_map->count > MAX_PRIORITY_LEVELS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: Ring map exceeds max priority levels.");
        return -EINVAL;
    }
    for (uint32_t i = 0; i < ring_map->count; ++i) {
        const auto &entry = ring_map->entries[i];
        if (entry.region.ptr == nullptr) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: Invalid ring map entry at index %u: region.ptr is NULL", i);
            return -EINVAL;
        }
    }
    if (conf->max_nodes < 1 || conf->max_nodes > MAX_NODES_LIMIT) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: max_nodes must be between 1 and %u", MAX_NODES_LIMIT);
        return -EINVAL;
    }
    if (conf->num_rings < 1 || conf->num_rings > MAX_PRIORITY_LEVELS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: num_rings must be between 1 and %u", MAX_PRIORITY_LEVELS);
        return -EINVAL;
    }
    if (conf->ring_descs == nullptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: ring_descs must not be NULL.");
        return -EINVAL;
    }
    if (conf->cpu_id < 0) {
        cpu_id_ = -1;
    } else {
        if (!check_cpu_id_valid(conf->cpu_id)) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid arguments: cpu_id is out of range.");
            return -EINVAL;
        } else {
            cpu_id_ = conf->cpu_id;
        }
    }

    init_region_ptr_ = static_cast<char *>(init_area->ptr);
    conf_ = *conf;
    return 0;
}

// 2. 构建映射表 (NodeID <-> Index)
int UBShmTransport::build_node_mapping(const ub_ring_region_map_t *ring_map)
{
    std::set<uint32_t> all_ids;
    for (uint32_t i = 0; i < ring_map->count; ++i) {
        all_ids.insert(ring_map->entries[i].node_id);
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "Add Node ID: %u", ring_map->entries[i].node_id);
    }
    // 限制检查
    if (all_ids.size() > MAX_NODES_LIMIT || all_ids.size() != conf_.max_nodes) {
        ATOMIC_LOG(LOG_LEVEL_ERROR,
                   "Invalid arguments: Node count %u exceeds limit %u or does not match configured max_nodes %u",
                   all_ids.size(), MAX_NODES_LIMIT, conf_.max_nodes);
        return -EINVAL;
    }

    // 排序保证所有节点视角一致
    std::vector<uint32_t> sorted_ids(all_ids.begin(), all_ids.end());
    std::sort(sorted_ids.begin(), sorted_ids.end());

    node_id_to_idx_.clear();
    idx_to_node_id_ = sorted_ids;

    for (uint32_t i = 0; i < sorted_ids.size(); ++i) {
        node_id_to_idx_[sorted_ids[i]] = i;
    }

    if (node_id_to_idx_.find(conf_.current_node_id) == node_id_to_idx_.end()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Current node ID %u not found in ring map!", conf_.current_node_id);
        return -EINVAL;
    }

    return 0;
}

int32_t UBShmTransport::get_compact_index(uint32_t node_id) const
{
    auto it = node_id_to_idx_.find(node_id);
    return (it != node_id_to_idx_.end()) ? (int32_t)it->second : -1;
}

// 3. 构建基址表
int UBShmTransport::build_region_bases(const ub_ring_region_map_t *ring_map)
{
    size_t num_nodes = idx_to_node_id_.size();
    region_bases_.assign(num_nodes, nullptr);
    region_sizes_.assign(num_nodes, 0);

    for (uint32_t i = 0; i < ring_map->count; ++i) {
        const auto &entry = ring_map->entries[i];
        int32_t idx = get_compact_index(entry.node_id);

        if (idx >= 0) {
            region_bases_[idx] = static_cast<char *>(entry.region.ptr);
            region_sizes_[idx] = entry.region.size;
        }
    }

    // 设置本节点指针
    int32_t my_idx = get_compact_index(conf_.current_node_id);
    ring_region_ptr_ = region_bases_[my_idx];

    return 0;
}

// 4. 内存容量预检
int UBShmTransport::check_memory_capacity(const ub_shm_area_t *init_area)
{
    // A. 检查 Billboard
    if (init_area->size < sizeof(Billboard))
        return -EINVAL;

    // B. 检查本节点环区
    int32_t my_idx = get_compact_index(conf_.current_node_id);
    size_t my_limit = region_sizes_[my_idx];

    size_t total_needed = 0;
    uintptr_t curr_addr = reinterpret_cast<uintptr_t>(ring_region_ptr_);

    // B.1 锁环需求
    {
        size_t pad = (CACHELINE_SIZE - (curr_addr % CACHELINE_SIZE)) % CACHELINE_SIZE;
        size_t size = MPSCRingBuffer::CalculateMemorySize(LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE);
        total_needed += pad + size;
        curr_addr += pad + size;
    }

    // B.2 用户环需求
    for (uint32_t i = 0; i < conf_.num_rings; ++i) {
        const auto &desc = conf_.ring_descs[i];
        if (desc.priority == 0 || desc.priority >= MAX_PRIORITY_LEVELS) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Ring priority %u exceeds limit %u or = 0", desc.priority, MAX_PRIORITY_LEVELS);
            return -EINVAL;
        }
        if (desc.ring_capacity == 0 || (desc.ring_capacity & (desc.ring_capacity - 1)) != 0) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Ring capacity must be greater than 0 and must be a power of 2");
            return -EINVAL;
        }
        if (desc.max_msg_size < sizeof(message_t)) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Max message size must be at least %zu Bytes", sizeof(message_t));
            return -EINVAL;
        }

        size_t pad = (CACHELINE_SIZE - (curr_addr % CACHELINE_SIZE)) % CACHELINE_SIZE;
        if (pad > 0) {
            ATOMIC_LOG(LOG_LEVEL_DEBUG, "Padding user ring %u by %zu bytes", i, pad);
        }
        size_t size = MPSCRingBuffer::CalculateMemorySize(desc.ring_capacity, desc.max_msg_size);
        total_needed += pad + size;
        curr_addr += pad + size;
    }

    if (my_limit < total_needed) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Ring region OOM! Needed: %zu, Has: %zu", total_needed, my_limit);
        return -ENOMEM;
    }
    return UB_COMM_OK;
}

// 5. 深拷贝配置
int UBShmTransport::deep_copy_ring_descs()
{
    if (conf_.num_rings > 0 && conf_.ring_descs) {
        try {
            ring_descs_storage_.assign(conf_.ring_descs, conf_.ring_descs + conf_.num_rings);
            conf_.ring_descs = ring_descs_storage_.data();
        } catch (const std::bad_alloc &) {
            return -ENOMEM;
        } catch (...) {
            return -ENOMEM;
        }
    } else {
        conf_.num_rings = 0;
    }
    return UB_COMM_OK;
}

// 6. 线程池
void UBShmTransport::init_thread_pool()
{
    ATOMIC_LOG(LOG_LEVEL_INFO, "Initializing thread pool with %u threads", num_threads);
    if (num_threads > 0) {
        worker_pool_ = new ThreadPool(num_threads);
    } else {
        worker_pool_ = nullptr;
    }
    active_async_tasks_.store(0);
}

// 7. 创建本地环
int UBShmTransport::create_local_rings(std::vector<uint64_t> &out_offsets)
{
    char *current_mem_addr = ring_region_ptr_;

    // 初始化最大消息大小全局变量
    max_msg_size_global_ = 0;

    auto align_addr = [&](char *&addr) {
        uintptr_t raw = reinterpret_cast<uintptr_t>(addr);
        size_t pad = (CACHELINE_SIZE - (raw % CACHELINE_SIZE)) % CACHELINE_SIZE;
        addr += pad;
    };

    // --- A. 创建内置锁环 (Priority 0) ---
    {
        align_addr(current_mem_addr);

        // 预检
        size_t size = MPSCRingBuffer::CalculateMemorySize(LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE);
        try {
            volatile char *p = current_mem_addr;
            *p = 0;
            *(p + size - 1) = 0;
        } catch (...) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to access shared memory at %p", (void *)current_mem_addr);
            return -EFAULT;
        }

        MPSCRingBuffer *ring = new (current_mem_addr)
            MPSCRingBuffer(reinterpret_cast<uint8_t *>(current_mem_addr), LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE);

        local_rings_[LOCK_RING_PRIORITY] = ring;
        local_active_priorities_.push_back(LOCK_RING_PRIORITY);
        out_offsets[LOCK_RING_PRIORITY] = static_cast<uint64_t>(current_mem_addr - ring_region_ptr_);

        current_mem_addr += size;
        if (LOCK_RING_MSG_SIZE > max_msg_size_global_)
            max_msg_size_global_ = LOCK_RING_MSG_SIZE;

        ATOMIC_LOG(
            LOG_LEVEL_INFO,
            "Created built-in LOCK ring at Priority 0, Capacity: %u, Max Msg Size: %u Bytes,  Total Size: %zu Bytes",
            LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE, size);
    }

    // --- B. 创建用户环 ---
    if (conf_.num_rings == 0) {
        ATOMIC_LOG(LOG_LEVEL_INFO, "No user-defined rings to create.");
    }
    for (uint32_t i = 0; i < conf_.num_rings; ++i) {
        const auto &desc = conf_.ring_descs[i];

        // 校验：用户不能使用 Prio 0
        if (desc.priority == 0) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "User defined ring priority cannot be 0 (Reserved for Lock)");
            return -EINVAL;
        }
        if (desc.priority >= MAX_PRIORITY_LEVELS) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Ring priority %u exceeds limit %u", desc.priority, MAX_PRIORITY_LEVELS);
            return -EINVAL;
        }
        for (const auto &active_priority : local_active_priorities_) {
            if (active_priority == desc.priority) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "Ring priority %u is already in use.", desc.priority);
                return -EEXIST;
            }
        }

        align_addr(current_mem_addr);

        size_t size = MPSCRingBuffer::CalculateMemorySize(desc.ring_capacity, desc.max_msg_size);
        try {
            volatile char *p = current_mem_addr;
            *p = 0;
            *(p + size - 1) = 0;
        } catch (...) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to access shared memory at %p", (void *)current_mem_addr);
            return -EFAULT;
        }
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "Creating ring buffer [Prio: %u, Cap: %u, Size: %zu, Addr: %p]", desc.priority,
                   desc.ring_capacity, size, (void *)current_mem_addr);
        MPSCRingBuffer *ring = new (current_mem_addr)
            MPSCRingBuffer(reinterpret_cast<uint8_t *>(current_mem_addr), desc.ring_capacity, desc.max_msg_size);

        // 用户配置 Prio N -> 物理 Prio N (直接对应)
        local_rings_[desc.priority] = ring;
        local_active_priorities_.push_back(desc.priority);

        // 计算 Offset 并保存到出参
        out_offsets[desc.priority] = static_cast<uint64_t>(current_mem_addr - ring_region_ptr_);

        current_mem_addr += size;
        if (desc.max_msg_size > max_msg_size_global_) {
            max_msg_size_global_ = desc.max_msg_size;
        }

        ATOMIC_LOG(LOG_LEVEL_INFO,
                   "Created user ring at Priority %u, Capacity: %u, Max Msg Size: %u Bytes,  Total Size: %zu Bytes",
                   desc.priority, desc.ring_capacity, desc.max_msg_size, size);
    }

    // 增加 Global Buffer 余量（有可能越界）
    max_msg_size_global_ += sizeof(message_header_t) + 64;
    return UB_COMM_OK;
}

// 8. 发布
int UBShmTransport::publish_to_billboard(const std::vector<uint64_t> &offsets)
{
    // [基础检查]
    if ((uintptr_t)init_region_ptr_ % CACHELINE_SIZE != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Init region must be 64-byte aligned!");
        return -EINVAL;
    }
    int32_t my_idx = get_compact_index(conf_.current_node_id);
    if (my_idx < 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to get compact index for current node ID: %d", conf_.current_node_id);
        return UB_COMM_ERR_PEER_NODE_NOT_FOUND;
    }

    Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);
    NodeBoardInfo &my_info = board->nodes[my_idx];

    // ============================================================
    // 阶段 1: 初始化无效值
    // ============================================================
    for (int p = 0; p < MAX_PRIORITY_LEVELS; ++p) {
        // 使用 Non-Temporal Store 写入 UINT64_MAX
        ub_nt_store64((volatile uint64_t *)&my_info.ring_offsets[p], UINT64_MAX);
    }
    my_info.consumer_heartbeat_seq.store(0, std::memory_order_relaxed);
    my_info.heartbeat_interval_ms.store(
        static_cast<uint32_t>(heartbeat_interval_us_.load(std::memory_order_relaxed) / US_PER_MS),
        std::memory_order_relaxed);
    // 屏障：确保无效值写入完成
    arm_sfence();

    // ============================================================
    // 阶段 2: 写入有效的 Offset
    // ============================================================
    bool has_update = false;
    for (int p = 0; p < MAX_PRIORITY_LEVELS; ++p) {
        if (offsets[p] != UINT64_MAX) {
            // 使用 Non-Temporal Store 写入实际 Offset
            ub_nt_store64((volatile uint64_t *)&my_info.ring_offsets[p], offsets[p]);
            ATOMIC_LOG(LOG_LEVEL_DEBUG, "Updated ring offset for priority %d: %llu", p, offsets[p]);
            has_update = true;
        }
    }

    if (has_update) {
        // 屏障：确保数据落盘，防止 Ready 标志先被看到而数据没到
        arm_sfence();
    }
    uint64_t seq = local_consumer_heartbeat_seq_.fetch_add(1, std::memory_order_relaxed) + 1;
    my_info.consumer_heartbeat_seq.store(seq, std::memory_order_release);

    // ============================================================
    // 阶段 3: 标记 Ready
    // ============================================================
    // 写入 true (1)
    ub_nt_store8((volatile void *)&my_info.initialized, 1);
    ATOMIC_LOG(LOG_LEVEL_DEBUG, "Publish Node %d initialized flag: %d", conf_.current_node_id,
               my_info.initialized.load(std::memory_order_relaxed));

    // 屏障：确保 Ready 标志对全网可见
    arm_sfence();
    return UB_COMM_OK;
}

// 9. 等待
void UBShmTransport::wait_for_cluster_ready()
{
    using namespace std::chrono;
    const milliseconds kTimeout(10000);
    auto deadline = steady_clock::now() + kTimeout;

    Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);

    // 遍历 idx (0 ~ N-1)
    for (uint32_t idx = 0; idx < idx_to_node_id_.size(); ++idx) {
        uint32_t real_id = idx_to_node_id_[idx];
        if (real_id == conf_.current_node_id)
            continue;

        bool ready = false;
        while (steady_clock::now() < deadline) {
            // [步骤 1] 刷新 Line 0 来检查 initialized
            bool expected = true;
            board->nodes[idx].initialized.compare_exchange_strong(expected, true, std::memory_order_relaxed);
            arm_sfence();

            // [步骤 2] 检查是否 Ready
            if (board->nodes[idx].initialized.load(std::memory_order_acquire)) {
                ATOMIC_LOG(LOG_LEVEL_INFO, "Node %d initialization ready", real_id);

                // [步骤 3] 刷新 Line 0 和 Line 1
                force_refresh_whole_struct(&board->nodes[idx]);

                // 现在读取 ring_offsets 是安全的了
                ready = true;
                break;
            }
            std::this_thread::sleep_for(milliseconds(100));
        }
        if (!ready)
            ATOMIC_LOG(LOG_LEVEL_WARN, "Node %d init timed out", real_id);
    }
}

// 10. 预热
void UBShmTransport::preload_remote_table()
{
    size_t num_nodes = idx_to_node_id_.size();
    remote_lookup_table_.assign(num_nodes, std::vector<MPSCRingBuffer *>(MAX_PRIORITY_LEVELS, nullptr));

    Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);

    for (uint32_t idx = 0; idx < num_nodes; ++idx) {
        // 跳过自己
        if (idx_to_node_id_[idx] == conf_.current_node_id) {
            for (uint32_t p = 0; p < MAX_PRIORITY_LEVELS; ++p) {
                // local_rings_ 在 create_local_rings 中已经填好了
                if (local_rings_[p] != nullptr) {
                    remote_lookup_table_[idx][p] = local_rings_[p];
                    ATOMIC_LOG(LOG_LEVEL_INFO, "Preloaded SELF Ring %d (Local Shortcut), at %p", p,
                               (void *)local_rings_[p]);
                }
            }
            continue; // 处理完自己，直接下一个
        }

        // [关键] 强制刷新该节点的结构体 (Line 0 + Line 1)
        // - 如果是本地 CC：触发 Write-Allocate，拉取最新数据。
        // - 如果是远端 NC：安全执行伪写，无副作用。
        force_refresh_whole_struct(&board->nodes[idx]);

        // 读取 initialized (Acquire 语义)
        if (!board->nodes[idx].initialized.load(std::memory_order_acquire)) {
            continue;
        }

        // 此时，因为前面 force_refresh_whole_struct 已经刷了 Line 1 (ring_offsets 后半段)，
        // 且 Line 0 (前半段) 也被刷了，所以这里的 ring_offsets 读取是新鲜的。

        for (uint32_t p = 0; p < MAX_PRIORITY_LEVELS; ++p) {
            uint64_t offset = board->nodes[idx].ring_offsets[p].load(std::memory_order_acquire);
            if (offset != UINT64_MAX) {
                // 使用 Compact Index 查基址表
                ATOMIC_LOG(LOG_LEVEL_INFO, "Preloaded Node %d (logic id = %d) Ring %d offset %llu",
                           idx_to_node_id_[idx], idx, p, offset);
                if (region_bases_[idx]) {
                    char *addr = region_bases_[idx] + offset;
                    remote_lookup_table_[idx][p] = reinterpret_cast<MPSCRingBuffer *>(addr);
                    (void)try_populate_cache(idx_to_node_id_[idx], p);
                    ATOMIC_LOG(LOG_LEVEL_INFO, "Preloaded Node %d (logic id = %d) Ring %d at %p", idx_to_node_id_[idx],
                               idx, p, (void *)addr);
                }
            }
        }
    }
}

// 11. 启动
void UBShmTransport::start_dispatcher()
{
    ATOMIC_LOG(LOG_LEVEL_INFO, "Starting dispatcher thread");
    stop_flag_.store(false);
    dispatcher_thread_ = std::thread(&UBShmTransport::run_dispatcher_loop, this);
}

void UBShmTransport::refresh_local_consumer_heartbeat()
{
    int32_t my_idx = get_compact_index(conf_.current_node_id);
    if (my_idx < 0 || init_region_ptr_ == nullptr) {
        return;
    }
    Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);
    uint64_t seq = local_consumer_heartbeat_seq_.fetch_add(1, std::memory_order_relaxed) + 1;
    board->nodes[my_idx].consumer_heartbeat_seq.store(seq, std::memory_order_release);
}

void UBShmTransport::run_consumer_heartbeat_loop()
{
    while (!reliability_stop_flag_.load(std::memory_order_relaxed)) {
        refresh_local_consumer_heartbeat();
        uint64_t interval_us = heartbeat_interval_us_.load(std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::microseconds(interval_us));
    }
}

void UBShmTransport::run_producer_heartbeat_monitor()
{
    Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);
    while (!reliability_stop_flag_.load(std::memory_order_relaxed)) {
        uint64_t now = steady_time_us();
        for (uint32_t idx = 0; idx < idx_to_node_id_.size(); ++idx) {
            uint32_t node_id = idx_to_node_id_[idx];
            if (node_id == conf_.current_node_id || node_id >= MAX_NODES_LIMIT) {
                continue;
            }

            force_refresh_whole_struct(&board->nodes[idx]);
            if (!board->nodes[idx].initialized.load(std::memory_order_acquire)) {
                continue;
            }

            uint64_t heartbeat_seq = board->nodes[idx].consumer_heartbeat_seq.load(std::memory_order_acquire);
            uint32_t remote_interval_ms = board->nodes[idx].heartbeat_interval_ms.load(std::memory_order_acquire);
            if (remote_interval_ms == 0) {
                remote_interval_ms = DEFAULT_HEARTBEAT_INTERVAL_MS;
            }
            uint64_t local_timeout_us = heartbeat_timeout_us_.load(std::memory_order_relaxed);
            uint64_t check_interval_us = heartbeat_check_interval_us_.load(std::memory_order_relaxed);
            uint64_t effective_timeout_us = std::max<uint64_t>(
                local_timeout_us, std::max<uint64_t>(static_cast<uint64_t>(remote_interval_ms) * 3ULL * US_PER_MS,
                                                     check_interval_us * 2ULL));
            peer_heartbeat_timeout_us_[node_id].store(effective_timeout_us, std::memory_order_relaxed);

            bool alive = false;
            uint64_t old_seq = peer_heartbeat_seq_[node_id].load(std::memory_order_relaxed);
            if (heartbeat_seq != 0 && heartbeat_seq != old_seq) {
                peer_heartbeat_seq_[node_id].store(heartbeat_seq, std::memory_order_relaxed);
                peer_heartbeat_seen_us_[node_id].store(now, std::memory_order_relaxed);
                alive = true;
            } else {
                uint64_t last_seen = peer_heartbeat_seen_us_[node_id].load(std::memory_order_relaxed);
                alive = last_seen != 0 && now >= last_seen && (now - last_seen) <= effective_timeout_us;
            }

            bool was_alive = peer_alive_[node_id].exchange(alive, std::memory_order_acq_rel);
            if (was_alive && !alive) {
                ATOMIC_LOG(LOG_LEVEL_WARN,
                           "Consumer heartbeat timeout, node=%u heartbeat_seq=%llu last_seen_us=%llu now_us=%llu "
                           "effective_timeout_us=%llu remote_interval_ms=%u",
                           node_id, heartbeat_seq, peer_heartbeat_seen_us_[node_id].load(std::memory_order_relaxed),
                           now, effective_timeout_us, remote_interval_ms);
            }
        }
        uint64_t interval_us = heartbeat_check_interval_us_.load(std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::microseconds(interval_us));
    }
}

void UBShmTransport::start_reliability_threads()
{
    reliability_stop_flag_.store(false, std::memory_order_release);
    refresh_local_consumer_heartbeat();
    consumer_heartbeat_thread_ = std::thread(&UBShmTransport::run_consumer_heartbeat_loop, this);
    producer_heartbeat_thread_ = std::thread(&UBShmTransport::run_producer_heartbeat_monitor, this);
}

void UBShmTransport::stop_reliability_threads()
{
    reliability_stop_flag_.store(true, std::memory_order_release);
    if (consumer_heartbeat_thread_.joinable()) {
        consumer_heartbeat_thread_.join();
    }
    if (producer_heartbeat_thread_.joinable()) {
        producer_heartbeat_thread_.join();
    }
}

// =========================================================
// 运行时逻辑
// =========================================================

int UBShmTransport::get_remote_ring(uint32_t node_id, uint32_t priority, MPSCRingBuffer **out_ring)
{
    if (out_ring == nullptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Target ring is nullptr.");
        return -EINVAL;
    }
    *out_ring = nullptr;

    int32_t idx = get_compact_index(node_id);
    if (idx < 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid node_id: %u", node_id);
        return UB_COMM_ERR_PEER_NODE_NOT_FOUND;
    }
    if (priority >= MAX_PRIORITY_LEVELS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid priority: %u", priority);
        return -EINVAL;
    }

    // 1. 查缓存
    if (remote_lookup_table_[idx][priority]) {
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "Cache hit for Node %d Priority %d, at %p", node_id, priority,
                   (void *)remote_lookup_table_[idx][priority]);
        *out_ring = remote_lookup_table_[idx][priority];
        return UB_COMM_OK;
    }
    ATOMIC_LOG(LOG_LEVEL_DEBUG, "Cache miss for Node %d Priority %d, try Billboard.", node_id, priority);

    // 2. 查 Billboard (Lazy Load)
    Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);

    // 必须在此处重新刷缓存，防止读取陈旧的 NULL
    force_refresh_whole_struct(&board->nodes[idx]);
    if (!board->nodes[idx].initialized.load(std::memory_order_acquire)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR,
                   "Cannot find ring buffer for Node %d Priority %d in cache or billboard, node not initialized", idx,
                   priority);
        return UB_COMM_ERR_PEER_NOT_READY;
    }
    uint64_t offset = board->nodes[idx].ring_offsets[priority].load(std::memory_order_acquire);
    if (offset == UINT64_MAX || !region_bases_[idx]) {
        ATOMIC_LOG(
            LOG_LEVEL_ERROR,
            "Cannot find ring buffer for Node %d Priority %d in cache or billboard, invalid offset or region base", idx,
            priority);
        return UB_COMM_ERR_RING_NOT_FOUND;
    }

    char *addr = region_bases_[idx] + offset;
    MPSCRingBuffer *ring = reinterpret_cast<MPSCRingBuffer *>(addr);

    remote_lookup_table_[idx][priority] = ring;
    *out_ring = ring;
    return UB_COMM_OK;
}

int UBShmTransport::send(const message_t *msg)
{
    if (__builtin_expect(!msg, 0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Invalid message pointer");
        return -EINVAL;
    }

    // 身份伪造检查
    if (__builtin_expect(conf_.current_node_id != msg->header.src_node_id, 0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Source and current node IDs are different, current: %d, source: %d",
                   conf_.current_node_id, msg->header.src_node_id);
        return -EPERM;
    }

    const uint32_t dest_id = static_cast<uint32_t>(msg->header.dest_node_id);
    const uint32_t body_len = msg->header.body_length;
    ATOMIC_LOG(LOG_LEVEL_DEBUG,
               "Node %d sending message to Node %d, priority: %d, type: %d, body length: %d, src node id: %d, src "
               "thread id: %d",
               conf_.current_node_id, dest_id, msg->header.priority, msg->header.msg_type, msg->header.body_length,
               msg->header.src_node_id, msg->header.src_thread_id);

    // 锁消息判断
    uint32_t prio;
    if (msg->header.msg_type == MSG_TYPE_DIST_LOCK || msg->header.msg_type == MSG_TYPE_SYS_PEER_EXIT ||
        msg->header.msg_type == MSG_TYPE_SYS_FLOW_CONFIG_UPDATE) {
        prio = LOCK_RING_PRIORITY;
    } else {
        prio = msg->header.priority;
        if (__builtin_expect(prio == 0, 0)) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Priority 0 is reserved for locks.");
            return -EINVAL;
        }
    }

    if (__builtin_expect(prio >= MAX_PRIORITY_LEVELS, 0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Invalid priority %u", prio);
        return -EINVAL;
    }
    if (__builtin_expect(dest_id != conf_.current_node_id && dest_id < MAX_NODES_LIMIT &&
                             !peer_alive_[dest_id].load(std::memory_order_acquire),
                         0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: destination consumer heartbeat timeout, node=%u", dest_id);
        return UB_COMM_ERR_PEER_NOT_READY;
    }

    // --- 分流逻辑 ---
    int ret = UB_COMM_OK;

    if (dest_id == conf_.current_node_id) {
        // [Local Loopback] 本地环，直接由 local_rings_ 管理，不需要走这套 Cache 逻辑
        // 因为 local_rings_ 就在本地内存，本来就快
        MPSCRingBuffer *ring = local_rings_[prio];
        if (!ring) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Local ring not found for Priority %d", prio);
            return UB_COMM_ERR_RING_NOT_FOUND;
        }
        ret = ring->enqueue_local(&msg->header, msg->body, body_len);
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "Sending msg: Node %d Prio %d at %p", dest_id, prio, (void *)ring);
    } else {
        // [Remote Send] 走缓存逻辑
        std::shared_lock<std::shared_mutex> lock(cache_mutex_);
        // TODO: ring_caches_ 这里仍然直接使用逻辑 node_id 作为数组下标。
        // 稀疏 node_id 场景下存在设计风险，当前先保留原逻辑，仅用注释提示。
        auto &cache = ring_caches_[dest_id][prio];

        // 1. 检查缓存是否就绪
        if (__builtin_expect(!cache.initialized.load(std::memory_order_acquire), 0)) {
            // 缓存未命中，尝试加载
            ret = try_populate_cache(dest_id, prio);
            if (ret != UB_COMM_OK) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Cache population failed for Node %d Prio %d, ret=%d", dest_id,
                           prio, ret);
                return ret;
            }
        }
        // 2. 走到这里，说明 cache 里的数据绝对可用
        // 直接调用静态入队，传入 cache 里的参数
        ret = MPSCRingBuffer::enqueue_remote(cache.raw_ptr, &msg->header, msg->body, body_len, cache.mask, cache.stride,
                                             cache.max_size, cache.shadow_head, cache.cached_threshold,
                                             cache.cached_threshold_version);
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "Sending msg: Node %d Prio %d at %p", dest_id, prio, cache.raw_ptr);
    }
    if (ret < 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed: Enqueue failed for Node %d Prio %d, ret = %d", dest_id, prio, ret);
        return ret;
    }
    ATOMIC_LOG(LOG_LEVEL_DEBUG, "Send succeeded: Node %d Prio %d, ret=%d", dest_id, prio, ret);
    return ret;
}

bool UBShmTransport::query_inited(const uint8_t node_id)
{
    // Verify the node ID exists
    if (node_id_to_idx_.find(node_id) == node_id_to_idx_.end()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid node ID: %u.", node_id);
        return false;
    }
    if (node_id == conf_.current_node_id) {
        return init_complete_;
    }
    bool is_inited = (try_populate_cache(node_id, 0) == UB_COMM_OK);
    ATOMIC_LOG(LOG_LEVEL_INFO, "Node %d status: %s", node_id, is_inited ? "Initialized" : "Uninitialized");
    return is_inited;
}

void UBShmTransport::run_dispatcher_loop()
{
    if (cpu_id_ != -1) {
        pin_this_thread_to_cpu(cpu_id_);
    }
    std::vector<char> buffer(max_msg_size_global_);
    while (!stop_flag_.load(std::memory_order_relaxed)) {
        int cnt = poll_and_dispatch_once(buffer);
        if (cnt == 0)
            cpu_relax_arm();
    }
}

int UBShmTransport::poll_and_dispatch_once(std::vector<char> &recv_buffer)
{
    int total_processed = 0;
    size_t current_idx = 0;
    // 限制单次调度的最大处理量，防止高优先级死循环导致线程卡死
    const int MAX_BATCH_PER_POLL = 8192;
    while (current_idx < local_active_priorities_.size()) {
        // 如果处理总数超过限制，强制退出，让出CPU给其他线程, 避免饿死低优先级太久
        if (total_processed >= MAX_BATCH_PER_POLL) {
            break;
        }
        uint32_t prio = local_active_priorities_[current_idx];
        MPSCRingBuffer *ring = local_rings_[prio];
        if (!ring) {
            current_idx++;
            continue;
        }

        // 尝试出队
        uint32_t len = ring->dequeue(recv_buffer.data(), recv_buffer.size());

        if (len > 0) {
            // 1. 处理消息
            auto hdr = reinterpret_cast<message_header_t *>(recv_buffer.data());
            dispatch_internal(recv_buffer.data(), len);
            total_processed++;
            // 2. 重置当前索引
            current_idx = 0;
        } else {
            // 3. 当前优先级无数据，才允许向下查看更低优先级
            current_idx++;
        }
    }

    return total_processed;
}

int UBShmTransport::dispatch_internal(const void *data, uint32_t len)
{
    if (__builtin_expect(len < sizeof(message_header_t), 0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid message length");
        return -EMSGSIZE;
    }
    const message_header_t *hdr = reinterpret_cast<const message_header_t *>(data);
    if (__builtin_expect(hdr->msg_type >= callbacks_.size(), 0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Msg type out of range: %d", hdr->msg_type);
        return -EINVAL;
    }
    cb_info_t info;
    {
        std::shared_lock lk(cb_mu_);
        info = callbacks_[hdr->msg_type];
    }
    if (__builtin_expect(info.func == nullptr, 0)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "No callback for type: %d", (int)hdr->msg_type);
        return -ENOENT;
    }
    ATOMIC_LOG(LOG_LEVEL_DEBUG,
               "Node %d received message: type=%d, body_length=%d, src_node_id=%d, dest_node_id=%d, src_thread_id=%d, "
               "priority=%d",
               conf_.current_node_id, hdr->msg_type, hdr->body_length, hdr->src_node_id, hdr->dest_node_id,
               hdr->src_thread_id, hdr->priority);
    const char *body_ptr = static_cast<const char *>(data) + sizeof(message_header_t);
    if (info.type == UB_FUNC_SYNC) {
        message_t m;
        m.header = *hdr;
        m.body = const_cast<char *>(body_ptr);
        info.func(&m, info.ctx);
    } else {
        if (__builtin_expect(!worker_pool_, 0)) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Worker pool is not initialized.");
            return -EPIPE;
        }

        // 异步拷贝
        uint32_t body_len = len - sizeof(message_header_t);
        std::string body_copy(body_ptr, body_len);
        auto f = info.func;
        auto ctx = info.ctx;
        auto hdr_copy = *hdr;
        worker_pool_->enqueue([f, ctx, hdr_copy, body = std::move(body_copy)]() mutable {
            message_t m;
            m.header = hdr_copy;
            m.body = const_cast<char *>(body.data());
            f(&m, ctx);
        });
    }
    return 0;
}

int UBShmTransport::register_func(uint8_t msg_type, ub_func_type_t func_type, ub_callback_t func, void *ctx)
{
    if (msg_type >= callbacks_.size()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid msg type");
        return -EINVAL;
    }
    if (func_type != UB_FUNC_SYNC && func_type != UB_FUNC_ASYNC) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid function type");
        return -EINVAL;
    }
    std::unique_lock lk(cb_mu_);
    callbacks_[msg_type] = cb_info_t(func, ctx, func_type);
    ATOMIC_LOG(LOG_LEVEL_INFO, "Registered callback for type %d", (int)msg_type);
    return 0;
}

int UBShmTransport::register_func_for_lock(uint8_t msg_type, ub_func_type_t func_type, ub_callback_t func, void *ctx)
{
    if (msg_type != MSG_TYPE_DIST_LOCK) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "This register interface is only for distributed lock.");
        return -EINVAL;
    }
    if (func_type != UB_FUNC_SYNC && func_type != UB_FUNC_ASYNC) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid function type");
        return -EINVAL;
    }
    std::unique_lock lk(cb_mu_);
    callbacks_[MSG_TYPE_DIST_LOCK] = cb_info_t(func, ctx, func_type);
    return 0;
}

int UBShmTransport::recv(void *buffer, size_t capacity)
{
    return 0;
}

int UBShmTransport::get_status(uint8_t node_id, uint8_t priority, ub_comm_queue_status_t *status)
{
    if (status == nullptr) {
        return -EINVAL;
    }
    if (priority >= MAX_PRIORITY_LEVELS) {
        return -EINVAL;
    }

    MPSCRingBuffer *ring = nullptr;
    if (node_id == conf_.current_node_id) {
        ring = local_rings_[priority];
        if (ring == nullptr) {
            return UB_COMM_ERR_RING_NOT_FOUND;
        }
    } else {
        std::shared_lock<std::shared_mutex> lock(cache_mutex_);
        int ret = get_remote_ring(node_id, priority, &ring);
        if (ret != UB_COMM_OK) {
            return ret;
        }
    }

    ring->get_status(status);
    return UB_COMM_OK;
}

int UBShmTransport::set_congestion_threshold(uint8_t priority, uint32_t congestion_threshold_percent)
{
    if (priority == LOCK_RING_PRIORITY || priority >= MAX_PRIORITY_LEVELS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid priority %u", priority);
        return -EINVAL;
    }
    MPSCRingBuffer *ring = local_rings_[priority];
    if (ring == nullptr) {
        return UB_COMM_ERR_RING_NOT_FOUND;
    }
    int ret = ring->configure_congestion_threshold(congestion_threshold_percent);
    if (ret != UB_COMM_OK) {
        return ret;
    }

    broadcast_flow_config_update(priority, ring->get_congestion_threshold(), ring->get_congestion_threshold_version());
    return UB_COMM_OK;
}

int UBShmTransport::config_heartbeat(const ub_comm_queue_heartbeat_config_t *request,
                                     ub_comm_queue_heartbeat_config_t *effective)
{
    if (request == nullptr && effective == nullptr) {
        return -EINVAL;
    }
    if (request != nullptr) {
        if (request->heartbeat_interval_ms == 0 || request->check_interval_ms == 0 || request->timeout_ms == 0) {
            return -EINVAL;
        }

        uint64_t min_timeout_ms = static_cast<uint64_t>(request->check_interval_ms) * 2ULL;
        if (static_cast<uint64_t>(request->timeout_ms) < min_timeout_ms) {
            return -EINVAL;
        }

        heartbeat_interval_us_.store(static_cast<uint64_t>(request->heartbeat_interval_ms) * US_PER_MS,
                                     std::memory_order_release);
        heartbeat_check_interval_us_.store(static_cast<uint64_t>(request->check_interval_ms) * US_PER_MS,
                                           std::memory_order_release);
        heartbeat_timeout_us_.store(static_cast<uint64_t>(request->timeout_ms) * US_PER_MS, std::memory_order_release);
        int32_t my_idx = get_compact_index(conf_.current_node_id);
        if (init_region_ptr_ != nullptr && my_idx >= 0) {
            Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);
            board->nodes[my_idx].heartbeat_interval_ms.store(request->heartbeat_interval_ms, std::memory_order_release);
        }
        ATOMIC_LOG(LOG_LEVEL_INFO,
                   "Updated heartbeat config, heartbeat_interval_ms=%u check_interval_ms=%u timeout_ms=%u",
                   request->heartbeat_interval_ms, request->check_interval_ms, request->timeout_ms);
    }

    if (effective != nullptr) {
        effective->heartbeat_interval_ms =
            static_cast<uint32_t>(heartbeat_interval_us_.load(std::memory_order_acquire) / US_PER_MS);
        effective->check_interval_ms =
            static_cast<uint32_t>(heartbeat_check_interval_us_.load(std::memory_order_acquire) / US_PER_MS);
        effective->timeout_ms =
            static_cast<uint32_t>(heartbeat_timeout_us_.load(std::memory_order_acquire) / US_PER_MS);
    }

    return UB_COMM_OK;
}

void UBShmTransport::broadcast_flow_config_update(uint8_t priority, uint32_t threshold, uint64_t version)
{
    if (!init_complete_ || remote_lookup_table_.empty()) {
        return;
    }

    FlowConfigUpdateMessage body{};
    body.threshold = threshold;
    body.version = version;
    body.priority = priority;

    message_t msg{};
    msg.header.msg_type = MSG_TYPE_SYS_FLOW_CONFIG_UPDATE;
    msg.header.src_node_id = conf_.current_node_id;
    msg.header.body_length = sizeof(body);
    msg.header.priority = LOCK_RING_PRIORITY;
    msg.body = reinterpret_cast<char *>(&body);

    for (uint32_t idx = 0; idx < idx_to_node_id_.size(); ++idx) {
        uint32_t node_id = idx_to_node_id_[idx];
        if (node_id == conf_.current_node_id) {
            continue;
        }
        msg.header.dest_node_id = static_cast<uint8_t>(node_id);
        int ret = send(&msg);
        if (ret < 0) {
            ATOMIC_LOG(LOG_LEVEL_WARN,
                       "Failed to broadcast flow config update, dest=%u priority=%u threshold=%u version=%llu ret=%d",
                       node_id, priority, threshold, version, ret);
        }
    }
}

bool UBShmTransport::get_is_for_lock() const
{
    return is_for_lock;
}

int UBShmTransport::set_is_for_lock(bool is_for_lock)
{
    this->is_for_lock = is_for_lock;
    return 0;
}

int UBShmTransport::try_populate_cache(uint32_t node_id, uint32_t prio)
{
    ATOMIC_LOG(LOG_LEVEL_DEBUG, "Trying to populate cache for node %d, priority %d", node_id, prio);
    if (prio >= MAX_PRIORITY_LEVELS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid priority %u", prio);
        return -EINVAL;
    }

    // TODO: ring_caches_ 这里仍然直接使用逻辑 node_id 作为数组下标。
    // 如果后面修这个问题，需要连同 send/remove_node_cache 一起改，避免索引体系混用。
    auto &cache = ring_caches_[node_id][prio];

    // 1. 利用现有的 get_remote_ring 逻辑去读公告牌
    MPSCRingBuffer *ring = nullptr;
    int ret = get_remote_ring(node_id, prio, &ring);

    if (ret != UB_COMM_OK) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to get remote ring for node %d, priority %d, ret=%d", node_id, prio, ret);
        return ret;
    }

    // 2. 读取常量
    cache.raw_ptr = ring;
    cache.max_size = ring->get_max_msg_size();
    cache.stride = ring->get_entry_stride();

    uint32_t num = ring->get_entry_num();
    // 确保是 2 的幂 (防止除 0 或 mask 错误)
    if (num == 0 || (num & (num - 1)) != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Remote ring size invalid: %d", num);
        return -EINVAL;
    }
    cache.mask = num - 1;
    cache.shadow_head.store(0, std::memory_order_relaxed);
    cache.cached_threshold.store(ring->get_congestion_threshold(), std::memory_order_relaxed);
    cache.cached_threshold_version.store(ring->get_congestion_threshold_version(), std::memory_order_relaxed);

    // 3. 标记初始化完成 (Release 保证前面的写入可见)
    cache.initialized.store(true, std::memory_order_release);
    ATOMIC_LOG(LOG_LEVEL_DEBUG,
               "Cache populated for node %d, priority %d successfully. slot_size=%d, stride=%d, num=%d", node_id, prio,
               cache.max_size, cache.stride, cache.mask + 1);

    return UB_COMM_OK;
}

void UBShmTransport::deinit_and_broadcast()
{
    ATOMIC_LOG(LOG_LEVEL_INFO, "Node %d is shutting down...", conf_.current_node_id);
    // 0. 停止分发线程，不再产生新任务
    stop_flag_.store(true, std::memory_order_release);
    if (dispatcher_thread_.joinable()) {
        dispatcher_thread_.join();
    }
    stop_reliability_threads();
    if (init_complete_) {
        // -----------------------------------------------------------------
        // 1. 伪造环满 (立即生效，阻断未来的拉取)
        // -----------------------------------------------------------------
        for (auto *ring : local_rings_) {
            if (ring) {
                ring->trigger_force_full();
            }
        }
        std::atomic_thread_fence(std::memory_order_release);

        // -----------------------------------------------------------------
        // 2.清除公告牌 (防止新连接)
        // -----------------------------------------------------------------
        if (init_region_ptr_) {
            Billboard *board = reinterpret_cast<Billboard *>(init_region_ptr_);
            NodeBoardInfo &my_node = board->nodes[get_compact_index(conf_.current_node_id)];

            // 1. 先把 offsets 全部无效化 (双重保险)
            // 即使 A 读到了 initialized=true (极小概率的并发)，读到的 offset 也是无效的
            for (int i = 0; i < MAX_PRIORITY_LEVELS; ++i) {
                my_node.ring_offsets[i].store(UINT64_MAX, std::memory_order_release);
            }

            // 2. 标记为未初始化 (即下线)
            my_node.initialized.store(false, std::memory_order_release);
            my_node.consumer_heartbeat_seq.store(0, std::memory_order_release);
            my_node.heartbeat_interval_ms.store(0, std::memory_order_release);

            // 3. 利用 padding 强制刷一下 Cache，虽然 atomic release 已经足够
            my_node.cache_probe_pad = 0xDEAD;

            ATOMIC_LOG(LOG_LEVEL_INFO, "Node %d set initialized=false in Billboard.", conf_.current_node_id);
        }

        // -----------------------------------------------------------------
        // 3. 广播 "Peer Exit" 消息 (通知其他节点删缓存)
        // -----------------------------------------------------------------
        message_t exit_msg;
        exit_msg.header.msg_type = MSG_TYPE_SYS_PEER_EXIT; // 之前定义的系统消息 ID
        exit_msg.header.src_node_id = conf_.current_node_id;
        exit_msg.header.body_length = 0;
        exit_msg.header.priority = 0;
        exit_msg.body = nullptr;
        // 给所有活跃节点发消息
        for (uint32_t i = 0; i < idx_to_node_id_.size(); ++i) {
            if (idx_to_node_id_[i] == conf_.current_node_id || remote_lookup_table_[i][0] == nullptr)
                continue;
            exit_msg.header.dest_node_id = idx_to_node_id_[i];
            send(&exit_msg);
        }

        // 稍微等一下，让广播消息飞出去，防止我这边 munmap 太快导致消息丢了
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // -----------------------------------------------------------------
    // 4. 资源回收
    // -----------------------------------------------------------------
    if (worker_pool_) {
        delete worker_pool_;
        worker_pool_ = nullptr;
    }
}
} // namespace ub_comm_queue
