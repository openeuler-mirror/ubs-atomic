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

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include "ub_comm_errno.h"
#include "ub_dist_comm_queue.h"
#include "util.h"
#include "ub_atomic_log_print.h"

namespace ub_comm_queue {

/**
 * @brief 无锁多生产者单消费者环形队列 (Multi-Producer Single-Consumer Ring Buffer)
 * 专为共享内存环境设计，手动管理内存布局，避免使用堆指针。
 */
class MPSCRingBuffer {
public:
    // 计算创建该实例所需的共享内存总大小
    static size_t CalculateMemorySize(uint32_t capacity, uint32_t max_msg_size);

    /**
     * @brief 构造函数 (使用 Placement New 在共享内存上构造)
     * @param buffer_start 共享内存起始地址
     * @param capacity 队列容量
     * @param max_msg_size 单条消息最大体大小
     */
    MPSCRingBuffer(uint8_t *buffer_start, uint32_t capacity, uint32_t max_msg_size);

    ~MPSCRingBuffer() = default;

    // 禁止拷贝和移动（因为由于偏移量计算，对象位置必须固定）
    MPSCRingBuffer(const MPSCRingBuffer &) = delete;
    MPSCRingBuffer &operator=(const MPSCRingBuffer &) = delete;

    /**
     * @brief 入队 (线程安全，支持多生产者)
     * @return UB_COMM_OK 表示写入成功且未拥塞；
     *         UB_COMM_SEND_CONGESTED 表示写入成功但目标环已进入稳定拥塞；
     *         负数表示写入失败。
     */
    // 远端用
    static int enqueue_remote(MPSCRingBuffer *remote_this, const void *hdr, const void *body,
                               uint32_t body_len,
                               uint64_t mask,        // 缓存的 entry_num - 1
                               uint64_t stride,      // 缓存的 entry_stride
                               uint64_t max_size,    // 缓存的 max_msg_size
                               std::atomic<uint64_t> &shadow_head, // 缓存的 shadow_head (引用)
                               std::atomic<uint32_t> &cached_threshold,
                               std::atomic<uint64_t> &cached_threshold_version
    );
    // 本地用  
    int enqueue_local(const void* hdr, const void* body, uint32_t body_len);

    /**
     * @brief 出队 (单消费者)
     * @param buffer 接收缓冲区
     * @param buffer_cap 缓冲区最大容量
     * @return 实际读取的字节数，0 表示无数据
     */
    uint32_t dequeue(void *buffer, uint32_t buffer_cap);

    static constexpr size_t GetDataOffset()
    {
        size_t sz = sizeof(MPSCRingBuffer);
        return (sz + 63) & ~63;
    }

    uint64_t get_entry_num() const
    {
        return entry_num_;
    }
    uint64_t get_max_msg_size() const
    {
        return max_msg_size_;
    }
    uint64_t get_entry_stride() const
    {
        return entry_stride_;
    }
    uint32_t get_congestion_threshold() const;
    uint64_t get_congestion_threshold_version() const;
    // 伪造满环状态，用于节点下线时快速阻断后续生产者写入。
    void trigger_force_full();
    void set_half_write_timeout_us(uint64_t timeout_us);

    // 更新本地环的拥塞阈值。字段为原子写，允许运行期调整。
    int configure_congestion_threshold(uint32_t congestion_threshold_percent);

    // 返回本地估计的状态快照，不加锁，适合维测路径调用。
    void get_status(ub_comm_queue_status_t *status);

private:
    struct Entry; // 前置声明

    // 获取指定索引的 Entry 指针
    Entry *get_entry(uint64_t idx) const;
    uint32_t get_flow_threshold() const;

    // 影子统计：只记录峰值和边缘状态，不参与入队判定的正确性。
    void record_depth(uint64_t used);
    void record_full_fail();
    void record_cas_fail();
    int sample_flow_state(uint64_t used, const char *event);
    void log_flow_edge(bool enter, uint64_t used, const char *event, uint64_t ts_us);

    // tail/head 是并发变化的，这里返回的是悲观近似值，用于流控提示和查询。
    uint64_t approximate_used() const;
    int flow_result_after_enqueue(uint64_t new_tail, std::atomic<uint64_t> &cached_head, const char *event);
    int flow_result_after_enqueue_cached(uint64_t new_tail, std::atomic<uint64_t> &cached_head,
                                         std::atomic<uint32_t> &cached_threshold,
                                         std::atomic<uint64_t> &cached_threshold_version,
                                         const char *event);
    uint32_t refresh_cached_threshold(std::atomic<uint32_t> &cached_threshold,
                                      std::atomic<uint64_t> &cached_threshold_version) const;
    bool try_skip_stale_reserved_entry(Entry *entry, uint64_t cur_head);
    

private:
    // 成员变量需按 CacheLine 对齐以避免伪共享
    alignas(CACHELINE_SIZE) std::atomic<uint64_t> head_;
    alignas(CACHELINE_SIZE) std::atomic<uint64_t> tail_;

    // 以下变量初始化后只读，可以放在一起
    uint64_t entry_stride_;
    uint64_t entry_num_;
    uint64_t max_msg_size_;
    uint32_t index_mask_;
    std::atomic<uint64_t> local_producer_cached_head_;

    alignas(CACHELINE_SIZE) std::atomic<uint32_t> congestion_threshold_;
    std::atomic<uint64_t> congestion_threshold_version_;
    std::atomic<uint8_t> congested_;
    std::atomic<uint64_t> max_depth_;
    std::atomic<uint64_t> half_write_timeout_us_;
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
    std::atomic<uint64_t> full_fail_count_;
    std::atomic<uint64_t> cas_fail_count_;
    std::atomic<uint64_t> congestion_enter_ts_us_;
    std::atomic<uint64_t> congestion_exit_ts_us_;
#endif
};

} // namespace ub_comm_queue
