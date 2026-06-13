/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "MPSCRingBuffer.h"
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#define MSG_HEADER_LEN sizeof(message_header_t)

namespace ub_comm_queue {

static constexpr int MAX_CAS_RETRIES = 1000;
// Defaults keep flow-control enabled without requiring old callers to set the new config fields.
static constexpr uint32_t DEFAULT_CONGESTION_THRESHOLD_PERCENT = 80;
static constexpr uint64_t FLOW_CONFIG_REFRESH_INTERVAL = 1024;
#ifdef UB_COMM_QUEUE_TEST
static constexpr uint64_t HALF_WRITE_TIMEOUT_US = 1000ULL;
static constexpr uint32_t STALE_RESERVED_PROBE_MASK = 0;
#else
static constexpr uint64_t HALF_WRITE_TIMEOUT_US = 5000000ULL;
static constexpr uint32_t STALE_RESERVED_PROBE_MASK = 511;
#endif
static constexpr uint32_t LOCAL_STALE_STATE_SLOTS = 32;

struct MPSCRingBuffer::Entry {
    std::atomic<uint64_t> ready_seq; // readable only when ready_seq == head + 1
    uint8_t data[];                  // Flexible array member
};

struct LocalStaleReservationState {
    const MPSCRingBuffer *ring{nullptr};
    uint64_t head_seq{UINT64_MAX};
    uint64_t first_seen_us{0};
};

static LocalStaleReservationState &get_local_stale_state(const MPSCRingBuffer *ring)
{
    static thread_local LocalStaleReservationState states[LOCAL_STALE_STATE_SLOTS];
    uint32_t start = (reinterpret_cast<uintptr_t>(ring) >> 6) & (LOCAL_STALE_STATE_SLOTS - 1);
    for (uint32_t i = 0; i < LOCAL_STALE_STATE_SLOTS; ++i) {
        uint32_t idx = (start + i) & (LOCAL_STALE_STATE_SLOTS - 1);
        if (states[idx].ring == ring || states[idx].ring == nullptr) {
            states[idx].ring = ring;
            return states[idx];
        }
    }

    states[start] = {};
    states[start].ring = ring;
    return states[start];
}

static uint32_t calculate_flow_threshold(uint32_t capacity, uint32_t percent)
{
    uint32_t effective_percent = percent;

    uint64_t threshold = (static_cast<uint64_t>(capacity) * effective_percent + 99ULL) / 100ULL;
    if (percent != 0 && threshold == 0) {
        threshold = 1;
    }
    if (threshold > capacity) {
        threshold = capacity;
    }
    return static_cast<uint32_t>(threshold);
}

size_t MPSCRingBuffer::CalculateMemorySize(uint32_t capacity, uint32_t max_msg_size)
{
    size_t entry_header_size = sizeof(Entry);
    size_t single_entry_size = entry_header_size + max_msg_size;

    // 确保每个 Entry 按 CacheLine 对齐，避免伪共享 (False Sharing)
    if (single_entry_size % CACHELINE_SIZE != 0) {
        single_entry_size += (CACHELINE_SIZE - (single_entry_size % CACHELINE_SIZE));
    }

    size_t header_size = MPSCRingBuffer::GetDataOffset();
    return header_size + (single_entry_size * capacity);
}

MPSCRingBuffer::MPSCRingBuffer(uint8_t *buffer_start, uint32_t capacity, uint32_t max_msg_size)
    : entry_num_(capacity),
      max_msg_size_(max_msg_size),
      head_(0),
      tail_(0),
      local_producer_cached_head_(0),
      congestion_threshold_(calculate_flow_threshold(capacity, DEFAULT_CONGESTION_THRESHOLD_PERCENT)),
      congestion_threshold_version_(1),
      congested_(0),
      max_depth_(0)
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
      ,
      full_fail_count_(0),
      cas_fail_count_(0),
      congestion_enter_ts_us_(0),
      congestion_exit_ts_us_(0)
#endif
{
    if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid ring buffer capacity,  must be a power of 2");
        abort();
    }
    index_mask_ = entry_num_ - 1;
    // 计算 Entry 的步长 (Stride)
    size_t raw_size = sizeof(Entry) + max_msg_size;
    entry_stride_ = raw_size;
    if (entry_stride_ % CACHELINE_SIZE != 0) {
        entry_stride_ += (CACHELINE_SIZE - (entry_stride_ % CACHELINE_SIZE));
    }

    // 初始化所有 Entry 状态
    for (uint32_t i = 0; i < capacity; i++) {
        Entry *entry = get_entry(i);
        entry->ready_seq.store(0, std::memory_order_relaxed);
    }

    // 确保初始化写入内存 (ARM Store Barrier)
#if defined(__aarch64__) || defined(__arm__)
    asm volatile("dmb oshst" ::: "memory");
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

static uint64_t now_us()
{
    struct timespec ts {
    };
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

static uint64_t steady_us()
{
    struct timespec ts {
    };
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

uint32_t MPSCRingBuffer::get_flow_threshold() const
{
    return congestion_threshold_.load(std::memory_order_relaxed);
}

uint32_t MPSCRingBuffer::get_congestion_threshold() const
{
    return get_flow_threshold();
}

uint64_t MPSCRingBuffer::get_congestion_threshold_version() const
{
    return congestion_threshold_version_.load(std::memory_order_acquire);
}

void MPSCRingBuffer::record_depth(uint64_t used)
{
    uint64_t old = max_depth_.load(std::memory_order_relaxed);
    // Monotonic best-effort peak tracking: losing this race only means another producer recorded a newer value.
    while (used > old &&
           !max_depth_.compare_exchange_weak(old, used, std::memory_order_relaxed, std::memory_order_relaxed)) {}
}

void MPSCRingBuffer::record_full_fail()
{
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
    full_fail_count_.fetch_add(1, std::memory_order_relaxed);
#endif
}

void MPSCRingBuffer::record_cas_fail()
{
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
    cas_fail_count_.fetch_add(1, std::memory_order_relaxed);
#endif
}

void MPSCRingBuffer::log_flow_edge(bool enter, uint64_t used, const char *event, uint64_t ts_us)
{
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
    ATOMIC_LOG(LOG_LEVEL_WARN,
               "{event:\"queue_flow_%s\", trigger:\"%s\", ring:%p, ts_us:%llu, used:%llu, total:%llu, free:%llu, "
               "threshold:%u, max_depth:%llu, full_fail_count:%llu, cas_fail_count:%llu}",
               enter ? "enter_congestion" : "exit_congestion", event, (void *)this, ts_us, used, entry_num_,
               (used >= entry_num_) ? 0ULL : (entry_num_ - used), get_flow_threshold(),
               max_depth_.load(std::memory_order_relaxed), full_fail_count_.load(std::memory_order_relaxed),
               cas_fail_count_.load(std::memory_order_relaxed));
#else
    ATOMIC_LOG(LOG_LEVEL_WARN,
               "{event:\"queue_flow_%s\", trigger:\"%s\", ring:%p, ts_us:%llu, used:%llu, total:%llu, free:%llu, "
               "threshold:%u, max_depth:%llu}",
               enter ? "enter_congestion" : "exit_congestion", event, (void *)this, ts_us, used, entry_num_,
               (used >= entry_num_) ? 0ULL : (entry_num_ - used), get_flow_threshold(),
               max_depth_.load(std::memory_order_relaxed));
#endif
}

int MPSCRingBuffer::sample_flow_state(uint64_t used, const char *event)
{
    if (used > entry_num_) {
        used = entry_num_;
    }
    record_depth(used);

    const bool sample_congested = (used >= get_flow_threshold());

    // Hot-path sampling is edge-only. Per-sample debounce counters are intentionally avoided because
    // they cause cache-line bouncing between producers and the single consumer in ping-pong traffic.
    if (sample_congested) {
        if (congested_.load(std::memory_order_acquire) != 0) {
            return UB_COMM_SEND_CONGESTED;
        }
        uint8_t expected = 0;
        if (congested_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            uint64_t ts = now_us();
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
            congestion_enter_ts_us_.store(ts, std::memory_order_relaxed);
#endif
            log_flow_edge(true, used, event, ts);
        }
        return UB_COMM_SEND_CONGESTED;
    }

    if (congested_.load(std::memory_order_acquire) == 0) {
        return UB_COMM_OK;
    }

    uint8_t expected = 1;
    if (congested_.compare_exchange_strong(expected, 0, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        uint64_t ts = now_us();
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
        congestion_exit_ts_us_.store(ts, std::memory_order_relaxed);
#endif
        log_flow_edge(false, used, event, ts);
    } else {
        return congested_.load(std::memory_order_acquire) ? UB_COMM_SEND_CONGESTED : UB_COMM_OK;
    }

    return UB_COMM_OK;
}

uint64_t MPSCRingBuffer::approximate_used() const
{
    // Load tail before head to avoid under-estimating occupancy when a producer reserves a slot concurrently.
    uint64_t tail = tail_.load(std::memory_order_acquire);
    uint64_t head = head_.load(std::memory_order_acquire);
    uint64_t used = tail >= head ? (tail - head) : 0;
    return used > entry_num_ ? entry_num_ : used;
}

bool MPSCRingBuffer::try_skip_stale_reserved_entry(Entry *entry, uint64_t cur_head)
{
    uint64_t tail = tail_.load(std::memory_order_acquire);
    if (tail <= cur_head) {
        return false;
    }

    const uint64_t expected_seq = cur_head + 1;
    if (entry->ready_seq.load(std::memory_order_acquire) == expected_seq) {
        return false;
    }

    LocalStaleReservationState &state = get_local_stale_state(this);

    uint64_t now = steady_us();
    if (state.head_seq != cur_head) {
        state.head_seq = cur_head;
        state.first_seen_us = now;
        return false;
    }

    if (state.first_seen_us == 0 || now < state.first_seen_us || now - state.first_seen_us < HALF_WRITE_TIMEOUT_US) {
        return false;
    }

    if (entry->ready_seq.load(std::memory_order_acquire) == expected_seq) {
        state.head_seq = UINT64_MAX;
        state.first_seen_us = 0;
        return false;
    }

    entry->ready_seq.store(0, std::memory_order_relaxed);
    head_.store(expected_seq, std::memory_order_release);
    state.head_seq = UINT64_MAX;
    state.first_seen_us = 0;
    if (congested_.load(std::memory_order_acquire) != 0) {
        (void)sample_flow_state(approximate_used(), "skip_stale_reserved");
    }
    ATOMIC_LOG(LOG_LEVEL_WARN, "Skipped stale reserved ring entry, ring=%p, head=%llu, timeout_us=%llu", (void *)this,
               cur_head, HALF_WRITE_TIMEOUT_US);
    return true;
}

int MPSCRingBuffer::flow_result_after_enqueue(uint64_t new_tail, std::atomic<uint64_t> &cached_head, const char *event)
{
    const uint32_t threshold = get_flow_threshold();
    uint64_t cached = cached_head.load(std::memory_order_relaxed);
    uint64_t used = new_tail >= cached ? (new_tail - cached) : 0;

    if (__builtin_expect(used < threshold, 1)) {
        return UB_COMM_OK;
    }

    // Refresh only when the stale producer-side estimate reaches the flow-control watermark.
    // This keeps the normal path local while avoiding false congestion in ping-pong traffic.
    uint64_t fresh = head_.load(std::memory_order_acquire);
    cached_head.store(fresh, std::memory_order_relaxed);
    used = new_tail >= fresh ? (new_tail - fresh) : 0;

    return sample_flow_state(used, event);
}

uint32_t MPSCRingBuffer::refresh_cached_threshold(std::atomic<uint32_t> &cached_threshold,
                                                  std::atomic<uint64_t> &cached_threshold_version) const
{
    uint64_t remote_version = congestion_threshold_version_.load(std::memory_order_acquire);
    if (remote_version != cached_threshold_version.load(std::memory_order_relaxed)) {
        cached_threshold.store(get_flow_threshold(), std::memory_order_relaxed);
        cached_threshold_version.store(remote_version, std::memory_order_relaxed);
    }

    return cached_threshold.load(std::memory_order_relaxed);
}

int MPSCRingBuffer::flow_result_after_enqueue_cached(uint64_t new_tail, std::atomic<uint64_t> &cached_head,
                                                     std::atomic<uint32_t> &cached_threshold,
                                                     std::atomic<uint64_t> &cached_threshold_version, const char *event)
{
    uint32_t threshold = cached_threshold.load(std::memory_order_relaxed);
    if (__builtin_expect(cached_threshold_version.load(std::memory_order_relaxed) == 0 ||
                             (new_tail & (FLOW_CONFIG_REFRESH_INTERVAL - 1)) == 0,
                         0)) {
        threshold = refresh_cached_threshold(cached_threshold, cached_threshold_version);
    }

    if (__builtin_expect(threshold == 0, 0)) {
        return UB_COMM_SEND_CONGESTED;
    }

    uint64_t cached = cached_head.load(std::memory_order_relaxed);
    uint64_t used = new_tail >= cached ? (new_tail - cached) : 0;

    if (__builtin_expect(used < threshold, 1)) {
        return UB_COMM_OK;
    }

    threshold = refresh_cached_threshold(cached_threshold, cached_threshold_version);
    uint64_t fresh = head_.load(std::memory_order_acquire);
    cached_head.store(fresh, std::memory_order_relaxed);
    used = new_tail >= fresh ? (new_tail - fresh) : 0;

    return sample_flow_state(used, event);
}

int MPSCRingBuffer::configure_congestion_threshold(uint32_t congestion_threshold_percent)
{
    uint32_t percent = congestion_threshold_percent;
    if (percent > 100) {
        return -EINVAL;
    }

    congestion_threshold_.store(calculate_flow_threshold(entry_num_, percent), std::memory_order_relaxed);
    congestion_threshold_version_.fetch_add(1, std::memory_order_release);
    sample_flow_state(approximate_used(), "configure");
    return UB_COMM_OK;
}

void MPSCRingBuffer::get_status(ub_comm_queue_status_t *status)
{
    if (status == nullptr) {
        return;
    }
    uint64_t used = approximate_used();
    record_depth(used);
    status->used = used;
    status->total = entry_num_;
    status->free = (used >= entry_num_) ? 0 : (entry_num_ - used);
    status->congestion_threshold = get_flow_threshold();
    status->max_depth = max_depth_.load(std::memory_order_relaxed);
#ifdef UB_COMM_QUEUE_ENABLE_DEBUG_STATS
    status->full_fail_count = full_fail_count_.load(std::memory_order_relaxed);
    status->cas_fail_count = cas_fail_count_.load(std::memory_order_relaxed);
    status->congestion_enter_ts_us = congestion_enter_ts_us_.load(std::memory_order_relaxed);
    status->congestion_exit_ts_us = congestion_exit_ts_us_.load(std::memory_order_relaxed);
#endif
    if (used >= entry_num_) {
        status->state = UB_COMM_QUEUE_FULL;
    } else if (status->congestion_threshold == 0 || used >= status->congestion_threshold) {
        status->state = UB_COMM_QUEUE_CONGESTED;
    } else if (used == 0) {
        status->state = UB_COMM_QUEUE_IDLE;
    } else {
        status->state = UB_COMM_QUEUE_NORMAL;
    }
}

MPSCRingBuffer::Entry *MPSCRingBuffer::get_entry(uint64_t idx) const
{
    uintptr_t base = reinterpret_cast<uintptr_t>(this) + MPSCRingBuffer::GetDataOffset();
    return reinterpret_cast<Entry *>(base + idx * entry_stride_);
}

// ===========================================================================
// [场景 A] 本地入队 (CC 模式)
// ===========================================================================
int MPSCRingBuffer::enqueue_local(const void *hdr, const void *body, uint32_t body_len)
{
    uint32_t total_len = MSG_HEADER_LEN + body_len;

    // 1. 参数校验
    if (__builtin_expect(hdr == nullptr, 0))
        return -EINVAL;
    if (__builtin_expect(body_len > 0 && body == nullptr, 0))
        return -EINVAL;

    // 2. 检查大小 (本地读取，极快)
    if (__builtin_expect(total_len > max_msg_size_, 0))
        return -EMSGSIZE;

    // 2. 加载 Tail
    uint64_t curr_tail = tail_.load(std::memory_order_relaxed);

    // 3. CAS 循环抢占 (完全复用远端的逻辑，保证一致性)
    while (true) {
        // --- 判满逻辑 (Tail - Head) ---
        // 使用 member 变量 local_producer_cached_head_ 做缓存
        uint64_t cached_head = local_producer_cached_head_.load(std::memory_order_relaxed);
        if (__builtin_expect((cached_head + entry_num_) <= curr_tail, 0)) {
            // 本地缓存认为满了 -> 读最新的 Head
            uint64_t fresh_head = head_.load(std::memory_order_acquire);
            local_producer_cached_head_.store(fresh_head, std::memory_order_relaxed);

            if ((fresh_head + entry_num_) <= curr_tail) {
                // 真满了
                record_full_fail();
                (void)sample_flow_state(entry_num_, "enqueue_full");
                ATOMIC_LOG(LOG_LEVEL_WARN, "Local ring full!");
                return UB_COMM_ERR_RING_FULL;
            }
        }

        // --- 抢占 Tail ---
        uint64_t next_tail = curr_tail + 1;
        if (tail_.compare_exchange_weak(curr_tail, next_tail, std::memory_order_release, std::memory_order_relaxed)) {
            break; // 抢到了
        }
        record_cas_fail();
        // CAS 失败会自动更新 curr_tail，CPU Relax
        cpu_relax_arm();
    }

    // 4. 计算地址 (本地直接算，复用 GetDataOffset 保证对齐)
    char *data_start = reinterpret_cast<char *>(this) + MPSCRingBuffer::GetDataOffset();
    Entry *entry = reinterpret_cast<Entry *>(data_start + ((curr_tail & index_mask_) * entry_stride_));

    // 5. 写入数据 (Scatter)
    if (body_len > 0) {
        std::memcpy(entry->data, hdr, MSG_HEADER_LEN);
        std::memcpy(entry->data + MSG_HEADER_LEN, body, body_len);
    } else {
        std::memcpy(entry->data, hdr, MSG_HEADER_LEN);
    }

    // 6. 提交
    entry->ready_seq.store(curr_tail + 1, std::memory_order_release);

    return flow_result_after_enqueue(curr_tail + 1, local_producer_cached_head_, "enqueue_success");
}

// ===========================================================================
// [场景 B] 远端入队 (NC 模式)
// ===========================================================================
int MPSCRingBuffer::enqueue_remote(MPSCRingBuffer *remote_this, const void *hdr, const void *body, uint32_t body_len,
                                   uint64_t mask, uint64_t stride, uint64_t max_size,
                                   std::atomic<uint64_t> &shadow_head, std::atomic<uint32_t> &cached_threshold,
                                   std::atomic<uint64_t> &cached_threshold_version)
{
    // 0. [Local] 参数检查
    if (__builtin_expect(remote_this == nullptr, 0))
        return -EINVAL;
    if (__builtin_expect(hdr == nullptr, 0))
        return -EINVAL;
    if (__builtin_expect(body_len > 0 && body == nullptr, 0))
        return -EINVAL;

    uint32_t total_len = MSG_HEADER_LEN + body_len;
    if (__builtin_expect(total_len > max_size, 0))
        return -EMSGSIZE;

    // 1. [Remote] 加载当前 Tail
    uint64_t curr_tail = remote_this->tail_.load(std::memory_order_relaxed);

    // 计算容量常数
    const uint32_t capacity = mask + 1;
    int retry_count = 0;

    // 3. [Remote] CAS 循环抢占
    // 使用 while(true) 配合内部 break
    while (true) {
        // 超过重试次数，直接放弃
        if (__builtin_expect(retry_count++ > MAX_CAS_RETRIES, 0)) {
            remote_this->record_cas_fail();
            return UB_COMM_ERR_RING_BUSY;
        }

        // --- A. 判满逻辑 ---
        uint64_t cached_head = shadow_head.load(std::memory_order_relaxed);
        if (__builtin_expect((cached_head + capacity) <= curr_tail, 0)) {
            uint64_t fresh_head = remote_this->head_.load(std::memory_order_acquire);
            shadow_head.store(fresh_head, std::memory_order_relaxed);
            if ((fresh_head + capacity) <= curr_tail) {
                remote_this->record_full_fail();
                (void)remote_this->sample_flow_state(capacity, "enqueue_full");
                return UB_COMM_ERR_RING_FULL;
            }
        }

        // --- B. 尝试抢占 ---
        uint64_t next_tail = curr_tail + 1;

        if (remote_this->tail_.compare_exchange_weak(curr_tail, next_tail, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
            break; // 成功！
        }

        // --- C. 失败处理 ---
        // CAS 失败说明有人抢先了，curr_tail 已经被更新为最新值

        // [防御 3] CPU Relax / Yield
        remote_this->record_cas_fail();
        cpu_relax_arm();
        // 继续下一轮循环，尝试抢新的 curr_tail
    }

    // 4. [Calc] 计算地址 (本地运算)
    uintptr_t base_addr = reinterpret_cast<uintptr_t>(remote_this);
    char *data_start = reinterpret_cast<char *>(base_addr + MPSCRingBuffer::GetDataOffset());

    // (idx & mask) * stride
    Entry *entry = reinterpret_cast<Entry *>(data_start + ((curr_tail & mask) * stride));

    // 5. [Remote Write] 写入数据
    // 拷贝 Header

    if (body_len > 0) {
        std::memcpy(entry->data, hdr, MSG_HEADER_LEN);
        std::memcpy(entry->data + MSG_HEADER_LEN, body, body_len);
    } else {
        std::memcpy(entry->data, hdr, MSG_HEADER_LEN);
    }

    // 6. [Fence] 保证数据先于 Flag
    // arm_light_fence();

    // 7. [Commit] 提交
    entry->ready_seq.store(curr_tail + 1, std::memory_order_release);

    return remote_this->flow_result_after_enqueue_cached(curr_tail + 1, shadow_head, cached_threshold,
                                                         cached_threshold_version, "enqueue_success");
}

uint32_t MPSCRingBuffer::dequeue(void *buffer, uint32_t buffer_cap)
{
    // 接收端一定是 CC的
    uint64_t cur_head = head_.load(std::memory_order_relaxed);
    char *data_start = reinterpret_cast<char *>(this) + MPSCRingBuffer::GetDataOffset();
    Entry *entry = reinterpret_cast<Entry *>(data_start + ((cur_head & index_mask_) * entry_stride_));
    const uint64_t expected_seq = cur_head + 1;

    // 精确检查：ready_seq 确定 Entry 是否属于当前 head，避免跳过后晚提交产生 ABA。
    if (entry->ready_seq.load(std::memory_order_acquire) != expected_seq) {
        static thread_local uint32_t stale_reserved_probe = 0;
        if (__builtin_expect((++stale_reserved_probe & STALE_RESERVED_PROBE_MASK) != 0, 1)) {
            return 0;
        }
        (void)try_skip_stale_reserved_entry(entry, cur_head);
        return 0;
    }

    auto *hdr = reinterpret_cast<message_header_t *>(entry->data);
    uint32_t real_len = sizeof(message_header_t) + hdr->body_length;
    uint32_t copy_len = (real_len > buffer_cap) ? buffer_cap : real_len;

    std::memcpy(buffer, entry->data, copy_len);
    // 重置状态
    entry->ready_seq.store(0, std::memory_order_relaxed);
    // 推进 Head
    head_.store(expected_seq, std::memory_order_release);
    if (congested_.load(std::memory_order_acquire) != 0) {
        (void)sample_flow_state(approximate_used(), "dequeue");
    }
    // 释放资源计数
    return copy_len;
}

void MPSCRingBuffer::trigger_force_full()
{
    // 1. 获取当前的 tail (生产者写到的位置)
    uint64_t curr_tail = tail_.load(std::memory_order_relaxed);

    // 2. 计算一个能让环瞬间变满的 head 值
    // (tail - head) >= entry_num
    uint64_t fake_head = curr_tail - entry_num_;

    // 3. 写入伪造的 head
    // 使用 Release 语义，尽快让远端看到
    head_.store(fake_head, std::memory_order_release);
}

} // namespace ub_comm_queue
