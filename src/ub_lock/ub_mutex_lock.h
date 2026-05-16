/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include "local_lock.h"
#include "ring_queue.h"
#include "ub_dist_lock.h"

struct alignas(UB_CACHELINE_SIZE) ub_mutex_lock {
    std::atomic<uint64_t> lock_owner;     /* LOCK_INVALID_OWNER: unlocked, otherwise NodeID + TID */
    std::atomic<uint32_t> waiting_count;  /* fast-path waiter indicator */
    std::atomic<uint32_t> queue_head;     /* wait queue head index */
    std::atomic<uint32_t> queue_tail;     /* wait queue tail index */
    std::atomic<int32_t> is_inited;       /* 0: empty, 1: initializing, 2: ready */
    uint8_t _pad[40];
    ub_waiter_t wait_queue[UB_MAX_NODES]; /* FIFO wait queue */
};

namespace ublock {

class MutexLock {
public:
    explicit MutexLock(ub_mutex_lock_t *shm) : lock_shm_(shm) {}

    void lock_create();
    void lock_free();
    ub_lock_result_t lock(time_ms_t timeout_ms, const ub_location_t &location);
    ub_lock_result_t unlock(const ub_location_t &location);

private:
    ub_mutex_lock_t *lock_shm_;

    ub_lock_result_t acquire_global(const steady_time_point &deadline, const ub_location_t &location);
    bool is_ready() const;
    bool try_lock_fast(uint64_t identify, bool is_awakened, uint32_t slot);
    void create_wait_queue();
    ub_lock_result_t enqueue_waiter(const ub_location_t &location, uint32_t &out_ticket);
    void clean_timeout_waiter(uint32_t ticket);
    void clean_outqueue_waiter(uint32_t ticket);
    void wake_one_waiter(const ub_location_t &location);
    ub_lock_result_t notify_waiter(ub_waiter_t &waiter, const ub_location_t &location);
};

} // namespace ublock
