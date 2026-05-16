/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "ub_spin_lock.h"

#include <thread>

#include "ub_atomic_log_print.h"

namespace ublock {

namespace {
constexpr int32_t SPIN_INIT_EMPTY = 0;
constexpr int32_t SPIN_INITING = 1;
constexpr int32_t SPIN_READY = 2;
constexpr time_ms_t DEFAULT_SPIN_TIMEOUT_MS = 10000;
constexpr uint32_t SPIN_YIELD_INTERVAL = 1024;
constexpr uint32_t SPIN_INIT_WAIT_ROUNDS = 1024 * 1024;

inline bool is_invalid_node(uint8_t node_id)
{
    return node_id >= UB_MAX_NODES;
}
} // namespace

void SpinLock::lock_init()
{
    while (true) {
        int32_t state = lock_shm_->init_state.load(std::memory_order_acquire);
        if (state == SPIN_READY) {
            return;
        }

        if (state == SPIN_INIT_EMPTY ||
            (state != SPIN_INITING && state != SPIN_READY)) {
            int32_t expected = state;
            if (lock_shm_->init_state.compare_exchange_strong(expected, SPIN_INITING, std::memory_order_acq_rel,
                                                              std::memory_order_acquire)) {
                lock_shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_relaxed);
                lock_shm_->init_state.store(SPIN_READY, std::memory_order_release);
                return;
            }
            continue;
        }

        for (uint32_t i = 0; i < SPIN_INIT_WAIT_ROUNDS; ++i) {
            if (lock_shm_->init_state.load(std::memory_order_acquire) == SPIN_READY) {
                return;
            }
            if ((i % SPIN_YIELD_INTERVAL) == 0) {
                std::this_thread::yield();
            }
            cpu_relax();
        }
        int32_t expected = SPIN_INITING;
        (void)lock_shm_->init_state.compare_exchange_strong(expected, SPIN_INIT_EMPTY, std::memory_order_acq_rel,
                                                            std::memory_order_acquire);
    }
}

bool SpinLock::is_ready() const
{
    return lock_shm_->init_state.load(std::memory_order_acquire) == SPIN_READY;
}

ub_lock_result_t SpinLock::lock(time_ms_t timeout_ms, const ub_location_t &location)
{
    if (is_invalid_node(location.node_id) || !is_ready()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid spin lock parameter, node_id=%d", location.node_id);
        return UB_LOCK_ERROR;
    }

    const uint64_t identify = make_global_owner(location.node_id, location.tid);
    if (lock_shm_->lock_owner.load(std::memory_order_acquire) == identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "recursive spin lock is not supported");
        return UB_LOCK_ERROR;
    }

    const time_ms_t effective_timeout = timeout_ms == 0 ? DEFAULT_SPIN_TIMEOUT_MS : timeout_ms;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_timeout);
    uint32_t spin_round = 0;
    while (true) {
        uint64_t expected = LOCK_INVALID_OWNER;
        if (lock_shm_->lock_owner.compare_exchange_weak(expected, identify, std::memory_order_acq_rel,
                                                        std::memory_order_acquire)) {
            return UB_LOCK_SUCCESS;
        }

        if ((++spin_round % SPIN_YIELD_INTERVAL) == 0) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return UB_LOCK_TIMEOUT;
            }
            std::this_thread::yield();
        }
        cpu_relax();
    }
}

ub_lock_result_t SpinLock::unlock(const ub_location_t &location)
{
    if (is_invalid_node(location.node_id) || !is_ready()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid spin unlock parameter, node_id=%d", location.node_id);
        return UB_LOCK_ERROR;
    }

    const uint64_t identify = make_global_owner(location.node_id, location.tid);
    const uint64_t owner = lock_shm_->lock_owner.load(std::memory_order_acquire);
    if (owner != identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "spin unlock failed because caller does not own the lock");
        return UB_LOCK_ERROR;
    }

    uint64_t expected = identify;
    if (!lock_shm_->lock_owner.compare_exchange_strong(expected, LOCK_INVALID_OWNER, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "spin unlock failed because global owner changed");
        return UB_LOCK_ERROR;
    }
    return UB_LOCK_SUCCESS;
}

} // namespace ublock

extern "C" void ub_spin_lock_init(ub_spin_lock_t *lock)
{
    if (!lock) {
        return;
    }
    ublock::SpinLock impl(lock);
    impl.lock_init();
}

extern "C" ub_lock_result_t ub_spin_lock(ub_spin_lock_t *lock, time_ms_t timeout_ms, const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    ublock::SpinLock impl(lock);
    return impl.lock(timeout_ms, *location);
}

extern "C" ub_lock_result_t ub_spin_unlock(ub_spin_lock_t *lock, const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    ublock::SpinLock impl(lock);
    return impl.unlock(*location);
}
