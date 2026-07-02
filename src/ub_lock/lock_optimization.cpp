/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "inner_distribute_lock.h"

namespace ublock {

namespace {

inline bool local_lock_state_is_idle(uint64_t state)
{
    return state == 0;
}

// Keep acquire helpers single-attempt: LocalLock::lock_* owns spin rounds,
// deadline checks and queue fallback.
bool local_lock_try_acquire_s(LocalLockStateWord &state_word, int32_t tid)
{
    const uint32_t lane = local_lock_lane_for_tid(tid);
    const uint64_t state = state_word.load(std::memory_order_acquire);
    if ((state & LOCAL_LOCK_X_FLAG) != 0) {
        return false;
    }

    uint16_t lane_value = local_lock_lane_value(state, lane);
    if ((lane_value & LOCAL_LOCK_READER_SENTINEL) != 0) {
        return false;
    }

    const uint16_t count = static_cast<uint16_t>(lane_value & LOCAL_LOCK_READER_COUNT_MASK);
    if (count == LOCAL_LOCK_READER_COUNT_MASK) {
        return false;
    }

    const uint16_t desired =
        static_cast<uint16_t>((lane_value & ~static_cast<uint16_t>(LOCAL_LOCK_READER_COUNT_MASK)) | (count + 1u));
    return state_word.compare_exchange_lane_weak(lane, lane_value, desired, std::memory_order_acquire,
                                                 std::memory_order_acquire);
}

bool local_lock_try_release_s(LocalLockStateWord &state_word, int32_t tid, uint64_t &state_after_release)
{
    const uint32_t lane = local_lock_lane_for_tid(tid);
    const uint64_t state = state_word.load(std::memory_order_acquire);
    uint16_t lane_value = local_lock_lane_value(state, lane);

    // Unlock cannot be abandoned; retry only the owning reader lane.
    while ((lane_value & LOCAL_LOCK_READER_SENTINEL) == 0) {
        const uint16_t count = static_cast<uint16_t>(lane_value & LOCAL_LOCK_READER_COUNT_MASK);
        if (count == 0) {
            return false;
        }

        const uint16_t desired =
            static_cast<uint16_t>((lane_value & ~static_cast<uint16_t>(LOCAL_LOCK_READER_COUNT_MASK)) | (count - 1u));
        if (state_word.compare_exchange_lane_strong(lane, lane_value, desired, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
            state_after_release = state_word.load(std::memory_order_acquire);
            return true;
        }
        cpu_relax();
    }
    return false;
}

bool local_lock_try_acquire_x(LocalLockStateWord &state_word)
{
    uint64_t state = state_word.load(std::memory_order_acquire);
    if (!local_lock_state_is_idle(state)) {
        return false;
    }
    return state_word.compare_exchange_weak(state, LOCAL_LOCK_X_STATE, std::memory_order_acq_rel,
                                            std::memory_order_acquire);
}

bool local_lock_try_acquire_sx(LocalLockStateWord &state_word)
{
    const uint64_t state = state_word.load(std::memory_order_acquire);
    if ((state & (LOCAL_LOCK_SX_FLAG | LOCAL_LOCK_X_FLAG | LOCAL_LOCK_READER_SENTINEL_MASK)) != 0) {
        return false;
    }

    uint16_t lane_value = local_lock_lane_value(state, LOCAL_LOCK_SX_LANE);
    if ((lane_value & (LOCAL_LOCK_SX_FLAG | LOCAL_LOCK_READER_SENTINEL)) != 0) {
        return false;
    }

    const uint16_t desired = static_cast<uint16_t>(lane_value | LOCAL_LOCK_SX_FLAG);
    return state_word.compare_exchange_lane_weak(LOCAL_LOCK_SX_LANE, lane_value, desired, std::memory_order_acq_rel,
                                                 std::memory_order_acquire);
}

bool local_lock_try_release_sx(LocalLockStateWord &state_word, uint64_t &state_after_release)
{
    const uint16_t clear_sx_mask = static_cast<uint16_t>(~static_cast<uint16_t>(LOCAL_LOCK_SX_FLAG));
    const uint16_t old_lane = state_word.fetch_and_lane(LOCAL_LOCK_SX_LANE, clear_sx_mask, std::memory_order_acq_rel);
    if ((old_lane & LOCAL_LOCK_SX_FLAG) == 0) {
        return false;
    }

    state_after_release = state_word.load(std::memory_order_acquire);
    return true;
}

ub_lock_result_t finish_local_sx_acquire(LocalLock &lock, bool is_awakened, uint32_t slot, int32_t tid)
{
    lock.lock_sx_owner.store(tid, std::memory_order_release);
    lock.sx_recursive_.store(1, std::memory_order_release);
    if (is_awakened) {
        lock.clean_outqueue_waiter(slot);
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t enqueue_sx_waiter_if_needed(LocalLock &lock, bool is_awakened, int32_t tid, uint32_t &slot)
{
    if (is_awakened) {
        return UB_LOCK_SUCCESS;
    }
    ub_lock_result_t ret = lock.enqueue_waiter(UB_LOCK_SX, tid, slot);
    if (ret != UB_LOCK_SUCCESS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "The local waiting queue is full"); // 队列满
    }
    return ret;
}

} // namespace

/*
    本地锁的实现
*/

void LocalLock::init_()
{
    lock_word.store(0, std::memory_order_release);
    read_count.store(0, std::memory_order_relaxed);
    waiting_count.store(0, std::memory_order_release);

    lock_x_owner.store(0, std::memory_order_release);
    lock_sx_owner.store(0, std::memory_order_release);
    x_recursive_.store(0, std::memory_order_release);
    sx_recursive_.store(0, std::memory_order_release);

    local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
    remote_release_in_progress_.store(false, std::memory_order_release);
    hold_global.store(false, std::memory_order_release);
    global_state_.store(GLOBAL_IDLE, std::memory_order_release);
    global_read_ref_count_.store(0, std::memory_order_release);
    create_wait_queue();
}

bool LocalLock::try_begin_remote_release()
{
    bool expected = false;
    return remote_release_in_progress_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                               std::memory_order_acquire);
}

void LocalLock::end_remote_release()
{
    remote_release_in_progress_.store(false, std::memory_order_release);
}

bool LocalLock::is_held()
{
    return !local_lock_state_is_idle(lock_word.load(std::memory_order_acquire));
}
/* ---------------- queue: std array实现FIFO---------------- */
void LocalLock::create_wait_queue()
{
    ring_queue_init<UB_MAX_CAPACITY>(q_head.v, q_tail.v, waiting_count, q.data(), [](local_waiter_t &slot) {
        slot.tid = 0;
        slot.mode = UB_LOCK_I;
    });
}

ub_lock_result_t LocalLock::enqueue_waiter(ub_lock_mode_t mode, int32_t tid, uint32_t &out_ticket)
{
    return ring_queue_enqueue<UB_MAX_CAPACITY>(
        q_head.v, q_tail.v, waiting_count, q.data(),
        [&](local_waiter_t &slot) {
            slot.mode = mode;
            slot.tid = tid;
        },
        out_ticket);
}

ub_lock_result_t LocalLock::outqueue_waiter(local_waiter_t *&out)
{
    return ring_queue_outqueue<UB_MAX_CAPACITY>(q_head.v, waiting_count, q.data(), out);
}

void LocalLock::clean_timeout_waiter(uint32_t ticket)
{
    ring_queue_clean_timeout<UB_MAX_CAPACITY>(q_head.v, q_tail.v, waiting_count, q.data(), ticket);
}

void LocalLock::clean_outqueue_waiter(uint32_t ticket)
{
    ring_queue_pop_head<UB_MAX_CAPACITY>(q_head.v, q_tail.v, waiting_count, q.data(), ticket);
}

bool LocalLock::peek_head_waiting_mode_clean(ub_lock_mode_t &mode_out)
{
    return ring_queue_peek_head_mode_clean<UB_MAX_CAPACITY>(q_head.v, q_tail.v, waiting_count, q.data(), mode_out);
}

bool LocalLock::notify_one(local_waiter_t &w)
{
    bool found = WaiterRegistry::instance().notify_local_waiter(w.tid);
    if (!found) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "Local wait ctx has been notified: %d", w.tid);
    }
    return found;
}

void LocalLock::dequeue_and_notify_one()
{
    int sanity = UB_MAX_CAPACITY * 2;
    while (sanity-- > 0) {
        local_waiter_t *w = nullptr;
        if (outqueue_waiter(w) == UB_LOCK_SUCCESS) {
            (void)notify_one(*w);
            return;
        }
    }
}

void LocalLock::wake_after_unlock_exclusive()
{
    // 唤醒规则：
    // 1) 首个 X：只唤醒 X
    // 2) 首个 S：连续唤醒 S/SX，直到遇到 X
    // 3) 首个 SX：唤醒 SX，然后连续唤醒 S，直到遇到 SX 或 X

    ub_lock_mode_t head_mode;
    if (!peek_head_waiting_mode_clean(head_mode)) {
        return;
    }

    if (head_mode == UB_LOCK_X) {
        dequeue_and_notify_one();
        return;
    }

    if (head_mode == UB_LOCK_S) {
        dequeue_and_notify_one();
        bool meet_sx = false;
        while (true) {
            ub_lock_mode_t m;
            if (!peek_head_waiting_mode_clean(m)) {
                return;
            }
            if (m == UB_LOCK_X) {
                return;
            }
            if (m == UB_LOCK_S) {
                dequeue_and_notify_one();
                continue;
            }
            if (m == UB_LOCK_SX && !meet_sx) {
                meet_sx = true;
                dequeue_and_notify_one();
                continue;
            }
            return;
        }
    }

    if (head_mode == UB_LOCK_SX) {
        dequeue_and_notify_one();
        while (true) {
            ub_lock_mode_t m;
            if (!peek_head_waiting_mode_clean(m)) {
                return;
            }
            if (m == UB_LOCK_X || m == UB_LOCK_SX) {
                return;
            }
            if (m == UB_LOCK_S) {
                dequeue_and_notify_one();
                continue;
            }
            return;
        }
    }
}

/* ---------------- lock ---------------- */

ub_lock_result_t LocalLock::lock_s(int32_t tid, steady_time_point deadline)
{
    uint32_t slot = 0;
    bool is_awakened = false;
    ub_lock_result_t ret;

    while (true) {
        // 自旋阶段
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened)
                break;
            if (read_count.load(std::memory_order_relaxed) > LOCK_S_THRESHOLD && !is_awakened) {
                if (ub_lock_ptr_->waiting_count.load(std::memory_order_acquire) > 0) { // 入本地队列等待
                    break;
                }
                read_count.store(0, std::memory_order_relaxed);
            }
            if (local_lock_try_acquire_s(lock_word, tid)) {
                read_count.fetch_add(1, std::memory_order_relaxed);
                if (is_awakened) {
                    clean_outqueue_waiter(slot);
                }
                return UB_LOCK_SUCCESS;
            }
            if (is_awakened) {
                cpu_relax();
                --i;
                if (std::chrono::steady_clock::now() > deadline) {
                    clean_timeout_waiter(slot);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0) {
                cpu_relax();
            }
        }
        // 排队阶段
        {
            local_wait_ctx_t ctx;
            WaiterGuard guard(tid, &ctx);
            ret = enqueue_waiter(UB_LOCK_S, tid, slot);
            if (ret != UB_LOCK_SUCCESS) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The local waiting queue is full"); // 队列满
                return ret;
            }
            // double check 自我唤醒
            bool self_handoff = false;
            uint64_t current_val = lock_word.load(std::memory_order_acquire);
            if (local_lock_state_is_idle(current_val)) { // 锁完全空闲条件
                self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_CAPACITY>(q_head.v, q_tail.v, q.data(), slot);
            }
            // 睡眠等待 防止无限循环
            if (!self_handoff) {
                if (!ctx.wait(deadline)) {
                    clean_timeout_waiter(slot);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
            }
            is_awakened = true;
        }
    }
}

ub_lock_result_t LocalLock::lock_x(bool allow_recursive, int32_t tid, steady_time_point deadline)
{
    if (lock_x_owner.load(std::memory_order_acquire) == tid) {
        if (allow_recursive) {
            x_recursive_.fetch_add(1, std::memory_order_acq_rel);
            return UB_LOCK_SUCCESS;
        }
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Local recursive lock is not allowed!");
        return UB_LOCK_CONFLICT;
    }
    uint32_t slot = 0;
    bool is_awakened = false;
    ub_lock_result_t ret;
    while (true) {
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened)
                break;
            if (local_lock_try_acquire_x(lock_word)) {
                lock_x_owner.store(tid, std::memory_order_release);
                x_recursive_.store(1, std::memory_order_release);
                if (is_awakened) {
                    clean_outqueue_waiter(slot);
                }
                return UB_LOCK_SUCCESS;
            }
            if (is_awakened) {
                cpu_relax();
                --i;
                if (std::chrono::steady_clock::now() > deadline) {
                    clean_timeout_waiter(slot);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0) {
                cpu_relax();
            }
        }
        {
            local_wait_ctx_t ctx;
            WaiterGuard guard(tid, &ctx);
            ret = enqueue_waiter(UB_LOCK_X, tid, slot);
            if (ret != UB_LOCK_SUCCESS) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock waiting queue is full"); //队列满
                return ret;
            }
            // double check 自我唤醒
            bool self_handoff = false;
            uint64_t current_val = lock_word.load(std::memory_order_acquire);
            if (local_lock_state_is_idle(current_val)) { // 锁完全空闲条件
                self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_CAPACITY>(q_head.v, q_tail.v, q.data(), slot);
            }
            // 睡眠等待 防止无限循环
            if (!self_handoff) {
                if (!ctx.wait(deadline)) {
                    clean_timeout_waiter(slot);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
            }
            is_awakened = true;
        }
    }
}

ub_lock_result_t LocalLock::lock_sx(bool allow_recursive, int32_t tid, steady_time_point deadline)
{
    if (lock_sx_owner.load(std::memory_order_acquire) == tid) {
        if (allow_recursive) {
            sx_recursive_.fetch_add(1, std::memory_order_acq_rel);
            return UB_LOCK_SUCCESS;
        }
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Local recursive lock is not allowed!");
        return UB_LOCK_CONFLICT;
    }
    uint32_t slot = 0;
    bool is_awakened = false;
    ub_lock_result_t ret;

    while (true) {
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened)
                break;
            if (local_lock_try_acquire_sx(lock_word)) {
                return finish_local_sx_acquire(*this, is_awakened, slot, tid);
            }
            if (!is_awakened && (i & 0xF) == 0) {
                cpu_relax();
            }
            if (!is_awakened) {
                continue;
            }
            cpu_relax();
            --i;
            if (std::chrono::steady_clock::now() > deadline) {
                clean_timeout_waiter(slot);
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock hold timeout");
                return UB_LOCK_TIMEOUT;
            }
        }
        local_wait_ctx_t ctx;
        WaiterGuard guard(tid, &ctx);
        ret = enqueue_sx_waiter_if_needed(*this, is_awakened, tid, slot);
        if (ret != UB_LOCK_SUCCESS) {
            return ret;
        }
        bool self_handoff = false;
        uint64_t current_val = lock_word.load(std::memory_order_acquire);
        if (local_lock_state_is_idle(current_val)) { // 锁完全空闲条件
            self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_CAPACITY>(q_head.v, q_tail.v, q.data(), slot);
        }
        // 睡眠等待 防止无限循环
        if (!self_handoff && !ctx.wait(deadline)) {
            clean_timeout_waiter(slot);
            ATOMIC_LOG(LOG_LEVEL_ERROR, "The local lock hold timeout");
            return UB_LOCK_TIMEOUT;
        }
        is_awakened = true;
    }
}

/* ---------------- unlock ---------------- */

ub_lock_result_t LocalLock::unlock_s(int32_t tid)
{
    uint64_t state_after_release = 0;
    if (!local_lock_try_release_s(lock_word, tid, state_after_release)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Hold the lock before unlocking");
        return UB_LOCK_ERROR;
    }
    // 唤醒等待者（仅当最后一个读者释放）
    if (local_lock_state_is_idle(state_after_release) && waiting_count.load(std::memory_order_acquire) > 0) {
        wake_after_unlock_exclusive();
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t LocalLock::unlock_x(bool allow_recursive, int32_t tid)
{
    if (lock_x_owner.load(std::memory_order_acquire) != tid) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Only the holder can release it");
        return UB_LOCK_ERROR;
    }

    if (allow_recursive) {
        uint32_t old = x_recursive_.fetch_sub(1, std::memory_order_acq_rel);
        if (old > 1)
            return UB_LOCK_SUCCESS;
    }

    x_recursive_.store(0, std::memory_order_release);
    lock_x_owner.store(0, std::memory_order_release);
    lock_word.store(0, std::memory_order_release);
    if (waiting_count.load(std::memory_order_acquire) > 0) {
        wake_after_unlock_exclusive();
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t LocalLock::unlock_sx(bool allow_recursive, int32_t tid)
{
    if (lock_sx_owner.load(std::memory_order_acquire) != tid) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Only the holder can release it");
        return UB_LOCK_ERROR;
    }

    if (allow_recursive) {
        uint32_t old = sx_recursive_.fetch_sub(1, std::memory_order_acq_rel);
        if (old > 1)
            return UB_LOCK_SUCCESS;
    }

    sx_recursive_.store(0, std::memory_order_release);
    lock_sx_owner.store(0, std::memory_order_release);

    uint64_t state_after_release = 0;
    if (!local_lock_try_release_sx(lock_word, state_after_release)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Hold the lock before unlocking");
        return UB_LOCK_ERROR;
    }
    if (waiting_count.load(std::memory_order_acquire) > 0) {
        if (local_lock_state_is_idle(state_after_release)) { // sx降级，仅当最后一个读者释放，唤醒队列的第一个
            wake_after_unlock_exclusive();
        } else {
            // 等待队列中第一个必须为SX才唤醒
            ub_lock_mode_t head_mode;
            if (!peek_head_waiting_mode_clean(head_mode)) {
                return UB_LOCK_SUCCESS;
            }
            if (head_mode == UB_LOCK_SX) {
                wake_after_unlock_exclusive();
            }
        }
    }
    return UB_LOCK_SUCCESS;
}

bool LocalLock::try_inc_global_ref()
{
    if (global_state_.load(std::memory_order_acquire) != LocalLock::GLOBAL_HELD) {
        return false;
    }

    int32_t ref = global_read_ref_count_.load(std::memory_order_acquire);
    while (ref > 0) {
        // 检查引用计数是否即将溢出
        if (ref >= INT32_MAX) {
            return false;
        }
        // 再次确认 state 仍是 HELD
        if (global_state_.load(std::memory_order_acquire) != LocalLock::GLOBAL_HELD) {
            return false;
        }
        if (global_read_ref_count_.compare_exchange_weak(ref, ref + 1, std::memory_order_acq_rel,
                                                         std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

ub_lock_result_t DistributedLock::delay_unlock(ub_lock_mode_t mode, uint8_t node_id)
{
    if (node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid delay_unlock node_id %u >= UB_MAX_NODES", node_id);
        return UB_LOCK_ERROR;
    }
    rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    const ub_location_t location = {.tid = 0, .node_id = node_id}; // node_id才是关键
    switch (mode) {
        case UB_LOCK_S: {
            int32_t old = rw_lock_shm_->lock_word.fetch_add(1, std::memory_order_acq_rel);
            int32_t now = old + 1;
            // 唤醒等待者（仅当最后一个读者释放）
            if (now == X_LOCK_DECR && rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
                wake_after_unlock_exclusive(location); // 失败没有合适的等待者（超时）
            }
            clear_shared_owner_bitmap(rw_lock_shm_, location.node_id);
            return UB_LOCK_SUCCESS;
        }
        case UB_LOCK_SX: {
            // 真正释放 SX：把 lock_word 加回 half，并清 owner
            rw_lock_shm_->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);
            // fetch_add 更稳健：允许并发读者在 SX 期间增减 lock_word
            int32_t old = rw_lock_shm_->lock_word.fetch_add(X_LOCK_HALF_DECR, std::memory_order_acq_rel);
            int32_t now = old + X_LOCK_HALF_DECR;
            // 唤醒等待者（仅当最后一个读者释放）释放后按批量规则唤醒
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
                if (now == X_LOCK_DECR) { // sx降级，仅当最后一个读者释放，唤醒队列的第一个
                    wake_after_unlock_exclusive(location);
                } else {
                    // 等待队列中第一个必须为SX才唤醒
                    ub_lock_mode_t head_mode;
                    if (!peek_head_waiting_mode_clean(head_mode)) {
                        return UB_LOCK_SUCCESS;
                    }
                    if (head_mode == UB_LOCK_SX) {
                        wake_after_unlock_exclusive(location);
                    }
                }
            }
            return UB_LOCK_SUCCESS;
        }
        case UB_LOCK_X: {
            rw_lock_shm_->lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->x_recursive.store(0, std::memory_order_release);
            rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
            // 出队唤醒队等待者
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
                wake_after_unlock_exclusive(location);
            }
            return UB_LOCK_SUCCESS;
        }
        default:
            return UB_LOCK_ERROR;
    }
}
/*
    本地锁加锁 + 延迟释放冲突
    1、调用本地锁加锁(has_local_locks获取)
    2、成功后判断释放开启有延迟释放（全局锁中的reserve_lock_owner）
    3、开启则表示延迟释放冲突：
        1、如果是本地线程之间的冲突，直接本地直接释放delay_unlock
        2、如果是非本地线程之间的冲突，走消息队列通知：
            a、调用send接口发送目的端（reserve_lock_owner）消息，带上对端本地锁地址(has_local_locks获取), 然后加入等待队列
            b、对端polling线程拿到本地锁实例地址，本地锁中获取对应的全局锁地址(ub_lock_ptr_)
            c、对端polling线程会判断本地锁中是否被持有，如果是的话直接结束，否则调用unlock（走正常流程unlock的接口）
*/
ub_lock_result_t DistributedLock::delay_release_local_lock(LocalLock &local_lock, ub_lock_mode_t mode,
                                                           const ub_location_t &location)
{
    // 原子获取并清除牌子，确保只有一个线程处理这把遗产锁
    auto reserve_mode = local_lock.local_is_reserve_lock.exchange(UB_LOCK_I, std::memory_order_acq_rel);
    if (reserve_mode != UB_LOCK_I) { // 本地有线程延迟释放过
        if (reserve_mode == mode) {
            // 抹除全局声明，防止其他节点并发
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
            return UB_LOCK_CONFLICT; // 继承
        }
        // 本地冲突：本地释放全局锁，走正常 unlock 流程
        (void)delay_unlock(reserve_mode, location.node_id);
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t DistributedLock::delay_release_ub_lock(ub_lock_mode_t mode, LocalLock &local_lock,
                                                        const ub_location_t &location)
{
    // 拿远端标志，检查是否开启过延迟释放
    uint64_t reserve_lock_owner = rw_lock_shm_->reserve_lock_owner.load(std::memory_order_acquire);
    if (reserve_lock_owner == LOCK_INVALID_OWNER) {
        return UB_LOCK_SUCCESS;
    }
    const ub_location_t owner{.tid = 0, .node_id = static_cast<uint8_t>((reserve_lock_owner >> 32) & 0xFF)};
    if (owner.node_id == location.node_id) {
        return UB_LOCK_SUCCESS;
    }
    if (owner.node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid reserve owner node_id %u >= UB_MAX_NODES", owner.node_id);
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        return UB_LOCK_ERROR;
    }

    ub_rw_lock_t *remote_lock = reinterpret_cast<ub_rw_lock_t *>(rw_lock_shm_->node_registry[owner.node_id]);

    if (remote_lock == nullptr) {
        return UB_LOCK_SUCCESS; // 走全局锁路径
    }
    if ((reinterpret_cast<uintptr_t>(remote_lock) % UB_CACHELINE_SIZE) != 0u) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid node_registry lock pointer %p for node %u", remote_lock, owner.node_id);
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        return UB_LOCK_ERROR;
    }

    local_msg_body_t body{};
    body.tid = 0;
    body.addr = remote_lock; // 关键：对端可解引用的本地锁地址 token
    body.type = UB_RELEASE;
    body.mode = mode;

    return notify_unlock(owner, location.node_id, body); // 消息通知
}

} // namespace ublock
