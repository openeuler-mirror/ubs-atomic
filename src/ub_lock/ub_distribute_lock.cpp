/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "inner_distribute_lock.h"

namespace ublock {

DistributedLock::~DistributedLock() {};

std::shared_ptr<LocalLock> lookup_local_lock(ub_rw_lock_t *shm)
{
    std::shared_lock<std::shared_mutex> lk(g_ll_registry.mtx);
    auto it = g_ll_registry.map.find(shm);
    return it == g_ll_registry.map.end() ? nullptr : it->second;
}

void register_local_lock(ub_rw_lock_t *shm, std::shared_ptr<LocalLock> ll)
{
    std::unique_lock<std::shared_mutex> lk(g_ll_registry.mtx);
    g_ll_registry.map.try_emplace(shm, std::move(ll));
}

std::shared_ptr<LocalLock> unregister_local_lock(ub_rw_lock_t *shm)
{
    std::shared_ptr<LocalLock> ll_holder = nullptr;
    {
        std::unique_lock<std::shared_mutex> lk(g_ll_registry.mtx);
        auto it = g_ll_registry.map.find(shm);
        if (it != g_ll_registry.map.end()) {
            ll_holder = std::move(it->second);
            g_ll_registry.map.erase(it);
        }
    }
    return ll_holder;
}

void DistributedLock::dequeue_and_notify_one(const ub_location_t &location)
{
    int sanity = UB_MAX_CAPACITY * 2;
    while (sanity-- > 0) {
        ub_waiter_t *w = nullptr;
        if (outqueue_waiter(w) == UB_LOCK_SUCCESS) {
            (void)notify_waiters(*w, location);
            return;
        }
    }
}

void DistributedLock::wake_after_unlock_exclusive(const ub_location_t &location)
{
    // 规则：
    // 1) 首个 X：只唤醒 X
    // 2) 首个 S：连续唤醒 S/SX，直到遇到 X
    // 3) 首个 SX：唤醒 SX，然后连续唤醒 S，直到遇到 SX 或 X
    ub_lock_mode_t head_mode;
    // 先把队首无效项清干净，并确认队首是 WAITING
    if (!peek_head_waiting_mode_clean(head_mode)) {
        return; // 空
    }

    // 首个是 X：只唤醒一个 X
    if (head_mode == UB_LOCK_X) {
        dequeue_and_notify_one(location);
        return;
    }

    // B) 首个是 S：唤醒 S，然后继续唤醒后续 S/SX，直到遇到 X
    if (head_mode == UB_LOCK_S) {
        dequeue_and_notify_one(location);
        bool meet_sx = false;
        while (true) {
            ub_lock_mode_t m;
            if (!peek_head_waiting_mode_clean(m)) {
                return; // 空
            }
            if (m == UB_LOCK_X) {
                return; // 遇到 X 停止（不出队）
            }
            if (m == UB_LOCK_S) {
                dequeue_and_notify_one(location);
                continue;
            }
            if (m == UB_LOCK_SX && !meet_sx) {
                meet_sx = true;
                dequeue_and_notify_one(location);
                continue;
            }
            return;
        }
    }

    // C) 首个是 SX：唤醒 SX，然后继续唤醒后续 S，直到遇到 SX 或 X
    if (head_mode == UB_LOCK_SX) {
        dequeue_and_notify_one(location);
        while (true) {
            ub_lock_mode_t m;
            if (!peek_head_waiting_mode_clean(m)) {
                return;
            }
            if (m == UB_LOCK_X || m == UB_LOCK_SX) {
                return; // 遇到 X 或 SX 停止（不出队）
            }
            if (m == UB_LOCK_S) {
                dequeue_and_notify_one(location);
                continue;
            }
            return;
        }
    }
}

void DistributedLock::lock_create(const ub_lock_config_t &config, const ub_location_t &location)
{
    if (location.node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid node_id [0, %d) : location.node_id=%d", UB_MAX_NODES, location.node_id);
        return;
    }

    // 快路径：已经初始化
    if (rw_lock_shm_->is_inited.fetch_add(1u, std::memory_order_acq_rel) > 0) {
        ATOMIC_LOG(LOG_LEVEL_INFO, "UB lock is already init!");
        if (!lookup_local_lock(rw_lock_shm_)) {
            auto ll_sp = std::make_shared<LocalLock>(rw_lock_shm_);
            register_local_lock(rw_lock_shm_, ll_sp);
            rw_lock_shm_->node_registry[location.node_id] = reinterpret_cast<uintptr_t>(rw_lock_shm_);
        }
        register_message_process_func();
        return;
    }

    // 1. 初始化 lock_word（无锁状态）
    std::memset(rw_lock_shm_->_pad_core, 0, sizeof(rw_lock_shm_->_pad_core));
    std::memset(rw_lock_shm_->_pad_qt, 0, sizeof(rw_lock_shm_->_pad_qt));
    std::memset(rw_lock_shm_->_pad_qh, 0, sizeof(rw_lock_shm_->_pad_qh));
    std::memset(rw_lock_shm_->_pad_misc, 0, sizeof(rw_lock_shm_->_pad_misc));
    rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    // 可重入
    rw_lock_shm_->x_recursive.store(0, std::memory_order_release);
    rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);

    // 2. 清空 owner (LOCK_INVALID_OWNER 表示无效)
    rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    rw_lock_shm_->lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
    rw_lock_shm_->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);

    // 4. 初始化等待队列
    create_wait_queue();

    // 5. 初始化 local lock 表
    auto ll = std::make_shared<LocalLock>(rw_lock_shm_);
    register_local_lock(rw_lock_shm_, ll);
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        rw_lock_shm_->node_registry[i] = 0;
    }
    rw_lock_shm_->node_registry[location.node_id] = reinterpret_cast<uintptr_t>(rw_lock_shm_);

    register_message_process_func();
    ATOMIC_LOG(LOG_LEVEL_INFO, "UB lock create success!");
    return;
}

void DistributedLock::lock_free(const ub_location_t &location)
{
    if (location.node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid node_id [0, %d) : location.node_id=%d", UB_MAX_NODES, location.node_id);
        return;
    }
    // 从 registry 删除，拿回 shared_ptr（让其在函数末尾析构）
    auto ll = unregister_local_lock(rw_lock_shm_);
    // 栈上/静态数组，所以只需标记为回收即可
    rw_lock_shm_->is_inited.fetch_sub(1u, std::memory_order_acq_rel);
    ATOMIC_LOG(LOG_LEVEL_INFO, "UB lock free success!");
    return;
}

/*
lock_s(): lock_word 初始值是X_LOCK_DECR
  ├─ 1. 参数检查（policy）
  ├─ 2. 是否允许本地锁（延迟释放 → 本地锁）
  ├─ 3. 若允许本地锁 → local_try_lock(S) （成功到4）
  ├─ 4. 快路径：没有watiers lock_word >0 可以CAS(lock_word -= 1)
  │     ├─ 成功 → SUCCESS
  │     └─ 失败
  ├─ 5. 否则 → 入等待队列 睡眠等待唤醒（condition_variable ）
  ├─ 6. 如何是超时唤醒clean_timeout_waiter（condition_variable ）
*/
ub_lock_result_t DistributedLock::verify_param(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 规则：node_id 范围[0, 255)
    if (location.node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid node_id [0, %d) : location.node_id=%d", UB_MAX_NODES, location.node_id);
        return UB_LOCK_ERROR;
    }
    return UB_LOCK_SUCCESS;
}

// 统一的清理与回滚逻辑 (失败/超时调用)
void DistributedLock::cleanup_and_unlock_local(LocalLock *local_lock)
{
    if (local_lock) {
        // 恢复状态为 IDLE，重置引用计数
        local_lock->global_state_.store(LocalLock::GLOBAL_IDLE, std::memory_order_release);
        local_lock->global_read_ref_count_.store(0, std::memory_order_release);
        // 唤醒 Follower 让它们重新去抢
        local_lock->global_pending_cv_.notify_all();

        // 释放之前持有的本地锁
        local_lock->unlock_s();
    }
}

// 尝试获取全局物理锁 S
// 成功返回 true，失败返回 false
bool DistributedLock::try_acquire_global_s(LocalLock *local_lock, bool &is_awakened, uint32_t slot)
{
    int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);

    // CAS: lock_word -= 1
    if (cur > 0 && rw_lock_shm_->lock_word.compare_exchange_weak(cur, cur - 1, std::memory_order_acq_rel,
                                                                 std::memory_order_acquire)) {
        // 成功获取 S 锁
        if (is_awakened) {
            clean_outqueue_waiter(slot);
        }
        if (local_lock) {
            // Leader 必须初始化计数器为 1
            local_lock->global_read_ref_count_.store(1, std::memory_order_release);
            // 切换状态为 HELD，开放 Follower 上车
            local_lock->global_state_.store(LocalLock::GLOBAL_HELD, std::memory_order_release);

            // 唤醒所有在 wait 的 Follower (移出锁外通知)
            local_lock->global_pending_cv_.notify_all();
        }
        return true;
    }
    return false;
}

ub_lock_result_t DistributedLock::lock_s(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(policy, location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    ub_lock_result_t ret;
    LocalLock *local_lock = nullptr;
    steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(policy.timeout_ts);

    // 优先尝试本地锁（如果允许）
    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    local_lock = ll_sp.get();

    if (local_lock) {
        ret = local_lock->lock_s(location.tid, deadline);
        if (ret != UB_LOCK_SUCCESS) {
            return ret; // 本地锁超时或失败，直接返回结果
        }
        while (true) {
            // 已经有人持有全局锁了
            if (local_lock->try_inc_global_ref()) {
                return UB_LOCK_SUCCESS;
            }
            //还没人持有，尝试竞争 Leader
            if (local_lock->global_state_.load(std::memory_order_relaxed) == LocalLock::GLOBAL_IDLE) {
                int expected = LocalLock::GLOBAL_IDLE;
                if (local_lock->global_state_.compare_exchange_strong(expected, LocalLock::GLOBAL_PENDING,
                                                                      std::memory_order_acq_rel)) {
                    break;
                }
            }
            //  有人在抢，Follower (PENDING) -> 等待结果通知
            {
                std::unique_lock<std::mutex> lk(local_lock->global_pending_mtx_);
                bool ok = local_lock->global_pending_cv_.wait_until(lk, deadline, [&] {
                    return local_lock->global_state_.load(std::memory_order_acquire) != LocalLock::GLOBAL_PENDING;
                });

                if (!ok || std::chrono::steady_clock::now() > deadline) {
                    local_lock->unlock_s();
                    return UB_LOCK_TIMEOUT;
                }
            }
        }
        // 本地锁成功或没有本地锁，往下继续尝试全局锁
        ret = delay_release_local_lock(*local_lock, UB_LOCK_S, location);
        if (ret == UB_LOCK_CONFLICT) {
            local_lock->global_read_ref_count_.store(1, std::memory_order_release);
            local_lock->global_state_.store(LocalLock::GLOBAL_HELD, std::memory_order_release);
            // 唤醒所有在 wait 的 Follower
            local_lock->global_pending_cv_.notify_all();
            return UB_LOCK_SUCCESS; // 本地线程延迟释放过，直接获取
        }
    }
    uint32_t slot = 0;
    bool is_awakened = false;
    while (true) {
        // 自旋
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            // 若已有 waiters，不走快路径（防止饥饿）
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
                break;
            }
            if (try_acquire_global_s(local_lock, is_awakened, slot)) {
                return UB_LOCK_SUCCESS;
            }
            if (is_awakened) {
                cpu_relax();
                i--;
                if (std::chrono::steady_clock::now() > deadline) {
                    clean_timeout_waiter(slot);
                    cleanup_and_unlock_local(local_lock);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0)
                cpu_relax();
        }
        // 等待队列
        {
            local_wait_ctx_t ctx;
            WaiterGuard guard(location.tid, &ctx);
            ret = enqueue_waiter(UB_LOCK_S, location, slot);
            if (ret != UB_LOCK_SUCCESS) {
                cleanup_and_unlock_local(local_lock);
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock waiting queue is full"); //队列满
                return ret;
            }
            // double check 自我唤醒
            bool self_handoff = false;
            int32_t current_val = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            if (current_val == X_LOCK_DECR) { // 锁完全空闲条件
                self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
                    rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->wait_queue, slot);
            }
            (void)delay_release_ub_lock(UB_LOCK_S, *local_lock, location);
            // 睡眠等待 防止无限循环
            if (!self_handoff) {
                if (!ctx.wait(deadline)) {
                    // timeout：清理队列 slot
                    clean_timeout_waiter(slot);
                    cleanup_and_unlock_local(local_lock);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
            }
            is_awakened = true;
        }
    }
}

/*
unlock_s():
  1. 校验 inited / recycled（可选）
  2. 解析 policy（allow_delay_release => allow_local_lock）
  3. 全局释放：lock_word += 1（fetch_add）
  4. 若 new == X_LOCK_DECR 且 has_waiters=true → notify_unlock()
  5. 若走了本地锁降级 → local_try_unlock(S)
  6. return

 */
ub_lock_result_t DistributedLock::unlock_s(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(policy, location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    // 先解全局锁再解本地锁
    std::shared_ptr<LocalLock> ll_sp;
    ll_sp = lookup_local_lock(rw_lock_shm_);
    LocalLock *local_lock = ll_sp.get();
    if (!local_lock)
        return UB_LOCK_ERROR;
    ub_lock_result_t ret;
    int32_t old_ref = local_lock->global_read_ref_count_.fetch_sub(1, std::memory_order_acq_rel);
    if (old_ref > 1) {
        // 还有其他本地读，只解本地锁
        ret = local_lock->unlock_s();
        return ret;
    }
    // 本节点最后一个读者
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    // 预判是否满足延迟释放条件
    bool can_delay = policy.allow_delay_release && (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) &&
                     (local_lock->waiting_count.load(std::memory_order_acquire) == 0);
    if (!can_delay) {
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        local_lock->local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
        const int32_t old = rw_lock_shm_->lock_word.fetch_add(1, std::memory_order_acq_rel);
        const int32_t now = old + 1;
        if (now > X_LOCK_DECR) {
            // 读锁释放过多，逻辑错误
            rw_lock_shm_->lock_word.fetch_sub(1, std::memory_order_release);
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Hold the lock before unlocking");
            return UB_LOCK_ERROR;
        }
        // 唤醒等待者（仅当最后一个读者释放）
        if (now == X_LOCK_DECR && rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
            wake_after_unlock_exclusive(location);
        }
    } else {
        rw_lock_shm_->reserve_lock_owner.store(identify, std::memory_order_release);
        local_lock->local_is_reserve_lock.store(UB_LOCK_S, std::memory_order_release);
    }
    int expected = LocalLock::GLOBAL_HELD;
    local_lock->global_state_.compare_exchange_strong(expected, LocalLock::GLOBAL_IDLE, std::memory_order_acq_rel,
                                                      std::memory_order_acquire);
    local_lock->read_count.store(0, std::memory_order_release);
    ret = local_lock->unlock_s();
    return ret;
}
/*
lock_x():
  1. 参数解析（policy / recursive）
  2. 若 recursive 且 owner == 当前线程：
       - x_recursive++
       - return SUCCESS
  3. 尝试 fast path：
       if lock_word == X_LOCK_DECR and no waiters:
           CAS(lock_word, X_LOCK_DECR → 0)
           set owner
           x_recursive = 1
           return SUCCESS
  4. 自旋等待 SPIN_WAIT_ROUNDS：
       重复步骤 3

  5. 慢路径：
       enqueue waiter
       sleep (cv / sync cell)
  6. 被唤醒：
       goto fast path
*/
ub_lock_result_t DistributedLock::lock_x(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(policy, location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    ub_lock_result_t ret;
    LocalLock *local_lock = nullptr;
    steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(policy.timeout_ts);
    // 优先尝试本地锁（如果允许）
    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    local_lock = ll_sp.get();
    if (local_lock) {
        ret = local_lock->lock_x(policy.recursive, location.tid, deadline);
        if (ret != UB_LOCK_SUCCESS) {
            return ret; // 本地锁超时或失败，直接返回结果
        }
        // 本地锁成功或没有本地锁，往下继续尝试全局锁
        ret = delay_release_local_lock(*local_lock, UB_LOCK_X, location);
        if (ret == UB_LOCK_CONFLICT) {
            rw_lock_shm_->lock_owner_x.store(identify, std::memory_order_release);
            return UB_LOCK_SUCCESS; // 本地线程延迟释放过，直接获取
        }
    }

    // 可重入：如果自己已持有 X 锁
    uint64_t owner = rw_lock_shm_->lock_owner_x.load(std::memory_order_acquire);
    if (owner == identify) {
        if (policy.recursive) {
            rw_lock_shm_->x_recursive.fetch_add(1, std::memory_order_acq_rel);
            return UB_LOCK_SUCCESS;
        }
        if (local_lock) {
            local_lock->unlock_x(policy.recursive, location.tid);
        }
        ATOMIC_LOG(LOG_LEVEL_ERROR, "UB recursive lock is not allowed!");
        return UB_LOCK_ERROR;
    }

    uint32_t slot = 0;
    bool is_awakened = false;
    while (true) {
        // 快路径：自旋固定轮次
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            // 若已有 waiters，不走快路径（防止饥饿）
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
                break;
            }
            int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            // X 锁要求：没有读者/写者，即 cur == X_LOCK_DECR
            if (cur == X_LOCK_DECR) {
                // CAS: lock_word 从 X_LOCK_DECR -> 0 （等价于 -X_LOCK_DECR）
                if (rw_lock_shm_->lock_word.compare_exchange_weak(cur, 0, std::memory_order_acq_rel,
                                                                  std::memory_order_acquire)) {
                    // 成功获取 X 锁：记录 owner
                    rw_lock_shm_->lock_owner_x.store(identify, std::memory_order_release);
                    rw_lock_shm_->x_recursive.store(1, std::memory_order_release);
                    if (is_awakened) {
                        clean_outqueue_waiter(slot);
                    }
                    return UB_LOCK_SUCCESS;
                }
            }
            if (is_awakened) {
                cpu_relax();
                i--;
                if (std::chrono::steady_clock::now() > deadline) {
                    clean_timeout_waiter(slot);
                    if (local_lock) {
                        local_lock->unlock_x(policy.recursive, location.tid);
                    }
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0)
                cpu_relax();
        }

        {
            local_wait_ctx_t ctx;
            WaiterGuard guard(location.tid, &ctx);
            ret = enqueue_waiter(UB_LOCK_X, location, slot);
            if (ret != UB_LOCK_SUCCESS) {
                if (local_lock) {
                    local_lock->unlock_x(policy.recursive, location.tid);
                }
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock waiting queue is full"); //队列满
                return ret;
            }
            // double check 自我唤醒
            bool self_handoff = false;
            int32_t current_val = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            if (current_val == X_LOCK_DECR) { // 锁完全空闲条件
                self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
                    rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->wait_queue, slot);
            }
            (void)delay_release_ub_lock(UB_LOCK_X, *local_lock, location);
            // 睡眠等待 防止无限循环
            if (!self_handoff) {
                if (!ctx.wait(deadline)) {
                    // timeout：清理队列 slot
                    clean_timeout_waiter(slot);
                    if (local_lock) {
                        local_lock->unlock_x(policy.recursive, location.tid);
                    }
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
            }
            is_awakened = true;
        }
    }
}

ub_lock_result_t DistributedLock::unlock_x(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(policy, location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    // 先解全局锁再解本地锁
    std::shared_ptr<LocalLock> ll_sp;
    ll_sp = lookup_local_lock(rw_lock_shm_);
    LocalLock *local_lock = ll_sp.get();
    if (!local_lock)
        return UB_LOCK_ERROR;
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    // 预判是否满足延迟释放条件
    bool can_delay = policy.allow_delay_release && (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) &&
                     (local_lock->waiting_count.load(std::memory_order_acquire) == 0);
    if (!can_delay) {
        uint64_t owner = rw_lock_shm_->lock_owner_x.load(std::memory_order_acquire);
        if (owner != identify) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Only the holder can release it");
            return UB_LOCK_ERROR;
        }
        // 可重入：先递减递归层数，若仍 x_recursive>0，说明只是退出一层，不释放全局写锁
        bool release_global = true;
        if (policy.recursive) {
            uint16_t old = rw_lock_shm_->x_recursive.fetch_sub(1, std::memory_order_release);
            if (old > 1) {
                release_global = false; // 还有重入层，写锁继续保持
            } else {                    // old == 1：本次释放是最后一层，继续走真正 unlock
                release_global = true;
            }
        }
        if (release_global) {
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
            local_lock->local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
            rw_lock_shm_->lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->x_recursive.store(0, std::memory_order_release);
            rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);

            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
                wake_after_unlock_exclusive(location);
            }
        }
    } else {
        rw_lock_shm_->reserve_lock_owner.store(identify, std::memory_order_release);
        local_lock->local_is_reserve_lock.store(UB_LOCK_X, std::memory_order_release);
    }

    local_lock->global_state_.store(LocalLock::GLOBAL_IDLE, std::memory_order_release);
    ub_lock_result_t ret = local_lock->unlock_x(policy.recursive, location.tid);
    return ret;
}

ub_lock_result_t DistributedLock::lock_sx(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(policy, location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    ub_lock_result_t ret;
    LocalLock *local_lock = nullptr;
    steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(policy.timeout_ts);

    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    local_lock = ll_sp.get();

    // 走本地锁：如果没有 local_lock，则回退走全局锁
    if (local_lock) {
        ret = local_lock->lock_sx(policy.recursive, location.tid, deadline);
        if (ret != UB_LOCK_SUCCESS) {
            return ret; // 本地锁超时或失败，直接返回结果
        }
        ret = delay_release_local_lock(*local_lock, UB_LOCK_SX, location);
        if (ret == UB_LOCK_CONFLICT) {
            rw_lock_shm_->lock_owner_sx.store(identify, std::memory_order_release);
            return UB_LOCK_SUCCESS; // 本地线程延迟释放过，直接获取
        }
    }
    // 递归快路径：已持有 SX 且允许递归
    uint64_t owner = rw_lock_shm_->lock_owner_sx.load(std::memory_order_acquire);
    if (owner == identify) {
        if (policy.recursive) {
            rw_lock_shm_->sx_recursive.fetch_add(1, std::memory_order_acq_rel);
            return UB_LOCK_SUCCESS;
        }
        if (local_lock) {
            local_lock->unlock_sx(policy.recursive, location.tid);
        }
        ATOMIC_LOG(LOG_LEVEL_ERROR, "UB recursive lock is not allowed");
        return UB_LOCK_ERROR;
    }

    uint32_t slot = 0;
    bool is_awakened = false; // 唤醒优先
    while (true) {
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
                break;
            }
            // SX 条件：必须在“上半区”（表示当前没有 SX/X），并且 cur 足够大
            int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            // 等价于：读者数 < X_LOCK_HALF_DECR
            if (cur > X_LOCK_HALF_DECR) {
                int32_t next = cur - X_LOCK_HALF_DECR;

                if (rw_lock_shm_->lock_word.compare_exchange_weak(cur, next, std::memory_order_acq_rel,
                                                                  std::memory_order_acquire)) {
                    // 获得 SX：设置 owner + recursion=1
                    rw_lock_shm_->lock_owner_sx.store(identify, std::memory_order_release);
                    rw_lock_shm_->sx_recursive.store(1, std::memory_order_release);
                    if (is_awakened) {
                        clean_outqueue_waiter(slot);
                    }
                    return UB_LOCK_SUCCESS;
                }
            }
            if (is_awakened) { // Handoff
                cpu_relax();
                i--;
                if (std::chrono::steady_clock::now() > deadline) {
                    clean_timeout_waiter(slot);
                    if (local_lock) {
                        local_lock->unlock_sx(policy.recursive, location.tid);
                    }
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0)
                cpu_relax();
        }
        {
            local_wait_ctx_t ctx;
            WaiterGuard guard(location.tid, &ctx);
            ret = enqueue_waiter(UB_LOCK_SX, location, slot);
            if (ret != UB_LOCK_SUCCESS) {
                if (local_lock) {
                    local_lock->unlock_sx(policy.recursive, location.tid);
                }
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock waiting queue is full"); //队列满
                return ret;
            }
            // double check 自我唤醒
            bool self_handoff = false;
            int32_t current_val = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            if (current_val == X_LOCK_DECR) { // 锁完全空闲条件
                self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
                    rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->wait_queue, slot);
            }
            (void)delay_release_ub_lock(UB_LOCK_SX, *local_lock, location);
            // 睡眠等待 防止无限循环
            if (!self_handoff) {
                // 睡眠等待 防止无限循环
                if (!ctx.wait(deadline)) {
                    clean_timeout_waiter(slot);
                    if (local_lock) {
                        local_lock->unlock_sx(policy.recursive, location.tid);
                    }
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
            }
            is_awakened = true;
        }
    }
}

ub_lock_result_t DistributedLock::unlock_sx(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(policy, location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    // 先解全局锁再解本地锁
    std::shared_ptr<LocalLock> ll_sp;
    ll_sp = lookup_local_lock(rw_lock_shm_);
    LocalLock *local_lock = ll_sp.get();
    if (!local_lock)
        return UB_LOCK_ERROR;
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    // 预判是否满足延迟释放条件
    bool can_delay = policy.allow_delay_release && (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) &&
                     (local_lock->waiting_count.load(std::memory_order_acquire) == 0);
    if (!can_delay) {
        uint64_t owner = rw_lock_shm_->lock_owner_sx.load(std::memory_order_acquire);
        if (owner != identify) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Only the holder can release it");
            return UB_LOCK_ERROR;
        }
        // 递归：只退一层，不释放 SX
        bool release_global = true;
        if (policy.recursive) {
            uint32_t old = rw_lock_shm_->sx_recursive.fetch_sub(1, std::memory_order_release);
            if (old > 1) {
                release_global = false; // 还有重入层，写锁继续保持
            } else {                    // old == 1：本次释放是最后一层，继续走真正 unlock
                release_global = true;
            }
        }
        if (release_global) {
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
            local_lock->local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
            rw_lock_shm_->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);
            int32_t sx_old = rw_lock_shm_->lock_word.fetch_add(X_LOCK_HALF_DECR, std::memory_order_acq_rel);
            int32_t now = sx_old + X_LOCK_HALF_DECR;
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
                if (now == X_LOCK_DECR) {
                    wake_after_unlock_exclusive(location);
                } else {
                    ub_lock_mode_t head_mode;
                    if (!peek_head_waiting_mode_clean(head_mode)) {
                        return UB_LOCK_SUCCESS;
                    }
                    if (head_mode == UB_LOCK_SX) {
                        wake_after_unlock_exclusive(location);
                    }
                }
            }
        }
    } else {
        rw_lock_shm_->reserve_lock_owner.store(identify, std::memory_order_release);
        local_lock->local_is_reserve_lock.store(UB_LOCK_SX, std::memory_order_release);
    }

    local_lock->global_state_.store(LocalLock::GLOBAL_IDLE, std::memory_order_release);
    ub_lock_result_t ret = local_lock->unlock_sx(policy.recursive, location.tid);
    return ret;
}

} // namespace ublock
