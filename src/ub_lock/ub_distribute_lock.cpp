/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "inner_distribute_lock.h"

#include <string>

namespace ublock {

DistributedLock::~DistributedLock() {}

bool is_valid_rebuild_mode(ub_lock_mode_t mode)
{
    return mode == UB_LOCK_I || mode == UB_LOCK_S || mode == UB_LOCK_SX || mode == UB_LOCK_X;
}

const char *lock_mode_name(ub_lock_mode_t mode)
{
    switch (mode) {
        case UB_LOCK_S:
            return "S";
        case UB_LOCK_SX:
            return "SX";
        case UB_LOCK_X:
            return "X";
        case UB_LOCK_I:
            return "I";
        default:
            return "UNKNOWN";
    }
}

uint32_t owner_node(uint64_t owner)
{
    return owner == LOCK_INVALID_OWNER ? static_cast<uint32_t>(UB_MAX_NODES) :
                                         static_cast<uint32_t>(owner >> UB_LOCK_OWNER_NODE_SHIFT);
}

int32_t owner_tid(uint64_t owner)
{
    return owner == LOCK_INVALID_OWNER ? 0 : static_cast<int32_t>(owner & 0xFFFFFFFFu);
}

bool has_valid_owner(uint64_t owner)
{
    return owner != LOCK_INVALID_OWNER;
}

struct TimeoutGlobalSnapshot {
    int32_t lock_word{0};
    uint64_t owner_x{LOCK_INVALID_OWNER};
    uint64_t owner_sx{LOCK_INVALID_OWNER};
    uint64_t reserve_owner{LOCK_INVALID_OWNER};
    uint32_t shared_bitmap{0};
    uint32_t waiting_count{0};
};

struct TimeoutLocalSnapshot {
    int32_t lock_word{0};
    uint32_t read_count{0};
    uint32_t waiting_count{0};
    int32_t owner_x{0};
    int32_t owner_sx{0};
    ub_lock_mode_t reserve_mode{UB_LOCK_I};
};

struct TimeoutGlobalLogInfo {
    TimeoutGlobalSnapshot snapshot;
    std::string owner_x_desc;
    std::string owner_sx_desc;
    std::string reserve_owner_desc;
    std::string blocker_desc;
};

struct TimeoutLocalLogInfo {
    TimeoutLocalSnapshot snapshot;
    std::string blocker_desc;
};

struct TimeoutLogContext {
    const ub_location_t *location{nullptr};
    ub_lock_mode_t request_mode{UB_LOCK_I};
    const char *wait_scope{nullptr};
    std::string blocker;
    ub_rw_lock_t *lock{nullptr};
};

std::string format_owner(uint64_t owner)
{
    if (!has_valid_owner(owner)) {
        return "none";
    }
    return "node=" + std::to_string(owner_node(owner)) + ",tid=" + std::to_string(owner_tid(owner));
}

std::string format_global_blocker(ub_lock_mode_t request_mode, const TimeoutGlobalSnapshot &snapshot)
{
    if (has_valid_owner(snapshot.owner_x)) {
        return "X(node=" + std::to_string(owner_node(snapshot.owner_x)) +
               ",tid=" + std::to_string(owner_tid(snapshot.owner_x)) + ")";
    }
    if ((request_mode == UB_LOCK_X || request_mode == UB_LOCK_SX) && has_valid_owner(snapshot.owner_sx)) {
        return "SX(node=" + std::to_string(owner_node(snapshot.owner_sx)) +
               ",tid=" + std::to_string(owner_tid(snapshot.owner_sx)) + ")";
    }
    if (request_mode == UB_LOCK_X && snapshot.shared_bitmap != 0u) {
        return "S";
    }
    if (request_mode == UB_LOCK_SX && snapshot.lock_word <= X_LOCK_HALF_DECR && snapshot.shared_bitmap != 0u) {
        return "S";
    }
    if (has_valid_owner(snapshot.reserve_owner)) {
        return "DELAYED(node=" + std::to_string(owner_node(snapshot.reserve_owner)) +
               ",tid=" + std::to_string(owner_tid(snapshot.reserve_owner)) + ")";
    }
    if (snapshot.shared_bitmap != 0u) {
        return "S";
    }
    if (snapshot.waiting_count > 0u) {
        return "WAITERS(count=" + std::to_string(snapshot.waiting_count) + ")";
    }
    return "UNKNOWN";
}

bool timeout_is_local_wait(const char *reason)
{
    return reason != nullptr && reason[0] == 'l';
}

bool timeout_is_same_node_wait(const char *reason)
{
    return reason != nullptr && reason[0] == 'f';
}

const char *timeout_wait_scope_name(const char *reason)
{
    if (timeout_is_local_wait(reason)) {
        return "local";
    }
    if (timeout_is_same_node_wait(reason)) {
        return "same_node";
    }
    return "global";
}

std::string format_local_blocker(ub_lock_mode_t request_mode, int32_t request_tid, const TimeoutLocalSnapshot &snapshot)
{
    if (snapshot.owner_x != 0) {
        return "X(tid=" + std::to_string(snapshot.owner_x) + "," +
               (snapshot.owner_x == request_tid ? "self" : "other") + ")";
    }
    if ((request_mode == UB_LOCK_X || request_mode == UB_LOCK_SX) && snapshot.owner_sx != 0) {
        return "SX(tid=" + std::to_string(snapshot.owner_sx) + "," +
               (snapshot.owner_sx == request_tid ? "self" : "other") + ")";
    }
    if (request_mode == UB_LOCK_X && snapshot.read_count != 0u) {
        return "S(read_count=" + std::to_string(snapshot.read_count) + ")";
    }
    if (request_mode == UB_LOCK_SX && snapshot.lock_word <= X_LOCK_HALF_DECR && snapshot.read_count != 0u) {
        return "S(read_count=" + std::to_string(snapshot.read_count) + ")";
    }
    if (snapshot.waiting_count != 0u) {
        return "WAITERS(count=" + std::to_string(snapshot.waiting_count) + ")";
    }
    return "UNKNOWN";
}

TimeoutGlobalSnapshot collect_global_timeout_snapshot(ub_rw_lock_t *lock)
{
    TimeoutGlobalSnapshot snapshot{};
    snapshot.lock_word = lock->lock_word.load(std::memory_order_acquire);
    snapshot.owner_x = lock->lock_owner_x.load(std::memory_order_acquire);
    snapshot.owner_sx = lock->lock_owner_sx.load(std::memory_order_acquire);
    snapshot.reserve_owner = lock->reserve_lock_owner.load(std::memory_order_acquire);
    snapshot.shared_bitmap = lock->shared_owner_bitmap.load(std::memory_order_acquire);
    snapshot.waiting_count = lock->waiting_count.load(std::memory_order_acquire);
    return snapshot;
}

TimeoutLocalSnapshot collect_local_timeout_snapshot(LocalLock *local_lock)
{
    TimeoutLocalSnapshot snapshot{};
    snapshot.lock_word = local_lock->lock_word.v.load(std::memory_order_acquire);
    snapshot.read_count = local_lock->read_count.load(std::memory_order_acquire);
    snapshot.waiting_count = local_lock->waiting_count.load(std::memory_order_acquire);
    snapshot.owner_x = local_lock->lock_x_owner.load(std::memory_order_acquire);
    snapshot.owner_sx = local_lock->lock_sx_owner.load(std::memory_order_acquire);
    snapshot.reserve_mode = local_lock->local_is_reserve_lock.load(std::memory_order_acquire);
    return snapshot;
}

TimeoutGlobalLogInfo make_global_timeout_log_info(ub_lock_mode_t request_mode, ub_rw_lock_t *lock)
{
    TimeoutGlobalLogInfo info{};
    info.snapshot = collect_global_timeout_snapshot(lock);
    info.owner_x_desc = format_owner(info.snapshot.owner_x);
    info.owner_sx_desc = format_owner(info.snapshot.owner_sx);
    info.reserve_owner_desc = format_owner(info.snapshot.reserve_owner);
    info.blocker_desc = format_global_blocker(request_mode, info.snapshot);
    return info;
}

TimeoutLocalLogInfo make_local_timeout_log_info(ub_lock_mode_t request_mode, int32_t request_tid, LocalLock *local_lock)
{
    TimeoutLocalLogInfo info{};
    info.snapshot = collect_local_timeout_snapshot(local_lock);
    info.blocker_desc = format_local_blocker(request_mode, request_tid, info.snapshot);
    return info;
}

std::string select_timeout_blocker(const char *reason, const TimeoutGlobalLogInfo &global_info,
                                   const TimeoutLocalLogInfo &local_info)
{
    if (timeout_is_local_wait(reason)) {
        return local_info.blocker_desc;
    }
    if (timeout_is_same_node_wait(reason)) {
        return "same_node_s_leader";
    }
    return global_info.blocker_desc;
}

void log_timeout_null_lock(const TimeoutLogContext &ctx)
{
    ATOMIC_LOG(LOG_LEVEL_ERROR, "UB lock timeout: wait=%s request=%s node=%u tid=%d lock=null", ctx.wait_scope,
               lock_mode_name(ctx.request_mode), static_cast<unsigned>(ctx.location->node_id), ctx.location->tid);
}

void log_timeout_without_local(const TimeoutLogContext &ctx, const TimeoutGlobalLogInfo &global_info)
{
    ATOMIC_LOG(LOG_LEVEL_ERROR,
               "UB lock timeout: wait=%s request=%s node=%u tid=%d lock=%p blocker=%s global[x=%s sx=%s "
               "reserve=%s waiters=%u] local=null",
               ctx.wait_scope, lock_mode_name(ctx.request_mode), static_cast<unsigned>(ctx.location->node_id),
               ctx.location->tid, static_cast<void *>(ctx.lock), global_info.blocker_desc.c_str(),
               global_info.owner_x_desc.c_str(), global_info.owner_sx_desc.c_str(),
               global_info.reserve_owner_desc.c_str(), global_info.snapshot.waiting_count);
}

void log_timeout_with_local(const TimeoutLogContext &ctx, const TimeoutGlobalLogInfo &global_info,
                            const TimeoutLocalLogInfo &local_info)
{
    ATOMIC_LOG(LOG_LEVEL_ERROR,
               "UB lock timeout: wait=%s request=%s node=%u tid=%d lock=%p blocker=%s global[x=%s sx=%s reserve=%s "
               "waiters=%u] local[x=%d sx=%d readers=%u waiters=%u reserve=%s]",
               ctx.wait_scope, lock_mode_name(ctx.request_mode), static_cast<unsigned>(ctx.location->node_id),
               ctx.location->tid, static_cast<void *>(ctx.lock), ctx.blocker.c_str(), global_info.owner_x_desc.c_str(),
               global_info.owner_sx_desc.c_str(), global_info.reserve_owner_desc.c_str(),
               global_info.snapshot.waiting_count, local_info.snapshot.owner_x, local_info.snapshot.owner_sx,
               local_info.snapshot.read_count, local_info.snapshot.waiting_count,
               lock_mode_name(local_info.snapshot.reserve_mode));
}

bool is_valid_query_result_entry(const ub_lock_query_result_t &entry)
{
    switch (entry.held_mode) {
        case UB_LOCK_I:
            return entry.holder_tid == 0 && entry.recursive_count == 0u && !entry.has_shared_ref;
        case UB_LOCK_S:
            return entry.holder_tid == 0 && entry.recursive_count == 0u && entry.has_shared_ref;
        case UB_LOCK_X:
            return entry.holder_tid != 0 && entry.recursive_count > 0u && !entry.has_shared_ref;
        case UB_LOCK_SX:
            return entry.holder_tid != 0 && entry.recursive_count > 0u;
        default:
            return false;
    }
}

bool is_valid_delayed_release_rebuild_state(const ub_lock_query_result_t *reserve_entry,
                                            const ub_lock_query_result_t *x_holder,
                                            const ub_lock_query_result_t *sx_holder, uint32_t shared_count)
{
    if (reserve_entry == nullptr) {
        return true;
    }

    switch (reserve_entry->reserve_mode) {
        case UB_LOCK_X:
            return x_holder == nullptr && sx_holder == nullptr && shared_count == 0u;
        case UB_LOCK_SX:
            return x_holder == nullptr && sx_holder == nullptr;
        case UB_LOCK_S:
            return x_holder == nullptr;
        default:
            return false;
    }
}

void replay_delayed_release_state(ub_rw_lock_t *lock, const ub_lock_query_result_t *reserve_entry,
                                  uint32_t &shared_bitmap, uint32_t &shared_count,
                                  const ub_lock_query_result_t *sx_holder, uint64_t reserve_owner)
{
    if (reserve_entry == nullptr) {
        return;
    }

    lock->reserve_lock_owner.store(reserve_owner, std::memory_order_release);
    switch (reserve_entry->reserve_mode) {
        case UB_LOCK_X:
            lock->lock_word.store(0, std::memory_order_release);
            lock->lock_owner_x.store(reserve_owner, std::memory_order_release);
            lock->x_recursive.store(1u, std::memory_order_release);
            break;
        case UB_LOCK_SX:
            lock->lock_word.store(static_cast<int32_t>(X_LOCK_HALF_DECR - shared_count), std::memory_order_release);
            lock->lock_owner_sx.store(reserve_owner, std::memory_order_release);
            lock->sx_recursive.store(1u, std::memory_order_release);
            break;
        case UB_LOCK_S:
            if ((shared_bitmap & (1u << reserve_entry->node_id)) == 0u) {
                shared_bitmap |= (1u << reserve_entry->node_id);
                ++shared_count;
            }
            if (sx_holder != nullptr) {
                lock->lock_word.store(static_cast<int32_t>(X_LOCK_HALF_DECR - shared_count), std::memory_order_release);
            } else {
                lock->lock_word.store(static_cast<int32_t>(X_LOCK_DECR - shared_count), std::memory_order_release);
            }
            break;
        default:
            break;
    }
}

void reset_shared_lock_for_rebuild(ub_rw_lock_t *lock)
{
    lock->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    lock->waiting_count.store(0u, std::memory_order_release);
    lock->queue_head.store(0u, std::memory_order_release);
    lock->queue_tail.store(0u, std::memory_order_release);
    lock->shared_owner_bitmap.store(0u, std::memory_order_release);
    lock->lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock->sx_recursive.store(0u, std::memory_order_release);
    lock->x_recursive.store(0u, std::memory_order_release);
    std::memset(lock->_pad_misc, 0, sizeof(lock->_pad_misc));

    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        lock->wait_queue[i].seq.store(UB_WAIT_EMPTY, std::memory_order_release);
        lock->wait_queue[i].mode = UB_LOCK_I;
        lock->wait_queue[i].location = {0xFF, 0};
    }
}

constexpr int32_t REBUILD_INIT_IN_PROGRESS = -1;

bool try_begin_shared_rebuild_init(ub_rw_lock_t *lock)
{
    int32_t expected = 0;
    return lock->is_inited.compare_exchange_strong(expected, REBUILD_INIT_IN_PROGRESS, std::memory_order_acq_rel,
                                                   std::memory_order_acquire);
}

void wait_shared_rebuild_init_done(ub_rw_lock_t *lock)
{
    while (lock->is_inited.load(std::memory_order_acquire) == REBUILD_INIT_IN_PROGRESS) {
        cpu_relax();
    }
}

void clear_node_registry_for_rebuild(ub_rw_lock_t *lock)
{
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        lock->node_registry[i] = 0;
    }
}

std::shared_ptr<LocalLock> switch_local_lock_binding(ub_rw_lock_t *old_lock, ub_rw_lock_t *new_lock)
{
    if (old_lock == nullptr || new_lock == nullptr) {
        return nullptr;
    }

    std::unique_lock<std::shared_mutex> lk(g_ll_registry.mtx);
    auto new_it = g_ll_registry.map.find(new_lock);
    if (old_lock == new_lock) {
        if (new_it == g_ll_registry.map.end() || !new_it->second) {
            return nullptr;
        }
        new_it->second->ub_lock_ptr_ = new_lock;
        return new_it->second;
    }

    auto old_it = g_ll_registry.map.find(old_lock);
    if (old_it == g_ll_registry.map.end() || !old_it->second) {
        if (new_it == g_ll_registry.map.end() || !new_it->second) {
            return nullptr;
        }
        new_it->second->ub_lock_ptr_ = new_lock;
        return new_it->second;
    }

    std::shared_ptr<LocalLock> local_lock = std::move(old_it->second);
    g_ll_registry.map.erase(old_it);
    local_lock->ub_lock_ptr_ = new_lock;

    if (new_it == g_ll_registry.map.end()) {
        g_ll_registry.map.emplace(new_lock, local_lock);
    } else {
        new_it->second = local_lock;
    }
    return local_lock;
}

const ub_lock_query_result_t *find_query_result_for_node(const ub_lock_rebuild_info_t &rebuild_info, uint8_t node_id)
{
    for (uint32_t i = 0; i < rebuild_info.query_result_count; ++i) {
        if (rebuild_info.query_results[i].node_id == node_id) {
            return &rebuild_info.query_results[i];
        }
    }
    return nullptr;
}

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

void DistributedLock::wake_s_chain(const ub_location_t &location)
{
    dequeue_and_notify_one(location);
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

void DistributedLock::wake_sx_chain(const ub_location_t &location)
{
    dequeue_and_notify_one(location);
    while (true) {
        ub_lock_mode_t m;
        if (!peek_head_waiting_mode_clean(m)) {
            return;
        }
        if (m == UB_LOCK_X || m == UB_LOCK_SX) {
            return;
        }
        if (m == UB_LOCK_S) {
            dequeue_and_notify_one(location);
            continue;
        }
        return;
    }
}

void DistributedLock::wake_after_unlock_exclusive(const ub_location_t &location)
{
    ub_lock_mode_t head_mode;
    if (!peek_head_waiting_mode_clean(head_mode)) {
        return;
    }

    if (head_mode == UB_LOCK_X) {
        dequeue_and_notify_one(location);
        return;
    }

    if (head_mode == UB_LOCK_S) {
        wake_s_chain(location);
        return;
    }

    if (head_mode == UB_LOCK_SX) {
        wake_sx_chain(location);
        return;
    }
}

void DistributedLock::lock_create(const ub_lock_config_t &config, const ub_location_t &location)
{
    if (location.node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid node_id [0, %d) : location.node_id=%d", UB_MAX_NODES, location.node_id);
        return;
    }

    if (rw_lock_shm_->is_inited.fetch_add(1u, std::memory_order_acq_rel) > 0) {
        ATOMIC_LOG(LOG_LEVEL_INFO, "UB lock is already init!");
        std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
        if (!ll_sp) {
            ll_sp = std::make_shared<LocalLock>(rw_lock_shm_);
            register_local_lock(rw_lock_shm_, ll_sp);
        }
        rw_lock_shm_->node_registry[location.node_id] = reinterpret_cast<uintptr_t>(rw_lock_shm_);
        register_message_process_func();
        return;
    }

    std::memset(rw_lock_shm_->_pad_core, 0, sizeof(rw_lock_shm_->_pad_core));
    std::memset(rw_lock_shm_->_pad_qt, 0, sizeof(rw_lock_shm_->_pad_qt));
    std::memset(rw_lock_shm_->_pad_qh, 0, sizeof(rw_lock_shm_->_pad_qh));
    std::memset(rw_lock_shm_->_pad_misc, 0, sizeof(rw_lock_shm_->_pad_misc));
    rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    rw_lock_shm_->shared_owner_bitmap.store(0, std::memory_order_release);
    rw_lock_shm_->x_recursive.store(0, std::memory_order_release);
    rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);

    rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    rw_lock_shm_->lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
    rw_lock_shm_->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);

    create_wait_queue();

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
    rw_lock_shm_->node_registry[location.node_id] = 0;
    auto ll = unregister_local_lock(rw_lock_shm_);

    int32_t cur = rw_lock_shm_->is_inited.load(std::memory_order_acquire);
    while (cur > 0u) {
        if (rw_lock_shm_->is_inited.compare_exchange_weak(cur, cur - 1u, std::memory_order_acq_rel,
                                                          std::memory_order_acquire)) {
            break;
        }
    }
    ATOMIC_LOG(LOG_LEVEL_INFO, "UB lock free success!");
    return;
}

ub_lock_result_t DistributedLock::verify_param(const ub_location_t &location)
{
    if (location.node_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid node_id [0, %d) : location.node_id=%d", UB_MAX_NODES, location.node_id);
        return UB_LOCK_ERROR;
    }
    return UB_LOCK_SUCCESS;
}

void DistributedLock::cleanup_and_unlock_local(LocalLock *local_lock)
{
    if (local_lock) {
        local_lock->global_state_.store(LocalLock::GLOBAL_IDLE, std::memory_order_release);
        local_lock->global_read_ref_count_.store(0, std::memory_order_release);
        local_lock->hold_global.store(false, std::memory_order_release);
        // 唤醒 Follower 让它们重新去抢
        local_lock->global_pending_cv_.notify_all();
        local_lock->unlock_s();
    }
}

inline void set_shared_owner_bitmap(ub_rw_lock_t *lock, uint8_t process_id)
{
    uint32_t mask = (1u << process_id);
    lock->shared_owner_bitmap.fetch_or(mask, std::memory_order_release);
}

inline bool try_claim_delayed_owner(std::atomic<uint64_t> &owner_slot, uint64_t identify)
{
    uint64_t expected = LOCK_INVALID_OWNER;
    return owner_slot.compare_exchange_strong(expected, identify, std::memory_order_acq_rel, std::memory_order_acquire);
}

void DistributedLock::dump_timeout_holder_info(const ub_location_t &location, ub_lock_mode_t request_mode,
                                               LocalLock *local_lock, const char *reason)
{
    TimeoutLogContext ctx{&location, request_mode, timeout_wait_scope_name(reason), "", rw_lock_shm_};
    if (rw_lock_shm_ == nullptr) {
        log_timeout_null_lock(ctx);
        return;
    }

    TimeoutGlobalLogInfo global_info = make_global_timeout_log_info(request_mode, rw_lock_shm_);
    if (local_lock == nullptr) {
        log_timeout_without_local(ctx, global_info);
        return;
    }

    TimeoutLocalLogInfo local_info = make_local_timeout_log_info(request_mode, location.tid, local_lock);
    ctx.blocker = select_timeout_blocker(reason, global_info, local_info);
    log_timeout_with_local(ctx, global_info, local_info);
}

bool DistributedLock::wait_follower_s(const ub_location_t &location, LocalLock *local_lock,
                                      const steady_time_point &deadline)
{
    std::unique_lock<std::mutex> lk(local_lock->global_pending_mtx_);
    if (local_lock->global_pending_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
        if (std::chrono::steady_clock::now() >= deadline) {
            dump_timeout_holder_info(location, UB_LOCK_S, local_lock, "follower_global_wait");
            local_lock->unlock_s();
            ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
            return false;
        }
    }
    return true;
}

ub_lock_result_t DistributedLock::spin_wait_s_loop(const ub_location_t &location, LocalLock *local_lock,
                                                   const steady_time_point &deadline)
{
    uint32_t slot = 0;
    bool is_awakened = false;
    while (true) {
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
                break;
            }
            // 【关键修复】：内联还原，严格保证加锁和计数的强一致性
            int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            if (cur > 0 && rw_lock_shm_->lock_word.compare_exchange_weak(cur, cur - 1, std::memory_order_acq_rel,
                                                                         std::memory_order_acquire)) {
                if (is_awakened) {
                    clean_outqueue_waiter(slot);
                }
                if (local_lock) {
                    local_lock->global_read_ref_count_.store(1, std::memory_order_release);
                    local_lock->hold_global.store(true, std::memory_order_release);
                    local_lock->global_state_.store(LocalLock::GLOBAL_HELD, std::memory_order_release);
                    local_lock->global_pending_cv_.notify_all();
                }
                set_shared_owner_bitmap(rw_lock_shm_, location.node_id);
                return UB_LOCK_SUCCESS;
            }

            if (is_awakened) {
                cpu_relax();
                --i;
                if (std::chrono::steady_clock::now() > deadline) {
                    dump_timeout_holder_info(location, UB_LOCK_S, local_lock, "global_spin_wait");
                    clean_timeout_waiter(slot);
                    cleanup_and_unlock_local(local_lock);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0) {
                cpu_relax();
            }
        }

        local_wait_ctx_t ctx;
        WaiterGuard guard(location.tid, &ctx);
        ub_lock_result_t ret = enqueue_waiter(UB_LOCK_S, location, slot);
        if (ret != UB_LOCK_SUCCESS) {
            cleanup_and_unlock_local(local_lock);
            ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock waiting queue is full");
            return ret;
        }

        bool self_handoff = false;
        int32_t current_val = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
        if (current_val == X_LOCK_DECR) {
            self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
                rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->wait_queue, slot);
        }
        (void)delay_release_ub_lock(UB_LOCK_S, *local_lock, location);

        if (!self_handoff) {
            if (!ctx.wait(deadline)) {
                dump_timeout_holder_info(location, UB_LOCK_S, local_lock, "global_queue_wait");
                clean_timeout_waiter(slot);
                cleanup_and_unlock_local(local_lock);
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                return UB_LOCK_TIMEOUT;
            }
        }
        is_awakened = true;
    }
}

ub_lock_result_t DistributedLock::lock_s(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    ub_lock_result_t ret;
    LocalLock *local_lock = nullptr;
    steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(policy.timeout_ts);

    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "lookup_local_lock failed");
        return UB_LOCK_ERROR;
    }

    local_lock = ll_sp.get();
    ret = local_lock->lock_s(location.tid, deadline);
    if (ret != UB_LOCK_SUCCESS) {
        if (ret == UB_LOCK_TIMEOUT) {
            dump_timeout_holder_info(location, UB_LOCK_S, local_lock, "local_wait");
        }
        return ret;
    }
    while (true) {
        if (local_lock->try_inc_global_ref()) {
            return UB_LOCK_SUCCESS;
        }
        if (local_lock->global_state_.load(std::memory_order_relaxed) == LocalLock::GLOBAL_IDLE) {
            int expected = LocalLock::GLOBAL_IDLE;
            if (local_lock->global_state_.compare_exchange_strong(
                    expected, LocalLock::GLOBAL_PENDING, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        if (!wait_follower_s(location, local_lock, deadline))
            return UB_LOCK_TIMEOUT;
    }

    ret = delay_release_local_lock(*local_lock, UB_LOCK_S, location);
    if (ret == UB_LOCK_CONFLICT) {
        local_lock->global_read_ref_count_.store(1, std::memory_order_release);
        local_lock->hold_global.store(true, std::memory_order_release);
        local_lock->global_state_.store(LocalLock::GLOBAL_HELD, std::memory_order_release);
        local_lock->global_pending_cv_.notify_all();
        set_shared_owner_bitmap(rw_lock_shm_, location.node_id);
        return UB_LOCK_SUCCESS;
    }

    return spin_wait_s_loop(location, local_lock, deadline);
}

ub_lock_result_t DistributedLock::unlock_s(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "lookup_local_lock failed");
        return UB_LOCK_ERROR;
    }
    LocalLock *local_lock = ll_sp.get();
    ub_lock_result_t ret;
    int32_t old_ref = local_lock->global_read_ref_count_.fetch_sub(1, std::memory_order_acq_rel);
    if (old_ref > 1) {
        ret = local_lock->unlock_s();
        return ret;
    }

    uint64_t identify = make_global_owner(location.node_id, location.tid);
    bool can_delay = policy.allow_delay_release && (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) &&
                     (local_lock->waiting_count.load(std::memory_order_acquire) == 0);
    const bool use_delay = can_delay && try_claim_delayed_owner(rw_lock_shm_->reserve_lock_owner, identify);
    if (!use_delay) {
        local_lock->local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
        const int32_t old = rw_lock_shm_->lock_word.fetch_add(1, std::memory_order_acq_rel);
        const int32_t now = old + 1;
        if (now > X_LOCK_DECR) {
            rw_lock_shm_->lock_word.fetch_sub(1, std::memory_order_release);
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Hold the lock before unlocking");
            return UB_LOCK_ERROR;
        }
        if (now == X_LOCK_DECR && rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
            wake_after_unlock_exclusive(location);
        }
        clear_shared_owner_bitmap(rw_lock_shm_, location.node_id);
    } else {
        local_lock->local_is_reserve_lock.store(UB_LOCK_S, std::memory_order_release);
    }
    local_lock->hold_global.store(false, std::memory_order_release);
    int expected = LocalLock::GLOBAL_HELD;
    local_lock->global_state_.compare_exchange_strong(expected, LocalLock::GLOBAL_IDLE, std::memory_order_acq_rel,
                                                      std::memory_order_acquire);
    local_lock->read_count.store(0, std::memory_order_release);
    ret = local_lock->unlock_s();
    return ret;
}

ub_lock_result_t DistributedLock::spin_wait_x_loop(const ub_location_t &location, LocalLock *local_lock,
                                                   const steady_time_point &deadline, bool is_recursive)
{
    uint32_t slot = 0;
    bool is_awakened = false;
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    while (true) {
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
                break;
            }
            int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            if (cur == X_LOCK_DECR && rw_lock_shm_->lock_word.compare_exchange_weak(cur, 0, std::memory_order_acq_rel,
                                                                                    std::memory_order_acquire)) {
                rw_lock_shm_->lock_owner_x.store(identify, std::memory_order_release);
                rw_lock_shm_->x_recursive.store(1, std::memory_order_release);
                local_lock->hold_global.store(true, std::memory_order_release);
                if (is_awakened) {
                    clean_outqueue_waiter(slot);
                }
                return UB_LOCK_SUCCESS;
            }
            if (is_awakened) {
                cpu_relax();
                --i;
                if (std::chrono::steady_clock::now() > deadline) {
                    dump_timeout_holder_info(location, UB_LOCK_X, local_lock, "global_spin_wait");
                    clean_timeout_waiter(slot);
                    local_lock->unlock_x(is_recursive, location.tid);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0) {
                cpu_relax();
            }
        }

        local_wait_ctx_t ctx;
        WaiterGuard guard(location.tid, &ctx);
        ub_lock_result_t ret = enqueue_waiter(UB_LOCK_X, location, slot);
        if (ret != UB_LOCK_SUCCESS) {
            local_lock->unlock_x(is_recursive, location.tid);
            ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock waiting queue is full");
            return ret;
        }

        bool self_handoff = false;
        int32_t current_val = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
        if (current_val == X_LOCK_DECR) {
            self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
                rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->wait_queue, slot);
        }
        (void)delay_release_ub_lock(UB_LOCK_X, *local_lock, location);

        if (!self_handoff) {
            if (!ctx.wait(deadline)) {
                dump_timeout_holder_info(location, UB_LOCK_X, local_lock, "global_queue_wait");
                clean_timeout_waiter(slot);
                local_lock->unlock_x(is_recursive, location.tid);
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                return UB_LOCK_TIMEOUT;
            }
        }
        is_awakened = true;
    }
}

ub_lock_result_t DistributedLock::lock_x(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    ub_lock_result_t ret;
    LocalLock *local_lock = nullptr;
    steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(policy.timeout_ts);

    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "lookup_local_lock failed");
        return UB_LOCK_ERROR;
    }
    local_lock = ll_sp.get();
    ret = local_lock->lock_x(policy.recursive, location.tid, deadline);
    if (ret != UB_LOCK_SUCCESS) {
        if (ret == UB_LOCK_TIMEOUT) {
            dump_timeout_holder_info(location, UB_LOCK_X, local_lock, "local_wait");
        }
        return ret;
    }

    ret = delay_release_local_lock(*local_lock, UB_LOCK_X, location);
    if (ret == UB_LOCK_CONFLICT) {
        rw_lock_shm_->lock_owner_x.store(identify, std::memory_order_release);
        rw_lock_shm_->x_recursive.store(1u, std::memory_order_release);
        local_lock->hold_global.store(true, std::memory_order_release);
        return UB_LOCK_SUCCESS; // 本地线程延迟释放过，直接获取
    }

    uint64_t owner = rw_lock_shm_->lock_owner_x.load(std::memory_order_acquire);
    if (owner == identify) {
        if (policy.recursive) {
            local_lock->hold_global.store(true, std::memory_order_release);
            rw_lock_shm_->x_recursive.fetch_add(1, std::memory_order_acq_rel);
            return UB_LOCK_SUCCESS;
        }
        local_lock->unlock_x(policy.recursive, location.tid);
        ATOMIC_LOG(LOG_LEVEL_ERROR, "UB recursive lock is not allowed!");
        return UB_LOCK_ERROR;
    }

    return spin_wait_x_loop(location, local_lock, deadline, policy.recursive);
}

ub_lock_result_t DistributedLock::unlock_x(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "lookup_local_lock failed");
        return UB_LOCK_ERROR;
    }

    LocalLock *local_lock = ll_sp.get();
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    bool can_delay = policy.allow_delay_release && (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) &&
                     (local_lock->waiting_count.load(std::memory_order_acquire) == 0);
    const bool use_delay = can_delay && try_claim_delayed_owner(rw_lock_shm_->reserve_lock_owner, identify);
    if (!use_delay) {
        uint64_t owner = rw_lock_shm_->lock_owner_x.load(std::memory_order_acquire);
        if (owner != identify) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Only the holder can release it");
            return UB_LOCK_ERROR;
        }
        bool release_global = true;
        if (policy.recursive) {
            uint32_t old = rw_lock_shm_->x_recursive.fetch_sub(1, std::memory_order_release);
            if (old > 1) {
                release_global = false;
            }
        }
        if (release_global) {
            local_lock->local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
            local_lock->hold_global.store(false, std::memory_order_release);
            rw_lock_shm_->lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->x_recursive.store(0u, std::memory_order_release);
            rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
                wake_after_unlock_exclusive(location);
            }
        }
    } else {
        local_lock->local_is_reserve_lock.store(UB_LOCK_X, std::memory_order_release);
        local_lock->hold_global.store(false, std::memory_order_release);
    }

    local_lock->global_state_.store(LocalLock::GLOBAL_IDLE, std::memory_order_release);
    return local_lock->unlock_x(policy.recursive, location.tid);
}

ub_lock_result_t DistributedLock::spin_wait_sx_loop(const ub_location_t &location, LocalLock *local_lock,
                                                    const steady_time_point &deadline, bool is_recursive)
{
    uint32_t slot = 0;
    bool is_awakened = false;
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    while (true) {
        for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
            if (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
                break;
            }
            int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
            if (cur > X_LOCK_HALF_DECR) {
                int32_t next = cur - X_LOCK_HALF_DECR;
                if (rw_lock_shm_->lock_word.compare_exchange_weak(cur, next, std::memory_order_acq_rel,
                                                                  std::memory_order_acquire)) {
                    rw_lock_shm_->lock_owner_sx.store(identify, std::memory_order_release);
                    rw_lock_shm_->sx_recursive.store(1u, std::memory_order_release);
                    local_lock->hold_global.store(true, std::memory_order_release);
                    if (is_awakened) {
                        clean_outqueue_waiter(slot);
                    }
                    return UB_LOCK_SUCCESS;
                }
            }
            if (is_awakened) {
                cpu_relax();
                --i;
                if (std::chrono::steady_clock::now() > deadline) {
                    dump_timeout_holder_info(location, UB_LOCK_SX, local_lock, "global_spin_wait");
                    clean_timeout_waiter(slot);
                    local_lock->unlock_sx(is_recursive, location.tid);
                    ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                    return UB_LOCK_TIMEOUT;
                }
                continue;
            }
            if ((i & 0xF) == 0) {
                cpu_relax();
            }
        }

        local_wait_ctx_t ctx;
        WaiterGuard guard(location.tid, &ctx);
        ub_lock_result_t ret = enqueue_waiter(UB_LOCK_SX, location, slot);
        if (ret != UB_LOCK_SUCCESS) {
            local_lock->unlock_sx(is_recursive, location.tid);
            ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock waiting queue is full");
            return ret;
        }

        bool self_handoff = false;
        int32_t current_val = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
        if (current_val == X_LOCK_DECR) {
            self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
                rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->wait_queue, slot);
        }
        (void)delay_release_ub_lock(UB_LOCK_SX, *local_lock, location);

        if (!self_handoff) {
            if (!ctx.wait(deadline)) {
                dump_timeout_holder_info(location, UB_LOCK_SX, local_lock, "global_queue_wait");
                clean_timeout_waiter(slot);
                local_lock->unlock_sx(is_recursive, location.tid);
                ATOMIC_LOG(LOG_LEVEL_ERROR, "The UB lock hold timeout");
                return UB_LOCK_TIMEOUT;
            }
        }
        is_awakened = true;
    }
}

ub_lock_result_t DistributedLock::lock_sx(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    ub_lock_result_t ret;
    LocalLock *local_lock = nullptr;
    steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(policy.timeout_ts);

    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "lookup_local_lock failed");
        return UB_LOCK_ERROR;
    }
    local_lock = ll_sp.get();

    ret = local_lock->lock_sx(policy.recursive, location.tid, deadline);
    if (ret != UB_LOCK_SUCCESS) {
        if (ret == UB_LOCK_TIMEOUT) {
            dump_timeout_holder_info(location, UB_LOCK_SX, local_lock, "local_wait");
        }
        return ret;
    }
    ret = delay_release_local_lock(*local_lock, UB_LOCK_SX, location);
    if (ret == UB_LOCK_CONFLICT) {
        rw_lock_shm_->lock_owner_sx.store(identify, std::memory_order_release);
        rw_lock_shm_->sx_recursive.store(1u, std::memory_order_release);
        local_lock->hold_global.store(true, std::memory_order_release);
        return UB_LOCK_SUCCESS; // 本地线程延迟释放过，直接获取
    }

    uint64_t owner = rw_lock_shm_->lock_owner_sx.load(std::memory_order_acquire);
    if (owner == identify) {
        if (policy.recursive) {
            local_lock->hold_global.store(true, std::memory_order_release);
            rw_lock_shm_->sx_recursive.fetch_add(1, std::memory_order_acq_rel);
            return UB_LOCK_SUCCESS;
        }
        local_lock->unlock_sx(policy.recursive, location.tid);
        ATOMIC_LOG(LOG_LEVEL_ERROR, "UB recursive lock is not allowed");
        return UB_LOCK_ERROR;
    }

    return spin_wait_sx_loop(location, local_lock, deadline, policy.recursive);
}

ub_lock_result_t DistributedLock::unlock_sx(const ub_lock_policy_t &policy, const ub_location_t &location)
{
    // 参数校验
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }
    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "lookup_local_lock failed");
        return UB_LOCK_ERROR;
    }

    LocalLock *local_lock = ll_sp.get();
    uint64_t identify = make_global_owner(location.node_id, location.tid);
    bool can_delay = policy.allow_delay_release && (rw_lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) &&
                     (local_lock->waiting_count.load(std::memory_order_acquire) == 0);
    const bool use_delay = can_delay && try_claim_delayed_owner(rw_lock_shm_->reserve_lock_owner, identify);
    if (!use_delay) {
        uint64_t owner = rw_lock_shm_->lock_owner_sx.load(std::memory_order_acquire);
        if (owner != identify) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "Only the holder can release it");
            return UB_LOCK_ERROR;
        }
        bool release_global = true;
        if (policy.recursive && rw_lock_shm_->sx_recursive.fetch_sub(1, std::memory_order_release) > 1) {
            release_global = false;
        }
        if (release_global) {
            local_lock->local_is_reserve_lock.store(UB_LOCK_I, std::memory_order_release);
            local_lock->hold_global.store(false, std::memory_order_release);
            rw_lock_shm_->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->sx_recursive.store(0u, std::memory_order_release);
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
        local_lock->local_is_reserve_lock.store(UB_LOCK_SX, std::memory_order_release);
        local_lock->hold_global.store(false, std::memory_order_release);
    }

    local_lock->global_state_.store(LocalLock::GLOBAL_IDLE, std::memory_order_release);
    return local_lock->unlock_sx(policy.recursive, location.tid);
}

ub_lock_result_t DistributedLock::query_holder(const ub_location_t &location, ub_lock_query_result_t &result)
{
    if (verify_param(location) == UB_LOCK_ERROR) {
        return UB_LOCK_ERROR;
    }

    result = {};
    result.node_id = location.node_id;
    result.held_mode = UB_LOCK_I;
    result.has_shared_ref = false;
    result.reserve_mode = UB_LOCK_I;

    std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(rw_lock_shm_);
    if (!ll_sp) {
        return UB_LOCK_SUCCESS;
    }
    LocalLock *local_lock = ll_sp.get();
    result.reserve_mode = local_lock->local_is_reserve_lock.load(std::memory_order_acquire);
    result.has_shared_ref = local_lock->global_read_ref_count_.load(std::memory_order_acquire) > 0;
    const bool hold_global = local_lock->hold_global.load(std::memory_order_acquire);
    const int32_t x_owner = local_lock->lock_x_owner.load(std::memory_order_acquire);
    if (hold_global && x_owner != 0) {
        result.held_mode = UB_LOCK_X;
        result.holder_tid = x_owner;
        result.recursive_count = local_lock->x_recursive_.load(std::memory_order_acquire);
        if (result.recursive_count == 0u) {
            result.recursive_count = 1u;
        }
        return UB_LOCK_SUCCESS;
    }

    const int32_t sx_owner = local_lock->lock_sx_owner.load(std::memory_order_acquire);
    if (hold_global && sx_owner != 0) {
        result.held_mode = UB_LOCK_SX;
        result.holder_tid = sx_owner;
        result.recursive_count = local_lock->sx_recursive_.load(std::memory_order_acquire);
        if (result.recursive_count == 0u) {
            result.recursive_count = 1u;
        }
        return UB_LOCK_SUCCESS;
    }

    if (result.has_shared_ref) {
        result.held_mode = UB_LOCK_S;
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t DistributedLock::rebuild(ub_rw_lock_t *old_lock, const ub_lock_rebuild_info_t &rebuild_info,
                                          const ub_location_t &location)
{
    // 验证入参
    if (old_lock == nullptr || location.node_id >= UB_MAX_NODES || rebuild_info.query_result_count == 0u ||
        rebuild_info.query_result_count > UB_MAX_NODES ||
        (rebuild_info.query_result_count > 0u && rebuild_info.query_results == nullptr)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid rebuild_info");
        return UB_LOCK_ERROR;
    }

    const uint8_t local_node_id = location.node_id;
    const ub_lock_query_result_t *local_result = find_query_result_for_node(rebuild_info, local_node_id);
    if (local_result == nullptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "missing local query result during rebuild");
        return UB_LOCK_ERROR;
    }

    const ub_lock_query_result_t *x_holder = nullptr;
    const ub_lock_query_result_t *sx_holder = nullptr;
    const ub_lock_query_result_t *reserve_entry = nullptr;
    uint32_t shared_bitmap = 0u;
    uint32_t shared_count = 0u;
    bool seen_nodes[UB_MAX_NODES] = {false};

    for (uint32_t i = 0; i < rebuild_info.query_result_count; ++i) {
        const ub_lock_query_result_t &entry = rebuild_info.query_results[i];
        if (entry.node_id >= UB_MAX_NODES || seen_nodes[entry.node_id] || !is_valid_rebuild_mode(entry.held_mode) ||
            !is_valid_rebuild_mode(entry.reserve_mode) || !is_valid_query_result_entry(entry)) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid rebuild entry");
            return UB_LOCK_ERROR;
        }
        seen_nodes[entry.node_id] = true;

        if (entry.reserve_mode != UB_LOCK_I) {
            const bool delayed_with_shared = (entry.held_mode == UB_LOCK_S && entry.has_shared_ref);
            if ((entry.held_mode != UB_LOCK_I && !delayed_with_shared) || reserve_entry != nullptr) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "conflicting delayed-release rebuild entry");
                return UB_LOCK_ERROR;
            }
            reserve_entry = &entry;
        }

        if (entry.has_shared_ref) {
            shared_bitmap |= (1u << entry.node_id);
            ++shared_count;
        }

        if (entry.held_mode == UB_LOCK_I) {
            continue;
        }
        if (entry.held_mode == UB_LOCK_S) {
            continue;
        }
        if (entry.held_mode == UB_LOCK_X) {
            if (x_holder != nullptr || sx_holder != nullptr || shared_count > 0u) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "conflicting X rebuild entry");
                return UB_LOCK_ERROR;
            }
            x_holder = &entry;
            continue;
        }
        if (entry.held_mode == UB_LOCK_SX) {
            if (sx_holder != nullptr || x_holder != nullptr) {
                ATOMIC_LOG(LOG_LEVEL_ERROR, "conflicting SX rebuild entry");
                return UB_LOCK_ERROR;
            }
            sx_holder = &entry;
        }
    }

    if (x_holder != nullptr && (sx_holder != nullptr || shared_count > 0u)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "X holder cannot coexist with other holders");
        return UB_LOCK_ERROR;
    }
    if (!is_valid_delayed_release_rebuild_state(reserve_entry, x_holder, sx_holder, shared_count)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid delayed-release rebuild combination");
        return UB_LOCK_ERROR;
    }

    if (!lookup_local_lock(old_lock)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "missing old local lock before rebuild");
        return UB_LOCK_ERROR;
    }

    const bool do_shared_init = try_begin_shared_rebuild_init(rw_lock_shm_);
    if (!do_shared_init) {
        wait_shared_rebuild_init_done(rw_lock_shm_);
    }

    std::shared_ptr<LocalLock> ll = switch_local_lock_binding(old_lock, rw_lock_shm_);
    if (!ll) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "missing local lock binding during rebuild");
        return UB_LOCK_ERROR;
    }
    if (do_shared_init) {
        clear_node_registry_for_rebuild(rw_lock_shm_);
        reset_shared_lock_for_rebuild(rw_lock_shm_);

        if (x_holder != nullptr) {
            rw_lock_shm_->lock_word.store(0, std::memory_order_release);
            rw_lock_shm_->lock_owner_x.store(make_global_owner(x_holder->node_id, x_holder->holder_tid),
                                             std::memory_order_release);
            rw_lock_shm_->x_recursive.store(x_holder->recursive_count, std::memory_order_release);
        } else if (sx_holder != nullptr) {
            rw_lock_shm_->lock_word.store(static_cast<int32_t>(X_LOCK_HALF_DECR - shared_count),
                                          std::memory_order_release);
            rw_lock_shm_->lock_owner_sx.store(make_global_owner(sx_holder->node_id, sx_holder->holder_tid),
                                              std::memory_order_release);
            rw_lock_shm_->sx_recursive.store(sx_holder->recursive_count, std::memory_order_release);
        } else if (shared_count > 0u) {
            rw_lock_shm_->lock_word.store(static_cast<int32_t>(X_LOCK_DECR - shared_count), std::memory_order_release);
        }

        if (reserve_entry != nullptr) {
            replay_delayed_release_state(rw_lock_shm_, reserve_entry, shared_bitmap, shared_count, sx_holder,
                                         make_global_owner(reserve_entry->node_id, location.tid));
        }
        rw_lock_shm_->shared_owner_bitmap.store(shared_bitmap, std::memory_order_release);

        rw_lock_shm_->node_registry[local_node_id] = reinterpret_cast<uintptr_t>(rw_lock_shm_);
        register_message_process_func();
        rw_lock_shm_->is_inited.store(static_cast<int32_t>(rebuild_info.query_result_count), std::memory_order_release);
        return UB_LOCK_SUCCESS;
    }

    rw_lock_shm_->node_registry[local_node_id] = reinterpret_cast<uintptr_t>(rw_lock_shm_);
    register_message_process_func();
    return UB_LOCK_SUCCESS;
}

static inline uint32_t parse_owner_pid(uint64_t owner)
{
    return (uint32_t)(owner >> 32);
}

static inline uint32_t bit_count_u32(uint32_t value)
{
    uint32_t count = 0;
    while (value != 0u) {
        value &= (value - 1u);
        ++count;
    }
    return count;
}

static inline uint32_t valid_shared_owner_count(uint32_t bitmap)
{
    constexpr uint32_t kValidOwnerMask = (UB_MAX_NODES >= 32u) ? 0xFFFFFFFFu : ((1u << UB_MAX_NODES) - 1u);
    return bit_count_u32(bitmap & kValidOwnerMask);
}

static inline uint32_t expected_shared_owner_count(int32_t lock_word)
{
    if (lock_word > 0 && lock_word < X_LOCK_HALF_DECR) {
        return static_cast<uint32_t>(X_LOCK_HALF_DECR - lock_word); // sx + s
    }
    if (lock_word > X_LOCK_HALF_DECR && lock_word < X_LOCK_DECR) {
        return static_cast<uint32_t>(X_LOCK_DECR - lock_word); // s only
    }
    return 0u;
}

static inline bool try_release_one_shared_owner(std::atomic<int32_t> &lock_word)
{
    int32_t cur = lock_word.load(std::memory_order_acquire);
    while (true) {
        int32_t upper_bound = 0;
        if (cur > 0 && cur < X_LOCK_HALF_DECR) {
            upper_bound = X_LOCK_HALF_DECR;
        } else if (cur > X_LOCK_HALF_DECR && cur < X_LOCK_DECR) {
            upper_bound = X_LOCK_DECR;
        } else {
            return false;
        }

        const int32_t next = cur + 1;
        if (next > upper_bound) {
            return false;
        }

        if (lock_word.compare_exchange_weak(cur, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
    }
}

static inline bool try_take_valid_owner(std::atomic<uint64_t> &owner_slot, uint64_t &owner_out,
                                        uint32_t retry_times = 3u)
{
    for (uint32_t i = 0; i < retry_times; ++i) {
        uint64_t owner = owner_slot.load(std::memory_order_acquire);
        if (owner != LOCK_INVALID_OWNER &&
            owner_slot.compare_exchange_weak(owner, LOCK_INVALID_OWNER, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            owner_out = owner;
            return true;
        }
        cpu_relax();
    }
    owner_out = LOCK_INVALID_OWNER;
    return false;
}

void DistributedLock::recover_shared_lock(uint32_t process_id)
{
    if (process_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "invalid process_id=%d, valid range [0, %d).", process_id, UB_MAX_NODES);
        return;
    }

    const uint32_t mask = (1u << process_id);
    const int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
    const uint32_t expected = expected_shared_owner_count(cur);
    const uint32_t actual = valid_shared_owner_count(rw_lock_shm_->shared_owner_bitmap.load(std::memory_order_acquire));

    auto clear_failed_owner_bit = [&]() -> bool {
        uint32_t bitmap = rw_lock_shm_->shared_owner_bitmap.load(std::memory_order_acquire);
        while ((bitmap & mask) != 0u) {
            const uint32_t new_bitmap = bitmap & ~mask;
            if (rw_lock_shm_->shared_owner_bitmap.compare_exchange_weak(bitmap, new_bitmap, std::memory_order_acq_rel,
                                                                        std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    };

    auto force_release_to_bitmap = [&]() {
        int32_t old = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
        while (true) {
            const uint32_t expected_now = expected_shared_owner_count(old);
            if (expected_now <= actual) {
                return;
            }
            const int32_t next = old + static_cast<int32_t>(expected_now - actual);
            if (rw_lock_shm_->lock_word.compare_exchange_weak(old, next, std::memory_order_acq_rel,
                                                              std::memory_order_acquire)) {
                return;
            }
        }
    };

    if (actual == expected) {
        if (clear_failed_owner_bit()) {
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->lock_word.fetch_add(1, std::memory_order_acq_rel);
        }
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "recover_shared_lock recover stale shared owner bitmap");
        return;
    }

    if (actual < expected) {
        (void)clear_failed_owner_bit();
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        force_release_to_bitmap();
        ATOMIC_LOG(LOG_LEVEL_WARN, "recover_shared_lock force release");
        return;
    }

    if (actual > expected) {
        if (clear_failed_owner_bit()) {
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        }
        ATOMIC_LOG(LOG_LEVEL_WARN, "recover_shared_lock cleared stale shared owner bitmap");
        return;
    }
}

void DistributedLock::recover_exclusive_x(uint32_t process_id)
{
    uint64_t owner = LOCK_INVALID_OWNER;
    if (!try_take_valid_owner(rw_lock_shm_->lock_owner_x, owner, 5u)) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "lock_owner_x is invalid");
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        rw_lock_shm_->x_recursive.store(0, std::memory_order_release);
        rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    } else {
        const uint32_t pid = parse_owner_pid(owner);
        if (pid != process_id) {
            rw_lock_shm_->lock_owner_x.store(owner, std::memory_order_release);
            ATOMIC_LOG(LOG_LEVEL_DEBUG, "process_id is invalid");
        } else {
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->x_recursive.store(0, std::memory_order_release);
            rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
        }
    }
}

void DistributedLock::recover_exclusive_sx(uint32_t process_id)
{
    uint64_t owner = LOCK_INVALID_OWNER;
    if (!try_take_valid_owner(rw_lock_shm_->lock_owner_sx, owner, 5u)) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "lock_owner_sx is invalid");
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);
        rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    } else {
        const uint32_t pid = parse_owner_pid(owner);
        if (pid != process_id) {
            rw_lock_shm_->lock_owner_sx.store(owner, std::memory_order_release);
            ATOMIC_LOG(LOG_LEVEL_DEBUG, "process_id is invalid");
        } else {
            rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
            rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);
            rw_lock_shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
        }
    }
}

void DistributedLock::recover_shared_sx_s(uint32_t process_id)
{
    uint64_t owner = LOCK_INVALID_OWNER;
    const bool has_valid_owner = try_take_valid_owner(rw_lock_shm_->lock_owner_sx, owner, 5u);
    const uint32_t pid = has_valid_owner ? parse_owner_pid(owner) : static_cast<uint32_t>(UB_MAX_NODES);
    if (!has_valid_owner || pid == process_id) {
        rw_lock_shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
        rw_lock_shm_->sx_recursive.store(0, std::memory_order_release);
        rw_lock_shm_->lock_word.fetch_add(X_LOCK_HALF_DECR, std::memory_order_acq_rel);
    } else {
        rw_lock_shm_->lock_owner_sx.store(owner, std::memory_order_release);
    }
    recover_shared_lock(process_id);
}

ub_lock_result_t DistributedLock::recover(const uint32_t process_id, const ub_location_t &location)
{
    if (process_id >= UB_MAX_NODES) {
        ATOMIC_LOG(LOG_LEVEL_WARN, "invalid process_id=%d, valid range [0, %d).", process_id, UB_MAX_NODES);
        return UB_LOCK_ERROR;
    }
    int32_t cur = rw_lock_shm_->lock_word.load(std::memory_order_acquire);
    if (cur == 0) {
        recover_exclusive_x(process_id);
    } else if (cur == X_LOCK_HALF_DECR) {
        recover_exclusive_sx(process_id);
    } else if (cur > 0 && cur < X_LOCK_HALF_DECR) {
        recover_shared_sx_s(process_id);
    } else if (cur > X_LOCK_HALF_DECR && cur < X_LOCK_DECR) {
        recover_shared_lock(process_id);
    } else if (cur == X_LOCK_DECR) {
        ATOMIC_LOG(LOG_LEVEL_DEBUG, "no process holding the lock");
    }

    for (uint32_t i = 0; i < UB_MAX_NODES; i++) {
        ub_waiter_t &slot = rw_lock_shm_->wait_queue[i];
        uint32_t seq = slot.seq.load(std::memory_order_acquire);
        if (seq == UB_WAIT_WRITING) {
            (void)ring_queue_try_mark_timeout(slot, rw_lock_shm_->waiting_count);
            continue;
        }
        uint32_t pid = slot.location.node_id;
        if (pid != process_id) {
            continue;
        }
        (void)ring_queue_try_mark_timeout(slot, rw_lock_shm_->waiting_count);
    }

    if (rw_lock_shm_->lock_word.load(std::memory_order_acquire) == X_LOCK_DECR) {
        wake_after_unlock_exclusive(location);
    }

    cur = rw_lock_shm_->is_inited.load(std::memory_order_acquire);
    while (cur > 1u) {
        if (rw_lock_shm_->is_inited.compare_exchange_weak(cur, cur - 1u, std::memory_order_acq_rel,
                                                          std::memory_order_acquire)) {
            break;
        }
    }
    return UB_LOCK_SUCCESS;
}

} // namespace ublock
