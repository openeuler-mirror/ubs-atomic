/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "ub_mutex_lock.h"

#include <array>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "UBShmTransport.h"
#include "inner_distribute_lock.h"
#include "ub_atomic_log_print.h"

using ub_comm_queue::UBShmTransport;
extern UBShmTransport *g_transport;

namespace ublock {

namespace {
constexpr int32_t MUTEX_INIT_EMPTY = 0;
constexpr int32_t MUTEX_INITING = 1;
constexpr int32_t MUTEX_READY = 2;
constexpr time_ms_t DEFAULT_MUTEX_TIMEOUT_MS = 10000;
constexpr uint32_t MUTEX_YIELD_INTERVAL = 32;
constexpr uint32_t MUTEX_INIT_WAIT_ROUNDS = 1024 * 1024;
constexpr int MUTEX_WAKE_SCAN_LIMIT = UB_MAX_NODES * 2;

inline bool is_invalid_node(uint8_t node_id)
{
    return node_id >= UB_MAX_NODES;
}

struct MessageDeleter {
    void operator()(message_t *msg) const
    {
        if (!msg) {
            return;
        }
        delete[] msg->body;
        delete msg;
    }
};
using MessagePtr = std::unique_ptr<message_t, MessageDeleter>;

struct MutexLocalLock {
    std::atomic<uint64_t> owner{LOCK_INVALID_OWNER};
    std::atomic<uint32_t> active_users{0};
    std::atomic<uint32_t> waiting_count{0};
    std::atomic<uint32_t> queue_head{0};
    std::atomic<uint32_t> queue_tail{0};
    std::array<local_waiter_t, UB_MAX_CAPACITY> q{};

    MutexLocalLock()
    {
        ring_queue_init<UB_MAX_CAPACITY>(queue_head, queue_tail, waiting_count, q.data(), [](local_waiter_t &slot) {
            slot.mode = UB_LOCK_I;
            slot.tid = 0;
        });
    }
};

uint32_t mutex_owner_node(uint64_t owner)
{
    return owner == LOCK_INVALID_OWNER ? static_cast<uint32_t>(UB_MAX_NODES) :
                                         static_cast<uint32_t>(owner >> MUTEX_YIELD_INTERVAL);
}

int32_t mutex_owner_tid(uint64_t owner)
{
    return owner == LOCK_INVALID_OWNER ? 0 : static_cast<int32_t>(owner & 0xFFFFFFFFu);
}

std::string format_mutex_owner(uint64_t owner)
{
    if (owner == LOCK_INVALID_OWNER) {
        return "none";
    }
    return "node=" + std::to_string(mutex_owner_node(owner)) + ",tid=" + std::to_string(mutex_owner_tid(owner));
}

bool mutex_timeout_is_local(const char *wait_scope)
{
    return wait_scope != nullptr && wait_scope[0] == 'l';
}

std::string format_mutex_blocker(const char *wait_scope, uint64_t global_owner, uint32_t global_waiters,
                                 uint64_t local_owner)
{
    if (mutex_timeout_is_local(wait_scope)) {
        if (local_owner == LOCK_INVALID_OWNER) {
            return "local_mutex";
        }
        return "local_owner(" + format_mutex_owner(local_owner) + ")";
    }
    if (global_owner != LOCK_INVALID_OWNER) {
        return "global_owner(" + format_mutex_owner(global_owner) + ")";
    }
    if (global_waiters != 0u) {
        return "WAITERS(count=" + std::to_string(global_waiters) + ")";
    }
    return "UNKNOWN";
}

void dump_mutex_timeout_info(ub_mutex_lock_t *lock, const ub_location_t &location, const char *wait_scope,
                             const MutexLocalLock *local_lock)
{
    const uint64_t global_owner = lock == nullptr ? LOCK_INVALID_OWNER :
                                                    lock->lock_owner.load(std::memory_order_acquire);
    const uint32_t global_waiters = lock == nullptr ? 0u : lock->waiting_count.load(std::memory_order_acquire);
    const uint64_t local_owner = local_lock == nullptr ? LOCK_INVALID_OWNER :
                                                         local_lock->owner.load(std::memory_order_acquire);
    const uint32_t local_active_users =
        local_lock == nullptr ? 0u : local_lock->active_users.load(std::memory_order_acquire);
    const uint32_t local_waiters = local_lock == nullptr ? 0u :
                                                           local_lock->waiting_count.load(std::memory_order_acquire);
    const std::string global_owner_desc = format_mutex_owner(global_owner);
    const std::string local_owner_desc = format_mutex_owner(local_owner);
    const std::string blocker = format_mutex_blocker(wait_scope, global_owner, global_waiters, local_owner);

    ATOMIC_LOG(LOG_LEVEL_ERROR,
               "UB mutex lock timeout: wait=%s request=X node=%u tid=%d lock=%p blocker=%s "
               "global[owner=%s waiters=%u] local[owner=%s waiters=%u active_users=%u]",
               wait_scope, static_cast<unsigned>(location.node_id), location.tid, static_cast<void *>(lock),
               blocker.c_str(), global_owner_desc.c_str(), global_waiters, local_owner_desc.c_str(), local_waiters,
               local_active_users);
}

struct MutexLocalLockRegistry {
    std::unordered_map<ub_mutex_lock_t *, std::shared_ptr<MutexLocalLock>> map;
    std::shared_mutex mtx;
};

MutexLocalLockRegistry g_mutex_local_registry;

std::shared_ptr<MutexLocalLock> lookup_mutex_local_lock(ub_mutex_lock_t *shm)
{
    std::shared_lock<std::shared_mutex> lk(g_mutex_local_registry.mtx);
    auto it = g_mutex_local_registry.map.find(shm);
    return it == g_mutex_local_registry.map.end() ? nullptr : it->second;
}

std::shared_ptr<MutexLocalLock> acquire_mutex_local_lock(ub_mutex_lock_t *shm)
{
    std::shared_lock<std::shared_mutex> lk(g_mutex_local_registry.mtx);
    auto it = g_mutex_local_registry.map.find(shm);
    if (it == g_mutex_local_registry.map.end()) {
        return nullptr;
    }
    it->second->active_users.fetch_add(1u, std::memory_order_acq_rel);
    return it->second;
}

void release_mutex_local_lock(const std::shared_ptr<MutexLocalLock> &local_lock)
{
    if (local_lock) {
        local_lock->active_users.fetch_sub(1u, std::memory_order_acq_rel);
    }
}

ub_lock_result_t enqueue_mutex_local_waiter(MutexLocalLock &local_lock, int32_t tid, uint32_t &out_ticket)
{
    return ring_queue_enqueue<UB_MAX_CAPACITY>(
        local_lock.queue_head, local_lock.queue_tail, local_lock.waiting_count, local_lock.q.data(),
        [&](local_waiter_t &slot) {
            slot.mode = UB_LOCK_X;
            slot.tid = tid;
        },
        out_ticket);
}

void clean_mutex_local_timeout_waiter(MutexLocalLock &local_lock, uint32_t ticket)
{
    ring_queue_clean_timeout<UB_MAX_CAPACITY>(local_lock.queue_head, local_lock.queue_tail, local_lock.waiting_count,
                                              local_lock.q.data(), ticket);
}

void clean_mutex_local_outqueue_waiter(MutexLocalLock &local_lock, uint32_t ticket)
{
    ring_queue_pop_head<UB_MAX_CAPACITY>(local_lock.queue_head, local_lock.queue_tail, local_lock.waiting_count,
                                         local_lock.q.data(), ticket);
}

void wake_one_mutex_local_waiter(MutexLocalLock &local_lock)
{
    int sanity = UB_MAX_CAPACITY * 2;
    while (sanity-- > 0) {
        ring_queue_advance_dead_head<UB_MAX_CAPACITY>(local_lock.queue_head, local_lock.queue_tail,
                                                      local_lock.q.data());
        const uint32_t ticket = local_lock.queue_head.load(std::memory_order_acquire);
        local_waiter_t *waiter = nullptr;
        if (ring_queue_outqueue<UB_MAX_CAPACITY>(local_lock.queue_head, local_lock.waiting_count, local_lock.q.data(),
                                                 waiter) == UB_LOCK_SUCCESS) {
            const bool found = WaiterRegistry::instance().notify_local_waiter(waiter->tid);
            if (found) {
                return;
            }
            ATOMIC_LOG(LOG_LEVEL_WARN, "mutex local wait ctx has been notified: %d", waiter->tid);
            clean_mutex_local_outqueue_waiter(local_lock, ticket);
            continue;
        }
        if (local_lock.waiting_count.load(std::memory_order_acquire) == 0) {
            return;
        }
    }
}

enum class MutexLocalSpinResult
{
    ACQUIRED,
    NEED_WAIT,
    TIMEOUT
};

struct MutexLocalLockContext {
    ub_mutex_lock_t *lock;
    MutexLocalLock &local_lock;
    uint64_t identify;
    const ub_location_t &location;
    const steady_time_point &deadline;
};

bool try_claim_mutex_local(MutexLocalLockContext &ctx, bool is_awakened, uint32_t slot)
{
    uint64_t expected = LOCK_INVALID_OWNER;
    if (!ctx.local_lock.owner.compare_exchange_weak(expected, ctx.identify, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
        return false;
    }
    if (is_awakened) {
        clean_mutex_local_outqueue_waiter(ctx.local_lock, slot);
    }
    return true;
}

MutexLocalSpinResult spin_try_mutex_local(MutexLocalLockContext &ctx, bool is_awakened, uint32_t slot)
{
    if (is_awakened) {
        while (std::chrono::steady_clock::now() <= ctx.deadline) {
            if (try_claim_mutex_local(ctx, is_awakened, slot)) {
                return MutexLocalSpinResult::ACQUIRED;
            }
            cpu_relax();
        }
        dump_mutex_timeout_info(ctx.lock, ctx.location, "local", &ctx.local_lock);
        clean_mutex_local_timeout_waiter(ctx.local_lock, slot);
        return MutexLocalSpinResult::TIMEOUT;
    }

    for (uint32_t i = 0; i < SPIN_WAIT_ROUNDS; ++i) {
        if (ctx.local_lock.waiting_count.load(std::memory_order_acquire) > 0) {
            return MutexLocalSpinResult::NEED_WAIT;
        }
        if (try_claim_mutex_local(ctx, is_awakened, slot)) {
            return MutexLocalSpinResult::ACQUIRED;
        }
        if ((i & 0xF) == 0) {
            cpu_relax();
        }
    }
    return MutexLocalSpinResult::NEED_WAIT;
}

ub_lock_result_t wait_mutex_local_handoff(MutexLocalLockContext &lock_ctx, uint32_t &slot)
{
    local_wait_ctx_t wait_ctx;
    WaiterGuard guard(lock_ctx.location.tid, &wait_ctx);
    ub_lock_result_t ret = enqueue_mutex_local_waiter(lock_ctx.local_lock, lock_ctx.location.tid, slot);
    if (ret != UB_LOCK_SUCCESS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "The mutex local waiting queue is full");
        return ret;
    }

    bool self_handoff = false;
    if (lock_ctx.local_lock.owner.load(std::memory_order_acquire) == LOCK_INVALID_OWNER) {
        self_handoff = ring_queue_try_self_handoff_if_head<UB_MAX_CAPACITY>(
            lock_ctx.local_lock.queue_head, lock_ctx.local_lock.queue_tail, lock_ctx.local_lock.q.data(), slot);
    }

    if (!self_handoff && !wait_ctx.wait(lock_ctx.deadline)) {
        dump_mutex_timeout_info(lock_ctx.lock, lock_ctx.location, "local", &lock_ctx.local_lock);
        clean_mutex_local_timeout_waiter(lock_ctx.local_lock, slot);
        return UB_LOCK_TIMEOUT;
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t lock_mutex_local(ub_mutex_lock_t *lock, MutexLocalLock &local_lock, uint64_t identify,
                                  const ub_location_t &location, const steady_time_point &deadline)
{
    uint32_t slot = 0;
    bool is_awakened = false;
    MutexLocalLockContext ctx{lock, local_lock, identify, location, deadline};

    while (std::chrono::steady_clock::now() <= deadline) {
        MutexLocalSpinResult spin_ret = spin_try_mutex_local(ctx, is_awakened, slot);
        if (spin_ret == MutexLocalSpinResult::ACQUIRED) {
            return UB_LOCK_SUCCESS;
        }
        if (spin_ret == MutexLocalSpinResult::TIMEOUT) {
            return UB_LOCK_TIMEOUT;
        }

        ub_lock_result_t wait_ret = wait_mutex_local_handoff(ctx, slot);
        if (wait_ret != UB_LOCK_SUCCESS) {
            return wait_ret;
        }
        is_awakened = true;
    }

    dump_mutex_timeout_info(lock, location, "local", &local_lock);
    return UB_LOCK_TIMEOUT;
}

void unlock_mutex_local(MutexLocalLock &local_lock, uint64_t identify)
{
    const uint64_t owner = local_lock.owner.load(std::memory_order_acquire);
    if (owner != identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex local unlock failed because caller does not own the local lock");
        return;
    }
    local_lock.owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    if (local_lock.waiting_count.load(std::memory_order_acquire) > 0) {
        wake_one_mutex_local_waiter(local_lock);
    }
}

void register_mutex_local_lock(ub_mutex_lock_t *shm, std::shared_ptr<MutexLocalLock> ll)
{
    std::unique_lock<std::shared_mutex> lk(g_mutex_local_registry.mtx);
    g_mutex_local_registry.map.try_emplace(shm, std::move(ll));
}

std::shared_ptr<MutexLocalLock> unregister_mutex_local_lock_if_idle(ub_mutex_lock_t *shm)
{
    std::shared_ptr<MutexLocalLock> ll_holder = nullptr;
    {
        std::unique_lock<std::shared_mutex> lk(g_mutex_local_registry.mtx);
        auto it = g_mutex_local_registry.map.find(shm);
        if (it != g_mutex_local_registry.map.end() &&
            it->second->owner.load(std::memory_order_acquire) == LOCK_INVALID_OWNER &&
            it->second->waiting_count.load(std::memory_order_acquire) == 0u &&
            it->second->active_users.load(std::memory_order_acquire) == 0u) {
            ll_holder = std::move(it->second);
            g_mutex_local_registry.map.erase(it);
        }
    }
    return ll_holder;
}

static inline void mutex_backoff_sleep(uint32_t attempt)
{
    uint32_t us = 50u << (attempt > 7 ? 7 : attempt);
    if (us > 5000u) {
        us = 5000u;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(us));
}

static inline ub_lock_result_t mutex_send_with_retry(const message_t *msg, uint32_t max_attempts)
{
    for (uint32_t i = 0; i < max_attempts; ++i) {
        int ret = g_transport->send(msg);
        if (ret == UB_LOCK_SUCCESS) {
            return UB_LOCK_SUCCESS;
        }
        mutex_backoff_sleep(i);
    }
    return UB_LOCK_ERROR;
}

static inline bool wait_mutex_ready(ub_mutex_lock_t *lock)
{
    for (uint32_t i = 0; i < MUTEX_INIT_WAIT_ROUNDS; ++i) {
        if (lock->is_inited.load(std::memory_order_acquire) == MUTEX_READY) {
            return true;
        }
        if ((i % MUTEX_YIELD_INTERVAL) == 0) {
            std::this_thread::yield();
        }
        cpu_relax();
    }
    return lock->is_inited.load(std::memory_order_acquire) == MUTEX_READY;
}

static message_t *create_mutex_message(const ub_location_t &waiter_location, uint8_t src_node_id)
{
    MessagePtr msg(new message_t());
    uint32_t body_len = sizeof(local_msg_body_t);
    msg->body = new char[body_len];

    local_msg_body_t body = {.tid = waiter_location.tid, .addr = nullptr, .type = UB_GRANT, .mode = UB_LOCK_X};
    std::memcpy(msg->body, &body, body_len);

    msg->header.body_length = body_len;
    msg->header.dest_node_id = waiter_location.node_id;
    msg->header.src_node_id = src_node_id;
    msg->header.msg_type = 0xFF;
    msg->header.priority = 0;
    return msg.release();
}
} // namespace

void MutexLock::create_wait_queue()
{
    ring_queue_init<UB_MAX_NODES>(lock_shm_->queue_head, lock_shm_->queue_tail, lock_shm_->waiting_count,
                                  lock_shm_->wait_queue, [](ub_waiter_t &slot) {
                                      slot.mode = UB_LOCK_I;
                                      slot.location = ub_location_t{.tid = 0, .node_id = 0xFF};
                                  });
}

ub_lock_result_t MutexLock::enqueue_waiter(const ub_location_t &location, uint32_t &out_ticket)
{
    return ring_queue_enqueue<UB_MAX_NODES>(
        lock_shm_->queue_head, lock_shm_->queue_tail, lock_shm_->waiting_count, lock_shm_->wait_queue,
        [&](ub_waiter_t &slot) {
            slot.mode = UB_LOCK_X;
            slot.location = location;
        },
        out_ticket);
}

void MutexLock::clean_timeout_waiter(uint32_t ticket)
{
    ring_queue_clean_timeout<UB_MAX_NODES>(lock_shm_->queue_head, lock_shm_->queue_tail, lock_shm_->waiting_count,
                                           lock_shm_->wait_queue, ticket);
}

void MutexLock::clean_outqueue_waiter(uint32_t ticket)
{
    ring_queue_pop_head<UB_MAX_NODES>(lock_shm_->queue_head, lock_shm_->queue_tail, lock_shm_->waiting_count,
                                      lock_shm_->wait_queue, ticket);
}

ub_lock_result_t MutexLock::notify_waiter(ub_waiter_t &waiter, const ub_location_t &location)
{
    if (waiter.location.node_id == location.node_id) {
        bool found = WaiterRegistry::instance().notify_local_waiter(waiter.location.tid);
        if (!found) {
            ATOMIC_LOG(LOG_LEVEL_WARN, "mutex wait ctx has been notified: %d", waiter.location.tid);
            return UB_LOCK_ERROR;
        }
        return UB_LOCK_SUCCESS;
    }

    if (g_transport == nullptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Transport handle is null");
        return UB_LOCK_ERROR;
    }

    MessagePtr msg(create_mutex_message(waiter.location, location.node_id));
    if (!msg) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to create mutex message");
        return UB_LOCK_ERROR;
    }
    ub_lock_result_t ret = mutex_send_with_retry(msg.get(), 5);
    return ret;
}

void MutexLock::wake_one_waiter(const ub_location_t &location)
{
    int sanity = MUTEX_WAKE_SCAN_LIMIT;
    while (sanity-- > 0) {
        ring_queue_advance_dead_head<UB_MAX_NODES>(lock_shm_->queue_head, lock_shm_->queue_tail, lock_shm_->wait_queue);
        const uint32_t ticket = lock_shm_->queue_head.load(std::memory_order_acquire);
        ub_waiter_t *waiter = nullptr;
        if (ring_queue_outqueue<UB_MAX_NODES>(lock_shm_->queue_head, lock_shm_->waiting_count, lock_shm_->wait_queue,
                                              waiter) == UB_LOCK_SUCCESS) {
            if (notify_waiter(*waiter, location) == UB_LOCK_SUCCESS) {
                return;
            }
            clean_outqueue_waiter(ticket);
            continue;
        }
        if (lock_shm_->waiting_count.load(std::memory_order_acquire) == 0) {
            return;
        }
    }
}

bool MutexLock::try_lock_fast(uint64_t identify, bool is_awakened, uint32_t slot)
{
    if (lock_shm_->waiting_count.load(std::memory_order_acquire) > 0 && !is_awakened) {
        return false;
    }

    uint64_t expected = LOCK_INVALID_OWNER;
    if (lock_shm_->lock_owner.compare_exchange_weak(expected, identify, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
        if (is_awakened) {
            clean_outqueue_waiter(slot);
        }
        return true;
    }
    return false;
}

void MutexLock::lock_create()
{
    int32_t state = lock_shm_->is_inited.load(std::memory_order_acquire);
    if (state != MUTEX_READY) {
        int32_t expected = state;
        const bool can_init = state == MUTEX_INIT_EMPTY || state < MUTEX_INIT_EMPTY || state > MUTEX_READY;
        if (can_init && lock_shm_->is_inited.compare_exchange_strong(expected, MUTEX_INITING, std::memory_order_acq_rel,
                                                                     std::memory_order_acquire)) {
            std::memset(lock_shm_->_pad, 0, sizeof(lock_shm_->_pad));
            lock_shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_relaxed);
            create_wait_queue();
            lock_shm_->is_inited.store(MUTEX_READY, std::memory_order_release);
        } else if (expected != MUTEX_READY && !wait_mutex_ready(lock_shm_)) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex lock create failed because init is not ready");
            return;
        }
    }

    if (!lookup_mutex_local_lock(lock_shm_)) {
        register_mutex_local_lock(lock_shm_, std::make_shared<MutexLocalLock>());
    }
    register_message_process_func();
}

void MutexLock::lock_free()
{
    std::shared_ptr<MutexLocalLock> local_lock = unregister_mutex_local_lock_if_idle(lock_shm_);
    if (local_lock) {
        return;
    }
    if (lookup_mutex_local_lock(lock_shm_)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex lock free failed because local owner or active users still exist");
    }
}

bool MutexLock::is_ready() const
{
    return lock_shm_->is_inited.load(std::memory_order_acquire) == MUTEX_READY;
}

ub_lock_result_t MutexLock::check_recursive_global_owner(uint64_t identify) const
{
    if (lock_shm_->lock_owner.load(std::memory_order_acquire) == identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "recursive mutex lock is not supported");
        return UB_LOCK_ERROR;
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t MutexLock::wait_global_handoff(const steady_time_point &deadline, const ub_location_t &location,
                                                uint32_t &slot)
{
    local_wait_ctx_t ctx;
    WaiterGuard guard(location.tid, &ctx);
    ub_lock_result_t ret = enqueue_waiter(location, slot);
    if (ret != UB_LOCK_SUCCESS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "The mutex lock waiting queue is full");
        return ret;
    }

    bool global_self_notify = false;
    if (lock_shm_->lock_owner.load(std::memory_order_acquire) == LOCK_INVALID_OWNER) {
        global_self_notify = ring_queue_try_self_handoff_if_head<UB_MAX_NODES>(
            lock_shm_->queue_head, lock_shm_->queue_tail, lock_shm_->wait_queue, slot);
    }

    if (!global_self_notify && !ctx.wait(deadline)) {
        dump_mutex_timeout_info(lock_shm_, location, "global", nullptr);
        clean_timeout_waiter(slot);
        return UB_LOCK_TIMEOUT;
    }
    return UB_LOCK_SUCCESS;
}

ub_lock_result_t MutexLock::acquire_global(const steady_time_point &deadline, const ub_location_t &location)
{
    const uint64_t identify = make_global_owner(location.node_id, location.tid);
    if (check_recursive_global_owner(identify) != UB_LOCK_SUCCESS) {
        return UB_LOCK_ERROR;
    }

    uint32_t wait_round = 0;
    uint32_t slot = 0;
    bool is_awakened = false;
    while (std::chrono::steady_clock::now() <= deadline) {
        if (try_lock_fast(identify, is_awakened, slot)) {
            return UB_LOCK_SUCCESS;
        }

        if (check_recursive_global_owner(identify) != UB_LOCK_SUCCESS) {
            return UB_LOCK_ERROR;
        }

        if (is_awakened) {
            cpu_relax();
            continue;
        }
        if ((++wait_round % MUTEX_YIELD_INTERVAL) == 0) {
            ub_lock_result_t ret = wait_global_handoff(deadline, location, slot);
            if (ret != UB_LOCK_SUCCESS) {
                return ret;
            }
            is_awakened = true;
            continue;
        }
        cpu_relax();
    }

    dump_mutex_timeout_info(lock_shm_, location, "global", nullptr);
    if (is_awakened) {
        clean_timeout_waiter(slot);
    }
    return UB_LOCK_TIMEOUT;
}

ub_lock_result_t MutexLock::lock(time_ms_t timeout_ms, const ub_location_t &location)
{
    if (is_invalid_node(location.node_id) || !is_ready()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid mutex lock node_id=%d", location.node_id);
        return UB_LOCK_ERROR;
    }

    const uint64_t identify = make_global_owner(location.node_id, location.tid);
    if (lock_shm_->lock_owner.load(std::memory_order_acquire) == identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "recursive mutex lock is not supported");
        return UB_LOCK_ERROR;
    }

    std::shared_ptr<MutexLocalLock> local_lock = acquire_mutex_local_lock(lock_shm_);
    if (!local_lock) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex lock was not created in current process");
        return UB_LOCK_ERROR;
    }
    const time_ms_t effective_timeout = timeout_ms == 0 ? DEFAULT_MUTEX_TIMEOUT_MS : timeout_ms;
    const steady_time_point deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_timeout);
    if (local_lock->owner.load(std::memory_order_acquire) == identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "recursive mutex lock is not supported");
        release_mutex_local_lock(local_lock);
        return UB_LOCK_ERROR;
    }

    ub_lock_result_t local_ret = lock_mutex_local(lock_shm_, *local_lock, identify, location, deadline);
    if (local_ret != UB_LOCK_SUCCESS) {
        release_mutex_local_lock(local_lock);
        return local_ret;
    }

    ub_lock_result_t ret = acquire_global(deadline, location);
    if (ret == UB_LOCK_SUCCESS) {
        release_mutex_local_lock(local_lock);
        return UB_LOCK_SUCCESS;
    }

    unlock_mutex_local(*local_lock, identify);
    release_mutex_local_lock(local_lock);
    return ret;
}

ub_lock_result_t MutexLock::unlock(const ub_location_t &location)
{
    if (is_invalid_node(location.node_id) || !is_ready()) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "invalid mutex unlock node_id=%d", location.node_id);
        return UB_LOCK_ERROR;
    }

    const uint64_t identify = make_global_owner(location.node_id, location.tid);
    const uint64_t owner = lock_shm_->lock_owner.load(std::memory_order_acquire);
    if (owner != identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex unlock failed because caller does not own the lock");
        return UB_LOCK_ERROR;
    }

    std::shared_ptr<MutexLocalLock> local_lock = acquire_mutex_local_lock(lock_shm_);
    if (!local_lock) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex unlock failed because lock was not created in current process");
        return UB_LOCK_ERROR;
    }
    if (local_lock->owner.load(std::memory_order_acquire) != identify) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex local owner is inconsistent");
        release_mutex_local_lock(local_lock);
        return UB_LOCK_ERROR;
    }

    uint64_t expected = identify;
    if (!lock_shm_->lock_owner.compare_exchange_strong(expected, LOCK_INVALID_OWNER, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "mutex unlock failed because global owner changed");
        release_mutex_local_lock(local_lock);
        return UB_LOCK_ERROR;
    }
    if (lock_shm_->waiting_count.load(std::memory_order_acquire) > 0) {
        wake_one_waiter(location);
    }
    unlock_mutex_local(*local_lock, identify);
    release_mutex_local_lock(local_lock);
    return UB_LOCK_SUCCESS;
}

} // namespace ublock

extern "C" void ub_mutex_lock_create(ub_mutex_lock_t *lock)
{
    if (!lock) {
        return;
    }
    ublock::MutexLock impl(lock);
    impl.lock_create();
}

extern "C" void ub_mutex_lock_free(ub_mutex_lock_t *lock)
{
    if (!lock) {
        return;
    }
    ublock::MutexLock impl(lock);
    impl.lock_free();
}

extern "C" ub_lock_result_t ub_mutex_lock(ub_mutex_lock_t *lock, time_ms_t timeout_ms, const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    ublock::MutexLock impl(lock);
    return impl.lock(timeout_ms, *location);
}

extern "C" ub_lock_result_t ub_mutex_unlock(ub_mutex_lock_t *lock, const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    ublock::MutexLock impl(lock);
    return impl.unlock(*location);
}
