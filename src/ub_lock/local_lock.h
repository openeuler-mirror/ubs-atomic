/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include "ub_dist_lock.h"

#define UB_CACHELINE_SIZE 64
/* Maximum number of slots in the FIFO queue */
#define UB_MAX_CAPACITY 256
/* Maximum number of nodes participating in the distributed lock */
#define UB_MAX_NODES 16
/* Maximum number of nodes participating in the distributed lock */
#define LOCK_S_THRESHOLD 100
/* State machine for one waiter slot in the FIFO queue */
typedef uint32_t ub_wait_state_t;

#define UB_WAIT_EMPTY ((ub_wait_state_t)0u)    /* slot unused */
#define UB_WAIT_WRITING ((ub_wait_state_t)1u)  /* producer writing waiter data */
#define UB_WAIT_WAITING ((ub_wait_state_t)2u)  /* waiting for notification */
#define UB_WAIT_NOTIFIED ((ub_wait_state_t)3u) /* notified (ready to wake up) */
#define UB_WAIT_TIMEOUT ((ub_wait_state_t)4u)  /* timed out */

typedef enum {
    UB_LOCK_S = 0,  /* shared (read) lock */
    UB_LOCK_SX = 1, /* shared-exclusive (upgrade intent) lock */
    UB_LOCK_X = 2,  /* exclusive (write) lock */
    UB_LOCK_I = 3,  /* Invalid lock type */
} ub_lock_mode_t;   /* Lock mode definition */

struct alignas(16) ub_waiter_t {
    std::atomic<uint32_t> seq; /* seq-based ring state machine */
    ub_lock_mode_t mode;       /* requested lock mode */
    ub_location_t location;    /* waiter identity */
}; /* One entry in the wait queue ring buffer */

struct alignas(UB_CACHELINE_SIZE) ub_rw_lock {
    std::atomic<int32_t> lock_word;      /* core lock state machine */
    std::atomic<uint32_t> waiting_count; /* fast-path waiter indicator */
    uint8_t _pad_core[56];

    std::atomic<uint32_t> queue_head; /* wait queue head index */
    uint8_t _pad_qt[60];
    std::atomic<uint32_t> queue_tail; /* wait queue tail index */
    uint8_t _pad_qh[60];

    std::atomic<uint64_t> lock_owner_x;       /* exclusive lock owner */
    std::atomic<uint64_t> lock_owner_sx;      /* shared-exclusive owner */
    std::atomic<uint64_t> reserve_lock_owner; /* delayed-release owner */
    std::atomic<uint32_t> sx_recursive;       /* number of recursive sx lock */
    std::atomic<uint32_t> x_recursive;        /* number of recursive x lock */
    std::atomic<int32_t> is_inited;           /* initialization flag */
    uint8_t _pad_misc[28];                    /* Fill remaining 32 bytes */

    ub_waiter_t wait_queue[UB_MAX_NODES];  /* FIFO wait queue */
    uintptr_t node_registry[UB_MAX_NODES]; /* UB lock table */
};

inline void cpu_relax()
{
#if defined(__aarch64__)
    asm volatile("yield" ::: "memory"); // ARM hint, not OS yield
#else
    asm volatile("" ::: "memory");
#endif
}

namespace ublock {

constexpr int32_t X_LOCK_DECR = 1 << 16;      // 65536
constexpr int32_t X_LOCK_HALF_DECR = 1 << 15; // 32768
constexpr uint32_t SPIN_WAIT_ROUNDS = 30;
constexpr uint64_t LOCK_INVALID_OWNER = static_cast<uint64_t>(255) << 32;

using steady_time_point = std::chrono::steady_clock::time_point;
// a lightweight cacheline padding wrapper for a single atomic
template <class T>
struct alignas(UB_CACHELINE_SIZE) CachelineAtomic {
    std::atomic<T> v;
    CachelineAtomic() noexcept = default;
    constexpr CachelineAtomic(T init) noexcept : v(init) {}
    CachelineAtomic(const CachelineAtomic &) = delete;
    CachelineAtomic &operator=(const CachelineAtomic &) = delete;
};

struct local_wait_ctx_t {
    std::mutex mtx;
    std::condition_variable cv;
    bool notified = false;

    bool wait(steady_time_point deadline)
    {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_until(lk, deadline, [&] { return notified; });
    }

    void notify()
    {
        std::lock_guard<std::mutex> lk(mtx);
        notified = true;
        cv.notify_one();
    }

    void reset()
    {
        std::lock_guard<std::mutex> lk(mtx);
        notified = false;
    }
};

class WaiterRegistry {
public:
    static WaiterRegistry &instance()
    {
        static WaiterRegistry inst;
        return inst;
    }
    // 注册：将 tid 映射到本地上下文
    void register_waiter(int32_t tid, local_wait_ctx_t *ctx)
    {
        std::unique_lock<std::shared_mutex> lock(map_mtx_);
        waiter_map_[tid] = ctx;
    }
    // 注销：等待结束或超时后移除
    void unregister_waiter(int32_t tid)
    {
        std::unique_lock<std::shared_mutex> lock(map_mtx_);
        waiter_map_.erase(tid);
    }
    // 查找并唤醒：根据 TID 找到上下文并唤醒
    // 返回 true 表示找到了本地线程并唤醒，false 表示该 TID 不在本地
    bool notify_local_waiter(int32_t tid)
    {
        std::shared_lock<std::shared_mutex> lock(map_mtx_);
        auto it = waiter_map_.find(tid);
        if (it != waiter_map_.end()) {
            // 找到了！唤醒它
            it->second->notify();
            return true;
        }
        return false;
    }

private:
    // 使用 shared_mutex 提高读并发 (Lookup频率高)
    std::shared_mutex map_mtx_;
    std::unordered_map<int32_t, local_wait_ctx_t *> waiter_map_;
};

class WaiterGuard {
public:
    WaiterGuard(int32_t tid, local_wait_ctx_t *ctx) : tid_(tid)
    {
        WaiterRegistry::instance().register_waiter(tid_, ctx);
    }

    ~WaiterGuard()
    {
        WaiterRegistry::instance().unregister_waiter(tid_);
    }

    // 禁止拷贝和移动，确保一一对应
    WaiterGuard(const WaiterGuard &) = delete;
    WaiterGuard &operator=(const WaiterGuard &) = delete;

private:
    int32_t tid_;
};

struct local_waiter_t {
    std::atomic<ub_wait_state_t> seq{UB_WAIT_EMPTY};
    ub_lock_mode_t mode{UB_LOCK_I};
    int32_t tid{0};
};

class LocalLock {
public:
    explicit LocalLock(ub_rw_lock_t *shm) : ub_lock_ptr_(shm)
    {
        init_();
    }
    ~LocalLock() = default;

    ub_lock_result_t lock_s(int32_t tid, steady_time_point deadline);
    ub_lock_result_t lock_sx(bool allow_recursive, int32_t tid, steady_time_point deadliney);
    ub_lock_result_t lock_x(bool allow_recursive, int32_t tid, steady_time_point deadline);

    ub_lock_result_t unlock_s();
    ub_lock_result_t unlock_sx(bool allow_recursive, int32_t tid);
    ub_lock_result_t unlock_x(bool allow_recursive, int32_t tid);

    ub_rw_lock_t *ub_lock_ptr_{nullptr}; // global lock virtual address

    void init_();
    // queue
    void create_wait_queue();
    ub_lock_result_t enqueue_waiter(ub_lock_mode_t mode, int32_t tid, uint32_t &out_ticket);
    ub_lock_result_t outqueue_waiter(local_waiter_t *&out);
    bool peek_head_waiting_mode_clean(ub_lock_mode_t &mode_out);
    void clean_timeout_waiter(uint32_t ticket);
    void clean_outqueue_waiter(uint32_t ticket);
    bool notify_one(local_waiter_t &w);
    void wake_after_unlock_exclusive(); // batch wake by rule
    void dequeue_and_notify_one();

    bool is_held();
    void end_remote_release();
    bool try_begin_remote_release();
    bool try_inc_global_ref();

    CachelineAtomic<int32_t> lock_word{0};
    CachelineAtomic<uint32_t> q_head{0};
    CachelineAtomic<uint32_t> q_tail{0};
    std::atomic<uint32_t> waiting_count{0};
    std::atomic<uint32_t> read_count{0};
    std::atomic<int32_t> lock_x_owner{0};
    std::atomic<int32_t> lock_sx_owner{0};
    std::atomic<uint32_t> x_recursive_{0};
    std::atomic<uint32_t> sx_recursive_{0};

    std::array<local_waiter_t, UB_MAX_CAPACITY> q{};

    std::atomic<ub_lock_mode_t> local_is_reserve_lock{UB_LOCK_I};
    std::atomic<bool> remote_release_in_progress_{false};

    enum GlobalState : int {
        GLOBAL_IDLE = 0,
        GLOBAL_PENDING = 1,
        GLOBAL_HELD = 2
    };
    std::atomic<int> global_state_{GLOBAL_IDLE};
    std::atomic<int32_t> global_read_ref_count_{0};
    std::mutex global_pending_mtx_;
    std::condition_variable global_pending_cv_;
};

struct LocalLockRegistry {
    std::unordered_map<ub_rw_lock_t *, std::shared_ptr<LocalLock>> map;
    mutable std::shared_mutex mtx;
};

inline LocalLockRegistry g_ll_registry;

std::shared_ptr<LocalLock> lookup_local_lock(ub_rw_lock_t *shm);
void register_local_lock(ub_rw_lock_t *shm, std::shared_ptr<LocalLock> ll);
std::shared_ptr<LocalLock> unregister_local_lock(ub_rw_lock_t *shm);

// NodeID + TID -> uint64_t
inline uint64_t make_global_owner(uint8_t node_id, int32_t tid)
{
    return (static_cast<uint64_t>(node_id) << 32) | static_cast<uint32_t>(tid);
}

} // namespace ublock