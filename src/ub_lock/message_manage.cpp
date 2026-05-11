/*
 	 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 	 */

#include <cstring>
#include <memory>
#include <mutex>
#include <regex>
#include <stdexcept>
#include <string>
#include "UBShmTransport.h"
#include "inner_distribute_lock.h"
#include "ub_dist_comm_queue.h"

using ub_comm_queue::UBShmTransport;
extern UBShmTransport *g_transport;
namespace ublock {

namespace {
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
std::once_flag g_register_msg_once_flag;
} // namespace

static inline void backoff_sleep(uint32_t attempt)
{
    // attempt starts from 0
    // 0: 50us, 1: 100us, 2: 200us ... capped at 5ms
    uint32_t us = 50u << (attempt > 7 ? 7 : attempt);
    if (us > 5000u) {
        us = 5000u;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(us));
}

static inline ub_lock_result_t send_with_retry(const message_t *msg, uint32_t max_attempts)
{
    for (uint32_t i = 0; i < max_attempts; ++i) {
        int ret = g_transport->send(msg);
        if (ret == UB_LOCK_SUCCESS) {
            return UB_LOCK_SUCCESS;
        }

        backoff_sleep(i);
    }
    return UB_LOCK_ERROR;
}

message_t *DistributedLock::create_message(const ub_location_t &location, uint8_t node_id,
                                           const local_msg_body_t &msg_body)
{
    message_t *msg = new message_t();

    uint32_t body_len = sizeof(local_msg_body_t);
    msg->body = new char[body_len];

    std::memcpy(msg->body, &msg_body, body_len);

    msg->header.body_length = body_len;
    msg->header.dest_node_id = location.node_id;
    msg->header.src_node_id = node_id;

    msg->header.msg_type = 0xFF;
    msg->header.priority = 0;

    return msg;
}

ub_lock_result_t DistributedLock::notify_waiters(ub_waiter_t &waiter, const ub_location_t &location)
{
    // local wakeup
    if (waiter.location.node_id == location.node_id) {
        bool found = WaiterRegistry::instance().notify_local_waiter(waiter.location.tid);
        if (!found) {
            ATOMIC_LOG(LOG_LEVEL_WARN, "Global wait ctx has been notified: %d", waiter.location.tid);
            return UB_LOCK_ERROR;
        }
        return UB_LOCK_SUCCESS;
    }
    // global wakeup
    auto advance_head_after_send_fail = [&]() {
        // only push head
        auto h = rw_lock_shm_->queue_head.load(std::memory_order_acquire);
        const uint32_t idx = static_cast<uint32_t>(h) & (UB_MAX_NODES - 1u);
        if (&rw_lock_shm_->wait_queue[idx] == &waiter) {
            (void)rw_lock_shm_->queue_head.compare_exchange_strong(h, h + 1u, std::memory_order_acq_rel,
                                                                   std::memory_order_acquire);
        }
    };
    if (g_transport == nullptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Transport handle is null");
        return UB_LOCK_ERROR;
    }
    local_msg_body_t body = {.tid = waiter.location.tid, .addr = nullptr, .type = UB_GRANT, .mode = waiter.mode};
    MessagePtr msg(create_message(waiter.location, location.node_id, body));
    if (!msg) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to create message");
        return UB_LOCK_ERROR;
    }

    ub_lock_result_t ret = send_with_retry(msg.get(), /*max_attempts=*/5);

    if (ret != UB_LOCK_SUCCESS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed after 5 retries");
        advance_head_after_send_fail();
        return UB_LOCK_ERROR;
    }

    return UB_LOCK_SUCCESS;
}

ub_lock_result_t DistributedLock::notify_unlock(const ub_location_t &location, uint8_t node_id,
                                                const local_msg_body_t &msg_body)
{
    MessagePtr msg(create_message(location, node_id, msg_body));
    if (!msg) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Failed to create message");
        return UB_LOCK_ERROR;
    }

    ub_lock_result_t ret = send_with_retry(msg.get(), /*max_attempts=*/5);

    if (ret != UB_LOCK_SUCCESS) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Send failed after 5 retries");
        return UB_LOCK_ERROR;
    }

    return UB_LOCK_SUCCESS;
}

void message_process_thread_func(const message_t *msg, void *ptr)
{
    if (!msg || !msg->body || msg->header.body_length <= 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid message: msg is null or length is zero");
        return;
    }

    local_msg_body_t body;
    std::memcpy(&body, msg->body, sizeof(local_msg_body_t));

    switch (body.type) {
        case UB_GRANT: {
            // wakeup
            bool found = WaiterRegistry::instance().notify_local_waiter(body.tid);
            if (!found) {
                ATOMIC_LOG(LOG_LEVEL_DEBUG, "UB GRANT: local wait ctx is null.");
            }
            break;
        }

        case UB_RELEASE: {
            // release
            ub_rw_lock_t *shm_lock = static_cast<ub_rw_lock_t *>(body.addr);
            if (!shm_lock) {
                ATOMIC_LOG(LOG_LEVEL_DEBUG, "UB RELEASE: ub lock is null.");
                break;
            }
            std::shared_ptr<LocalLock> ll_sp = lookup_local_lock(shm_lock);
            LocalLock *local_lock = ll_sp.get();
            if (!local_lock) {
                ATOMIC_LOG(LOG_LEVEL_DEBUG, "UB RELEASE: local lock is null.");
                break;
            }

            if (!local_lock->try_begin_remote_release()) {
                ATOMIC_LOG(LOG_LEVEL_DEBUG, "UB RELEASE: try_begin_remote_release is failed.");
                break;
            }
            auto old_mode = local_lock->local_is_reserve_lock.exchange(UB_LOCK_I, std::memory_order_acq_rel);
            if (old_mode == UB_LOCK_I) {
                // 令牌已经被别人拿走了，我直接退出，避免二次释放
                ATOMIC_LOG(LOG_LEVEL_DEBUG, "UB RELEASE: token already taken.");
                local_lock->end_remote_release();
                break;
            }

            if (local_lock->is_held()) {
                ATOMIC_LOG(LOG_LEVEL_DEBUG, "UB RELEASE: local lock is still held.");
                local_lock->end_remote_release();
                break;
            }
            DistributedLock impl(shm_lock);
            (void)impl.delay_unlock(old_mode, msg->header.dest_node_id);
            local_lock->end_remote_release();
            break;
        }

        default: {
            break;
        }
    }
}

void register_message_process_func()
{
    try {
        std::call_once(g_register_msg_once_flag, []() {
            if (g_transport == nullptr) {
                throw std::runtime_error("Transport handle is null");
            }
            int ret = g_transport->register_func_for_lock(0xFF, UB_FUNC_ASYNC, message_process_thread_func, nullptr);
            if (ret != 0) {
                throw std::runtime_error("register_func_for_lock failed");
            }
            ATOMIC_LOG(LOG_LEVEL_INFO, "register_message_process_func done");
        });
    } catch (const std::exception &e) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "register_message_process_func failed: %s", e.what());
    }
}
} // namespace ublock