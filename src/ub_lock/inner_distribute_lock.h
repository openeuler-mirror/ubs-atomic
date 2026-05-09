/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#pragma once

#include <cstring>
#include "local_lock.h"
#include "ring_queue.h"
#include "ub_atomic_log_print.h"
#include "ub_dist_comm_queue.h"
#include "ub_dist_lock.h"
namespace ublock {

typedef enum {
    UB_GRANT = 1,   //唤醒
    UB_RELEASE = 2, //释放
    UB_UNKNOWN = 0
} ub_message_type_t;

struct local_msg_body_t {
    int32_t tid; // 线程ID
    void *addr;  // 本地锁实例地址(延迟唤醒)
    ub_message_type_t type;
    ub_lock_mode_t mode;
};

class DistributedLock { // 逻辑封装
public:
    explicit DistributedLock(ub_rw_lock_t *shm) : rw_lock_shm_(shm) {};
    ~DistributedLock();

    // 锁获取和释放
    void lock_create(const ub_lock_config_t &config, const ub_location_t &location);
    void lock_free(const ub_location_t &location);

    // 锁状态管理
    ub_lock_result_t lock_s(const ub_lock_policy_t &policy, const ub_location_t &location);
    ub_lock_result_t lock_sx(const ub_lock_policy_t &policy, const ub_location_t &location);
    ub_lock_result_t lock_x(const ub_lock_policy_t &policy, const ub_location_t &location);

    ub_lock_result_t unlock_s(const ub_lock_policy_t &policy, const ub_location_t &location);
    ub_lock_result_t unlock_sx(const ub_lock_policy_t &policy, const ub_location_t &location);
    ub_lock_result_t unlock_x(const ub_lock_policy_t &policy, const ub_location_t &location);
    ub_lock_result_t delay_unlock(ub_lock_mode_t mode, uint8_t node_id);

private:
    ub_rw_lock_t *rw_lock_shm_; //指向共享内存数据

    void wake_after_unlock_exclusive(const ub_location_t &location);
    void dequeue_and_notify_one(const ub_location_t &location);
    ub_lock_result_t verify_param(const ub_lock_policy_t &policy, const ub_location_t &location);

    // 队列管理
    void create_wait_queue();
    ub_lock_result_t enqueue_waiter(ub_lock_mode_t mode, const ub_location_t &location, uint32_t &out_ticket);
    ub_lock_result_t outqueue_waiter(ub_waiter_t *&out_waiter);
    void clean_timeout_waiter(uint32_t ticket); // 清理等待队列中已经超时的 waiter
    void clean_outqueue_waiter(uint32_t ticket);
    void cleanup_and_unlock_local(LocalLock *local_lock);
    bool try_acquire_global_s(LocalLock *local_lock, bool &is_awakened, uint32_t slot);
    bool peek_head_waiting_mode_clean(ub_lock_mode_t &mode_out);

    // 消息管理
    ub_lock_result_t notify_waiters(ub_waiter_t &waiter, const ub_location_t &location); // 消息通知唤醒
    ub_lock_result_t notify_unlock(const ub_location_t &location, uint8_t node_id,
                                   const local_msg_body_t &msg_body); // 消息通知释放锁
    message_t *create_message(const ub_location_t &location, uint8_t node_id,
                              const local_msg_body_t &msg_body); // 构造消息结构体

    // 锁优化（本地锁降级+延迟释放）
    ub_lock_result_t delay_release_local_lock(LocalLock &local_lock, ub_lock_mode_t mode,
                                              const ub_location_t &location);
    ub_lock_result_t delay_release_ub_lock(ub_lock_mode_t mode, LocalLock &local_lock, const ub_location_t &location);
    ub_lock_result_t local_try_unlock_s(bool allow_delay_release, const ub_location_t &location);
    ub_lock_result_t local_try_unlock_x(bool allow_recursive, bool allow_delay_release, const ub_location_t &location);
    ub_lock_result_t local_try_unlock_sx(bool allow_recursive, bool allow_delay_release, const ub_location_t &location);
};

void message_process_thread_func(const message_t *msg, void *ctx); // 线程处理函数，用于处理消息队列中的消息
void register_message_process_func();                              //注册线程处理函数
} // namespace ublock