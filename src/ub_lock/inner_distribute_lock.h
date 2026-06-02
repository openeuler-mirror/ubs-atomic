/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * rmrs is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstring>
#include "local_lock.h"
#include "ring_queue.h"
#include "ub_atomic_log_print.h"
#include "ub_dist_comm_queue.h"
#include "ub_dist_lock.h"

namespace ublock {

typedef enum
{
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

inline void clear_shared_owner_bitmap(ub_rw_lock_t *lock, uint8_t process_id)
{
    uint32_t mask = ~(1u << process_id);
    lock->shared_owner_bitmap.fetch_and(mask, std::memory_order_acq_rel);
}

class DistributedLock { // 逻辑封装
public:
    explicit DistributedLock(ub_rw_lock_t *shm) : rw_lock_shm_(shm){};
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
    ub_lock_result_t recover(const uint32_t process_id, const ub_location_t &location);
    ub_lock_result_t query_holder(const ub_location_t &location, ub_lock_query_result_t &result);
    ub_lock_result_t rebuild(ub_rw_lock_t *old_lock, const ub_lock_rebuild_info_t &rebuild_info,
                             const ub_location_t &location);

private:
    ub_rw_lock_t *rw_lock_shm_; //指向共享内存数据

    void wake_after_unlock_exclusive(const ub_location_t &location);
    void dequeue_and_notify_one(const ub_location_t &location);
    ub_lock_result_t verify_param(const ub_location_t &location);

    // 队列管理
    void create_wait_queue();
    ub_lock_result_t enqueue_waiter(ub_lock_mode_t mode, const ub_location_t &location, uint32_t &out_ticket);
    ub_lock_result_t outqueue_waiter(ub_waiter_t *&out_waiter);
    void clean_timeout_waiter(uint32_t ticket); // 清理等待队列中已经超时的 waiter
    void clean_outqueue_waiter(uint32_t ticket);
    void recover_shared_lock(uint32_t process_id);
    void cleanup_and_unlock_local(LocalLock *local_lock);
    bool peek_head_waiting_mode_clean(ub_lock_mode_t &mode_out);

    // 消息管理
    ub_lock_result_t notify_waiters(ub_waiter_t &waiter, const ub_location_t &location);
    ub_lock_result_t notify_unlock(const ub_location_t &location, uint8_t node_id, const local_msg_body_t &msg_body);
    message_t *create_message(const ub_location_t &location, uint8_t node_id, const local_msg_body_t &msg_body);

    // 锁优化（本地锁降级+延迟释放）
    ub_lock_result_t delay_release_local_lock(LocalLock &local_lock, ub_lock_mode_t mode,
                                              const ub_location_t &location);
    ub_lock_result_t delay_release_ub_lock(ub_lock_mode_t mode, LocalLock &local_lock, const ub_location_t &location);

    void wake_s_chain(const ub_location_t &location);
    void wake_sx_chain(const ub_location_t &location);

    void dump_timeout_holder_info(const ub_location_t &location, ub_lock_mode_t request_mode, LocalLock *local_lock,
                                  const char *reason);
    bool wait_follower_s(const ub_location_t &location, LocalLock *local_lock, const steady_time_point &deadline);
    ub_lock_result_t spin_wait_s_loop(const ub_location_t &location, LocalLock *local_lock,
                                      const steady_time_point &deadline);
    ub_lock_result_t spin_wait_x_loop(const ub_location_t &location, LocalLock *local_lock,
                                      const steady_time_point &deadline, bool is_recursive);
    ub_lock_result_t spin_wait_sx_loop(const ub_location_t &location, LocalLock *local_lock,
                                       const steady_time_point &deadline, bool is_recursive);

    void recover_exclusive_x(uint32_t process_id);
    void recover_exclusive_sx(uint32_t process_id);
    void recover_shared_sx_s(uint32_t process_id);
};

void message_process_thread_func(const message_t *msg, void *ctx); // 线程处理函数，用于处理消息队列中的消息
void register_message_process_func();                              //注册线程处理函数
} // namespace ublock
