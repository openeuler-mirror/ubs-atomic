/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "inner_distribute_lock.h"

namespace ublock {

/*
slot uses seq as a state machine:
1) writable: seq == ticket
2) readable: seq == ticket + 1
3) pending:  seq == ticket + 2
4) released: seq == ticket + UB_MAX_CAPACITY
*/
void DistributedLock::create_wait_queue()
{
    ring_queue_init<UB_MAX_NODES>(rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->waiting_count,
                                  rw_lock_shm_->wait_queue, [](ub_waiter_t &slot) {
                                      slot.mode = UB_LOCK_I;
                                      slot.location = ub_location_t{.tid = 0, .node_id = 0xFF};
                                  });
}

ub_lock_result_t DistributedLock::enqueue_waiter(ub_lock_mode_t mode, const ub_location_t &location,
                                                 uint32_t &out_ticket)
{
    return ring_queue_enqueue<UB_MAX_NODES>(
        rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->waiting_count, rw_lock_shm_->wait_queue,
        [&](ub_waiter_t &slot) {
            slot.mode = mode;
            slot.location = location;
        },
        out_ticket);
}

ub_lock_result_t DistributedLock::outqueue_waiter(ub_waiter_t *&out_waiter)
{
    return ring_queue_outqueue<UB_MAX_NODES>(rw_lock_shm_->queue_head, rw_lock_shm_->waiting_count,
                                             rw_lock_shm_->wait_queue, out_waiter);
}

void DistributedLock::clean_timeout_waiter(uint32_t ticket)
{
    ring_queue_clean_timeout<UB_MAX_NODES>(rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail,
                                           rw_lock_shm_->waiting_count, rw_lock_shm_->wait_queue, ticket);
}

bool DistributedLock::peek_head_waiting_mode_clean(ub_lock_mode_t &mode_out)
{
    return ring_queue_peek_head_mode_clean<UB_MAX_NODES>(rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail,
                                                         rw_lock_shm_->waiting_count, rw_lock_shm_->wait_queue,
                                                         mode_out);
}

void DistributedLock::clean_outqueue_waiter(uint32_t ticket)
{
    ring_queue_pop_head<UB_MAX_NODES>(rw_lock_shm_->queue_head, rw_lock_shm_->queue_tail, rw_lock_shm_->waiting_count,
                                      rw_lock_shm_->wait_queue, ticket);
}

} // namespace ublock