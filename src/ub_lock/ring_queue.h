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

#include <atomic>
#include <cstdint>
#include "inner_distribute_lock.h"

namespace ublock {

template <typename Slot>
inline void ring_queue_reset_payload(Slot &slot)
{
    (void)slot;
}

inline void ring_queue_reset_payload(ub_waiter_t &slot)
{
    slot.mode = UB_LOCK_I;
    slot.location.tid = 0;
    slot.location.node_id = 0xFF;
}

inline void ring_queue_reset_payload(local_waiter_t &slot)
{
    slot.mode = UB_LOCK_I;
    slot.tid = 0;
}

template <typename Count>
inline void ring_queue_try_dec_count(Count &count)
{
    uint32_t cur = count.load(std::memory_order_acquire);
    while (cur > 0u) {
        if (count.compare_exchange_weak(cur, cur - 1u, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return;
        }
    }
}

template <typename Slot, typename Count>
inline bool ring_queue_try_mark_timeout(Slot &slot, Count &count)
{
    uint32_t seq = slot.seq.load(std::memory_order_acquire);
    while (seq == UB_WAIT_WRITING || seq == UB_WAIT_WAITING || seq == UB_WAIT_NOTIFIED) {
        uint32_t expected = seq;
        if (slot.seq.compare_exchange_weak(expected, UB_WAIT_TIMEOUT, std::memory_order_acq_rel,
                                           std::memory_order_acquire)) {
            ring_queue_reset_payload(slot);
            ring_queue_try_dec_count(count);
            return true;
        }
        seq = expected;
    }
    return false;
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail, typename Count, typename InitPayloadFn>
inline void ring_queue_init(Head &head, Tail &tail, Count &count, Slot *slots, InitPayloadFn init_payload)
{
    head.store(0u, std::memory_order_relaxed);
    tail.store(0u, std::memory_order_relaxed);
    count.store(0u, std::memory_order_relaxed);
    for (uint32_t i = 0; i < CAP; ++i) {
        slots[i].seq.store(UB_WAIT_EMPTY, std::memory_order_relaxed);
        init_payload(slots[i]);
    }
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail>
inline void ring_queue_advance_dead_head(Head &head, Tail &tail, Slot *slots)
{
    using head_type = std::decay_t<decltype(head.load())>;
    while (true) {
        head_type h = head.load(std::memory_order_acquire);
        head_type t = tail.load(std::memory_order_acquire);
        if (h == t) {
            return; // 没东西
        }
        const uint32_t idx = static_cast<uint32_t>(h) & (CAP - 1u);
        Slot &slot = slots[idx];
        const uint32_t seqv = slot.seq.load(std::memory_order_acquire);

        if (seqv == UB_WAIT_TIMEOUT) {
            head_type expected = h;
            (void)head.compare_exchange_weak(expected, h + static_cast<head_type>(1), std::memory_order_acq_rel,
                                             std::memory_order_acquire);
            continue;
        }
        return;
    }
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail, typename Count, typename WritePayloadFn>
inline ub_lock_result_t ring_queue_enqueue(Head &head, Tail &tail, Count &count, Slot *slots,
                                           WritePayloadFn write_payload, uint32_t &out_ticket)
{
    using head_type = typename std::decay_t<decltype(head.load())>;
    using tail_type = typename std::decay_t<decltype(tail.load())>;

    tail_type ticket_raw = 0;
    while (true) {
        const head_type h = head.load(std::memory_order_acquire);
        tail_type t = tail.load(std::memory_order_acquire);
        if ((static_cast<uint32_t>(t) - static_cast<uint32_t>(h)) >= CAP) {
            return UB_LOCK_ERROR;
        }
        const tail_type next = static_cast<tail_type>(t + static_cast<tail_type>(1));
        if (tail.compare_exchange_weak(t, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
            ticket_raw = t;
            break;
        }
        cpu_relax();
    }

    const uint32_t ticket = static_cast<uint32_t>(ticket_raw);
    const uint32_t idx = ticket & (CAP - 1u);
    Slot &slot = slots[idx];

    using seq_type = typename std::decay_t<decltype(slot.seq.load())>;
    seq_type seq = slot.seq.load(std::memory_order_relaxed);
    slot.seq.store(UB_WAIT_WRITING, std::memory_order_release);
    write_payload(slot);
    // Ready (Release)
    slot.seq.store(UB_WAIT_WAITING, std::memory_order_release);
    count.fetch_add(1u, std::memory_order_release);
    out_ticket = ticket;
    return UB_LOCK_SUCCESS;
}

template <uint32_t CAP, typename Slot, typename Head, typename Count>
inline ub_lock_result_t ring_queue_outqueue(Head &head, Count &count, Slot *slots, Slot *&out_slot)
{
    using head_type = typename std::decay_t<decltype(head.load())>;
    int sanity = static_cast<int>(CAP) * 4;
    while (sanity-- > 0) {
        head_type h = head.load(std::memory_order_acquire);
        const uint32_t cur = static_cast<uint32_t>(h);
        const uint32_t idx = cur & (CAP - 1u);
        Slot &slot = slots[idx];

        const uint32_t seqv = slot.seq.load(std::memory_order_acquire);
        if (seqv == UB_WAIT_WRITING) {
            cpu_relax();
            continue;
        }

        if (seqv == UB_WAIT_WAITING) {
            uint32_t expected_seq = UB_WAIT_WAITING;
            if (slot.seq.compare_exchange_strong(expected_seq, UB_WAIT_NOTIFIED, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                out_slot = &slot;
                return UB_LOCK_SUCCESS;
            }
            continue;
        }
        return UB_LOCK_ERROR;
    }
    return UB_LOCK_ERROR;
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail, typename Count>
inline void ring_queue_clean_timeout(Head &head, Tail &tail, Count &count, Slot *slots, uint32_t ticket)
{
    const uint32_t idx = ticket & (CAP - 1u);
    Slot &slot = slots[idx];
    (void)ring_queue_try_mark_timeout(slot, count);
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail, typename Count>
inline bool ring_queue_peek_head_mode_clean(Head &head, Tail &tail, Count &count, Slot *slots, ub_lock_mode_t &mode_out)
{
    using head_type = typename std::decay_t<decltype(head.load())>;
    while (true) {
        head_type h = head.load(std::memory_order_acquire);
        head_type t = tail.load(std::memory_order_acquire);
        if (h == t) {
            return false;
        }

        const uint32_t cur = static_cast<uint32_t>(h);
        const uint32_t idx = cur & (CAP - 1u);
        Slot &slot = slots[idx];
        const uint32_t seqv = slot.seq.load(std::memory_order_acquire);

        if (seqv == UB_WAIT_WRITING) {
            cpu_relax();
            continue;
        }

        if (seqv == UB_WAIT_WAITING) {
            mode_out = slot.mode;
            return true;
        }

        if (seqv == UB_WAIT_NOTIFIED) {
            return false;
        }
        if (seqv == UB_WAIT_TIMEOUT) {
            head_type expected = h;
            (void)head.compare_exchange_weak(expected, h + static_cast<head_type>(1), std::memory_order_acq_rel,
                                             std::memory_order_acquire);
            continue;
        }
        return false;
    }
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail, typename Count>
inline void ring_queue_pop_head(Head &head, Tail &tail, Count &count, Slot *slots, uint32_t ticket)
{
    const uint32_t idx = ticket & (CAP - 1u);
    Slot &slot = slots[idx];
    slot.seq.store(UB_WAIT_EMPTY, std::memory_order_release);
    ring_queue_reset_payload(slot);
    head.fetch_add(1u, std::memory_order_release);
    ring_queue_try_dec_count(count);
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail>
inline bool ring_queue_try_self_handoff_if_head(Head &head, Tail &tail, Slot *slots, uint32_t ticket)
{
    using head_type = std::decay_t<decltype(head.load())>;
    head_type h = head.load(std::memory_order_acquire);
    head_type t = tail.load(std::memory_order_acquire);
    if (h == t)
        return false;

    if (static_cast<uint32_t>(h) != ticket) {
        return false; // 不是队头，不能自唤醒
    }
    Slot &slot = slots[ticket & (CAP - 1)];
    uint32_t expected = UB_WAIT_WAITING;
    return slot.seq.compare_exchange_strong(expected, UB_WAIT_NOTIFIED, std::memory_order_acq_rel,
                                            std::memory_order_relaxed);
}

} // namespace ublock
