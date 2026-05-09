/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#pragma once

#include <atomic>
#include <cstdint>
#include "inner_distribute_lock.h"

namespace ublock {

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
    uint32_t old = count.fetch_add(1u, std::memory_order_relaxed);
    if (old >= CAP) {
        count.fetch_sub(1u, std::memory_order_relaxed);
        return UB_LOCK_ERROR;
    }

    const uint32_t ticket = static_cast<uint32_t>(tail.fetch_add(1u, std::memory_order_relaxed));
    const uint32_t idx = ticket & (CAP - 1u);
    Slot &slot = slots[idx];

    using seq_type = typename std::decay_t<decltype(slot.seq.load())>;
    seq_type seq = slot.seq.load(std::memory_order_relaxed);
    if (seq != UB_WAIT_TIMEOUT && seq != UB_WAIT_EMPTY) {
        count.fetch_sub(1u, std::memory_order_relaxed);
        return UB_LOCK_ERROR;
    }
    slot.seq.store(UB_WAIT_WRITING, std::memory_order_release);
    write_payload(slot);
    // Ready (Release)
    slot.seq.store(UB_WAIT_WAITING, std::memory_order_release);
    out_ticket = ticket;
    return UB_LOCK_SUCCESS;
}

template <uint32_t CAP, typename Slot, typename Head, typename Count>
inline ub_lock_result_t ring_queue_outqueue(Head &head, Count &count, Slot *slots, Slot *&out_slot)
{
    using head_type = typename std::decay_t<decltype(head.load())>;
    if (count.load(std::memory_order_acquire) == 0u) {
        return UB_LOCK_ERROR;
    }
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

    slot.seq.store(UB_WAIT_TIMEOUT, std::memory_order_release);
    count.fetch_sub(1u, std::memory_order_acq_rel);
}

template <uint32_t CAP, typename Slot, typename Head, typename Tail, typename Count>
inline bool ring_queue_peek_head_mode_clean(Head &head, Tail &tail, Count &count, Slot *slots, ub_lock_mode_t &mode_out)
{
    using head_type = typename std::decay_t<decltype(head.load())>;
    while (true) {
        if (count.load(std::memory_order_acquire) == 0u) {
            return false;
        }

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
    head.fetch_add(1u, std::memory_order_release);
    count.fetch_sub(1u, std::memory_order_acq_rel);
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
