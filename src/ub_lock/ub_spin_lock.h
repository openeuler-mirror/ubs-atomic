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
#include <chrono>
#include <cstdint>
#include "local_lock.h"
#include "ub_dist_lock.h"

struct alignas(UB_CACHELINE_SIZE) ub_spin_lock {
    std::atomic<int32_t> init_state;  /* 0: empty, 1: initializing, 2: ready */
    std::atomic<uint64_t> lock_owner; /* LOCK_INVALID_OWNER: unlocked, otherwise NodeID + TID */
};

namespace ublock {

class SpinLock {
public:
    explicit SpinLock(ub_spin_lock_t *shm) : lock_shm_(shm) {}

    void lock_init();
    ub_lock_result_t lock(time_ms_t timeout_ms, const ub_location_t &location);
    ub_lock_result_t unlock(const ub_location_t &location);

private:
    ub_spin_lock_t *lock_shm_;

    bool is_ready() const;
};

} // namespace ublock
