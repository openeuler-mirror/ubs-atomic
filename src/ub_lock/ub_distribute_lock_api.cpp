/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#include "inner_distribute_lock.h"

using ublock::DistributedLock;

namespace {
static const ub_lock_policy_t kDefaultPolicy = {.timeout_ts = 10000, .allow_delay_release = false, .recursive = false};
static const ub_lock_config_t kDefaultConfig = {.lease_time = 60000, .heartbeat_timeout = 500};
} // namespace

extern "C" void ub_rw_lock_create(ub_rw_lock_t *lock, const ub_lock_config_t *config, const ub_location_t *location)
{
    if (!lock || !location) {
        return;
    }
    const ub_lock_config_t *c = config ? config : &kDefaultConfig;
    DistributedLock impl(lock);
    impl.lock_create(*c, *location);
}

extern "C" void ub_rw_lock_free(ub_rw_lock_t *lock, const ub_location_t *location)
{
    if (!lock || !location) {
        return;
    }
    DistributedLock impl(lock);
    impl.lock_free(*location);
}

extern "C" ub_lock_result_t ub_rw_lock_s_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                              const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    const ub_lock_policy_t &p = policy ? *policy : kDefaultPolicy;
    DistributedLock impl(lock);
    return impl.lock_s(p, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_x_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                              const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    const ub_lock_policy_t &p = policy ? *policy : kDefaultPolicy;
    DistributedLock impl(lock);
    return impl.lock_x(p, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_sx_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                               const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    const ub_lock_policy_t &p = policy ? *policy : kDefaultPolicy;
    DistributedLock impl(lock);
    return impl.lock_sx(p, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_s_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                                const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    const ub_lock_policy_t &p = policy ? *policy : kDefaultPolicy;
    DistributedLock impl(lock);
    return impl.unlock_s(p, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_x_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                                const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    const ub_lock_policy_t &p = policy ? *policy : kDefaultPolicy;
    DistributedLock impl(lock);
    return impl.unlock_x(p, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_sx_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                                 const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }
    const ub_lock_policy_t &p = policy ? *policy : kDefaultPolicy;
    DistributedLock impl(lock);
    return impl.unlock_sx(p, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_recover(ub_rw_lock_t *lock, const uint32_t process_id,
                                               const ub_location_t *location)
{
    if (!lock || !location) {
        return UB_LOCK_ERROR;
    }

    DistributedLock impl(lock);
    return impl.recover(process_id, *location);
}

extern "C" ub_lock_result_t ub_rw_lock_query_holder(ub_rw_lock_t *lock, const ub_location_t *location,
                                                    ub_lock_query_result_t *result)
{
    if (!lock || !location || !result) {
        return UB_LOCK_ERROR;
    }

    DistributedLock impl(lock);
    return impl.query_holder(*location, *result);
}

extern "C" ub_lock_result_t ub_rw_lock_rebuild(ub_rw_lock_t *old_lock, ub_rw_lock_t *new_lock,
                                               const ub_lock_rebuild_info_t *rebuild_info,
                                               const ub_location_t *location)
{
    if (!old_lock || !new_lock || !rebuild_info || !location) {
        return UB_LOCK_ERROR;
    }

    DistributedLock impl(new_lock);
    return impl.rebuild(old_lock, *rebuild_info, *location);
}
