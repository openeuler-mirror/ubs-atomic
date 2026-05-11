/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#ifndef UB_DIST_LOCK_H
#define UB_DIST_LOCK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// 定义日志级别常量
#ifndef LOG_LEVEL_DEBUG
#define LOG_LEVEL_DEBUG 0
#define LOG_LEVEL_INFO 1
#define LOG_LEVEL_WARN 2
#define LOG_LEVEL_ERROR 3
#define LOG_LEVEL_CRITICAL 4
#endif

/* Shared-memory size reserved for the distributed read-write lock. 640B */
#define UB_RW_LOCK_SIZE (640)
/* Timestamp type in milliseconds */
typedef uint64_t time_ms_t;

/* Shared-memory layout of the distributed read-write lock */
typedef struct ub_rw_lock ub_rw_lock_t;

typedef enum {
    UB_LOCK_S = 0,  /* shared (read) lock */
    UB_LOCK_SX = 1, /* shared-exclusive (upgrade intent) lock */
    UB_LOCK_X = 2,  /* exclusive (write) lock */
    UB_LOCK_I = 3,  /* Invalid lock type */
} ub_lock_mode_t;   /* Lock mode definition */

typedef enum {
    UB_LOCK_SUCCESS = 0,  /* operation succeeded */
    UB_LOCK_TIMEOUT = 1,  /* lock timeout */
    UB_LOCK_CONFLICT = 2, /* lock conflict */
    UB_LOCK_ERROR = 3,    /* operation error */
} ub_lock_result_t;

typedef struct {
    int32_t tid;     /* thread identifier (or logical thread id) */
    uint8_t node_id; /* logical node identifier */
} ub_location_t;     /* Identifies a lock owner or waiter */

typedef struct {
    time_ms_t timeout_ts;     /* absolute timeout timestamp, default value: 10000*/
    bool allow_delay_release; /* allow delayed unlock, default value: false */
    bool recursive;           /* allow recursive locking, default value: false */
} ub_lock_policy_t;           /* Policy controlling one lock acquisition attempt */

typedef struct {
    time_ms_t lease_time;        /* lease duration for distributed lock, default value:60000 */
    time_ms_t heartbeat_timeout; /* heartbeat timeout threshold, default value:500 */
} ub_lock_config_t;              /* Configuration fixed at lock creation time */

typedef struct {
    uint8_t node_id;          /* queried node id */
    ub_lock_mode_t held_mode; /* recoverable holder mode on this node */
    int32_t holder_tid;       /* valid for X/SX, otherwise 0 */
    uint32_t recursive_count; /* valid for X/SX, otherwise 0 */
    ub_lock_mode_t reserve_mode; /* delayed-release mode on this node, otherwise UB_LOCK_I */
} ub_lock_query_result_t;     /* Minimal local snapshot used for rebuild */

typedef struct {
    const ub_lock_query_result_t *query_results; /* one result per queried node */
    uint32_t query_result_count;                 /* number of elements in query_results */
} ub_lock_rebuild_info_t;                      /* Aggregated cluster result for rebuild */

/* ============================================================
 * C ABI APIs
 * ============================================================ */

/*
 * @brief Initialize a distributed read-write lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] config     : lock configuration
 * @param[in] location   : caller location (node/thread)
 */
void ub_rw_lock_create(ub_rw_lock_t *lock, const ub_lock_config_t *config, const ub_location_t *location);

/*
 * @brief Release per-node resources associated with the lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] location   : caller location (node/thread)
 */
void ub_rw_lock_free(ub_rw_lock_t *lock, const ub_location_t *location);

/*
 * @brief Acquire a shared (read) lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] policy     : lock acquisition policy
 * @param[in] location   : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_TIME/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_s_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);

/*
 * @brief Acquire an exclusive (write) lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] policy     : lock acquisition policy
 * @param[in] location   : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_TIME/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_x_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);

/*
 * @brief Acquire a shared-exclusive (SX) lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] policy     : lock acquisition policy
 * @param[in] location   : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_TIME/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_sx_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);

/*
 * @brief Release a shared (read) lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] policy     : lock release policy
 * @param[in] location   : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_TIME/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_s_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);

/*
 * @brief Release an exclusive (write) lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] policy     : lock release policy
 * @param[in] location   : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_TIME/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_x_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);

/*
 * @brief Release a shared-exclusive (SX) lock.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] policy     : lock release policy
 * @param[in] location   : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_TIME/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_sx_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy,
                                      const ub_location_t *location);

/*
 * @brief recover a lock held by a failed process.
 * @param[in] lock         : pointer to shared-memory lock object
 * @param[in] process_id   : process id
 * @param[in] location     : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_recover(ub_rw_lock_t *lock, const uint32_t process_id, const ub_location_t *location);

/*
 * @brief query the minimal local holder state needed for rebuild.
 * @param[in] lock       : pointer to shared-memory lock object
 * @param[in] location   : caller location (node/thread)
 * @param[out] result    : normalized local holder snapshot
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_query_holder(ub_rw_lock_t *lock, const ub_location_t *location,
                                         ub_lock_query_result_t *result);

/*
 * @brief rebuild new shared-memory lock state from aggregated node query results.
 * @param[in] old_lock       : pointer to old shared-memory lock object used by local registry
 * @param[in] new_lock       : pointer to new shared-memory lock object
 * @param[in] rebuild_info   : aggregated query results from the cluster
 * @param[in] location       : caller location (node/thread)
 * @return lock result status(UB_LOCK_SUCCESS/UB_LOCK_ERROR)
 */
ub_lock_result_t ub_rw_lock_rebuild(ub_rw_lock_t *old_lock, ub_rw_lock_t *new_lock,
                                    const ub_lock_rebuild_info_t *rebuild_info, const ub_location_t *location);


#ifndef UB_ATOMIC_LOG_FUNC_TYPEDEF
#define UB_ATOMIC_LOG_FUNC_TYPEDEF
/*
* @brief log func
* @param level [in] : LogLevel
* @param file [in] : source file name
* @param func [in] : function name
* @param line [in] : line number
* @param message [in] : formatted message
*/
typedef int (*ub_atomic_log_func)(int level, const char *file, const char *func, uint32_t line, const char *message);

/*
* @brief register log function
* @param func [in] : user-defined log function pointer
*/
void ub_atomic_register_log_func(ub_atomic_log_func func);

/*
* @brief set log level threshold
* @param level [in] : log level threshold (LOG_LEVEL_DEBUG ~ LOG_LEVEL_CRITICAL)
* @return 0 on success, -1 on invalid level
*/
int ub_atomic_set_log_level(int level);
#endif
#ifdef __cplusplus
} /* extern "C" */

#endif

#endif /* UB_DIST_LOCK_H */
