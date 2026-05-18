/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "ub_dist_comm_queue.h"
#include <cerrno>
#include <iostream>
#include <new>
#include "UBShmTransport.h"
#include "ub_atomic_log_print.h"

using ub_comm_queue::UBShmTransport;

#ifdef __cplusplus
extern "C" {
#endif

// Global transport instance for lock
UBShmTransport *g_transport = nullptr;

int ub_comm_queue_init(ub_shm_comm_t *handle, ub_shm_area_t *init_region, ub_ring_region_map_t *ring_regions,
                       ub_comm_conf_t *conf)
{
    if (conf == nullptr || init_region == nullptr || ring_regions == nullptr) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Invalid initialization parameters");
        return -EINVAL;
    }
    try {
        UBShmTransport *tspt = new UBShmTransport();
        int ret = tspt->init(init_region, ring_regions, conf);
        if (ret != 0) {
            ATOMIC_LOG(LOG_LEVEL_ERROR, "UBShmTransport initialization error: %d", ret);
            delete tspt;
            return ret;
        }

        *handle = static_cast<void *>(tspt);
        if (g_transport == nullptr) {
            g_transport = tspt;
            g_transport->set_is_for_lock(true);
            ATOMIC_LOG(LOG_LEVEL_DEBUG, "This instance is for lock.");
        } else {
            tspt->set_is_for_lock(false);
            ATOMIC_LOG(LOG_LEVEL_DEBUG, "This instance is not for lock.");
        }
        ATOMIC_LOG(LOG_LEVEL_INFO, "UBShmTransport initialized successfully.");

        return 0;
    } catch (const std::bad_alloc &) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Memory allocation error");
        return -ENOMEM;
    } catch (...) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Unknown error");
        return -EFAULT;
    }
}

int ub_comm_queue_deinit(ub_shm_comm_t *handle)
{
    if (handle == nullptr || *handle == nullptr) {
        return -EINVAL;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    if (tspt->get_is_for_lock()) {
        g_transport = nullptr;
        ATOMIC_LOG(LOG_LEVEL_WARN, "This instance is for lock.");
    }
    delete tspt;
    *handle = nullptr;
    return 0;
}

bool ub_comm_queue_check_ready(ub_shm_comm_t *handle, const uint8_t node_id)
{
    if (handle == nullptr || *handle == nullptr) {
        return false;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->query_inited(node_id);
}


int ub_comm_queue_send(ub_shm_comm_t *handle, const message_t *msg)
{
    if (handle == nullptr || *handle == nullptr || msg == nullptr) {
        return -EINVAL;
    }
    if (msg->header.msg_type == ub_comm_queue::MSG_TYPE_DIST_LOCK) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Dist lock message is not supported.");
        return -EOPNOTSUPP;
    }
    if (msg->header.msg_type == ub_comm_queue::MSG_TYPE_SYS_PEER_EXIT) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Peer exit message is not supported.");
        return -EOPNOTSUPP;
    }
    if (msg->header.msg_type == ub_comm_queue::MSG_TYPE_SYS_FLOW_CONFIG_UPDATE) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Flow config update message is not supported.");
        return -EOPNOTSUPP;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->send(msg);
}

int ub_comm_queue_get_status(ub_shm_comm_t *handle, uint8_t node_id, uint8_t priority,
                             ub_comm_queue_status_t *status)
{
    if (handle == nullptr || *handle == nullptr || status == nullptr) {
        return -EINVAL;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->get_status(node_id, priority, status);
}

int ub_comm_queue_set_congestion_threshold(ub_shm_comm_t *handle, uint8_t priority,
                                           uint32_t congestion_threshold_percent)
{
    if (handle == nullptr || *handle == nullptr) {
        return -EINVAL;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->set_congestion_threshold(priority, congestion_threshold_percent);
}

int ub_comm_queue_config_heartbeat(ub_shm_comm_t *handle,
                                   const ub_comm_queue_heartbeat_config_t *request,
                                   ub_comm_queue_heartbeat_config_t *effective)
{
    if (handle == nullptr || *handle == nullptr) {
        return -EINVAL;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->config_heartbeat(request, effective);
}

int ub_comm_queue_get_heartbeat_status(ub_shm_comm_t *handle, uint8_t node_id,
                                       ub_comm_queue_heartbeat_status_t *status)
{
    if (handle == nullptr || *handle == nullptr || status == nullptr) {
        return -EINVAL;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->get_heartbeat_status(node_id, status);
}

int ub_comm_queue_recv(ub_shm_comm_t *handle, void *buffer, uint32_t length)
{
    if (handle == nullptr || *handle == nullptr || buffer == nullptr) {
        return -EINVAL;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->recv(buffer, static_cast<size_t>(length));
}

int ub_comm_queue_register_process_func(ub_shm_comm_t *handle, uint8_t msg_type, ub_func_type_t func_type,
                                        ub_callback_t func, void *ctx)
{
    if (handle == nullptr || *handle == nullptr || func == nullptr) {
        return -EINVAL;
    }
    if (msg_type == ub_comm_queue::MSG_TYPE_DIST_LOCK) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Register dist lock message is not supported.");
        return -EOPNOTSUPP;
    }
    if (msg_type == ub_comm_queue::MSG_TYPE_SYS_PEER_EXIT) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "Register peer exit message is not supported.");
        return -EOPNOTSUPP;
    }

    UBShmTransport *tspt = static_cast<UBShmTransport *>(*handle);
    return tspt->register_func(msg_type, func_type, func, ctx);
}

void ub_atomic_register_log_func(ub_atomic_log_func func)
{
    register_print_func(func);
}

int ub_atomic_set_log_level(int level)
{
    return set_log_level_threshold(level);
}
#ifdef __cplusplus
}
#endif
