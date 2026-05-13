#include <cerrno>

#include "gtest/gtest.h"

#include "UBShmTransport.h"
#include "ub_dist_comm_queue.h"

namespace ub_comm_queue {
namespace ut {
namespace {

void DummyCallback(const message_t *, void *) {}

message_t MakeMessage(uint8_t msgType)
{
    message_t msg{};
    msg.header.msg_type = msgType;
    msg.header.src_node_id = 0;
    msg.header.dest_node_id = 0;
    msg.header.priority = 1;
    return msg;
}

} // namespace

TEST(UbCommQueueApiTest, NullArgumentsReturnInvalidArgument)
{
    ub_shm_comm_t handle = nullptr;
    ub_shm_area_t initRegion{};
    ub_ring_region_map_t ringRegions{};
    ub_comm_conf_t conf{};
    message_t msg = MakeMessage(1);
    ub_comm_queue_status_t status{};
    char buffer[64] = {};

    EXPECT_EQ(ub_comm_queue_init(nullptr, &initRegion, &ringRegions, &conf), -EINVAL);
    EXPECT_EQ(ub_comm_queue_init(&handle, nullptr, &ringRegions, &conf), -EINVAL);
    EXPECT_EQ(ub_comm_queue_init(&handle, &initRegion, nullptr, &conf), -EINVAL);
    EXPECT_EQ(ub_comm_queue_init(&handle, &initRegion, &ringRegions, nullptr), -EINVAL);

    EXPECT_EQ(ub_comm_queue_deinit(nullptr), -EINVAL);
    EXPECT_EQ(ub_comm_queue_deinit(&handle), -EINVAL);
    EXPECT_FALSE(ub_comm_queue_check_ready(nullptr, 0));
    EXPECT_FALSE(ub_comm_queue_check_ready(&handle, 0));
    EXPECT_EQ(ub_comm_queue_send(nullptr, &msg), -EINVAL);
    EXPECT_EQ(ub_comm_queue_send(&handle, &msg), -EINVAL);
    EXPECT_EQ(ub_comm_queue_get_status(nullptr, 0, 1, &status), -EINVAL);
    EXPECT_EQ(ub_comm_queue_get_status(&handle, 0, 1, &status), -EINVAL);
    EXPECT_EQ(ub_comm_queue_set_congestion_threshold(nullptr, 1, 80), -EINVAL);
    EXPECT_EQ(ub_comm_queue_set_congestion_threshold(&handle, 1, 80), -EINVAL);
    EXPECT_EQ(ub_comm_queue_recv(nullptr, buffer, sizeof(buffer)), -EINVAL);
    EXPECT_EQ(ub_comm_queue_recv(&handle, buffer, sizeof(buffer)), -EINVAL);
    EXPECT_EQ(ub_comm_queue_register_process_func(nullptr, 1, UB_FUNC_SYNC, DummyCallback, nullptr), -EINVAL);
    EXPECT_EQ(ub_comm_queue_register_process_func(&handle, 1, UB_FUNC_SYNC, DummyCallback, nullptr), -EINVAL);
}

TEST(UbCommQueueApiTest, ReservedSystemMessagesAreRejectedByPublicApi)
{
    int fakeTransport = 0;
    ub_shm_comm_t handle = &fakeTransport;

    message_t distLock = MakeMessage(MSG_TYPE_DIST_LOCK);
    message_t peerExit = MakeMessage(MSG_TYPE_SYS_PEER_EXIT);
    message_t flowUpdate = MakeMessage(MSG_TYPE_SYS_FLOW_CONFIG_UPDATE);

    EXPECT_EQ(ub_comm_queue_send(&handle, &distLock), -EOPNOTSUPP);
    EXPECT_EQ(ub_comm_queue_send(&handle, &peerExit), -EOPNOTSUPP);
    EXPECT_EQ(ub_comm_queue_send(&handle, &flowUpdate), -EOPNOTSUPP);

    EXPECT_EQ(ub_comm_queue_register_process_func(&handle, MSG_TYPE_DIST_LOCK, UB_FUNC_SYNC, DummyCallback, nullptr),
              -EOPNOTSUPP);
    EXPECT_EQ(ub_comm_queue_register_process_func(&handle, MSG_TYPE_SYS_PEER_EXIT, UB_FUNC_SYNC, DummyCallback,
                                                  nullptr),
              -EOPNOTSUPP);
}

TEST(UbCommQueueApiTest, LogLevelValidation)
{
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_DEBUG), 0);
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_CRITICAL), 0);
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_CRITICAL + 1), -1);
}

} // namespace ut
} // namespace ub_comm_queue
