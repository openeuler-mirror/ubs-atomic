#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "UBShmTransport.h"
#include "ub_dist_comm_queue.h"

namespace ub_comm_queue {
namespace ut {
namespace {

void DummyCallback(const message_t *, void *) {}

extern "C" UBShmTransport *g_transport;

struct AlignedFree {
    void operator()(void *ptr) const
    {
        std::free(ptr);
    }
};

using AlignedBuffer = std::unique_ptr<void, AlignedFree>;

AlignedBuffer AllocAligned(size_t size)
{
    void *raw = nullptr;
    if (posix_memalign(&raw, CACHELINE_SIZE, size) != 0) {
        return AlignedBuffer(nullptr);
    }
    std::memset(raw, 0, size);
    return AlignedBuffer(raw);
}

size_t RingRegionSize(const std::vector<ub_ring_desc_t> &descs)
{
    size_t total = MPSCRingBuffer::CalculateMemorySize(LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE) + CACHELINE_SIZE;
    for (const auto &desc : descs) {
        total += MPSCRingBuffer::CalculateMemorySize(desc.ring_capacity, desc.max_msg_size) + CACHELINE_SIZE;
    }
    return total;
}

class ApiEnv {
public:
    explicit ApiEnv(uint8_t nodeId, std::vector<ub_ring_desc_t> descs = {{4, 128, 1}})
        : descs_(std::move(descs)),
          init_mem_(AllocAligned(sizeof(Billboard))),
          ring_mem_(AllocAligned(RingRegionSize(descs_)))
    {
        init_area_.size = sizeof(Billboard);
        init_area_.ptr = init_mem_.get();

        ring_entry_.region.size = RingRegionSize(descs_);
        ring_entry_.region.ptr = ring_mem_.get();
        ring_entry_.node_id = nodeId;
        ring_map_.entries = &ring_entry_;
        ring_map_.count = 1;

        conf_.cpu_id = -1;
        conf_.max_nodes = 1;
        conf_.current_node_id = nodeId;
        conf_.num_rings = static_cast<uint8_t>(descs_.size());
        conf_.ring_descs = descs_.data();
    }

    ub_shm_area_t *InitArea()
    {
        return &init_area_;
    }

    ub_ring_region_map_t *RingMap()
    {
        return &ring_map_;
    }

    ub_comm_conf_t *Conf()
    {
        return &conf_;
    }

private:
    std::vector<ub_ring_desc_t> descs_;
    AlignedBuffer init_mem_;
    AlignedBuffer ring_mem_;
    ub_shm_area_t init_area_{};
    ub_ring_region_info_t ring_entry_{};
    ub_ring_region_map_t ring_map_{};
    ub_comm_conf_t conf_{};
};

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

TEST(UbCommQueueApiTest, InitDeinitAndWrapperMethodsUseTransport)
{
    g_transport = nullptr;
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;

    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    ASSERT_NE(handle, nullptr);
    EXPECT_NE(g_transport, nullptr);
    EXPECT_TRUE(static_cast<UBShmTransport *>(handle)->get_is_for_lock());
    EXPECT_TRUE(ub_comm_queue_check_ready(&handle, 0));
    EXPECT_FALSE(ub_comm_queue_check_ready(&handle, 1));

    ub_comm_queue_status_t status{};
    EXPECT_EQ(ub_comm_queue_get_status(&handle, 0, 1, &status), UB_COMM_OK);
    EXPECT_EQ(status.total, 4u);
    EXPECT_EQ(ub_comm_queue_set_congestion_threshold(&handle, 1, 50), UB_COMM_OK);

    char recvBuf[16] = {};
    EXPECT_EQ(ub_comm_queue_recv(&handle, recvBuf, sizeof(recvBuf)), UB_COMM_OK);
    EXPECT_EQ(ub_comm_queue_register_process_func(&handle, 7, UB_FUNC_SYNC, DummyCallback, nullptr), UB_COMM_OK);

    message_t msg = MakeMessage(7);
    msg.header.body_length = 0;
    EXPECT_EQ(ub_comm_queue_send(&handle, &msg), UB_COMM_OK);

    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
    EXPECT_EQ(handle, nullptr);
    EXPECT_EQ(g_transport, nullptr);
}

TEST(UbCommQueueApiTest, SecondInitIsMarkedNonLockAndBadConfigReturnsError)
{
    g_transport = nullptr;
    ApiEnv firstEnv(0);
    ub_shm_comm_t first = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&first, firstEnv.InitArea(), firstEnv.RingMap(), firstEnv.Conf()), UB_COMM_OK);
    ASSERT_NE(g_transport, nullptr);

    ApiEnv badEnv(1);
    badEnv.Conf()->current_node_id = 2;
    ub_shm_comm_t bad = nullptr;
    EXPECT_EQ(ub_comm_queue_init(&bad, badEnv.InitArea(), badEnv.RingMap(), badEnv.Conf()), -EINVAL);
    EXPECT_EQ(bad, nullptr);

    ApiEnv secondEnv(2);
    ub_shm_comm_t second = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&second, secondEnv.InitArea(), secondEnv.RingMap(), secondEnv.Conf()), UB_COMM_OK);
    ASSERT_NE(second, nullptr);
    EXPECT_FALSE(static_cast<UBShmTransport *>(second)->get_is_for_lock());

    EXPECT_EQ(ub_comm_queue_deinit(&second), UB_COMM_OK);
    EXPECT_NE(g_transport, nullptr);
    EXPECT_EQ(ub_comm_queue_deinit(&first), UB_COMM_OK);
    EXPECT_EQ(g_transport, nullptr);
}

TEST(UbCommQueueApiTest, RegisterRejectsNullCallbackWithValidHandle)
{
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    EXPECT_EQ(ub_comm_queue_register_process_func(&handle, 7, UB_FUNC_SYNC, nullptr, nullptr), -EINVAL);
    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, LogLevelValidation)
{
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_DEBUG), 0);
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_CRITICAL), 0);
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_CRITICAL + 1), -1);
}

} // namespace ut
} // namespace ub_comm_queue
