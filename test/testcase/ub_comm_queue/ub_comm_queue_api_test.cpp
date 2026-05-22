#include <cerrno>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#define private public
#include "UBShmTransport.h"
#undef private
#include "ub_dist_comm_queue.h"

namespace ub_comm_queue {
namespace ut {
namespace {

void DummyCallback(const message_t *, void *) {}
std::atomic<uint32_t> g_async_callback_count{0};
std::atomic<uint32_t> g_log_callback_count{0};
char g_last_log_message[128] = {};

extern "C" UBShmTransport *g_transport;

void AsyncCallback(const message_t *, void *)
{
    g_async_callback_count.fetch_add(1, std::memory_order_acq_rel);
}

int CaptureLogger(int, const char *, const char *, uint32_t, const char *message)
{
    std::snprintf(g_last_log_message, sizeof(g_last_log_message), "%s", message);
    g_log_callback_count.fetch_add(1, std::memory_order_acq_rel);
    return 0;
}

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

class TwoNodeApiEnv {
public:
    explicit TwoNodeApiEnv(std::vector<ub_ring_desc_t> descs = {{8, 128, 1}})
        : descs_(std::move(descs)),
          init_mem_(AllocAligned(sizeof(Billboard))),
          ring_mem0_(AllocAligned(RingRegionSize(descs_))),
          ring_mem1_(AllocAligned(RingRegionSize(descs_)))
    {
        init_area_.size = sizeof(Billboard);
        init_area_.ptr = init_mem_.get();

        entries_[0].region.size = RingRegionSize(descs_);
        entries_[0].region.ptr = ring_mem0_.get();
        entries_[0].node_id = 0;
        entries_[1].region.size = RingRegionSize(descs_);
        entries_[1].region.ptr = ring_mem1_.get();
        entries_[1].node_id = 1;
        ring_map_.entries = entries_;
        ring_map_.count = 2;

        for (uint8_t node = 0; node < 2; ++node) {
            confs_[node].cpu_id = -1;
            confs_[node].max_nodes = 2;
            confs_[node].current_node_id = node;
            confs_[node].num_rings = static_cast<uint8_t>(descs_.size());
            confs_[node].ring_descs = descs_.data();
        }
    }

    ub_shm_area_t *InitArea()
    {
        return &init_area_;
    }

    ub_ring_region_map_t *RingMap()
    {
        return &ring_map_;
    }

    ub_comm_conf_t *Conf(uint8_t node)
    {
        return &confs_[node];
    }

private:
    std::vector<ub_ring_desc_t> descs_;
    AlignedBuffer init_mem_;
    AlignedBuffer ring_mem0_;
    AlignedBuffer ring_mem1_;
    ub_shm_area_t init_area_{};
    ub_ring_region_info_t entries_[2]{};
    ub_ring_region_map_t ring_map_{};
    ub_comm_conf_t confs_[2]{};
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

message_t MakeMessage(uint8_t src, uint8_t dst, uint8_t msgType)
{
    message_t msg = MakeMessage(msgType);
    msg.header.src_node_id = src;
    msg.header.dest_node_id = dst;
    return msg;
}

bool WaitUntil(std::chrono::milliseconds timeout, const std::function<bool()> &predicate)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

uint64_t TestSteadyUs()
{
    struct timespec ts {};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
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
    ub_comm_queue_heartbeat_config_t hb{};
    EXPECT_EQ(ub_comm_queue_config_heartbeat(nullptr, nullptr, &hb), -EINVAL);
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, nullptr, &hb), -EINVAL);
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

TEST(UbCommQueueApiTest, TransportRejectsInvalidRuntimeArguments)
{
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    auto *transport = static_cast<UBShmTransport *>(handle);

    ub_comm_queue_status_t status{};
    EXPECT_EQ(transport->register_func(7, static_cast<ub_func_type_t>(99), DummyCallback, nullptr), -EINVAL);
    EXPECT_EQ(transport->register_func_for_lock(7, UB_FUNC_SYNC, DummyCallback, nullptr), -EINVAL);
    EXPECT_EQ(transport->register_func_for_lock(MSG_TYPE_DIST_LOCK, static_cast<ub_func_type_t>(99), DummyCallback,
                                                nullptr),
              -EINVAL);
    EXPECT_EQ(transport->get_status(0, 1, nullptr), -EINVAL);
    EXPECT_EQ(transport->get_status(0, MAX_PRIORITY_LEVELS, &status), -EINVAL);
    EXPECT_EQ(transport->set_congestion_threshold(LOCK_RING_PRIORITY, 80), -EINVAL);
    EXPECT_EQ(transport->set_congestion_threshold(MAX_PRIORITY_LEVELS, 80), -EINVAL);

    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, AsyncCallbackDispatchesThroughThreadPool)
{
    g_async_callback_count.store(0, std::memory_order_release);
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    ASSERT_EQ(ub_comm_queue_register_process_func(&handle, 8, UB_FUNC_ASYNC, AsyncCallback, nullptr), UB_COMM_OK);

    message_t msg = MakeMessage(8);
    msg.header.body_length = 0;
    ASSERT_EQ(ub_comm_queue_send(&handle, &msg), UB_COMM_OK);
    EXPECT_TRUE(WaitUntil(std::chrono::milliseconds(300), []() {
        return g_async_callback_count.load(std::memory_order_acquire) == 1;
    }));

    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, UBCQ_IF_RCV_EER_001_QueryDefaultHeartbeatConfig)
{
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    ub_comm_queue_heartbeat_config_t effective{};
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, nullptr, &effective), UB_COMM_OK);
    EXPECT_EQ(effective.heartbeat_interval_ms, 100u);
    EXPECT_EQ(effective.check_interval_ms, 100u);
    EXPECT_EQ(effective.timeout_ms, 1000u);

    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, UBCQ_IF_RCV_EER_002_SetHeartbeatConfigWithoutQuery)
{
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    ub_comm_queue_heartbeat_config_t request{10, 5, 10};
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, &request, nullptr), UB_COMM_OK);

    ub_comm_queue_heartbeat_config_t effective{};
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, nullptr, &effective), UB_COMM_OK);
    EXPECT_EQ(effective.heartbeat_interval_ms, 10u);
    EXPECT_EQ(effective.check_interval_ms, 5u);
    EXPECT_EQ(effective.timeout_ms, 10u);

    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, UBCQ_IF_RCV_EER_003_SetHeartbeatConfigAndReturnEffective)
{
    ApiEnv env(0);
    ub_shm_comm_t handle = nullptr;
    ASSERT_EQ(ub_comm_queue_init(&handle, env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    ub_comm_queue_heartbeat_config_t request{20, 3, 8};
    ub_comm_queue_heartbeat_config_t effective{};
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, &request, &effective), UB_COMM_OK);
    EXPECT_EQ(effective.heartbeat_interval_ms, request.heartbeat_interval_ms);
    EXPECT_EQ(effective.check_interval_ms, request.check_interval_ms);
    EXPECT_EQ(effective.timeout_ms, request.timeout_ms);

    ub_comm_queue_heartbeat_config_t invalid{20, 3, 5};
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, &invalid, nullptr), -EINVAL);
    EXPECT_EQ(ub_comm_queue_config_heartbeat(&handle, nullptr, nullptr), -EINVAL);

    EXPECT_EQ(ub_comm_queue_deinit(&handle), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, HeartbeatMonitorUsesPeerDeclaredIntervalBeforeTimingOut)
{
    TwoNodeApiEnv env;
    ub_shm_comm_t node0 = nullptr;
    ub_shm_comm_t node1 = nullptr;
    int ret0 = -1;
    int ret1 = -1;
    std::thread init0([&]() { ret0 = ub_comm_queue_init(&node0, env.InitArea(), env.RingMap(), env.Conf(0)); });
    std::thread init1([&]() { ret1 = ub_comm_queue_init(&node1, env.InitArea(), env.RingMap(), env.Conf(1)); });
    init0.join();
    init1.join();
    ASSERT_EQ(ret0, UB_COMM_OK);
    ASSERT_EQ(ret1, UB_COMM_OK);

    ub_comm_queue_heartbeat_config_t slowConsumer{20, 1, 50};
    ub_comm_queue_heartbeat_config_t impatientProducer{1, 1, 2};
    ASSERT_EQ(ub_comm_queue_config_heartbeat(&node0, &slowConsumer, nullptr), UB_COMM_OK);
    ASSERT_EQ(ub_comm_queue_config_heartbeat(&node1, &impatientProducer, nullptr), UB_COMM_OK);

    auto *transport1 = static_cast<UBShmTransport *>(node1);
    ASSERT_TRUE(WaitUntil(std::chrono::milliseconds(200), [&]() {
        uint64_t lastSeen = transport1->peer_heartbeat_seen_us_[0].load(std::memory_order_acquire);
        uint64_t now = TestSteadyUs();
        return transport1->peer_heartbeat_seq_[0].load(std::memory_order_acquire) != 0 &&
               transport1->peer_alive_[0].load(std::memory_order_acquire) &&
               transport1->peer_heartbeat_timeout_us_[0].load(std::memory_order_acquire) >= 60 * 1000ULL &&
               lastSeen != 0 && now >= lastSeen && now - lastSeen < 10 * 1000ULL;
    }));

    auto *transport0 = static_cast<UBShmTransport *>(node0);
    transport0->reliability_stop_flag_.store(true, std::memory_order_release);
    EXPECT_TRUE(transport1->peer_alive_[0].load(std::memory_order_acquire));

    message_t msg = MakeMessage(1, 0, 9);
    msg.header.body_length = 0;

    EXPECT_TRUE(WaitUntil(std::chrono::milliseconds(200), [&]() {
        return ub_comm_queue_send(&node1, &msg) == UB_COMM_ERR_PEER_NOT_READY;
    }));

    EXPECT_EQ(ub_comm_queue_deinit(&node1), UB_COMM_OK);
    EXPECT_EQ(ub_comm_queue_deinit(&node0), UB_COMM_OK);
}

TEST(UbCommQueueApiTest, LogLevelValidation)
{
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_DEBUG), 0);
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_CRITICAL), 0);
    EXPECT_EQ(ub_atomic_set_log_level(LOG_LEVEL_CRITICAL + 1), -1);
}

TEST(UbCommQueueApiTest, LogRegistrationFormattingAndFiltering)
{
    g_log_callback_count.store(0, std::memory_order_release);
    std::memset(g_last_log_message, 0, sizeof(g_last_log_message));

    register_print_func(CaptureLogger);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG), 0);
    log_print(LOG_LEVEL_INFO, "file.cpp", "func", 10, "heartbeat %d", 7);
    EXPECT_EQ(g_log_callback_count.load(std::memory_order_acquire), 1u);
    EXPECT_STREQ(g_last_log_message, "heartbeat 7");

    log_print(LOG_LEVEL_DEBUG - 1, "file.cpp", "func", 10, "ignored");
    EXPECT_EQ(g_log_callback_count.load(std::memory_order_acquire), 1u);

    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_CRITICAL + 1), -1);
    register_print_func(nullptr);
    EXPECT_EQ(log_no_print(LOG_LEVEL_INFO, "file.cpp", "func", 10, "noop"), 0);
}

} // namespace ut
} // namespace ub_comm_queue
