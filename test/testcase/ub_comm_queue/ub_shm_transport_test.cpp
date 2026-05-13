#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <sys/sysinfo.h>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#define private public
#include "UBShmTransport.h"
#undef private

namespace ub_comm_queue {
namespace ut {
namespace {

struct FlowConfigUpdateMessage {
    uint64_t version;
    uint32_t threshold;
    uint8_t priority;
    uint8_t reserved[3];
};

} // namespace
} // namespace ut

void on_flow_config_update(const message_t *msg, void *ctx);
void on_peer_exit(const message_t *msg, void *ctx);

namespace ut {
namespace {

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

class TransportEnv {
public:
    explicit TransportEnv(std::vector<ub_ring_desc_t> descs = {{4, 128, 1}}, uint8_t currentNode = 0)
        : descs_(std::move(descs)),
          init_mem_(AllocAligned(sizeof(Billboard))),
          ring_mem_(AllocAligned(RingRegionSize(descs_)))
    {
        EXPECT_NE(init_mem_, nullptr);
        EXPECT_NE(ring_mem_, nullptr);

        init_area_.size = sizeof(Billboard);
        init_area_.ptr = init_mem_.get();

        ring_entry_.region.size = RingRegionSize(descs_);
        ring_entry_.region.ptr = ring_mem_.get();
        ring_entry_.node_id = currentNode;

        ring_map_.entries = &ring_entry_;
        ring_map_.count = 1;

        conf_.cpu_id = -1;
        conf_.max_nodes = 1;
        conf_.current_node_id = currentNode;
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

    void ShrinkRingRegion(size_t size)
    {
        ring_entry_.region.size = size;
    }

    void SetRingEntryPtr(void *ptr)
    {
        ring_entry_.region.ptr = ptr;
    }

    void SetRingMapCount(uint8_t count)
    {
        ring_map_.count = count;
    }

    ub_ring_region_info_t *RingEntry()
    {
        return &ring_entry_;
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

class TwoNodeTransportEnv {
public:
    explicit TwoNodeTransportEnv(std::vector<ub_ring_desc_t> descs = {{4, 128, 1}})
        : descs_(std::move(descs)),
          init_mem_(AllocAligned(sizeof(Billboard))),
          node0_mem_(AllocAligned(RingRegionSize(descs_))),
          node1_mem_(AllocAligned(RingRegionSize(descs_)))
    {
        EXPECT_NE(init_mem_, nullptr);
        EXPECT_NE(node0_mem_, nullptr);
        EXPECT_NE(node1_mem_, nullptr);

        init_area_.size = sizeof(Billboard);
        init_area_.ptr = init_mem_.get();

        entries_[0].region.size = RingRegionSize(descs_);
        entries_[0].region.ptr = node0_mem_.get();
        entries_[0].node_id = 0;

        entries_[1].region.size = RingRegionSize(descs_);
        entries_[1].region.ptr = node1_mem_.get();
        entries_[1].node_id = 1;

        ring_map_.entries = entries_;
        ring_map_.count = 2;

        conf_.cpu_id = -1;
        conf_.max_nodes = 2;
        conf_.current_node_id = 0;
        conf_.num_rings = static_cast<uint8_t>(descs_.size());
        conf_.ring_descs = descs_.data();

        BuildRemoteNode();
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

    MPSCRingBuffer *RemoteRing(uint8_t priority)
    {
        return remote_rings_[priority];
    }

    Billboard *Board()
    {
        return reinterpret_cast<Billboard *>(init_mem_.get());
    }

private:
    void BuildRemoteNode()
    {
        char *base = static_cast<char *>(node1_mem_.get());
        char *current = base;
        auto alignAddr = [](char *&addr) {
            uintptr_t raw = reinterpret_cast<uintptr_t>(addr);
            addr += (CACHELINE_SIZE - (raw % CACHELINE_SIZE)) % CACHELINE_SIZE;
        };

        std::vector<uint64_t> offsets(MAX_PRIORITY_LEVELS, UINT64_MAX);
        alignAddr(current);
        offsets[LOCK_RING_PRIORITY] = static_cast<uint64_t>(current - base);
        remote_rings_[LOCK_RING_PRIORITY] = new (current)
            MPSCRingBuffer(reinterpret_cast<uint8_t *>(current), LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE);
        current += MPSCRingBuffer::CalculateMemorySize(LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE);

        for (const auto &desc : descs_) {
            alignAddr(current);
            offsets[desc.priority] = static_cast<uint64_t>(current - base);
            remote_rings_[desc.priority] = new (current)
                MPSCRingBuffer(reinterpret_cast<uint8_t *>(current), desc.ring_capacity, desc.max_msg_size);
            current += MPSCRingBuffer::CalculateMemorySize(desc.ring_capacity, desc.max_msg_size);
        }

        auto *board = Board();
        NodeBoardInfo &node1 = board->nodes[1];
        for (int p = 0; p < MAX_PRIORITY_LEVELS; ++p) {
            node1.ring_offsets[p].store(offsets[p], std::memory_order_release);
        }
        node1.initialized.store(true, std::memory_order_release);
    }

    std::vector<ub_ring_desc_t> descs_;
    AlignedBuffer init_mem_;
    AlignedBuffer node0_mem_;
    AlignedBuffer node1_mem_;
    ub_shm_area_t init_area_{};
    ub_ring_region_info_t entries_[2]{};
    ub_ring_region_map_t ring_map_{};
    ub_comm_conf_t conf_{};
    std::array<MPSCRingBuffer *, MAX_PRIORITY_LEVELS> remote_rings_{};
};

struct CallbackCtx {
    std::mutex mu;
    std::condition_variable cv;
    int count = 0;
    message_header_t last_header{};
    std::string last_body;
};

void SyncCallback(const message_t *msg, void *ctx)
{
    auto *callbackCtx = static_cast<CallbackCtx *>(ctx);
    std::lock_guard<std::mutex> lk(callbackCtx->mu);
    callbackCtx->count++;
    callbackCtx->last_header = msg->header;
    callbackCtx->last_body.assign(msg->body, msg->body + msg->header.body_length);
    callbackCtx->cv.notify_all();
}

bool WaitForCallback(CallbackCtx &ctx, int expectedCount)
{
    std::unique_lock<std::mutex> lk(ctx.mu);
    return ctx.cv.wait_for(lk, std::chrono::milliseconds(500), [&] { return ctx.count >= expectedCount; });
}

message_t MakeMessage(uint8_t msgType, uint8_t priority, const char *body)
{
    message_t msg{};
    msg.header.src_thread_id = 77;
    msg.header.body_length = static_cast<uint32_t>(std::strlen(body));
    msg.header.dest_node_id = 0;
    msg.header.src_node_id = 0;
    msg.header.msg_type = msgType;
    msg.header.priority = priority;
    msg.body = const_cast<char *>(body);
    return msg;
}

} // namespace

TEST(UBShmTransportTest, InitRejectsInvalidTopLevelArguments)
{
    TransportEnv env;
    UBShmTransport transport;

    EXPECT_EQ(transport.init(nullptr, env.RingMap(), env.Conf()), -EINVAL);
    EXPECT_EQ(transport.init(env.InitArea(), nullptr, env.Conf()), -EINVAL);
    EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), nullptr), -EINVAL);
}

TEST(UBShmTransportTest, InitRejectsInvalidConfigAndMemory)
{
    {
        TransportEnv env;
        env.Conf()->max_nodes = 0;
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.SetRingMapCount(0);
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.SetRingEntryPtr(nullptr);
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env({{3, 128, 1}});
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env({{4, 128, 0}});
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env({{4, static_cast<uint32_t>(sizeof(message_t) - 1), 1}});
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.ShrinkRingRegion(64);
        UBShmTransport transport;
        EXPECT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), -ENOMEM);
    }
}

TEST(UBShmTransportTest, SetupValidationCoversRemainingArgumentBranches)
{
    {
        TransportEnv env;
        env.InitArea()->ptr = nullptr;
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.RingMap()->entries = nullptr;
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.InitArea()->size = sizeof(Billboard) - 1;
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.SetRingMapCount(MAX_PRIORITY_LEVELS + 1);
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.Conf()->num_rings = MAX_PRIORITY_LEVELS + 1;
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.Conf()->ring_descs = nullptr;
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    {
        TransportEnv env;
        env.Conf()->current_node_id = 7;
        UBShmTransport transport;
        ASSERT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
        EXPECT_EQ(transport.build_node_mapping(env.RingMap()), -EINVAL);
    }
    {
        TransportEnv env;
        env.Conf()->cpu_id = static_cast<int32_t>(get_nprocs());
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), -EINVAL);
    }
    if (get_nprocs() > 0) {
        TransportEnv env;
        env.Conf()->cpu_id = 0;
        UBShmTransport transport;
        EXPECT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
        EXPECT_EQ(transport.cpu_id_, 0);
    }
}

TEST(UBShmTransportTest, InitCreatesLocalRingsAndPublishesBillboard)
{
    TransportEnv env;
    UBShmTransport transport;

    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    EXPECT_TRUE(transport.query_inited(0));
    EXPECT_FALSE(transport.query_inited(1));
    EXPECT_NE(transport.local_rings_[LOCK_RING_PRIORITY], nullptr);
    EXPECT_NE(transport.local_rings_[1], nullptr);
    EXPECT_EQ(transport.remote_lookup_table_.size(), 1u);
    EXPECT_EQ(transport.remote_lookup_table_[0][LOCK_RING_PRIORITY], transport.local_rings_[LOCK_RING_PRIORITY]);
    EXPECT_EQ(transport.remote_lookup_table_[0][1], transport.local_rings_[1]);

    auto *board = reinterpret_cast<Billboard *>(env.InitArea()->ptr);
    EXPECT_TRUE(board->nodes[0].initialized.load(std::memory_order_acquire));
    EXPECT_NE(board->nodes[0].ring_offsets[LOCK_RING_PRIORITY].load(std::memory_order_acquire), UINT64_MAX);
    EXPECT_NE(board->nodes[0].ring_offsets[1].load(std::memory_order_acquire), UINT64_MAX);
}

TEST(UBShmTransportTest, LoopbackSendDispatchesSyncCallback)
{
    TransportEnv env;
    UBShmTransport transport;
    CallbackCtx ctx;

    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    ASSERT_EQ(transport.register_func(7, UB_FUNC_SYNC, SyncCallback, &ctx), UB_COMM_OK);

    const char body[] = "transport-body";
    message_t msg = MakeMessage(7, 1, body);
    EXPECT_EQ(transport.send(&msg), UB_COMM_OK);

    ASSERT_TRUE(WaitForCallback(ctx, 1));
    EXPECT_EQ(ctx.last_header.msg_type, 7);
    EXPECT_EQ(ctx.last_header.priority, 1);
    EXPECT_EQ(ctx.last_body, body);
}

TEST(UBShmTransportTest, LoopbackSendDispatchesAsyncCallback)
{
    TransportEnv env;
    UBShmTransport transport;
    CallbackCtx ctx;

    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    ASSERT_EQ(transport.register_func(8, UB_FUNC_ASYNC, SyncCallback, &ctx), UB_COMM_OK);

    const char body[] = "async-body";
    message_t msg = MakeMessage(8, 1, body);
    EXPECT_EQ(transport.send(&msg), UB_COMM_OK);

    ASSERT_TRUE(WaitForCallback(ctx, 1));
    EXPECT_EQ(ctx.last_header.msg_type, 8);
    EXPECT_EQ(ctx.last_body, body);
}

TEST(UBShmTransportTest, SendRejectsInvalidMessages)
{
    TransportEnv env;
    UBShmTransport transport;
    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    EXPECT_EQ(transport.send(nullptr), -EINVAL);

    message_t msg = MakeMessage(7, 1, "x");
    msg.header.src_node_id = 1;
    EXPECT_EQ(transport.send(&msg), -EPERM);

    msg = MakeMessage(7, 0, "x");
    EXPECT_EQ(transport.send(&msg), -EINVAL);

    msg = MakeMessage(7, MAX_PRIORITY_LEVELS, "x");
    EXPECT_EQ(transport.send(&msg), -EINVAL);

    msg = MakeMessage(7, 2, "x");
    EXPECT_EQ(transport.send(&msg), UB_COMM_ERR_RING_NOT_FOUND);

    msg = MakeMessage(7, 1, "x");
    msg.header.dest_node_id = 3;
    EXPECT_EQ(transport.send(&msg), UB_COMM_ERR_PEER_NODE_NOT_FOUND);
}

TEST(UBShmTransportTest, RegisterAndDispatchValidateArguments)
{
    TransportEnv env;
    UBShmTransport transport;
    CallbackCtx ctx;
    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    EXPECT_EQ(transport.register_func(1, static_cast<ub_func_type_t>(99), SyncCallback, &ctx), -EINVAL);
    EXPECT_EQ(transport.register_func_for_lock(1, UB_FUNC_SYNC, SyncCallback, &ctx), -EINVAL);
    EXPECT_EQ(transport.register_func_for_lock(MSG_TYPE_DIST_LOCK, static_cast<ub_func_type_t>(99), SyncCallback, &ctx),
              -EINVAL);
    EXPECT_EQ(transport.register_func_for_lock(MSG_TYPE_DIST_LOCK, UB_FUNC_SYNC, SyncCallback, &ctx), UB_COMM_OK);

    char tooSmall[sizeof(message_header_t) - 1] = {};
    EXPECT_EQ(transport.dispatch_internal(tooSmall, sizeof(tooSmall)), -EMSGSIZE);

    message_header_t hdr{};
    hdr.msg_type = 9;
    hdr.body_length = 0;
    EXPECT_EQ(transport.dispatch_internal(&hdr, sizeof(hdr)), -ENOENT);

    hdr.msg_type = 1;
    hdr.body_length = 0;
    ASSERT_EQ(transport.register_func(1, UB_FUNC_ASYNC, SyncCallback, &ctx), UB_COMM_OK);
    delete transport.worker_pool_;
    transport.worker_pool_ = nullptr;
    EXPECT_EQ(transport.dispatch_internal(&hdr, sizeof(hdr)), -EPIPE);
}

TEST(UBShmTransportTest, StatusAndThresholdApisValidateAndUpdateLocalRing)
{
    TransportEnv env;
    UBShmTransport transport;
    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    ub_comm_queue_status_t status{};
    EXPECT_EQ(transport.get_status(0, MAX_PRIORITY_LEVELS, &status), -EINVAL);
    EXPECT_EQ(transport.get_status(0, 1, nullptr), -EINVAL);
    EXPECT_EQ(transport.get_status(0, 2, &status), UB_COMM_ERR_RING_NOT_FOUND);

    EXPECT_EQ(transport.get_status(0, 1, &status), UB_COMM_OK);
    EXPECT_EQ(status.total, 4u);
    EXPECT_EQ(status.state, UB_COMM_QUEUE_IDLE);

    EXPECT_EQ(transport.set_congestion_threshold(LOCK_RING_PRIORITY, 50), -EINVAL);
    EXPECT_EQ(transport.set_congestion_threshold(MAX_PRIORITY_LEVELS, 50), -EINVAL);
    EXPECT_EQ(transport.set_congestion_threshold(2, 50), UB_COMM_ERR_RING_NOT_FOUND);
    EXPECT_EQ(transport.set_congestion_threshold(1, 101), -EINVAL);
    EXPECT_EQ(transport.set_congestion_threshold(1, 25), UB_COMM_OK);

    EXPECT_EQ(transport.get_status(0, 1, &status), UB_COMM_OK);
    EXPECT_EQ(status.congestion_threshold, 1u);
}

TEST(UBShmTransportTest, DeinitClearsBillboardAndStopsDispatcher)
{
    TransportEnv env;
    {
        UBShmTransport transport;
        ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    }

    auto *board = reinterpret_cast<Billboard *>(env.InitArea()->ptr);
    EXPECT_FALSE(board->nodes[0].initialized.load(std::memory_order_acquire));
    EXPECT_EQ(board->nodes[0].ring_offsets[LOCK_RING_PRIORITY].load(std::memory_order_acquire), UINT64_MAX);
    EXPECT_EQ(board->nodes[0].ring_offsets[1].load(std::memory_order_acquire), UINT64_MAX);
}

TEST(UBShmTransportTest, TwoNodeInitPreloadsRemoteAndRemoteSendWorks)
{
    TwoNodeTransportEnv env;
    UBShmTransport transport;

    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    ASSERT_TRUE(transport.query_inited(1));
    EXPECT_EQ(transport.remote_lookup_table_.size(), 2u);
    EXPECT_EQ(transport.remote_lookup_table_[1][LOCK_RING_PRIORITY], env.RemoteRing(LOCK_RING_PRIORITY));
    EXPECT_EQ(transport.remote_lookup_table_[1][1], env.RemoteRing(1));
    EXPECT_TRUE(transport.ring_caches_[1][1].initialized.load(std::memory_order_acquire));

    const char body[] = "remote-body";
    message_t msg = MakeMessage(9, 1, body);
    msg.header.dest_node_id = 1;
    EXPECT_EQ(transport.send(&msg), UB_COMM_OK);

    std::vector<char> out(128);
    ASSERT_EQ(env.RemoteRing(1)->dequeue(out.data(), out.size()), sizeof(message_header_t) + sizeof(body) - 1);
    auto *hdr = reinterpret_cast<message_header_t *>(out.data());
    EXPECT_EQ(hdr->dest_node_id, 1);
    EXPECT_EQ(hdr->msg_type, 9);
    EXPECT_EQ(std::memcmp(out.data() + sizeof(message_header_t), body, sizeof(body) - 1), 0);

    ub_comm_queue_status_t status{};
    EXPECT_EQ(transport.get_status(1, 1, &status), UB_COMM_OK);
    EXPECT_EQ(status.total, 4u);
}

TEST(UBShmTransportTest, RemoteCacheLazyLoadAndFailureBranches)
{
    TwoNodeTransportEnv env;
    UBShmTransport transport;
    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    transport.remote_lookup_table_[1][1] = nullptr;
    transport.ring_caches_[1][1].initialized.store(false, std::memory_order_release);

    MPSCRingBuffer *ring = nullptr;
    EXPECT_EQ(transport.get_remote_ring(1, MAX_PRIORITY_LEVELS, &ring), -EINVAL);
    EXPECT_EQ(transport.get_remote_ring(1, 1, nullptr), -EINVAL);
    EXPECT_EQ(transport.get_remote_ring(1, 1, &ring), UB_COMM_OK);
    EXPECT_EQ(ring, env.RemoteRing(1));

    transport.remote_lookup_table_[1][1] = nullptr;
    env.Board()->nodes[1].initialized.store(false, std::memory_order_release);
    EXPECT_EQ(transport.get_remote_ring(1, 1, &ring), UB_COMM_ERR_PEER_NOT_READY);

    env.Board()->nodes[1].initialized.store(true, std::memory_order_release);
    env.Board()->nodes[1].ring_offsets[1].store(UINT64_MAX, std::memory_order_release);
    EXPECT_EQ(transport.get_remote_ring(1, 1, &ring), UB_COMM_ERR_RING_NOT_FOUND);
    EXPECT_EQ(transport.try_populate_cache(1, MAX_PRIORITY_LEVELS), -EINVAL);
}

TEST(UBShmTransportTest, SystemCallbacksUpdateAndRemoveRemoteCache)
{
    TwoNodeTransportEnv env;
    UBShmTransport transport;
    ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);

    transport.update_cached_congestion_threshold(1, LOCK_RING_PRIORITY, 2, 10);
    transport.update_cached_congestion_threshold(77, 1, 2, 10);

    transport.ring_caches_[1][1].cached_threshold_version.store(5, std::memory_order_relaxed);
    transport.update_cached_congestion_threshold(1, 1, 3, 4);
    EXPECT_EQ(transport.ring_caches_[1][1].cached_threshold_version.load(std::memory_order_relaxed), 5u);

    transport.update_cached_congestion_threshold(1, 1, 3, 6);
    EXPECT_EQ(transport.ring_caches_[1][1].cached_threshold.load(std::memory_order_relaxed), 3u);
    EXPECT_EQ(transport.ring_caches_[1][1].cached_threshold_version.load(std::memory_order_relaxed), 6u);

    on_flow_config_update(nullptr, &transport);
    FlowConfigUpdateMessage update{};
    update.priority = 1;
    update.threshold = 2;
    update.version = 7;
    message_t updateMsg{};
    updateMsg.header.src_node_id = 1;
    updateMsg.header.body_length = sizeof(update);
    updateMsg.body = reinterpret_cast<char *>(&update);
    on_flow_config_update(&updateMsg, &transport);
    EXPECT_EQ(transport.ring_caches_[1][1].cached_threshold.load(std::memory_order_relaxed), 2u);
    EXPECT_EQ(transport.ring_caches_[1][1].cached_threshold_version.load(std::memory_order_relaxed), 7u);

    message_t peerExit{};
    peerExit.header.src_node_id = 1;
    on_peer_exit(&peerExit, &transport);
    EXPECT_FALSE(transport.ring_caches_[1][1].initialized.load(std::memory_order_acquire));
    EXPECT_EQ(transport.remote_lookup_table_[1][1], nullptr);
}

TEST(UBShmTransportTest, FlowConfigBroadcastAndDeinitBroadcastUseRemoteLockRing)
{
    TwoNodeTransportEnv env;
    std::vector<char> out(LOCK_RING_MSG_SIZE);

    {
        UBShmTransport transport;
        ASSERT_EQ(transport.init(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
        ASSERT_EQ(transport.set_congestion_threshold(1, 50), UB_COMM_OK);

        uint32_t len = env.RemoteRing(LOCK_RING_PRIORITY)->dequeue(out.data(), out.size());
        ASSERT_EQ(len, sizeof(message_header_t) + sizeof(FlowConfigUpdateMessage));
        auto *hdr = reinterpret_cast<message_header_t *>(out.data());
        EXPECT_EQ(hdr->msg_type, MSG_TYPE_SYS_FLOW_CONFIG_UPDATE);
        EXPECT_EQ(hdr->dest_node_id, 1);

        transport.deinit_and_broadcast();
        len = env.RemoteRing(LOCK_RING_PRIORITY)->dequeue(out.data(), out.size());
        ASSERT_EQ(len, sizeof(message_header_t));
        hdr = reinterpret_cast<message_header_t *>(out.data());
        EXPECT_EQ(hdr->msg_type, MSG_TYPE_SYS_PEER_EXIT);
        EXPECT_EQ(hdr->dest_node_id, 1);
    }
}

TEST(UBShmTransportTest, DirectHelpersCoverNoUserRingAndNoThreadPoolBranches)
{
    TransportEnv env({{4, 128, 1}});
    UBShmTransport transport;

    ASSERT_EQ(transport.setup_config_and_validate(env.InitArea(), env.RingMap(), env.Conf()), UB_COMM_OK);
    ASSERT_EQ(transport.build_node_mapping(env.RingMap()), UB_COMM_OK);
    ASSERT_EQ(transport.build_region_bases(env.RingMap()), UB_COMM_OK);
    ASSERT_EQ(transport.deep_copy_ring_descs(), UB_COMM_OK);

    transport.conf_.num_rings = 0;
    std::vector<uint64_t> offsets(MAX_PRIORITY_LEVELS, UINT64_MAX);
    EXPECT_EQ(transport.create_local_rings(offsets), UB_COMM_OK);

    transport.num_threads = 0;
    transport.init_thread_pool();
    EXPECT_EQ(transport.worker_pool_, nullptr);

    EXPECT_EQ(transport.recv(nullptr, 0), UB_COMM_OK);
    EXPECT_EQ(transport.set_is_for_lock(false), UB_COMM_OK);
    EXPECT_FALSE(transport.get_is_for_lock());
    EXPECT_EQ(transport.set_is_for_lock(true), UB_COMM_OK);
    EXPECT_TRUE(transport.get_is_for_lock());
}

} // namespace ut
} // namespace ub_comm_queue
