#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#define private public
#include "MPSCRingBuffer.h"
#undef private

namespace ub_comm_queue {
namespace ut {
namespace {

struct AlignedFree {
    void operator()(void *ptr) const
    {
        std::free(ptr);
    }
};

using AlignedBuffer = std::unique_ptr<void, AlignedFree>;

AlignedBuffer AllocRingMemory(uint32_t capacity, uint32_t maxMsgSize)
{
    void *raw = nullptr;
    size_t size = MPSCRingBuffer::CalculateMemorySize(capacity, maxMsgSize);
    if (posix_memalign(&raw, CACHELINE_SIZE, size) != 0) {
        return AlignedBuffer(nullptr);
    }
    std::memset(raw, 0, size);
    return AlignedBuffer(raw);
}

MPSCRingBuffer *ConstructRing(void *mem, uint32_t capacity, uint32_t maxMsgSize)
{
    return new (mem) MPSCRingBuffer(static_cast<uint8_t *>(mem), capacity, maxMsgSize);
}

message_header_t MakeHeader(uint32_t bodyLen)
{
    message_header_t hdr{};
    hdr.src_thread_id = 123;
    hdr.body_length = bodyLen;
    hdr.dest_node_id = 1;
    hdr.src_node_id = 0;
    hdr.msg_type = 7;
    hdr.priority = 1;
    return hdr;
}

void WriteReadyEntry(MPSCRingBuffer *ring, uint64_t seq, const message_header_t &hdr, const void *body,
                     uint32_t bodyLen)
{
    uintptr_t base = reinterpret_cast<uintptr_t>(ring) + MPSCRingBuffer::GetDataOffset();
    uintptr_t entry = base + ((seq & ring->index_mask_) * ring->entry_stride_);
    auto *ready = reinterpret_cast<std::atomic<uint64_t> *>(entry);
    auto *data = reinterpret_cast<char *>(entry + sizeof(std::atomic<uint64_t>));
    std::memcpy(data, &hdr, sizeof(hdr));
    if (bodyLen > 0) {
        std::memcpy(data + sizeof(hdr), body, bodyLen);
    }
    ready->store(seq + 1, std::memory_order_release);
}

} // namespace

TEST(MPSCRingBufferTest, CalculateMemorySizeAlignsEntries)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 100;

    size_t memSize = MPSCRingBuffer::CalculateMemorySize(capacity, maxMsgSize);
    size_t dataOffset = MPSCRingBuffer::GetDataOffset();

    EXPECT_EQ(dataOffset % CACHELINE_SIZE, 0u);
    EXPECT_GT(memSize, dataOffset);
    EXPECT_EQ((memSize - dataOffset) % capacity, 0u);
    EXPECT_EQ(((memSize - dataOffset) / capacity) % CACHELINE_SIZE, 0u);
}

TEST(MPSCRingBufferTest, EnqueueLocalAndDequeuePreserveHeaderAndBody)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    const char body[] = "hello ub queue";
    message_header_t hdr = MakeHeader(sizeof(body));

    EXPECT_EQ(ring->enqueue_local(&hdr, body, sizeof(body)), UB_COMM_OK);

    std::vector<char> out(maxMsgSize);
    uint32_t len = ring->dequeue(out.data(), out.size());
    ASSERT_EQ(len, sizeof(message_header_t) + sizeof(body));

    auto *outHdr = reinterpret_cast<message_header_t *>(out.data());
    EXPECT_EQ(outHdr->src_thread_id, hdr.src_thread_id);
    EXPECT_EQ(outHdr->body_length, hdr.body_length);
    EXPECT_EQ(outHdr->dest_node_id, hdr.dest_node_id);
    EXPECT_EQ(outHdr->src_node_id, hdr.src_node_id);
    EXPECT_EQ(outHdr->msg_type, hdr.msg_type);
    EXPECT_EQ(outHdr->priority, hdr.priority);
    EXPECT_EQ(std::memcmp(out.data() + sizeof(message_header_t), body, sizeof(body)), 0);
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), 0u);
}

TEST(MPSCRingBufferTest, EnqueueLocalWithEmptyBodyCopiesOnlyHeader)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    message_header_t hdr = MakeHeader(0);
    EXPECT_EQ(ring->enqueue_local(&hdr, nullptr, 0), UB_COMM_OK);

    std::vector<char> out(maxMsgSize);
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t));
    auto *outHdr = reinterpret_cast<message_header_t *>(out.data());
    EXPECT_EQ(outHdr->body_length, 0u);
    EXPECT_EQ(outHdr->msg_type, hdr.msg_type);
}

TEST(MPSCRingBufferTest, EnqueueRejectsOversizedMessageAndReportsFull)
{
    constexpr uint32_t capacity = 2;
    constexpr uint32_t maxMsgSize = sizeof(message_header_t) + 4;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    char body[8] = {};
    message_header_t tooLarge = MakeHeader(sizeof(body));
    EXPECT_EQ(ring->enqueue_local(&tooLarge, body, sizeof(body)), -EMSGSIZE);

    message_header_t hdr = MakeHeader(1);
    EXPECT_GE(ring->enqueue_local(&hdr, body, 1), UB_COMM_OK);
    EXPECT_GE(ring->enqueue_local(&hdr, body, 1), UB_COMM_OK);
    EXPECT_EQ(ring->enqueue_local(&hdr, body, 1), UB_COMM_ERR_RING_FULL);

    ub_comm_queue_status_t status{};
    ring->get_status(&status);
    EXPECT_EQ(status.used, capacity);
    EXPECT_EQ(status.free, 0u);
    EXPECT_EQ(status.state, UB_COMM_QUEUE_FULL);
}

TEST(MPSCRingBufferTest, CongestionThresholdAffectsStatusAndReturnValue)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    EXPECT_EQ(ring->configure_congestion_threshold(101), -EINVAL);
    EXPECT_EQ(ring->configure_congestion_threshold(50), UB_COMM_OK);
    EXPECT_EQ(ring->get_congestion_threshold(), 2u);

    char body[] = "x";
    message_header_t hdr = MakeHeader(sizeof(body));
    EXPECT_EQ(ring->enqueue_local(&hdr, body, sizeof(body)), UB_COMM_OK);
    EXPECT_EQ(ring->enqueue_local(&hdr, body, sizeof(body)), UB_COMM_SEND_CONGESTED);

    ub_comm_queue_status_t status{};
    ring->get_status(&status);
    EXPECT_EQ(status.used, 2u);
    EXPECT_EQ(status.congestion_threshold, 2u);
    EXPECT_EQ(status.state, UB_COMM_QUEUE_CONGESTED);
    EXPECT_GE(status.max_depth, 2u);

    std::vector<char> out(maxMsgSize);
    EXPECT_GT(ring->dequeue(out.data(), out.size()), 0u);
    EXPECT_GT(ring->dequeue(out.data(), out.size()), 0u);
    ring->get_status(&status);
    EXPECT_EQ(status.state, UB_COMM_QUEUE_IDLE);

    EXPECT_EQ(ring->configure_congestion_threshold(0), UB_COMM_OK);
    ring->get_status(&status);
    EXPECT_EQ(status.congestion_threshold, 0u);
    EXPECT_EQ(status.state, UB_COMM_QUEUE_CONGESTED);
}

TEST(MPSCRingBufferTest, StatusNullNormalAndTruncatedDequeue)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    ring->get_status(nullptr);

    const char body[] = "truncate-me";
    message_header_t hdr = MakeHeader(sizeof(body));
    ASSERT_EQ(ring->enqueue_local(&hdr, body, sizeof(body)), UB_COMM_OK);

    ub_comm_queue_status_t status{};
    ring->get_status(&status);
    EXPECT_EQ(status.used, 1u);
    EXPECT_EQ(status.state, UB_COMM_QUEUE_NORMAL);

    char small[sizeof(message_header_t)] = {};
    EXPECT_EQ(ring->dequeue(small, sizeof(small)), sizeof(small));
    auto *outHdr = reinterpret_cast<message_header_t *>(small);
    EXPECT_EQ(outHdr->body_length, sizeof(body));
}

TEST(MPSCRingBufferTest, EnqueueRemoteUsesCachedMetadata)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    std::atomic<uint64_t> shadowHead{0};
    std::atomic<uint32_t> cachedThreshold{0};
    std::atomic<uint64_t> cachedThresholdVersion{0};
    const char body[] = "remote";
    message_header_t hdr = MakeHeader(sizeof(body));

    EXPECT_EQ(MPSCRingBuffer::enqueue_remote(ring, &hdr, body, sizeof(body), ring->get_entry_num() - 1,
                                             ring->get_entry_stride(), ring->get_max_msg_size(), shadowHead,
                                             cachedThreshold, cachedThresholdVersion),
              UB_COMM_OK);
    EXPECT_EQ(cachedThreshold.load(), ring->get_congestion_threshold());
    EXPECT_EQ(cachedThresholdVersion.load(), ring->get_congestion_threshold_version());

    std::vector<char> out(maxMsgSize);
    ASSERT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t) + sizeof(body));
    EXPECT_EQ(std::memcmp(out.data() + sizeof(message_header_t), body, sizeof(body)), 0);
}

TEST(MPSCRingBufferTest, EnqueueRemoteEmptyBodyAndFullPaths)
{
    constexpr uint32_t capacity = 2;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    std::atomic<uint64_t> shadowHead{0};
    std::atomic<uint32_t> cachedThreshold{0};
    std::atomic<uint64_t> cachedThresholdVersion{0};
    message_header_t hdr = MakeHeader(0);

    EXPECT_EQ(MPSCRingBuffer::enqueue_remote(ring, &hdr, nullptr, 0, ring->get_entry_num() - 1,
                                             ring->get_entry_stride(), ring->get_max_msg_size(), shadowHead,
                                             cachedThreshold, cachedThresholdVersion),
              UB_COMM_OK);
    EXPECT_EQ(MPSCRingBuffer::enqueue_remote(ring, &hdr, nullptr, 0, ring->get_entry_num() - 1,
                                             ring->get_entry_stride(), ring->get_max_msg_size(), shadowHead,
                                             cachedThreshold, cachedThresholdVersion),
              UB_COMM_SEND_CONGESTED);
    EXPECT_EQ(MPSCRingBuffer::enqueue_remote(ring, &hdr, nullptr, 0, ring->get_entry_num() - 1,
                                             ring->get_entry_stride(), ring->get_max_msg_size(), shadowHead,
                                             cachedThreshold, cachedThresholdVersion),
              UB_COMM_ERR_RING_FULL);

    std::vector<char> out(maxMsgSize);
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t));
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t));
}

TEST(MPSCRingBufferTest, RemoteCachedThresholdZeroAndRefreshPaths)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    std::atomic<uint64_t> shadowHead{0};
    std::atomic<uint32_t> cachedThreshold{0};
    std::atomic<uint64_t> cachedThresholdVersion{ring->get_congestion_threshold_version()};
    message_header_t hdr = MakeHeader(0);

    EXPECT_EQ(MPSCRingBuffer::enqueue_remote(ring, &hdr, nullptr, 0, ring->get_entry_num() - 1,
                                             ring->get_entry_stride(), ring->get_max_msg_size(), shadowHead,
                                             cachedThreshold, cachedThresholdVersion),
              UB_COMM_SEND_CONGESTED);

    std::vector<char> out(maxMsgSize);
    ASSERT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t));

    ASSERT_EQ(ring->configure_congestion_threshold(100), UB_COMM_OK);
    cachedThreshold.store(1, std::memory_order_relaxed);
    cachedThresholdVersion.store(0, std::memory_order_relaxed);
    EXPECT_EQ(MPSCRingBuffer::enqueue_remote(ring, &hdr, nullptr, 0, ring->get_entry_num() - 1,
                                             ring->get_entry_stride(), ring->get_max_msg_size(), shadowHead,
                                             cachedThreshold, cachedThresholdVersion),
              UB_COMM_OK);
    EXPECT_EQ(cachedThreshold.load(std::memory_order_relaxed), ring->get_congestion_threshold());
    EXPECT_EQ(cachedThresholdVersion.load(std::memory_order_relaxed), ring->get_congestion_threshold_version());
}

TEST(MPSCRingBufferTest, TriggerForceFullMakesNextProducerFail)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    ring->trigger_force_full();
    EXPECT_EQ(ring->head_.load(std::memory_order_acquire), 0ULL - capacity);
}

TEST(MPSCRingBufferTest, HalfWrittenHeadSlotIsSkippedAfterLocalTimeout)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    ring->tail_.store(1, std::memory_order_relaxed); // slot 0 reserved but never committed

    const char body[] = "after-half-write";
    message_header_t hdr = MakeHeader(sizeof(body));
    WriteReadyEntry(ring, 1, hdr, body, sizeof(body));
    ring->tail_.store(2, std::memory_order_release);

    std::vector<char> out(maxMsgSize);
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), 0u);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), 0u);
    EXPECT_EQ(ring->head_.load(std::memory_order_acquire), 1u);

    ASSERT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t) + sizeof(body));
    auto *outHdr = reinterpret_cast<message_header_t *>(out.data());
    EXPECT_EQ(outHdr->msg_type, hdr.msg_type);
    EXPECT_EQ(std::memcmp(out.data() + sizeof(message_header_t), body, sizeof(body)), 0);
}

TEST(MPSCRingBufferTest, HalfWriteRecoveryDoesNotSkipWhenReservedSlotBecomesReady)
{
    constexpr uint32_t capacity = 4;
    constexpr uint32_t maxMsgSize = 128;
    auto mem = AllocRingMemory(capacity, maxMsgSize);
    ASSERT_NE(mem, nullptr);
    auto *ring = ConstructRing(mem.get(), capacity, maxMsgSize);

    ring->tail_.store(1, std::memory_order_release);

    std::vector<char> out(maxMsgSize);
    EXPECT_EQ(ring->dequeue(out.data(), out.size()), 0u);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    const char body[] = "late-ready";
    message_header_t hdr = MakeHeader(sizeof(body));
    WriteReadyEntry(ring, 0, hdr, body, sizeof(body));

    ASSERT_EQ(ring->dequeue(out.data(), out.size()), sizeof(message_header_t) + sizeof(body));
    EXPECT_EQ(ring->head_.load(std::memory_order_acquire), 1u);
    EXPECT_EQ(std::memcmp(out.data() + sizeof(message_header_t), body, sizeof(body)), 0);
}

TEST(MPSCRingBufferTest, InvalidCapacityAborts)
{
    auto mem = AllocRingMemory(3, 128);
    ASSERT_NE(mem, nullptr);
    EXPECT_DEATH({ ConstructRing(mem.get(), 3, 128); }, ".*");
}

} // namespace ut
} // namespace ub_comm_queue
