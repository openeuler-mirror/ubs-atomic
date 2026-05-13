#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "MPSCRingBuffer.h"

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

} // namespace ut
} // namespace ub_comm_queue
