/*
 * Reliability EER demo for ub_comm_queue.
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#define private public
#include "UBShmTransport.h"
#include "MPSCRingBuffer.h"
#undef private
#include "ub_dist_comm_queue.h"

using namespace ub_comm_queue;

namespace {
constexpr uint8_t NODE_PRODUCER = 0;
constexpr uint8_t NODE_CONSUMER = 1;
constexpr uint8_t PRIORITY = 1;
constexpr uint8_t TYPE_EER = 42;

struct AlignedFree {
    void operator()(void *ptr) const
    {
        std::free(ptr);
    }
};

using AlignedBuffer = std::unique_ptr<void, AlignedFree>;

std::atomic<uint32_t> g_received{0};

AlignedBuffer alloc_aligned(size_t size)
{
    void *raw = nullptr;
    if (posix_memalign(&raw, CACHELINE_SIZE, size) != 0) {
        return AlignedBuffer(nullptr);
    }
    std::memset(raw, 0, size);
    return AlignedBuffer(raw);
}

size_t ring_region_size(const std::vector<ub_ring_desc_t> &descs)
{
    size_t total = MPSCRingBuffer::CalculateMemorySize(LOCK_RING_CAPACITY, LOCK_RING_MSG_SIZE) + CACHELINE_SIZE;
    for (const auto &desc : descs) {
        total += MPSCRingBuffer::CalculateMemorySize(desc.ring_capacity, desc.max_msg_size) + CACHELINE_SIZE;
    }
    return total;
}

void on_msg(const message_t *msg, void *)
{
    uint32_t count = g_received.fetch_add(1, std::memory_order_acq_rel) + 1;
    printf("[receiver] msg type=%u src=%u count=%u\n", msg->header.msg_type, msg->header.src_node_id, count);
}

bool wait_until(std::chrono::milliseconds timeout, const char *tag, const std::function<bool()> &predicate)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            printf("[%s] condition reached\n", tag);
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    printf("[%s] timeout\n", tag);
    return predicate();
}

class TwoNodeGroup {
public:
    TwoNodeGroup()
        : descs_({{64, 128, PRIORITY}}),
          init_mem_(alloc_aligned(sizeof(Billboard))),
          ring_mem0_(alloc_aligned(ring_region_size(descs_))),
          ring_mem1_(alloc_aligned(ring_region_size(descs_)))
    {
        init_area_.size = sizeof(Billboard);
        init_area_.ptr = init_mem_.get();

        entries_[0].region.size = ring_region_size(descs_);
        entries_[0].region.ptr = ring_mem0_.get();
        entries_[0].node_id = NODE_PRODUCER;
        entries_[1].region.size = ring_region_size(descs_);
        entries_[1].region.ptr = ring_mem1_.get();
        entries_[1].node_id = NODE_CONSUMER;
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

    bool init()
    {
        int producer_ret = -1;
        int consumer_ret = -1;
        std::thread producer_init([&]() {
            producer_ret = ub_comm_queue_init(&producer_, &init_area_, &ring_map_, &confs_[NODE_PRODUCER]);
        });
        std::thread consumer_init([&]() {
            consumer_ret = ub_comm_queue_init(&consumer_, &init_area_, &ring_map_, &confs_[NODE_CONSUMER]);
        });
        producer_init.join();
        consumer_init.join();
        if (producer_ret != UB_COMM_OK || consumer_ret != UB_COMM_OK) {
            return false;
        }
        return ub_comm_queue_register_process_func(&consumer_, TYPE_EER, UB_FUNC_SYNC, on_msg, nullptr) == UB_COMM_OK;
    }

    ~TwoNodeGroup()
    {
        if (consumer_ != nullptr) {
            (void)ub_comm_queue_deinit(&consumer_);
        }
        if (producer_ != nullptr) {
            (void)ub_comm_queue_deinit(&producer_);
        }
    }

    ub_shm_comm_t *producer()
    {
        return &producer_;
    }

    ub_shm_comm_t *consumer()
    {
        return &consumer_;
    }

    UBShmTransport *producer_transport()
    {
        return static_cast<UBShmTransport *>(producer_);
    }

    UBShmTransport *consumer_transport()
    {
        return static_cast<UBShmTransport *>(consumer_);
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
    ub_shm_comm_t producer_{nullptr};
    ub_shm_comm_t consumer_{nullptr};
};

message_t make_msg()
{
    message_t msg{};
    msg.header.src_thread_id = static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    msg.header.body_length = 0;
    msg.header.dest_node_id = NODE_CONSUMER;
    msg.header.src_node_id = NODE_PRODUCER;
    msg.header.msg_type = TYPE_EER;
    msg.header.priority = PRIORITY;
    return msg;
}

int send_once(ub_shm_comm_t *handle)
{
    message_t msg = make_msg();
    return ub_comm_queue_send(handle, &msg);
}

bool reserve_consumer_slot_and_exit(UBShmTransport *consumer)
{
    MPSCRingBuffer *ring = consumer->local_rings_[PRIORITY];
    if (ring == nullptr) {
        return false;
    }

    uint64_t tail = ring->tail_.load(std::memory_order_relaxed);
    while (!ring->tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_release,
                                              std::memory_order_relaxed)) {
        cpu_relax_arm();
    }
    printf("[half-write] reserved tail=%llu and exiting thread before ready_seq\n",
           static_cast<unsigned long long>(tail));
    return true;
}

bool run_UBCQ_2N_SEND_EER_001()
{
    printf("[UBCQ_2N_SEND_EER_001] start\n");
    g_received.store(0, std::memory_order_release);
    TwoNodeGroup group;
    if (!group.init()) {
        return false;
    }

    std::atomic<bool> reserved{false};
    std::thread exit_thread([&]() {
        reserved.store(reserve_consumer_slot_and_exit(group.consumer_transport()), std::memory_order_release);
    });
    exit_thread.join();
    if (!reserved.load(std::memory_order_acquire)) {
        return false;
    }

    std::vector<std::thread> senders;
    for (int i = 0; i < 8; ++i) {
        senders.emplace_back([&]() {
            int ret = send_once(group.producer());
            printf("[UBCQ_2N_SEND_EER_001] concurrent send ret=%d\n", ret);
        });
    }
    for (auto &sender : senders) {
        sender.join();
    }

    if (!wait_until(std::chrono::seconds(8), "half-write-recover",
                    []() { return g_received.load(std::memory_order_acquire) >= 8; })) {
        return false;
    }

    std::thread after_recovery([&]() {
        int ret = send_once(group.producer());
        printf("[UBCQ_2N_SEND_EER_001] post-recovery send ret=%d\n", ret);
    });
    after_recovery.join();
    return wait_until(std::chrono::seconds(2), "post-recovery",
                      []() { return g_received.load(std::memory_order_acquire) >= 9; });
}

bool run_heartbeat_fault_case(const char *case_id, const ub_comm_queue_heartbeat_config_t *producer_cfg,
                              bool query_default)
{
    printf("[%s] start\n", case_id);
    g_received.store(0, std::memory_order_release);
    TwoNodeGroup group;
    if (!group.init()) {
        return false;
    }

    ub_comm_queue_heartbeat_config_t effective{};
    if (query_default) {
        if (ub_comm_queue_config_heartbeat(group.producer(), nullptr, &effective) != UB_COMM_OK) {
            return false;
        }
        printf("[%s] default heartbeat interval=%u check=%u timeout=%u\n", case_id,
               effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);
    } else if (producer_cfg != nullptr) {
        if (ub_comm_queue_config_heartbeat(group.producer(), producer_cfg, &effective) != UB_COMM_OK) {
            return false;
        }
        printf("[%s] producer heartbeat interval=%u check=%u timeout=%u\n", case_id,
               effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);
    }

    std::atomic<bool> stopped{false};
    std::thread producer_thread([&]() {
        while (!stopped.load(std::memory_order_acquire)) {
            int ret = send_once(group.producer());
            if (ret == UB_COMM_ERR_PEER_NOT_READY) {
                printf("[%s] producer detected consumer timeout\n", case_id);
                stopped.store(true, std::memory_order_release);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    group.consumer_transport()->stop_reliability_threads();
    printf("[%s] consumer heartbeat stopped\n", case_id);

    std::chrono::milliseconds detect_timeout = query_default ? std::chrono::milliseconds(1800) :
                                                               std::chrono::milliseconds(2300);
    bool detected = wait_until(detect_timeout, case_id,
                               [&]() { return stopped.load(std::memory_order_acquire); });
    producer_thread.join();
    if (!detected) {
        return false;
    }

    group.consumer_transport()->start_reliability_threads();
    printf("[%s] consumer heartbeat restored\n", case_id);
    return wait_until(std::chrono::seconds(2), case_id, [&]() {
        int ret = send_once(group.producer());
        return ret == UB_COMM_OK || ret == UB_COMM_SEND_CONGESTED;
    });
}

bool run_interface_cases()
{
    printf("[UBCQ_IF_RCV_EER_001/002/003] start\n");
    TwoNodeGroup group;
    if (!group.init()) {
        return false;
    }

    ub_comm_queue_heartbeat_config_t effective{};
    if (ub_comm_queue_config_heartbeat(group.producer(), nullptr, &effective) != UB_COMM_OK) {
        return false;
    }
    printf("[UBCQ_IF_RCV_EER_001] interval=%u check=%u timeout=%u\n",
           effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);

    ub_comm_queue_heartbeat_config_t request1{100, 100, 1500};
    if (ub_comm_queue_config_heartbeat(group.producer(), &request1, nullptr) != UB_COMM_OK) {
        return false;
    }
    printf("[UBCQ_IF_RCV_EER_002] set-only ok\n");

    ub_comm_queue_heartbeat_config_t request2{50, 20, 100};
    effective = {};
    if (ub_comm_queue_config_heartbeat(group.producer(), &request2, &effective) != UB_COMM_OK) {
        return false;
    }
    printf("[UBCQ_IF_RCV_EER_003] interval=%u check=%u timeout=%u\n",
           effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);
    return effective.heartbeat_interval_ms == request2.heartbeat_interval_ms &&
           effective.check_interval_ms == request2.check_interval_ms &&
           effective.timeout_ms == request2.timeout_ms;
}
} // namespace

int main()
{
    ub_atomic_set_log_level(LOG_LEVEL_INFO);
    ub_comm_queue_heartbeat_config_t normal_cfg{100, 100, 1500};
    bool ok = run_UBCQ_2N_SEND_EER_001();
    ok = run_heartbeat_fault_case("UBCQ_2N_RCV_EER_001", &normal_cfg, false) && ok;
    ok = run_heartbeat_fault_case("UBCQ_2N_RCV_EER_002", nullptr, true) && ok;
    ok = run_interface_cases() && ok;
    printf("%s\n", ok ? "PASS: ub_comm_queue reliability EER demo" : "FAIL: ub_comm_queue reliability EER demo");
    return ok ? 0 : 1;
}
