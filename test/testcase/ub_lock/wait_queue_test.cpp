#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#undef private

namespace ublock {
namespace ut {
using ublock::DistributedLock;

class WaitQueueTest : public ::testing::Test {
public:
    void SetUp() override
    {
        shm_ = new ub_rw_lock_t{};
        lock_ = new DistributedLock(shm_);
        lock_->create_wait_queue();
    }

    void TearDown() override
    {
        delete lock_;
        delete shm_;
        lock_ = nullptr;
        shm_ = nullptr;
    }

protected:
    DistributedLock *lock_{};
    ub_rw_lock_t *shm_{};
};

TEST_F(WaitQueueTest, CreateWaitQueueResetsState)
{
    lock_->create_wait_queue();

    EXPECT_EQ(lock_->rw_lock_shm_->queue_head.load(), 0u);
    EXPECT_EQ(lock_->rw_lock_shm_->queue_tail.load(), 0u);
    EXPECT_EQ(lock_->rw_lock_shm_->waiting_count.load(), 0u);

    for (uint16_t i = 0; i < UB_MAX_NODES; ++i) {
        EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[i].seq.load(), 0u);
        EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[i].mode, UB_LOCK_I);
        EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[i].location.node_id, 0xFF);
    }
}

TEST_F(WaitQueueTest, EnqueueWaiterSetsSlotAndWaiters)
{
    ub_location_t loc{1, 42};
    uint32_t ticket = 0;
    auto result = lock_->enqueue_waiter(UB_LOCK_S, loc, ticket);

    EXPECT_EQ(result, UB_LOCK_SUCCESS);
    EXPECT_EQ(ticket, 0u);
    EXPECT_EQ(lock_->rw_lock_shm_->queue_tail.load(), 1u);
    EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[0].mode, UB_LOCK_S);
    EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[0].location.node_id, loc.node_id);
    EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[0].location.tid, loc.tid);
    EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[0].seq.load(), 2u);
    EXPECT_EQ(lock_->rw_lock_shm_->waiting_count.load(), 1u);
}

TEST_F(WaitQueueTest, EnqueueWaiterFullReturnsError)
{
    ub_location_t loc{1, 1};
    uint32_t ticket = 0;
    lock_->rw_lock_shm_->queue_head.store(0u, std::memory_order_release);
    lock_->rw_lock_shm_->queue_tail.store(UB_MAX_NODES, std::memory_order_release);
    lock_->rw_lock_shm_->waiting_count.store(UB_MAX_NODES, std::memory_order_release);
    EXPECT_EQ(lock_->enqueue_waiter(UB_LOCK_S, loc, ticket), UB_LOCK_ERROR);
}

TEST_F(WaitQueueTest, OutqueueWaiterEmptyReturnsError)
{
    ub_waiter_t *out_waiter = nullptr;
    EXPECT_EQ(lock_->outqueue_waiter(out_waiter), UB_LOCK_ERROR);
}

TEST_F(WaitQueueTest, OutqueueWaiterReturnsNotifiedAndAdvancesHead)
{
    ub_location_t loc{2, 88};
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, loc, ticket), UB_LOCK_SUCCESS);

    ub_waiter_t *out_waiter = nullptr;
    EXPECT_EQ(lock_->outqueue_waiter(out_waiter), UB_LOCK_SUCCESS);
    ASSERT_NE(out_waiter, nullptr);

    EXPECT_EQ(out_waiter->seq.load(), 3u);
    EXPECT_EQ(lock_->rw_lock_shm_->queue_head.load(), 0u);
    EXPECT_EQ(lock_->rw_lock_shm_->waiting_count.load(), 1u);
}

TEST_F(WaitQueueTest, CleanTimeoutWaiterReleasesSlot)
{
    ub_location_t loc{3, 99};
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, loc, ticket), UB_LOCK_SUCCESS);

    lock_->clean_timeout_waiter(ticket);
    EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[ticket].seq.load(), 4u);
    EXPECT_EQ(lock_->rw_lock_shm_->waiting_count.load(), 0u);

    lock_->clean_timeout_waiter(ticket);
    EXPECT_EQ(lock_->rw_lock_shm_->wait_queue[ticket].seq.load(), 4u);
}

TEST_F(WaitQueueTest, PeekHeadWaitingModeCleanSkipsReleased)
{
    ub_location_t loc{4, 7};
    uint32_t t0 = 0;
    uint32_t t1 = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, loc, t0), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, loc, t1), UB_LOCK_SUCCESS);

    lock_->clean_timeout_waiter(t0);
    ub_lock_mode_t mode = UB_LOCK_I;
    EXPECT_TRUE(lock_->peek_head_waiting_mode_clean(mode));
    EXPECT_EQ(mode, UB_LOCK_X);
    EXPECT_EQ(lock_->rw_lock_shm_->queue_head.load(), 1u);
}

} // namespace ut
} // namespace ublock
