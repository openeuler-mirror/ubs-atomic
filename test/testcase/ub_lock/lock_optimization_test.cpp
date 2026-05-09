#include <sys/syscall.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#undef private

namespace ublock {
namespace ut {

static int32_t ub_get_tid_i32()
{
    return static_cast<int32_t>(::syscall(SYS_gettid));
}

static void WaitForLocalWaiters(LocalLock *lock)
{
    for (int i = 0; i < 1000; ++i) {
        if (lock->waiting_count.load(std::memory_order_acquire) > 0) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

class LocalLockTest : public ::testing::Test {
public:
    void SetUp() override
    {
        shm_ = new ub_rw_lock_t{};
        lock_ = new LocalLock(shm_);
    }

    void TearDown() override
    {
        delete lock_;
        delete shm_;
    }

protected:
    ub_rw_lock_t *shm_{};
    LocalLock *lock_{};
};

TEST_F(LocalLockTest, InitResetsState)
{
    lock_->lock_word.v.store(123, std::memory_order_release);
    lock_->read_count.store(5, std::memory_order_release);
    lock_->waiting_count.store(7, std::memory_order_release);
    lock_->lock_x_owner.store(11, std::memory_order_release);
    lock_->lock_sx_owner.store(22, std::memory_order_release);
    lock_->x_recursive_.store(2, std::memory_order_release);
    lock_->sx_recursive_.store(3, std::memory_order_release);
    lock_->q_head.v.store(5, std::memory_order_release);
    lock_->q_tail.v.store(6, std::memory_order_release);
    lock_->local_is_reserve_lock.store(UB_LOCK_X, std::memory_order_release);
    lock_->q[0].seq.store(99, std::memory_order_release);
    lock_->q[0].tid = 99;
    lock_->q[0].mode = UB_LOCK_X;

    lock_->init_();

    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_DECR);
    EXPECT_EQ(lock_->read_count.load(), 0u);
    EXPECT_EQ(lock_->waiting_count.load(), 0u);
    EXPECT_EQ(lock_->lock_x_owner.load(), 0);
    EXPECT_EQ(lock_->lock_sx_owner.load(), 0);
    EXPECT_EQ(lock_->x_recursive_.load(), 0);
    EXPECT_EQ(lock_->sx_recursive_.load(), 0);
    EXPECT_EQ(lock_->q_head.v.load(), 0);
    EXPECT_EQ(lock_->q_tail.v.load(), 0);
    EXPECT_EQ(lock_->local_is_reserve_lock.load(), UB_LOCK_I);

    for (uint16_t i = 0; i < UB_MAX_CAPACITY; ++i) {
        EXPECT_EQ(lock_->q[i].seq.load(), 0);
        EXPECT_EQ(lock_->q[i].tid, 0);
        EXPECT_EQ(lock_->q[i].mode, UB_LOCK_I);
    }
}

TEST_F(LocalLockTest, QueueEnqueueOutqueueAndTimeout)
{
    uint32_t ticket = 0;

    EXPECT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 7, ticket), UB_LOCK_SUCCESS);
    EXPECT_EQ(ticket, 0u);
    EXPECT_EQ(lock_->waiting_count.load(), 1u);
    EXPECT_EQ(lock_->q_tail.v.load(), 1);
    EXPECT_EQ(lock_->q[0].mode, UB_LOCK_S);
    EXPECT_EQ(lock_->q[0].tid, 7u);
    EXPECT_EQ(lock_->q[0].seq.load(), UB_WAIT_WAITING);

    local_waiter_t *out = nullptr;
    EXPECT_EQ(lock_->outqueue_waiter(out), UB_LOCK_SUCCESS);
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(lock_->q_head.v.load(), 0);
    EXPECT_EQ(lock_->waiting_count.load(), 1u);
    EXPECT_EQ(out->seq.load(), UB_WAIT_NOTIFIED);
}

TEST_F(LocalLockTest, CleanTimeoutWaiterReleasesSlot)
{
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, 9, ticket), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->waiting_count.load(), 1u);

    lock_->clean_timeout_waiter(ticket);
    EXPECT_EQ(lock_->waiting_count.load(), 0u);
    EXPECT_EQ(lock_->q[0].seq.load(), UB_WAIT_TIMEOUT);

    lock_->clean_timeout_waiter(ticket);
    EXPECT_EQ(lock_->waiting_count.load(), 0xFFFFFFFF);
    EXPECT_EQ(lock_->q[0].seq.load(), UB_WAIT_TIMEOUT);
}

TEST_F(LocalLockTest, PeekHeadWaitingModeCleanSkipsReleased)
{
    ub_lock_mode_t mode = UB_LOCK_I;
    EXPECT_FALSE(lock_->peek_head_waiting_mode_clean(mode));

    uint32_t t0 = 0;
    uint32_t t1 = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 1, t0), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, 2, t1), UB_LOCK_SUCCESS);

    lock_->clean_timeout_waiter(t0);
    EXPECT_FALSE(lock_->peek_head_waiting_mode_clean(mode));
    EXPECT_EQ(mode, UB_LOCK_I);
    EXPECT_EQ(lock_->q_head.v.load(), 1);
}

TEST_F(LocalLockTest, RemoteReleaseFlagAndHeldCheck)
{
    lock_->lock_word.v.store(X_LOCK_DECR, std::memory_order_release);
    EXPECT_FALSE(lock_->is_held());
    lock_->lock_word.v.store(X_LOCK_DECR - 1, std::memory_order_release);
    EXPECT_TRUE(lock_->is_held());

    EXPECT_TRUE(lock_->try_begin_remote_release());
    EXPECT_FALSE(lock_->try_begin_remote_release());
    lock_->end_remote_release();
    EXPECT_TRUE(lock_->try_begin_remote_release());
    lock_->end_remote_release();
}

TEST_F(LocalLockTest, LockXFastPathAndRecursiveBehavior)
{
    const int32_t tid = ub_get_tid_i32();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);

    lock_->lock_word.v.store(X_LOCK_DECR, std::memory_order_release);
    EXPECT_EQ(lock_->lock_x(false, tid, deadline), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->lock_x_owner.load(), static_cast<uint64_t>(tid));
    EXPECT_EQ(lock_->x_recursive_.load(), 1u);

    EXPECT_EQ(lock_->lock_x(true, tid, deadline), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->x_recursive_.load(), 2u);

    EXPECT_EQ(lock_->lock_x(false, tid, deadline), UB_LOCK_CONFLICT);
}

TEST_F(LocalLockTest, LockSxFastPathAndRecursiveBehavior)
{
    const int32_t tid = ub_get_tid_i32();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);

    lock_->lock_word.v.store(X_LOCK_DECR, std::memory_order_release);
    EXPECT_EQ(lock_->lock_sx(false, tid, deadline), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->lock_sx_owner.load(), static_cast<uint64_t>(tid));
    EXPECT_EQ(lock_->sx_recursive_.load(), 1u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_HALF_DECR);

    EXPECT_EQ(lock_->lock_sx(true, tid, deadline), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->sx_recursive_.load(), 2u);

    EXPECT_EQ(lock_->lock_sx(false, tid, deadline), UB_LOCK_CONFLICT);
}

TEST_F(LocalLockTest, UnlockXReleasesOrKeepsOnRecursive)
{
    const int32_t tid = ub_get_tid_i32();

    lock_->lock_x_owner.store(tid, std::memory_order_release);
    lock_->x_recursive_.store(2u, std::memory_order_release);
    lock_->lock_word.v.store(0, std::memory_order_release);
    EXPECT_EQ(lock_->unlock_x(true, tid), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->x_recursive_.load(), 1u);
    EXPECT_EQ(lock_->lock_word.v.load(), 0);

    lock_->x_recursive_.store(1u, std::memory_order_release);
    EXPECT_EQ(lock_->unlock_x(true, tid), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->x_recursive_.load(), 0u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_DECR);
    EXPECT_EQ(lock_->lock_x_owner.load(), 0u);
}

TEST_F(LocalLockTest, UnlockSxReleasesOrKeepsOnRecursive)
{
    const int32_t tid = ub_get_tid_i32();

    lock_->lock_sx_owner.store(tid, std::memory_order_release);
    lock_->sx_recursive_.store(2u, std::memory_order_release);
    lock_->lock_word.v.store(X_LOCK_HALF_DECR, std::memory_order_release);
    EXPECT_EQ(lock_->unlock_sx(true, tid), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->sx_recursive_.load(), 1u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_HALF_DECR);

    lock_->sx_recursive_.store(1u, std::memory_order_release);
    EXPECT_EQ(lock_->unlock_sx(true, tid), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->sx_recursive_.load(), 0u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_DECR);
    EXPECT_EQ(lock_->lock_sx_owner.load(), 0u);
}

TEST_F(LocalLockTest, LockSUnlockSUpdatesCounters)
{
    uint64_t tid = static_cast<uint64_t>(ub_get_tid_i32());
    uint64_t tid2 = tid + 1;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);
    EXPECT_EQ(lock_->lock_s(tid, deadline), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->read_count.load(), 1u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_DECR - 1);

    EXPECT_EQ(lock_->lock_s(tid2, deadline), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->read_count.load(), 2u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_DECR - 2);

    EXPECT_EQ(lock_->unlock_s(), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->unlock_s(), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->read_count.load(), 2u);
    EXPECT_EQ(lock_->lock_word.v.load(), X_LOCK_DECR);
}

TEST_F(LocalLockTest, WakeAfterUnlockExclusiveHeadXNotifiesOne)
{
    uint32_t t0 = 0;
    uint32_t t1 = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, 1, t0), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 2, t1), UB_LOCK_SUCCESS);

    lock_->wake_after_unlock_exclusive();

    EXPECT_EQ(lock_->waiting_count.load(), 2u);
    EXPECT_EQ(lock_->q_head.v.load(), 0);
}

TEST_F(LocalLockTest, WakeAfterUnlockExclusiveHeadSNotifiesUntilX)
{
    uint32_t t0 = 0;
    uint32_t t1 = 0;
    uint32_t t2 = 0;
    uint32_t t3 = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 1, t0), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_SX, 2, t1), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 3, t2), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, 4, t3), UB_LOCK_SUCCESS);

    lock_->wake_after_unlock_exclusive();

    EXPECT_EQ(lock_->waiting_count.load(), 4u);
    EXPECT_EQ(lock_->q_head.v.load(), 0);
}

TEST_F(LocalLockTest, WakeAfterUnlockExclusiveHeadSxNotifiesSUntilStop)
{
    uint32_t t0 = 0;
    uint32_t t1 = 0;
    uint32_t t2 = 0;
    uint32_t t3 = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_SX, 1, t0), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 2, t1), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, 3, t2), UB_LOCK_SUCCESS);
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, 4, t3), UB_LOCK_SUCCESS);

    lock_->wake_after_unlock_exclusive();

    EXPECT_EQ(lock_->waiting_count.load(), 4u);
    EXPECT_EQ(lock_->q_head.v.load(), 0);
}

TEST_F(LocalLockTest, LockSSlowPathTimeoutEnqueuesAndCleans)
{
    lock_->lock_word.v.store(0, std::memory_order_release);
    lock_->waiting_count.store(1, std::memory_order_release);
    auto deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds(1);

    EXPECT_EQ(lock_->lock_s(1, deadline), UB_LOCK_TIMEOUT);
    EXPECT_EQ(lock_->waiting_count.load(), 1u);
    EXPECT_EQ(lock_->q[0].seq.load(), UB_WAIT_TIMEOUT);
}

} // namespace ut
} // namespace ublock
