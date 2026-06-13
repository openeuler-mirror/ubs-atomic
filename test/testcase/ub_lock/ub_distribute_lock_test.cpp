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

#define MOCKER_CPP(api, TT) MOCKCPP_NS::mockAPI(#api, reinterpret_cast<TT>(api))

namespace ublock {
namespace ut {

static int32_t ub_get_tid_i32()
{
    return static_cast<int32_t>(::syscall(SYS_gettid));
}

static uint64_t LocalReaderStateForTid(int32_t tid, uint16_t count = 1)
{
    const uint32_t lane = local_lock_lane_for_tid(tid);
    return static_cast<uint64_t>(count) << (lane * LOCAL_LOCK_READER_LANE_BITS);
}

class UbDistributedLockTest : public ::testing::Test {
public:
    void SetUp() override
    {
        shm_ = new ub_rw_lock_t{};
        ResetLocalLockEntries();
        lock_ = new DistributedLock(shm_);
    }

    void TearDown() override
    {
        (void)unregister_local_lock(shm_);
        for (uint8_t i = 0; i < UB_MAX_NODES; ++i) {
            if (shm_->node_registry[i]) {
                shm_->node_registry[i] = 0;
            }
        }
        delete lock_;
        lock_ = nullptr;
        delete shm_;
        shm_ = nullptr;
        GlobalMockObject::verify();
    }

protected:
    void ResetLocalLockEntries()
    {
        for (uint8_t i = 0; i < UB_MAX_NODES; ++i) {
            shm_->node_registry[i] = 0;
        }
    }

    static constexpr uint8_t kInvalidNodeId = 0xFF;

    ub_rw_lock_t *shm_{};
    DistributedLock *lock_{};
};

static void WaitForWaiters(ub_rw_lock_t *shm)
{
    for (int i = 0; i < 1000; ++i) {
        if (shm->waiting_count.load(std::memory_order_acquire) > 0) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

TEST_F(UbDistributedLockTest, LookupRegisterAndUnregisterLocalLock)
{
    EXPECT_EQ(lookup_local_lock(shm_), nullptr);

    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    EXPECT_NE(lookup_local_lock(shm_), nullptr);

    auto removed = unregister_local_lock(shm_);
    EXPECT_EQ(lookup_local_lock(shm_), nullptr);
}

TEST_F(UbDistributedLockTest, LockCreateFastPathRegistersLocalLock)
{
    ub_lock_config_t config{};
    ub_location_t loc{ub_get_tid_i32(), 3};

    shm_->is_inited.store(1u, std::memory_order_release);
    lock_->lock_create(config, loc);

    auto ll_sp = lookup_local_lock(shm_);
    EXPECT_NE(ll_sp, nullptr);
}

TEST_F(UbDistributedLockTest, LockCreateFastPathSkipsWhenLocalLockExists)
{
    ub_lock_config_t config{};
    ub_location_t loc{ub_get_tid_i32(), 4};

    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    shm_->is_inited.store(1u, std::memory_order_release);

    lock_->lock_create(config, loc);

    EXPECT_EQ(lookup_local_lock(shm_), ll_sp);
}

TEST_F(UbDistributedLockTest, LockFreeReturnsOnInvalidNodeId)
{
    ub_location_t loc{ub_get_tid_i32(), 0xFF};
    lock_->lock_free(loc);
    EXPECT_EQ(shm_->is_inited.load(), 0u);
}

TEST_F(UbDistributedLockTest, LockCreateTest)
{
    ub_lock_config_t config{};
    ub_location_t loc{0, 0xFF};

    lock_->lock_create(config, loc);
    EXPECT_EQ(shm_->is_inited.load(), 0);

    loc.tid = ub_get_tid_i32();
    loc.node_id = 3;
    lock_->lock_create(config, loc);
    EXPECT_EQ(shm_->is_inited.load(), 1);

    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR);

    EXPECT_EQ(shm_->reserve_lock_owner.load(), LOCK_INVALID_OWNER);

    EXPECT_EQ(shm_->node_registry[0], 0);

    lock_->lock_free(loc);
    EXPECT_EQ(shm_->is_inited.load(), 0);

    ub_location_t loc2{loc.tid, 2};
    lock_->lock_create(config, loc2);
    EXPECT_EQ(shm_->is_inited.load(), 1);
}

TEST_F(UbDistributedLockTest, LockFreeWithoutLocalSlotStillDecrementsInit)
{
    ub_location_t loc{ub_get_tid_i32(), 5};
    shm_->is_inited.store(1u, std::memory_order_release);
    for (uint8_t i = 0; i < UB_MAX_NODES; ++i) {
        shm_->node_registry[i] = 0;
    }

    lock_->lock_free(loc);
    EXPECT_EQ(shm_->is_inited.load(), 0u);
}

TEST_F(UbDistributedLockTest, LockSFastPathDecrementsLockWord)
{
    ub_lock_policy_t policy{1000, true, false};
    ub_location_t loc{0, 0xFF};

    EXPECT_EQ(lock_->lock_s(policy, loc), UB_LOCK_ERROR);

    policy.allow_delay_release = false;
    EXPECT_EQ(lock_->lock_s(policy, loc), UB_LOCK_ERROR);

    loc.tid = ub_get_tid_i32();
    loc.node_id = 3;
    shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);

    EXPECT_EQ(lock_->lock_s(policy, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
}

TEST_F(UbDistributedLockTest, LockSUsesSlowPathAndTimesOut)
{
    shm_->lock_word.store(0, std::memory_order_release);
    lock_->create_wait_queue();

    ub_lock_policy_t policy{0, false, false};
    ub_location_t loc{1, 100};
    loc.tid = ub_get_tid_i32();

    EXPECT_EQ(lock_->lock_s(policy, loc), UB_LOCK_ERROR);
    EXPECT_EQ(shm_->queue_tail.load(), 0u);
    EXPECT_EQ(shm_->wait_queue[0].seq.load(), 0);
    EXPECT_EQ(shm_->waiting_count.load(), 0u);
}

TEST_F(UbDistributedLockTest, LockSSlowPathWakesAndAcquires)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    lock_->create_wait_queue();
    shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);

    ub_lock_policy_t policy{5000, false, false};
    ub_location_t loc{ub_get_tid_i32(), 1};

    ub_lock_result_t result = UB_LOCK_ERROR;
    std::thread t([&] { result = lock_->lock_s(policy, loc); });

    WaitForWaiters(shm_);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    lock_->dequeue_and_notify_one(loc);

    t.join();
    EXPECT_EQ(result, UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
}

TEST_F(UbDistributedLockTest, LockSRejectsDelayReleaseWithoutLocalLock)
{
    ub_lock_policy_t policy{1000, true, false};
    ub_location_t loc{ub_get_tid_i32(), 1};

    EXPECT_EQ(lock_->lock_s(policy, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistributedLockTest, LockSLocalLockConflictSkipsGlobal)
{
    ub_lock_policy_t policy{1000, false, false};
    ub_location_t loc{ub_get_tid_i32(), 2};

    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    ll_sp->lock_word.store(0, std::memory_order_release);
    ll_sp->read_count.store(1, std::memory_order_release);

    shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    EXPECT_EQ(lock_->lock_s(policy, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
}

TEST_F(UbDistributedLockTest, UnlockSDetectsOverRelease)
{
    shm_->lock_word.store(X_LOCK_DECR + 1, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, false};
    ub_location_t loc{1, 1};
    EXPECT_EQ(lock_->unlock_s(policy, loc), UB_LOCK_ERROR);

    loc.tid = ub_get_tid_i32();
    EXPECT_EQ(lock_->unlock_s(policy, loc), UB_LOCK_ERROR);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR + 1);

    EXPECT_EQ(lock_->unlock_s(policy, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistributedLockTest, UnlockSWakesOneWaiterWhenLastReaderReleases)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    lock_->create_wait_queue();
    shm_->lock_word.store(X_LOCK_DECR - 1, std::memory_order_release);
    shm_->shared_owner_bitmap.store(1u << 1, std::memory_order_release);
    ll_sp->global_read_ref_count_.store(1, std::memory_order_release);
    ll_sp->global_state_.store(LocalLock::GLOBAL_HELD, std::memory_order_release);
    ll_sp->read_count.store(1, std::memory_order_release);
    ub_location_t loc{1, 1};
    loc.tid = ub_get_tid_i32();
    ll_sp->lock_word.store(LocalReaderStateForTid(loc.tid), std::memory_order_release);

    ub_location_t waiter_loc{2, 3};
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_S, waiter_loc, ticket), UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::verify_param, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    ub_lock_policy_t policy{1000, false, false};

    EXPECT_EQ(lock_->unlock_s(policy, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->queue_head.load(), 0u);
    EXPECT_EQ(shm_->wait_queue[0].seq.load(), UB_WAIT_NOTIFIED);
    EXPECT_EQ(shm_->waiting_count.load(), 1u);
}

TEST_F(UbDistributedLockTest, LockXRecursiveIncrementsCounter)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    ub_location_t loc{5, 1};
    loc.tid = ub_get_tid_i32();
    shm_->x_recursive.store(1, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, true};

    EXPECT_EQ(lock_->lock_x(policy, loc), UB_LOCK_TIMEOUT);
    EXPECT_EQ(shm_->x_recursive.load(), 1);
}

TEST_F(UbDistributedLockTest, LockXFastPathSetsOwnerAndLockWord)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    ub_location_t loc{5, 10};
    loc.tid = ub_get_tid_i32();
    shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, false};

    EXPECT_EQ(lock_->lock_x(policy, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_word.load(), 0);
}

TEST_F(UbDistributedLockTest, LockXSlowPathTimesOut)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    shm_->lock_word.store(0, std::memory_order_release);
    lock_->create_wait_queue();

    ub_lock_policy_t policy{0, false, false};
    ub_location_t loc{7, 8};
    loc.tid = ub_get_tid_i32();

    EXPECT_EQ(lock_->lock_x(policy, loc), UB_LOCK_TIMEOUT);
    EXPECT_EQ(shm_->queue_tail.load(), 1u);
    EXPECT_EQ(shm_->wait_queue[0].seq.load(), UB_WAIT_TIMEOUT);
    EXPECT_EQ(shm_->waiting_count.load(), 0u);
}

TEST_F(UbDistributedLockTest, LockXSlowPathWakesAndAcquires)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    lock_->create_wait_queue();
    shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);
    shm_->reserve_lock_owner.store(0xFF, std::memory_order_release);

    ub_lock_policy_t policy{5000, false, false};
    ub_location_t loc{ub_get_tid_i32(), 2};

    ub_lock_result_t result = UB_LOCK_ERROR;
    std::thread t([&] { result = lock_->lock_x(policy, loc); });

    WaitForWaiters(shm_);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    lock_->dequeue_and_notify_one(loc);

    t.join();
    EXPECT_EQ(result, UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_word.load(), 0);
}

TEST_F(UbDistributedLockTest, UnlockXReturnsErrorForNonOwner)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);

    ub_location_t owner{1, 1};
    ub_location_t caller{2, 2};
    ub_lock_policy_t policy{1000, false, false};

    shm_->lock_owner_x.store(make_global_owner(owner.node_id, owner.tid), std::memory_order_release);
    ll_sp->lock_x_owner.store(caller.tid, std::memory_order_release);

    EXPECT_EQ(lock_->unlock_x(policy, caller), UB_LOCK_ERROR);
}

TEST_F(UbDistributedLockTest, UnlockXOwnerMatchesButLocalOwnerMismatch)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);

    ub_location_t caller{2, 3};
    ub_lock_policy_t policy{1000, false, false};

    shm_->lock_owner_x.store(make_global_owner(caller.node_id, caller.tid), std::memory_order_release);
    ll_sp->lock_x_owner.store(caller.tid + 1, std::memory_order_release);

    EXPECT_EQ(lock_->unlock_x(policy, caller), UB_LOCK_ERROR);
}

TEST_F(UbDistributedLockTest, UnlockXRecursiveKeepsLock)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    ub_location_t loc{9, 1};
    shm_->x_recursive.store(2, std::memory_order_release);
    shm_->lock_word.store(0, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, true};

    EXPECT_EQ(lock_->unlock_x(policy, loc), UB_LOCK_ERROR);
    EXPECT_EQ(shm_->x_recursive.load(), 2);
    EXPECT_EQ(shm_->lock_word.load(), 0);
}

TEST_F(UbDistributedLockTest, UnlockXReleasesWhenLastRecursiveLayer)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    ub_location_t loc{3, 4};
    shm_->x_recursive.store(1, std::memory_order_release);
    shm_->lock_word.store(0, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, true};

    EXPECT_EQ(lock_->unlock_x(policy, loc), UB_LOCK_ERROR);
    EXPECT_EQ(shm_->lock_word.load(), 0);
}

TEST_F(UbDistributedLockTest, LockSxFastPathSetsOwnerAndRecursion)
{
    auto ll_sp = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll_sp);
    ub_location_t loc{11, 4};
    shm_->lock_word.store(X_LOCK_DECR, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, false};

    EXPECT_EQ(lock_->lock_sx(policy, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->sx_recursive.load(), 1);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_HALF_DECR);
}

} // namespace ut
} // namespace ublock
