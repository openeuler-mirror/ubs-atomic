/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include <sys/syscall.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#include "ub_mutex_lock.h"
#include "ub_spin_lock.h"
#undef private

namespace ublock {
namespace ut {

static int32_t UbGetTidI32()
{
    return static_cast<int32_t>(::syscall(SYS_gettid));
}

static ub_location_t make_location(uint8_t node_id, int32_t tid)
{
    ub_location_t loc{};
    loc.node_id = node_id;
    loc.tid = tid;
    return loc;
}

constexpr int32_t K_INIT_STATE_EMPTY = 0;
constexpr int32_t K_INIT_STATE_READY = 2;
constexpr time_ms_t kMinTimeoutMs = 1;
constexpr time_ms_t kDefaultLockTimeoutMs = 100;
constexpr time_ms_t kWaitDeadlineMs = 500;
constexpr time_ms_t kConcurrentLockTimeoutMs = 5000;
constexpr time_ms_t kLongHoldTimeoutMs = 10000;
constexpr time_ms_t kUseDefaultTimeoutMs = 0;
constexpr int K_LOCK_UNLOCK_CYCLES = 10;
constexpr uint32_t K_NOTIFY_DELAY_MS = 50;
constexpr uint32_t K_OWNER_SWITCH_DELAY_MS = 20;
constexpr uint32_t K_DEFAULT_WAITER_COUNT = 3;

// =============================================================================
// SpinLock Unit Tests
// =============================================================================

class SpinLockTest : public ::testing::Test {
public:
    void SetUp() override
    {
        shm_ = new ub_spin_lock_t{};
        lock_ = new SpinLock(shm_);
    }

    void TearDown() override
    {
        delete lock_;
        lock_ = nullptr;
        delete shm_;
        shm_ = nullptr;
    }

protected:
    ub_spin_lock_t *shm_{};
    SpinLock *lock_{};
};

TEST_F(SpinLockTest, InitSetsReadyState)
{
    EXPECT_EQ(shm_->init_state.load(std::memory_order_acquire), K_INIT_STATE_EMPTY);
    lock_->lock_init();
    EXPECT_EQ(shm_->init_state.load(std::memory_order_acquire), K_INIT_STATE_READY);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_TRUE(lock_->is_ready());
}

TEST_F(SpinLockTest, InitIsIdempotent)
{
    lock_->lock_init();
    EXPECT_TRUE(lock_->is_ready());
    lock_->lock_init();
    EXPECT_TRUE(lock_->is_ready());
    EXPECT_EQ(shm_->init_state.load(std::memory_order_acquire), K_INIT_STATE_READY);
}

TEST_F(SpinLockTest, LockAcquiresSuccessfully)
{
    lock_->lock_init();
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    uint64_t expected = make_global_owner(loc.node_id, loc.tid);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), expected);
}

TEST_F(SpinLockTest, UnlockReleasesSuccessfully)
{
    lock_->lock_init();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

TEST_F(SpinLockTest, LockUnlockCycle)
{
    lock_->lock_init();
    ub_location_t loc = make_location(0, UbGetTidI32());
    for (int i = 0; i < K_LOCK_UNLOCK_CYCLES; ++i) {
        EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
        EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
    }
}

TEST_F(SpinLockTest, RecursiveLockRejected)
{
    lock_->lock_init();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
}

TEST_F(SpinLockTest, LockWithInvalidNodeIdReturnsError)
{
    lock_->lock_init();
    ub_location_t loc = make_location(UB_MAX_NODES, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
}

TEST_F(SpinLockTest, LockWhenNotReadyReturnsError)
{
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
}

TEST_F(SpinLockTest, UnlockWithInvalidNodeIdReturnsError)
{
    lock_->lock_init();
    ub_location_t loc = make_location(UB_MAX_NODES, UbGetTidI32());
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);
}

TEST_F(SpinLockTest, UnlockWhenNotReadyReturnsError)
{
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);
}

TEST_F(SpinLockTest, UnlockNotOwnerReturnsError)
{
    lock_->lock_init();
    ub_location_t loc1 = make_location(0, UbGetTidI32());
    ub_location_t loc2 = make_location(0, UbGetTidI32() + 1);
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc1), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->unlock(loc2), UB_LOCK_ERROR);
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(SpinLockTest, LockTimeoutOnContendedLock)
{
    lock_->lock_init();
    ub_location_t loc1 = make_location(0, UbGetTidI32());
    ub_location_t loc2 = make_location(0, UbGetTidI32() + 1);
    ASSERT_EQ(lock_->lock(kLongHoldTimeoutMs, loc1), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock_->lock(kMinTimeoutMs, loc2), UB_LOCK_TIMEOUT);
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(SpinLockTest, LockTimeoutUsesDefaultWhenZero)
{
    lock_->lock_init();
    ub_location_t loc1 = make_location(0, UbGetTidI32());
    ub_location_t loc2 = make_location(0, UbGetTidI32() + 1);
    ASSERT_EQ(lock_->lock(kLongHoldTimeoutMs, loc1), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock_->lock(kUseDefaultTimeoutMs, loc2), UB_LOCK_TIMEOUT);
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(SpinLockTest, ConcurrentLockUnlockDifferentThreads)
{
    lock_->lock_init();
    constexpr int32_t kNumThreads = 4;
    constexpr int32_t kIterations = 100;
    std::atomic<uint32_t> shared_counter{0};
    std::atomic<bool> ready{false};
    std::vector<std::thread> threads;

    for (int32_t i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([this, &shared_counter, &ready, i]() {
            ub_location_t loc = make_location(0, UbGetTidI32() + i);
            while (!ready.load(std::memory_order_acquire)) {
                cpu_relax();
            }
            for (int32_t j = 0; j < kIterations; ++j) {
                ASSERT_EQ(lock_->lock(kConcurrentLockTimeoutMs, loc), UB_LOCK_SUCCESS);
                uint32_t val = shared_counter.load(std::memory_order_acquire);
                shared_counter.store(val + 1, std::memory_order_release);
                ASSERT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
            }
        });
    }

    ready.store(true, std::memory_order_release);
    for (auto &t : threads) {
        t.join();
    }
    EXPECT_EQ(shared_counter.load(std::memory_order_acquire), kNumThreads * kIterations);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

TEST_F(SpinLockTest, UnlockFailsWhenOwnerChangedExternally)
{
    lock_->lock_init();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);

    shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);
}

// =============================================================================
// SpinLock C API Tests
// =============================================================================

TEST(SpinLockCApiTest, InitNullLockReturns)
{
    ub_spin_lock_init(nullptr);
}

TEST(SpinLockCApiTest, LockNullArgsReturnError)
{
    ub_spin_lock_t lock{};
    ub_location_t loc = make_location(0, 1);
    EXPECT_EQ(ub_spin_lock(nullptr, kDefaultLockTimeoutMs, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_spin_lock(&lock, 100, nullptr), UB_LOCK_ERROR);
}

TEST(SpinLockCApiTest, UnlockNullArgsReturnError)
{
    ub_spin_lock_t lock{};
    ub_location_t loc = make_location(0, 1);
    EXPECT_EQ(ub_spin_unlock(nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_spin_unlock(&lock, nullptr), UB_LOCK_ERROR);
}

TEST(SpinLockCApiTest, CApiLockUnlockCycle)
{
    ub_spin_lock_t lock{};
    ub_location_t loc = make_location(0, UbGetTidI32());
    ub_spin_lock_init(&lock);
    EXPECT_EQ(ub_spin_lock(&lock, kDefaultLockTimeoutMs, &loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(ub_spin_unlock(&lock, &loc), UB_LOCK_SUCCESS);
}

// =============================================================================
// MutexLock Unit Tests
// =============================================================================

class MutexLockTest : public ::testing::Test {
public:
    void SetUp() override
    {
        shm_ = new ub_mutex_lock_t{};
        lock_ = new MutexLock(shm_);
    }

    void TearDown() override
    {
        lock_->lock_free();
        delete lock_;
        lock_ = nullptr;
        delete shm_;
        shm_ = nullptr;
    }

protected:
    ub_mutex_lock_t *shm_{};
    MutexLock *lock_{};
};

TEST_F(MutexLockTest, CreateSetsReadyState)
{
    lock_->lock_create();
    EXPECT_EQ(shm_->is_inited.load(std::memory_order_acquire), K_INIT_STATE_READY);
    EXPECT_TRUE(lock_->is_ready());
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(shm_->queue_head.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(shm_->queue_tail.load(std::memory_order_acquire), 0u);
}

TEST_F(MutexLockTest, CreateIsIdempotent)
{
    lock_->lock_create();
    EXPECT_TRUE(lock_->is_ready());
    lock_->lock_create();
    EXPECT_TRUE(lock_->is_ready());
    EXPECT_EQ(shm_->is_inited.load(std::memory_order_acquire), K_INIT_STATE_READY);
}

TEST_F(MutexLockTest, CreateInitializesWaitQueue)
{
    lock_->lock_create();
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        EXPECT_EQ(shm_->wait_queue[i].seq.load(std::memory_order_acquire), UB_WAIT_EMPTY);
        EXPECT_EQ(shm_->wait_queue[i].mode, UB_LOCK_I);
    }
}

TEST_F(MutexLockTest, LockAcquiresSuccessfully)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    uint64_t expected = make_global_owner(loc.node_id, loc.tid);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), expected);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, UnlockReleasesSuccessfully)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

TEST_F(MutexLockTest, LockUnlockCycle)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    for (int i = 0; i < K_LOCK_UNLOCK_CYCLES; ++i) {
        EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
        EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
    }
}

TEST_F(MutexLockTest, RecursiveLockRejected)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, LockWithInvalidNodeIdReturnsError)
{
    lock_->lock_create();
    ub_location_t loc = make_location(UB_MAX_NODES, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, LockWhenNotReadyReturnsError)
{
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, UnlockWithInvalidNodeIdReturnsError)
{
    lock_->lock_create();
    ub_location_t loc = make_location(UB_MAX_NODES, UbGetTidI32());
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, UnlockWhenNotReadyReturnsError)
{
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, UnlockNotOwnerReturnsError)
{
    lock_->lock_create();
    ub_location_t loc1 = make_location(0, UbGetTidI32());
    ub_location_t loc2 = make_location(0, UbGetTidI32() + 1);
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc1), UB_LOCK_SUCCESS);
    EXPECT_EQ(lock_->unlock(loc2), UB_LOCK_ERROR);
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, LockTimeoutOnContendedLock)
{
    lock_->lock_create();
    ub_location_t loc1 = make_location(0, UbGetTidI32());
    ub_location_t loc2 = make_location(0, UbGetTidI32() + 1);
    ASSERT_EQ(lock_->lock(kLongHoldTimeoutMs, loc1), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock_->lock(kMinTimeoutMs, loc2), UB_LOCK_TIMEOUT);
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, LockTimeoutUsesDefaultWhenZero)
{
    lock_->lock_create();
    ub_location_t loc1 = make_location(0, UbGetTidI32());
    ub_location_t loc2 = make_location(0, UbGetTidI32() + 1);
    ASSERT_EQ(lock_->lock(kLongHoldTimeoutMs, loc1), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock_->lock(kUseDefaultTimeoutMs, loc2), UB_LOCK_TIMEOUT);
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, LockFreeWhenIdle)
{
    lock_->lock_create();
    EXPECT_TRUE(lock_->is_ready());
    lock_->lock_free();
    lock_->lock_create();
    EXPECT_TRUE(lock_->is_ready());
}

TEST_F(MutexLockTest, LockFreeWhenLockHeld)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);
    lock_->lock_free();
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, LockWithoutCreateReturnsError)
{
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, UnlockWithoutCreateReturnsError)
{
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, ConcurrentLockUnlockDifferentThreads)
{
    lock_->lock_create();
    constexpr int32_t kNumThreads = 4;
    constexpr int32_t kIterations = 50;
    std::atomic<uint32_t> shared_counter{0};
    std::atomic<bool> ready{false};
    std::vector<std::thread> threads;

    for (int32_t i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([this, &shared_counter, &ready, i]() {
            ub_location_t loc = make_location(0, UbGetTidI32() + i);
            while (!ready.load(std::memory_order_acquire)) {
                cpu_relax();
            }
            for (int32_t j = 0; j < kIterations; ++j) {
                ASSERT_EQ(lock_->lock(kConcurrentLockTimeoutMs, loc), UB_LOCK_SUCCESS);
                uint32_t val = shared_counter.load(std::memory_order_acquire);
                shared_counter.store(val + 1, std::memory_order_release);
                ASSERT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
            }
        });
    }

    ready.store(true, std::memory_order_release);
    for (auto &t : threads) {
        t.join();
    }
    EXPECT_EQ(shared_counter.load(std::memory_order_acquire), kNumThreads * kIterations);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

TEST_F(MutexLockTest, UnlockFailsWhenOwnerChangedExternally)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);

    shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_ERROR);

    shm_->lock_owner.store(make_global_owner(loc.node_id, loc.tid), std::memory_order_release);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, LockOnDifferentNodes)
{
    lock_->lock_create();
    ub_location_t loc0 = make_location(0, UbGetTidI32());
    ub_location_t loc1 = make_location(1, UbGetTidI32());

    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc0), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), make_global_owner(0, loc0.tid));
    EXPECT_EQ(lock_->unlock(loc0), UB_LOCK_SUCCESS);

    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc1), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), make_global_owner(1, loc1.tid));
    EXPECT_EQ(lock_->unlock(loc1), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, EnqueueWaiterReturnsErrorWhenQueueFull)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());

    shm_->queue_tail.store(UB_MAX_NODES, std::memory_order_release);
    shm_->queue_head.store(0, std::memory_order_release);

    uint32_t ticket = 0;
    EXPECT_EQ(lock_->enqueue_waiter(loc, ticket), UB_LOCK_ERROR);
}

TEST_F(MutexLockTest, CleanTimeoutWaiter)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(loc, ticket), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 1u);

    lock_->clean_timeout_waiter(ticket);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(shm_->wait_queue[0].seq.load(std::memory_order_acquire), UB_WAIT_TIMEOUT);
}

TEST_F(MutexLockTest, CleanOutqueueWaiter)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(loc, ticket), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 1u);

    lock_->clean_outqueue_waiter(ticket);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(shm_->queue_head.load(std::memory_order_acquire), 1u);
}

TEST_F(MutexLockTest, TryLockFastAcquiresWhenUnlocked)
{
    lock_->lock_create();
    uint64_t identify = make_global_owner(0, UbGetTidI32());
    EXPECT_TRUE(lock_->try_lock_fast(identify, false, 0));
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), identify);
}

TEST_F(MutexLockTest, TryLockFastFailsWhenLocked)
{
    lock_->lock_create();
    uint64_t identify1 = make_global_owner(0, UbGetTidI32());
    uint64_t identify2 = make_global_owner(0, UbGetTidI32() + 1);
    ASSERT_TRUE(lock_->try_lock_fast(identify1, false, 0));

    EXPECT_FALSE(lock_->try_lock_fast(identify2, false, 0));
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), identify1);
}

TEST_F(MutexLockTest, TryLockFastSkipsWhenWaitersExist)
{
    lock_->lock_create();
    uint64_t identify = make_global_owner(0, UbGetTidI32());
    shm_->waiting_count.store(1, std::memory_order_release);

    EXPECT_FALSE(lock_->try_lock_fast(identify, false, 0));
}

TEST_F(MutexLockTest, TryLockFastAcquiresWhenAwakened)
{
    lock_->lock_create();
    uint64_t identify = make_global_owner(0, UbGetTidI32());
    shm_->waiting_count.store(1, std::memory_order_release);

    EXPECT_TRUE(lock_->try_lock_fast(identify, true, 0));
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), identify);
}

TEST_F(MutexLockTest, CheckRecursiveGlobalOwner)
{
    lock_->lock_create();
    uint64_t identify = make_global_owner(0, UbGetTidI32());
    shm_->lock_owner.store(identify, std::memory_order_release);
    EXPECT_EQ(lock_->check_recursive_global_owner(identify), UB_LOCK_ERROR);

    shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    EXPECT_EQ(lock_->check_recursive_global_owner(identify), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, WaitGlobalHandoffTimeout)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    uint64_t holderIdentify = make_global_owner(0, UbGetTidI32() + 100);
    shm_->lock_owner.store(holderIdentify, std::memory_order_release);
    auto deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds(kMinTimeoutMs);
    uint32_t slot = 0;
    EXPECT_EQ(lock_->wait_global_handoff(deadline, loc, slot), UB_LOCK_TIMEOUT);
    shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
}

TEST_F(MutexLockTest, WaitGlobalHandoffSuccess)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kWaitDeadlineMs);
    uint32_t slot = 0;

    ub_lock_result_t result = UB_LOCK_ERROR;
    std::thread t(
        [this, &result, &deadline, &loc, &slot]() { result = lock_->wait_global_handoff(deadline, loc, slot); });

    std::this_thread::sleep_for(std::chrono::milliseconds(K_NOTIFY_DELAY_MS));
    WaiterRegistry::instance().notify_local_waiter(loc.tid);

    t.join();
    EXPECT_EQ(result, UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, AcquireGlobalSuccess)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kWaitDeadlineMs);
    EXPECT_EQ(lock_->acquire_global(deadline, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), make_global_owner(loc.node_id, loc.tid));
}

TEST_F(MutexLockTest, AcquireGlobalTimeout)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    uint64_t holder = make_global_owner(0, UbGetTidI32() + 100);
    shm_->lock_owner.store(holder, std::memory_order_release);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kMinTimeoutMs);
    EXPECT_EQ(lock_->acquire_global(deadline, loc), UB_LOCK_TIMEOUT);
    shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
}

TEST_F(MutexLockTest, AcquireGlobalRecursiveCheckInLoop)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    uint64_t identify = make_global_owner(loc.node_id, loc.tid);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kWaitDeadlineMs);
    std::thread t([this, identify]() {
        shm_->lock_owner.store(identify, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(K_OWNER_SWITCH_DELAY_MS));
        shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    });
    ub_lock_result_t ret = lock_->acquire_global(deadline, loc);
    t.join();
    EXPECT_TRUE(ret == UB_LOCK_ERROR || ret == UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, WaitGlobalHandoffSelfHandoff)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kWaitDeadlineMs);
    uint32_t slot = 0;
    EXPECT_EQ(lock_->wait_global_handoff(deadline, loc, slot), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, NotifyWaiterSameNodeNotFound)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ub_location_t waiter_loc = make_location(0, UbGetTidI32() + 200);
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(waiter_loc, ticket), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 1u);

    lock_->wake_one_waiter(loc);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 0u);
}

TEST_F(MutexLockTest, WakeOneWaiterLocalSuccess)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ub_location_t waiter_loc = make_location(0, UbGetTidI32() + 300);
    local_wait_ctx_t wait_ctx;
    WaiterRegistry::instance().register_waiter(waiter_loc.tid, &wait_ctx);

    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(waiter_loc, ticket), UB_LOCK_SUCCESS);

    lock_->wake_one_waiter(loc);
    EXPECT_EQ(shm_->wait_queue[ticket & (UB_MAX_NODES - 1)].seq.load(std::memory_order_acquire), UB_WAIT_NOTIFIED);

    WaiterRegistry::instance().unregister_waiter(waiter_loc.tid);
}

TEST_F(MutexLockTest, NotifyWaiterRemoteTransportNull)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ub_location_t remote_loc = make_location(1, 999);
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(remote_loc, ticket), UB_LOCK_SUCCESS);

    lock_->wake_one_waiter(loc);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 0u);
}

TEST_F(MutexLockTest, UnlockWithWaitersTriggersWakeOneWaiter)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);

    ub_location_t waiter_loc = make_location(0, UbGetTidI32() + 400);
    uint32_t ticket = 0;
    ASSERT_EQ(lock_->enqueue_waiter(waiter_loc, ticket), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->waiting_count.load(std::memory_order_acquire), 1u);

    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

TEST_F(MutexLockTest, LockRecursiveLocalLockDetection)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);

    shm_->lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    EXPECT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_ERROR);

    shm_->lock_owner.store(make_global_owner(loc.node_id, loc.tid), std::memory_order_release);
    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
}

TEST_F(MutexLockTest, UnlockWithWaitersAfterWakeOneWaiterCleanup)
{
    lock_->lock_create();
    ub_location_t loc = make_location(0, UbGetTidI32());
    ASSERT_EQ(lock_->lock(kDefaultLockTimeoutMs, loc), UB_LOCK_SUCCESS);

    for (uint32_t i = 0; i < K_DEFAULT_WAITER_COUNT; ++i) {
        ub_location_t waiter_loc = make_location(0, UbGetTidI32() + 500 + static_cast<int32_t>(i));
        uint32_t ticket = 0;
        ASSERT_EQ(lock_->enqueue_waiter(waiter_loc, ticket), UB_LOCK_SUCCESS);
    }
    EXPECT_GE(shm_->waiting_count.load(std::memory_order_acquire), K_DEFAULT_WAITER_COUNT);

    EXPECT_EQ(lock_->unlock(loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

} // namespace ut
} // namespace ublock

// =============================================================================
// MutexLock C API Tests
// =============================================================================

using ublock::ut::kDefaultLockTimeoutMs;
using ublock::ut::make_location;
using ublock::ut::UbGetTidI32;

TEST(MutexLockCApiTest, CreateNullLockReturns)
{
    ub_mutex_lock_create(nullptr);
}

TEST(MutexLockCApiTest, FreeNullLockReturns)
{
    ub_mutex_lock_free(nullptr);
}

TEST(MutexLockCApiTest, LockNullArgsReturnError)
{
    ub_mutex_lock_t lock{};
    ub_location_t loc = make_location(0, 1);
    EXPECT_EQ(ub_mutex_lock(nullptr, kDefaultLockTimeoutMs, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_mutex_lock(&lock, 100, nullptr), UB_LOCK_ERROR);
}

TEST(MutexLockCApiTest, UnlockNullArgsReturnError)
{
    ub_mutex_lock_t lock{};
    ub_location_t loc = make_location(0, 1);
    EXPECT_EQ(ub_mutex_unlock(nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_mutex_unlock(&lock, nullptr), UB_LOCK_ERROR);
}

TEST(MutexLockCApiTest, CApiLockUnlockCycle)
{
    ub_mutex_lock_t lock{};
    ub_location_t loc = make_location(0, UbGetTidI32());
    ub_mutex_lock_create(&lock);
    EXPECT_EQ(ub_mutex_lock(&lock, kDefaultLockTimeoutMs, &loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(ub_mutex_unlock(&lock, &loc), UB_LOCK_SUCCESS);
    ub_mutex_lock_free(&lock);
}

TEST(MutexLockCApiTest, CApiLockWithoutCreateReturnsError)
{
    ub_mutex_lock_t lock{};
    ub_location_t loc = make_location(0, UbGetTidI32());
    EXPECT_EQ(ub_mutex_lock(&lock, kDefaultLockTimeoutMs, &loc), UB_LOCK_ERROR);
}