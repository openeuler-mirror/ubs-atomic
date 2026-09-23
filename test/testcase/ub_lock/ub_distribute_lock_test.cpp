#include <sys/syscall.h>
#include <unistd.h>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>
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

namespace {
std::mutex init_test_mutex;
std::condition_variable init_test_cv;
bool init_test_entered = false;
bool init_test_resume = false;

void PauseCreateQueue(DistributedLock *impl)
{
    std::unique_lock<std::mutex> guard(init_test_mutex);
    init_test_entered = true;
    init_test_cv.notify_all();
    init_test_cv.wait(guard, [] { return init_test_resume; });
    auto *shm = impl->rw_lock_shm_;
    shm->waiting_count.store(0);
    shm->queue_head.store(0);
    shm->queue_tail.store(0);
    for (auto &slot : shm->wait_queue) {
        slot.seq.store(UB_WAIT_EMPTY);
        slot.mode = UB_LOCK_I;
        slot.location = {};
    }
}
} // namespace

TEST_F(UbDistributedLockTest, CreateRejectsUnreadyAndOverflowStates)
{
    const ub_location_t loc{11, 3};
    for (int32_t state : {-2, -1, -3, std::numeric_limits<int32_t>::max()}) {
        shm_->is_inited.store(state);
        shm_->lock_word.store(123);
        lock_->lock_create({}, loc);
        EXPECT_EQ(shm_->is_inited.load(), state);
        EXPECT_EQ(shm_->lock_word.load(), 123);
        EXPECT_EQ(lookup_local_lock(shm_), nullptr);
        EXPECT_EQ(shm_->node_registry[loc.node_id], 0u);
        (void)unregister_local_lock(shm_);
        shm_->node_registry[loc.node_id] = 0;
    }
}

TEST_F(UbDistributedLockTest, CreatePublishesOnlyAfterQueueAndRegistryInitialization)
{
    init_test_entered = false;
    init_test_resume = false;
    MOCKER_CPP(&DistributedLock::create_wait_queue, void (*)(DistributedLock *)).stubs().will(invoke(PauseCreateQueue));
    const ub_location_t first{11, 1};
    const ub_location_t second{22, 2};
    std::thread creator([&] { lock_->lock_create({}, first); });
    {
        std::unique_lock<std::mutex> guard(init_test_mutex);
        init_test_cv.wait(guard, [] { return init_test_entered; });
    }
    EXPECT_EQ(shm_->is_inited.load(), -2);
    EXPECT_EQ(lookup_local_lock(shm_), nullptr);
    lock_->lock_create({}, second); // 初始化者被确定性暂停，等待必然超限。
    EXPECT_EQ(shm_->is_inited.load(), -2);
    EXPECT_EQ(lookup_local_lock(shm_), nullptr);
    EXPECT_EQ(shm_->node_registry[second.node_id], 0u);
    {
        std::lock_guard<std::mutex> guard(init_test_mutex);
        init_test_resume = true;
    }
    init_test_cv.notify_all();
    creator.join();
    EXPECT_EQ(shm_->is_inited.load(), 1);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR);
    EXPECT_EQ(shm_->lock_owner_x.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(shm_->lock_owner_sx.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(shm_->reserve_lock_owner.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(shm_->node_registry[first.node_id], reinterpret_cast<uintptr_t>(shm_));
    for (const auto &slot : shm_->wait_queue) {
        EXPECT_EQ(slot.seq.load(), UB_WAIT_EMPTY);
        EXPECT_EQ(slot.mode, UB_LOCK_I);
    }
    lock_->lock_create({}, second);
    EXPECT_EQ(shm_->is_inited.load(), 2);
    EXPECT_EQ(shm_->node_registry[second.node_id], reinterpret_cast<uintptr_t>(shm_));
}

TEST_F(UbDistributedLockTest, ConcurrentCreateKeepsAllReadyReferences)
{
    std::atomic<bool> start{false};
    std::vector<std::thread> creators;
    for (uint8_t node = 0; node < 8; ++node) {
        creators.emplace_back([&, node] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            lock_->lock_create({}, ub_location_t{node + 1, node});
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &creator : creators) {
        creator.join();
    }
    EXPECT_EQ(shm_->is_inited.load(), 8);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR);
    for (uint8_t node = 0; node < 8; ++node) {
        EXPECT_EQ(shm_->node_registry[node], reinterpret_cast<uintptr_t>(shm_));
    }
    EXPECT_NE(lookup_local_lock(shm_), nullptr);
}

TEST_F(UbDistributedLockTest, CreateInitializationExceptionRestoresEmpty)
{
    MOCKER_CPP(&DistributedLock::create_wait_queue, void (*)())
        .stubs()
        .will(throws(std::runtime_error("初始化故障注入")));
    EXPECT_NO_THROW(lock_->lock_create({}, ub_location_t{11, 1}));
    EXPECT_EQ(shm_->is_inited.load(), 0);
    EXPECT_EQ(lookup_local_lock(shm_), nullptr);
    GlobalMockObject::verify();
    lock_->lock_create({}, ub_location_t{11, 1});
    EXPECT_EQ(shm_->is_inited.load(), 1);
}

TEST_F(UbDistributedLockTest, CreateRegistrationExceptionRollsBackReference)
{
    MOCKER(register_local_lock).stubs().will(throws(std::bad_alloc()));
    for (int32_t state : {0, 1}) {
        shm_->is_inited.store(state);
        EXPECT_NO_THROW(lock_->lock_create({}, ub_location_t{11, 1}));
        EXPECT_EQ(shm_->is_inited.load(), state);
        EXPECT_EQ(shm_->node_registry[1], 0u);
        EXPECT_EQ(lookup_local_lock(shm_), nullptr);
    }
}

TEST_F(UbDistributedLockTest, FreeAndRecoverPreserveNegativeInitStates)
{
    const ub_location_t loc{11, 1};
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    for (int32_t state : {-1, -2, -3}) {
        shm_->is_inited.store(state);
        shm_->lock_word.store(X_LOCK_DECR);
        shm_->node_registry[1] = 123;
        lock_->lock_free(loc);
        EXPECT_EQ(shm_->is_inited.load(), state);
        EXPECT_EQ(shm_->node_registry[1], 123u);
        EXPECT_EQ(lookup_local_lock(shm_), ll);
        EXPECT_EQ(lock_->recover(2, loc), UB_LOCK_ERROR);
        EXPECT_EQ(shm_->is_inited.load(), state);
        EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR);
    }
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

TEST_F(UbDistributedLockTest, UnlockSInvalidLanePreservesSharedState)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    const ub_location_t owner{4, 3};
    const ub_location_t second{5, 3};
    const ub_location_t invalid{6, 3};
    const ub_lock_policy_t immediate{100, false, false};
    for (int readers : {1, 2}) {
        for (bool delay : {false, true}) {
            for (bool waiting : {false, true}) {
                ll->init_();
                lock_->create_wait_queue();
                shm_->lock_word.store(X_LOCK_DECR);
                shm_->shared_owner_bitmap.store(0);
                shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER);
                ASSERT_EQ(lock_->lock_s(immediate, owner), UB_LOCK_SUCCESS);
                if (readers == 2) {
                    ASSERT_EQ(lock_->lock_s(immediate, second), UB_LOCK_SUCCESS);
                }
                uint32_t ticket = 0;
                if (waiting) {
                    ASSERT_EQ(lock_->enqueue_waiter(UB_LOCK_X, invalid, ticket), UB_LOCK_SUCCESS);
                }
                const uint64_t local_state = ll->lock_word.load();
                const ub_lock_policy_t policy{100, delay, false};
                EXPECT_EQ(lock_->unlock_s(policy, invalid), UB_LOCK_ERROR);
                EXPECT_EQ(ll->lock_word.load(), local_state);
                EXPECT_EQ(ll->global_read_ref_count_.load(), readers);
                EXPECT_EQ(ll->global_state_.load(), LocalLock::GLOBAL_HELD);
                EXPECT_TRUE(ll->hold_global.load());
                EXPECT_EQ(ll->local_is_reserve_lock.load(), UB_LOCK_I);
                EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
                EXPECT_EQ(shm_->shared_owner_bitmap.load(), 1u << owner.node_id);
                EXPECT_EQ(shm_->reserve_lock_owner.load(), LOCK_INVALID_OWNER);
                EXPECT_EQ(shm_->waiting_count.load(), waiting ? 1u : 0u);
                if (waiting) {
                    EXPECT_EQ(shm_->wait_queue[ticket].seq.load(), UB_WAIT_WAITING);
                    lock_->clean_timeout_waiter(ticket);
                }
                if (readers == 2) {
                    EXPECT_EQ(lock_->unlock_s(immediate, second), UB_LOCK_SUCCESS);
                    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
                }
                EXPECT_EQ(lock_->unlock_s(policy, owner), UB_LOCK_SUCCESS);
                EXPECT_EQ(ll->global_read_ref_count_.load(), 0);
                EXPECT_EQ(lock_->unlock_s(policy, owner), UB_LOCK_ERROR);
                EXPECT_EQ(ll->global_read_ref_count_.load(), 0);
                if (delay) {
                    EXPECT_EQ(shm_->reserve_lock_owner.load(), make_global_owner(owner.node_id, owner.tid));
                    EXPECT_EQ(ll->local_is_reserve_lock.exchange(UB_LOCK_I), UB_LOCK_S);
                    EXPECT_EQ(lock_->delay_unlock(UB_LOCK_S, owner.node_id), UB_LOCK_SUCCESS);
                }
                EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR);
                EXPECT_EQ(shm_->shared_owner_bitmap.load(), 0u);
            }
        }
    }
}

TEST_F(UbDistributedLockTest, UnlockSSentinelPreservesSharedState)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    const ub_location_t loc{4, 3};
    const uint64_t state = LOCAL_LOCK_X_STATE | LocalReaderStateForTid(loc.tid);
    const ub_lock_policy_t policy{100, true, false};
    for (int refs : {1, 2}) {
        ll->lock_word.store(state);
        ll->global_read_ref_count_.store(refs);
        shm_->lock_word.store(X_LOCK_DECR - 1);
        shm_->shared_owner_bitmap.store(1u << loc.node_id);
        shm_->reserve_lock_owner.store(LOCK_INVALID_OWNER);
        EXPECT_EQ(lock_->unlock_s(policy, loc), UB_LOCK_ERROR);
        EXPECT_EQ(ll->lock_word.load(), state);
        EXPECT_EQ(ll->global_read_ref_count_.load(), refs);
        EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
        EXPECT_EQ(shm_->shared_owner_bitmap.load(), 1u << loc.node_id);
        EXPECT_EQ(shm_->reserve_lock_owner.load(), LOCK_INVALID_OWNER);
    }
}

TEST_F(UbDistributedLockTest, UnlockSNonpositiveReferencePreservesLocalLane)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    const ub_location_t loc{4, 3};
    const ub_lock_policy_t policy{100, false, false};
    for (int refs : {0, -1}) {
        ll->lock_word.store(LocalReaderStateForTid(loc.tid));
        ll->global_read_ref_count_.store(refs);
        shm_->lock_word.store(X_LOCK_DECR - 1);
        EXPECT_EQ(lock_->unlock_s(policy, loc), UB_LOCK_ERROR);
        EXPECT_EQ(ll->global_read_ref_count_.load(), refs);
        EXPECT_EQ(ll->lock_word.load(), LocalReaderStateForTid(loc.tid));
        EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);
    }
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
