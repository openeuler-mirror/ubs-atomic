#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#undef private

#define MOCKER_CPP(api, TT) MOCKCPP_NS::mockAPI(#api, reinterpret_cast<TT>(api))

namespace ublock {
namespace ut {

namespace {

uint64_t MakeOwner(uint8_t nodeId, int32_t tid)
{
    return make_global_owner(nodeId, tid);
}

ub_location_t MakeLocation(uint8_t nodeId, int32_t tid)
{
    ub_location_t location{};
    location.node_id = nodeId;
    location.tid = tid;
    return location;
}

void InitRecoverLock(ub_rw_lock_t &lock)
{
    lock.lock_word.store(X_LOCK_DECR, std::memory_order_release);
    lock.waiting_count.store(0u, std::memory_order_release);
    lock.queue_head.store(0u, std::memory_order_release);
    lock.queue_tail.store(0u, std::memory_order_release);
    lock.lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.reserve_lock_owner.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.sx_recursive.store(0u, std::memory_order_release);
    lock.x_recursive.store(0u, std::memory_order_release);
    lock.is_inited.store(2, std::memory_order_release);
    lock.shared_owner_bitmap.store(0u, std::memory_order_release);
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        lock.wait_queue[i].seq.store(UB_WAIT_EMPTY, std::memory_order_release);
        lock.wait_queue[i].mode = UB_LOCK_I;
        lock.wait_queue[i].location = {};
        lock.node_registry[i] = 0;
    }
}

} // namespace

class UbDistributeLockApiTest : public ::testing::Test {
public:
    void SetUp() override
    {
        std::cout << "[Phase Setup Begin]" << std::endl;
        std::cout << "[Phase Setup End]" << std::endl;
    }

    void TearDown() override
    {
        std::cout << "[Phase TearDown Begin]" << std::endl;
        GlobalMockObject::verify();
        std::cout << "[Phase TearDown End]" << std::endl;
    }
};

TEST(UbDistributeLockApiTest, CreateAndFreeReturnOnNullArgs)
{
    ub_rw_lock_t lock{};
    ub_location_t loc{1, 2};
    ub_rw_lock_create(nullptr, nullptr, nullptr);
    ub_rw_lock_create(&lock, nullptr, nullptr);
    ub_rw_lock_create(nullptr, nullptr, &loc);
    ub_rw_lock_create(&lock, nullptr, &loc);

    ub_rw_lock_free(nullptr, nullptr);
    ub_rw_lock_free(&lock, nullptr);
    ub_rw_lock_free(nullptr, &loc);
    ub_rw_lock_free(&lock, &loc);
}

TEST(UbDistributeLockApiTest, LockApisReturnErrorOnNullArgs)
{
    ub_rw_lock_t lock{};
    ub_location_t loc{1, 2};

    EXPECT_EQ(ub_rw_lock_s_lock(nullptr, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_s_lock(&lock, nullptr, nullptr), UB_LOCK_ERROR);

    EXPECT_EQ(ub_rw_lock_x_lock(nullptr, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_x_lock(&lock, nullptr, nullptr), UB_LOCK_ERROR);

    EXPECT_EQ(ub_rw_lock_sx_lock(nullptr, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_sx_lock(&lock, nullptr, nullptr), UB_LOCK_ERROR);

    MOCKER_CPP(&DistributedLock::lock_s, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_s_lock(&lock, nullptr, &loc), UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::lock_x, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_x_lock(&lock, nullptr, &loc), UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::lock_sx, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_sx_lock(&lock, nullptr, &loc), UB_LOCK_SUCCESS);
}

TEST(UbDistributeLockApiTest, UnlockApisReturnErrorOnNullArgs)
{
    ub_rw_lock_t lock{};
    ub_location_t loc{1, 2};

    EXPECT_EQ(ub_rw_lock_s_unlock(nullptr, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_s_unlock(&lock, nullptr, nullptr), UB_LOCK_ERROR);

    EXPECT_EQ(ub_rw_lock_x_unlock(nullptr, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_x_unlock(&lock, nullptr, nullptr), UB_LOCK_ERROR);

    EXPECT_EQ(ub_rw_lock_sx_unlock(nullptr, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_sx_unlock(&lock, nullptr, nullptr), UB_LOCK_ERROR);

    MOCKER_CPP(&DistributedLock::unlock_s, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_s_unlock(&lock, nullptr, &loc), UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::unlock_x, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_x_unlock(&lock, nullptr, &loc), UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::unlock_sx, ub_lock_result_t(*)(const ub_lock_policy_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_sx_unlock(&lock, nullptr, &loc), UB_LOCK_SUCCESS);
    GlobalMockObject::verify();
}

TEST(UbDistributeLockApiTest, RecoverHandlesXLockHalfWrittenDuringLock)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(0, std::memory_order_release);
    lock.lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.reserve_lock_owner.store(MakeOwner(2, 22), std::memory_order_release);
    lock.x_recursive.store(1u, std::memory_order_release);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(2, MakeLocation(2, 22)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.lock_word.load(std::memory_order_acquire), X_LOCK_DECR);
    EXPECT_EQ(lock.lock_owner_x.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.reserve_lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.x_recursive.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(lock.is_inited.load(std::memory_order_acquire), 1);
}

TEST(UbDistributeLockApiTest, RecoverHandlesSxLockHalfWrittenDuringLock)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(X_LOCK_HALF_DECR, std::memory_order_release);
    lock.lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.reserve_lock_owner.store(MakeOwner(3, 33), std::memory_order_release);
    lock.sx_recursive.store(1u, std::memory_order_release);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(3, MakeLocation(3, 33)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.lock_word.load(std::memory_order_acquire), X_LOCK_DECR);
    EXPECT_EQ(lock.lock_owner_sx.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.reserve_lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.sx_recursive.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(lock.is_inited.load(std::memory_order_acquire), 1);
}

TEST(UbDistributeLockApiTest, RecoverHandlesSharedLockHalfWrittenDuringLock)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(X_LOCK_DECR - 1, std::memory_order_release);
    lock.shared_owner_bitmap.store(0u, std::memory_order_release);
    lock.reserve_lock_owner.store(MakeOwner(4, 44), std::memory_order_release);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(4, MakeLocation(4, 44)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.lock_word.load(std::memory_order_acquire), X_LOCK_DECR);
    EXPECT_EQ(lock.shared_owner_bitmap.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(lock.reserve_lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.is_inited.load(std::memory_order_acquire), 1);
}

TEST(UbDistributeLockApiTest, RecoverHandlesXLockHalfWrittenDuringUnlock)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(0, std::memory_order_release);
    lock.lock_owner_x.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.reserve_lock_owner.store(MakeOwner(5, 55), std::memory_order_release);
    lock.x_recursive.store(2u, std::memory_order_release);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(5, MakeLocation(5, 55)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.lock_word.load(std::memory_order_acquire), X_LOCK_DECR);
    EXPECT_EQ(lock.lock_owner_x.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.reserve_lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.x_recursive.load(std::memory_order_acquire), 0u);
}

TEST(UbDistributeLockApiTest, RecoverHandlesSxLockHalfWrittenDuringUnlock)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(X_LOCK_HALF_DECR, std::memory_order_release);
    lock.lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
    lock.reserve_lock_owner.store(MakeOwner(6, 66), std::memory_order_release);
    lock.sx_recursive.store(2u, std::memory_order_release);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(6, MakeLocation(6, 66)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.lock_word.load(std::memory_order_acquire), X_LOCK_DECR);
    EXPECT_EQ(lock.lock_owner_sx.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.reserve_lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.sx_recursive.load(std::memory_order_acquire), 0u);
}

TEST(UbDistributeLockApiTest, RecoverHandlesSharedUnlockHalfWrittenByReconcilingBitmap)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(X_LOCK_DECR - 2, std::memory_order_release);
    lock.shared_owner_bitmap.store(1u << 7, std::memory_order_release);
    lock.reserve_lock_owner.store(MakeOwner(2, 22), std::memory_order_release);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(2, MakeLocation(2, 22)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.lock_word.load(std::memory_order_acquire), X_LOCK_DECR - 1);
    EXPECT_EQ(lock.shared_owner_bitmap.load(std::memory_order_acquire), 1u << 7);
    EXPECT_EQ(lock.reserve_lock_owner.load(std::memory_order_acquire), LOCK_INVALID_OWNER);
}

TEST(UbDistributeLockApiTest, RecoverCleansEnqueueHalfWrittenWaiter)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(0, std::memory_order_release);
    lock.lock_owner_x.store(MakeOwner(1, 11), std::memory_order_release);
    lock.waiting_count.store(1u, std::memory_order_release);
    lock.wait_queue[0].seq.store(UB_WAIT_WRITING, std::memory_order_release);
    lock.wait_queue[0].mode = UB_LOCK_X;
    lock.wait_queue[0].location = MakeLocation(8, 88);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(8, MakeLocation(8, 88)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.wait_queue[0].seq.load(std::memory_order_acquire), UB_WAIT_TIMEOUT);
    EXPECT_EQ(lock.wait_queue[0].mode, UB_LOCK_I);
    EXPECT_EQ(lock.wait_queue[0].location.node_id, 0xFF);
    EXPECT_EQ(lock.wait_queue[0].location.tid, 0);
    EXPECT_EQ(lock.waiting_count.load(std::memory_order_acquire), 0u);
}

TEST(UbDistributeLockApiTest, RecoverCleansTargetProcessWaiterAfterPartialDequeue)
{
    ub_rw_lock_t lock{};
    InitRecoverLock(lock);
    lock.lock_word.store(0, std::memory_order_release);
    lock.lock_owner_x.store(MakeOwner(1, 11), std::memory_order_release);
    lock.waiting_count.store(2u, std::memory_order_release);
    lock.wait_queue[1].seq.store(UB_WAIT_NOTIFIED, std::memory_order_release);
    lock.wait_queue[1].mode = UB_LOCK_SX;
    lock.wait_queue[1].location = MakeLocation(9, 99);
    lock.wait_queue[2].seq.store(UB_WAIT_WAITING, std::memory_order_release);
    lock.wait_queue[2].mode = UB_LOCK_S;
    lock.wait_queue[2].location = MakeLocation(4, 44);

    DistributedLock impl(&lock);
    EXPECT_EQ(impl.recover(9, MakeLocation(9, 99)), UB_LOCK_SUCCESS);

    EXPECT_EQ(lock.wait_queue[1].seq.load(std::memory_order_acquire), UB_WAIT_TIMEOUT);
    EXPECT_EQ(lock.wait_queue[1].mode, UB_LOCK_I);
    EXPECT_EQ(lock.wait_queue[1].location.node_id, 0xFF);
    EXPECT_EQ(lock.wait_queue[2].seq.load(std::memory_order_acquire), UB_WAIT_WAITING);
    EXPECT_EQ(lock.wait_queue[2].mode, UB_LOCK_S);
    EXPECT_EQ(lock.waiting_count.load(std::memory_order_acquire), 1u);
}

TEST(UbDistributeLockApiTest, QueryHolderAndRebuildApisReturnErrorOnNullArgs)
{
    ub_rw_lock_t lock{};
    ub_location_t loc{1, 2};
    ub_lock_query_result_t query_result{};
    ub_lock_rebuild_info_t rebuild_info{};

    EXPECT_EQ(ub_rw_lock_query_holder(nullptr, &loc, &query_result), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_query_holder(&lock, nullptr, &query_result), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_query_holder(&lock, &loc, nullptr), UB_LOCK_ERROR);

    ub_rw_lock_t new_lock{};
    EXPECT_EQ(ub_rw_lock_rebuild(nullptr, &new_lock, &rebuild_info, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_rebuild(&lock, nullptr, &rebuild_info, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_rebuild(&lock, &new_lock, nullptr, &loc), UB_LOCK_ERROR);
    EXPECT_EQ(ub_rw_lock_rebuild(&lock, &new_lock, &rebuild_info, nullptr), UB_LOCK_ERROR);

    MOCKER_CPP(&DistributedLock::query_holder, ub_lock_result_t(*)(const ub_location_t &, ub_lock_query_result_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_query_holder(&lock, &loc, &query_result), UB_LOCK_SUCCESS);

    rebuild_info.query_results = &query_result;
    rebuild_info.query_result_count = 1;
    MOCKER_CPP(&DistributedLock::rebuild,
               ub_lock_result_t(*)(ub_rw_lock_t *, const ub_lock_rebuild_info_t &, const ub_location_t &))
        .stubs()
        .will(returnValue(UB_LOCK_SUCCESS));
    EXPECT_EQ(ub_rw_lock_rebuild(&lock, &new_lock, &rebuild_info, &loc), UB_LOCK_SUCCESS);
}

} // namespace ut
} // namespace ublock
