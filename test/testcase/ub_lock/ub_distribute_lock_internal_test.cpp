#include <sys/syscall.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#undef private

// ub_distribute_lock.cpp 中的一批内部 helper 定义在 ublock 命名空间但未在头文件声明，
// 这里显式前置声明，以便直接对其纯逻辑分支做单元测试。
namespace ublock {
bool is_valid_rebuild_mode(ub_lock_mode_t mode);
const char *lock_mode_name(ub_lock_mode_t mode);
uint32_t owner_node(uint64_t owner);
int32_t owner_tid(uint64_t owner);
bool has_valid_owner(uint64_t owner);
std::string format_owner(uint64_t owner);
bool is_valid_query_result_entry(const ub_lock_query_result_t &entry);
bool is_valid_delayed_release_rebuild_state(const ub_lock_query_result_t *reserve_entry,
                                            const ub_lock_query_result_t *x_holder,
                                            const ub_lock_query_result_t *sx_holder, uint32_t shared_count);
void replay_delayed_release_state(ub_rw_lock_t *lock, const ub_lock_query_result_t *reserve_entry,
                                  uint32_t &shared_bitmap, uint32_t &shared_count,
                                  const ub_lock_query_result_t *sx_holder, uint64_t reserve_owner);
void reset_shared_lock_for_rebuild(ub_rw_lock_t *lock);
bool try_begin_shared_rebuild_init(ub_rw_lock_t *lock);
void wait_shared_rebuild_init_done(ub_rw_lock_t *lock);
void clear_node_registry_for_rebuild(ub_rw_lock_t *lock);
std::shared_ptr<LocalLock> switch_local_lock_binding(ub_rw_lock_t *old_lock, ub_rw_lock_t *new_lock);
const ub_lock_query_result_t *find_query_result_for_node(const ub_lock_rebuild_info_t &rebuild_info, uint8_t node_id);
bool timeout_is_local_wait(const char *reason);
bool timeout_is_same_node_wait(const char *reason);
const char *timeout_wait_scope_name(const char *reason);
} // namespace ublock

namespace ublock {
namespace ut {

static int32_t CurTid()
{
    return static_cast<int32_t>(::syscall(SYS_gettid));
}

static ub_lock_query_result_t MakeEntry(uint8_t node_id, ub_lock_mode_t held_mode, int32_t holder_tid,
                                        uint32_t recursive_count, bool has_shared_ref, ub_lock_mode_t reserve_mode)
{
    ub_lock_query_result_t e{};
    e.node_id = node_id;
    e.held_mode = held_mode;
    e.holder_tid = holder_tid;
    e.recursive_count = recursive_count;
    e.has_shared_ref = has_shared_ref;
    e.reserve_mode = reserve_mode;
    return e;
}

// ---------------------------------------------------------------------------
// 纯 helper 函数
// ---------------------------------------------------------------------------

TEST(UbDistLockHelper, IsValidRebuildMode)
{
    EXPECT_TRUE(is_valid_rebuild_mode(UB_LOCK_I));
    EXPECT_TRUE(is_valid_rebuild_mode(UB_LOCK_S));
    EXPECT_TRUE(is_valid_rebuild_mode(UB_LOCK_SX));
    EXPECT_TRUE(is_valid_rebuild_mode(UB_LOCK_X));
    EXPECT_FALSE(is_valid_rebuild_mode(static_cast<ub_lock_mode_t>(99)));
}

TEST(UbDistLockHelper, LockModeNameAllCases)
{
    EXPECT_STREQ(lock_mode_name(UB_LOCK_S), "S");
    EXPECT_STREQ(lock_mode_name(UB_LOCK_SX), "SX");
    EXPECT_STREQ(lock_mode_name(UB_LOCK_X), "X");
    EXPECT_STREQ(lock_mode_name(UB_LOCK_I), "I");
    EXPECT_STREQ(lock_mode_name(static_cast<ub_lock_mode_t>(42)), "UNKNOWN");
}

TEST(UbDistLockHelper, OwnerAccessorsAndFormat)
{
    EXPECT_FALSE(has_valid_owner(LOCK_INVALID_OWNER));
    EXPECT_TRUE(has_valid_owner(make_global_owner(3, 7)));
    EXPECT_EQ(owner_node(LOCK_INVALID_OWNER), static_cast<uint32_t>(UB_MAX_NODES));
    EXPECT_EQ(owner_tid(LOCK_INVALID_OWNER), 0);
    EXPECT_EQ(owner_node(make_global_owner(3, 7)), 3u);
    EXPECT_EQ(owner_tid(make_global_owner(3, 7)), 7);
    EXPECT_EQ(format_owner(LOCK_INVALID_OWNER), "none");
    EXPECT_NE(format_owner(make_global_owner(3, 7)).find("node=3"), std::string::npos);
}

TEST(UbDistLockHelper, TimeoutReasonClassification)
{
    EXPECT_TRUE(timeout_is_local_wait("local_x"));
    EXPECT_FALSE(timeout_is_local_wait(nullptr));
    EXPECT_FALSE(timeout_is_local_wait("global"));
    EXPECT_TRUE(timeout_is_same_node_wait("follower"));
    EXPECT_FALSE(timeout_is_same_node_wait(nullptr));
    EXPECT_STREQ(timeout_wait_scope_name("local_x"), "local");
    EXPECT_STREQ(timeout_wait_scope_name("follower"), "same_node");
    EXPECT_STREQ(timeout_wait_scope_name("global"), "global");
    EXPECT_STREQ(timeout_wait_scope_name(nullptr), "global");
}

TEST(UbDistLockHelper, IsValidQueryResultEntryAllModes)
{
    // I: holder_tid==0 && recursive==0 && !has_shared_ref
    EXPECT_TRUE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_I, 5, 0, false, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_I, 0, 1, false, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_I, 0, 0, true, UB_LOCK_I)));
    // S: holder_tid==0 && recursive==0 && has_shared_ref
    EXPECT_TRUE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_S, 0, 0, true, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_S, 0, 0, false, UB_LOCK_I)));
    // X: holder_tid!=0 && recursive>0 && !has_shared_ref
    EXPECT_TRUE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_X, 5, 1, false, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_X, 0, 1, false, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_X, 5, 0, false, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_X, 5, 1, true, UB_LOCK_I)));
    // SX: holder_tid!=0 && recursive>0
    EXPECT_TRUE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_SX, 5, 2, false, UB_LOCK_I)));
    EXPECT_TRUE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_SX, 5, 2, true, UB_LOCK_I)));
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, UB_LOCK_SX, 0, 2, false, UB_LOCK_I)));
    // default
    EXPECT_FALSE(is_valid_query_result_entry(MakeEntry(1, static_cast<ub_lock_mode_t>(77), 0, 0, false, UB_LOCK_I)));
}

TEST(UbDistLockHelper, IsValidDelayedReleaseRebuildState)
{
    ub_lock_query_result_t x_holder = MakeEntry(2, UB_LOCK_X, 5, 1, false, UB_LOCK_I);
    ub_lock_query_result_t sx_holder = MakeEntry(3, UB_LOCK_SX, 6, 1, false, UB_LOCK_I);

    // reserve_entry == nullptr -> true
    EXPECT_TRUE(is_valid_delayed_release_rebuild_state(nullptr, &x_holder, &sx_holder, 3));

    // reserve X: 需要 x_holder==null && sx_holder==null && shared_count==0
    ub_lock_query_result_t reserve_x = MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_X);
    EXPECT_TRUE(is_valid_delayed_release_rebuild_state(&reserve_x, nullptr, nullptr, 0));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_x, &x_holder, nullptr, 0));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_x, nullptr, &sx_holder, 0));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_x, nullptr, nullptr, 1));

    // reserve SX: 需要 x_holder==null && sx_holder==null
    ub_lock_query_result_t reserve_sx = MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_SX);
    EXPECT_TRUE(is_valid_delayed_release_rebuild_state(&reserve_sx, nullptr, nullptr, 5));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_sx, &x_holder, nullptr, 0));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_sx, nullptr, &sx_holder, 0));

    // reserve S: 需要 x_holder==null
    ub_lock_query_result_t reserve_s = MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_S);
    EXPECT_TRUE(is_valid_delayed_release_rebuild_state(&reserve_s, nullptr, &sx_holder, 2));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_s, &x_holder, nullptr, 0));

    // default -> false
    ub_lock_query_result_t reserve_bad = MakeEntry(1, UB_LOCK_I, 0, 0, false, static_cast<ub_lock_mode_t>(88));
    EXPECT_FALSE(is_valid_delayed_release_rebuild_state(&reserve_bad, nullptr, nullptr, 0));
}

TEST(UbDistLockHelper, FindQueryResultForNode)
{
    ub_lock_query_result_t entries[3] = {
        MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I),
        MakeEntry(4, UB_LOCK_X, 9, 1, false, UB_LOCK_I),
        MakeEntry(7, UB_LOCK_S, 0, 0, true, UB_LOCK_I),
    };
    ub_lock_rebuild_info_t info{entries, 3};
    ASSERT_NE(find_query_result_for_node(info, 4), nullptr);
    EXPECT_EQ(find_query_result_for_node(info, 4)->holder_tid, 9);
    EXPECT_EQ(find_query_result_for_node(info, 99), nullptr);

    ub_lock_rebuild_info_t empty{entries, 0};
    EXPECT_EQ(find_query_result_for_node(empty, 1), nullptr);
}

TEST(UbDistLockHelper, ReplayDelayedReleaseStateNullReserveIsNoop)
{
    ub_rw_lock_t lock{};
    lock.lock_word.store(1234, std::memory_order_release);
    uint32_t bitmap = 0;
    uint32_t count = 0;
    replay_delayed_release_state(&lock, nullptr, bitmap, count, nullptr, 0);
    EXPECT_EQ(lock.lock_word.load(), 1234);
    EXPECT_EQ(bitmap, 0u);
    EXPECT_EQ(count, 0u);
}

TEST(UbDistLockHelper, ReplayDelayedReleaseStateX)
{
    ub_rw_lock_t lock{};
    uint32_t bitmap = 0;
    uint32_t count = 0;
    ub_lock_query_result_t reserve = MakeEntry(2, UB_LOCK_I, 0, 0, false, UB_LOCK_X);
    uint64_t owner = make_global_owner(2, 55);
    replay_delayed_release_state(&lock, &reserve, bitmap, count, nullptr, owner);
    EXPECT_EQ(lock.reserve_lock_owner.load(), owner);
    EXPECT_EQ(lock.lock_word.load(), 0);
    EXPECT_EQ(lock.lock_owner_x.load(), owner);
    EXPECT_EQ(lock.x_recursive.load(), 1u);
}

TEST(UbDistLockHelper, ReplayDelayedReleaseStateSX)
{
    ub_rw_lock_t lock{};
    uint32_t bitmap = 0;
    uint32_t count = 3;
    ub_lock_query_result_t reserve = MakeEntry(2, UB_LOCK_I, 0, 0, false, UB_LOCK_SX);
    uint64_t owner = make_global_owner(2, 66);
    replay_delayed_release_state(&lock, &reserve, bitmap, count, nullptr, owner);
    EXPECT_EQ(lock.lock_word.load(), static_cast<int32_t>(X_LOCK_HALF_DECR - 3));
    EXPECT_EQ(lock.lock_owner_sx.load(), owner);
    EXPECT_EQ(lock.sx_recursive.load(), 1u);
}

TEST(UbDistLockHelper, ReplayDelayedReleaseStateSWithAndWithoutSxHolder)
{
    // 无 sx_holder: lock_word = X_LOCK_DECR - shared_count
    {
        ub_rw_lock_t lock{};
        uint32_t bitmap = 0;
        uint32_t count = 1;
        ub_lock_query_result_t reserve = MakeEntry(3, UB_LOCK_I, 0, 0, false, UB_LOCK_S);
        replay_delayed_release_state(&lock, &reserve, bitmap, count, nullptr, make_global_owner(3, 1));
        EXPECT_EQ(bitmap & (1u << 3), 1u << 3);
        EXPECT_EQ(count, 2u); // 原有 1 + 新增 node3
        EXPECT_EQ(lock.lock_word.load(), static_cast<int32_t>(X_LOCK_DECR - 2));
    }
    // 有 sx_holder: lock_word = X_LOCK_HALF_DECR - shared_count
    {
        ub_rw_lock_t lock{};
        uint32_t bitmap = 0;
        uint32_t count = 0;
        ub_lock_query_result_t reserve = MakeEntry(4, UB_LOCK_I, 0, 0, false, UB_LOCK_S);
        ub_lock_query_result_t sx_holder = MakeEntry(5, UB_LOCK_SX, 6, 1, false, UB_LOCK_I);
        replay_delayed_release_state(&lock, &reserve, bitmap, count, &sx_holder, make_global_owner(4, 1));
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(lock.lock_word.load(), static_cast<int32_t>(X_LOCK_HALF_DECR - 1));
    }
    // bit 已置位: 不重复增加 shared_count
    {
        ub_rw_lock_t lock{};
        uint32_t bitmap = (1u << 6);
        uint32_t count = 1;
        ub_lock_query_result_t reserve = MakeEntry(6, UB_LOCK_I, 0, 0, false, UB_LOCK_S);
        replay_delayed_release_state(&lock, &reserve, bitmap, count, nullptr, make_global_owner(6, 1));
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(lock.lock_word.load(), static_cast<int32_t>(X_LOCK_DECR - 1));
    }
}

TEST(UbDistLockHelper, ReplayDelayedReleaseStateDefaultModeOnlySetsReserve)
{
    ub_rw_lock_t lock{};
    lock.lock_word.store(4321, std::memory_order_release);
    uint32_t bitmap = 0;
    uint32_t count = 0;
    ub_lock_query_result_t reserve = MakeEntry(2, UB_LOCK_I, 0, 0, false, static_cast<ub_lock_mode_t>(66));
    uint64_t owner = make_global_owner(2, 77);
    replay_delayed_release_state(&lock, &reserve, bitmap, count, nullptr, owner);
    EXPECT_EQ(lock.reserve_lock_owner.load(), owner);
    EXPECT_EQ(lock.lock_word.load(), 4321); // default 分支不修改 lock_word
}

TEST(UbDistLockHelper, ResetSharedLockForRebuildClearsState)
{
    ub_rw_lock_t lock{};
    lock.lock_word.store(11, std::memory_order_release);
    lock.waiting_count.store(5, std::memory_order_release);
    lock.queue_head.store(2, std::memory_order_release);
    lock.queue_tail.store(3, std::memory_order_release);
    lock.shared_owner_bitmap.store(0xF, std::memory_order_release);
    lock.lock_owner_x.store(make_global_owner(1, 1), std::memory_order_release);
    lock.lock_owner_sx.store(make_global_owner(2, 2), std::memory_order_release);
    lock.reserve_lock_owner.store(make_global_owner(3, 3), std::memory_order_release);
    lock.sx_recursive.store(4, std::memory_order_release);
    lock.x_recursive.store(6, std::memory_order_release);

    reset_shared_lock_for_rebuild(&lock);

    EXPECT_EQ(lock.lock_word.load(), X_LOCK_DECR);
    EXPECT_EQ(lock.waiting_count.load(), 0u);
    EXPECT_EQ(lock.queue_head.load(), 0u);
    EXPECT_EQ(lock.queue_tail.load(), 0u);
    EXPECT_EQ(lock.shared_owner_bitmap.load(), 0u);
    EXPECT_EQ(lock.lock_owner_x.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.lock_owner_sx.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.reserve_lock_owner.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(lock.sx_recursive.load(), 0u);
    EXPECT_EQ(lock.x_recursive.load(), 0u);
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        EXPECT_EQ(lock.wait_queue[i].seq.load(), UB_WAIT_EMPTY);
        EXPECT_EQ(lock.wait_queue[i].mode, UB_LOCK_I);
    }
}

TEST(UbDistLockHelper, TryBeginSharedRebuildInitOnceOnly)
{
    ub_rw_lock_t lock{}; // is_inited == 0
    EXPECT_TRUE(try_begin_shared_rebuild_init(&lock));
    EXPECT_EQ(lock.is_inited.load(), -1); // REBUILD_INIT_IN_PROGRESS
    EXPECT_FALSE(try_begin_shared_rebuild_init(&lock));
}

TEST(UbDistLockHelper, WaitSharedRebuildInitDoneReturnsImmediatelyWhenNotInProgress)
{
    ub_rw_lock_t lock{};
    lock.is_inited.store(1, std::memory_order_release);
    wait_shared_rebuild_init_done(&lock);
    EXPECT_EQ(lock.is_inited.load(), 1);
}

TEST(UbDistLockHelper, WaitSharedRebuildInitDoneSpinsUntilDone)
{
    ub_rw_lock_t lock{};
    lock.is_inited.store(-1, std::memory_order_release);
    std::thread t([&lock] { wait_shared_rebuild_init_done(&lock); });
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    lock.is_inited.store(2, std::memory_order_release);
    t.join();
    EXPECT_EQ(lock.is_inited.load(), 2);
}

TEST(UbDistLockHelper, ClearNodeRegistryForRebuild)
{
    ub_rw_lock_t lock{};
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        lock.node_registry[i] = static_cast<uintptr_t>(i + 100);
    }
    clear_node_registry_for_rebuild(&lock);
    for (uint32_t i = 0; i < UB_MAX_NODES; ++i) {
        EXPECT_EQ(lock.node_registry[i], 0u);
    }
}

// ---------------------------------------------------------------------------
// switch_local_lock_binding
// ---------------------------------------------------------------------------

TEST(UbDistLockSwitchBinding, NullArgsReturnNull)
{
    ub_rw_lock_t a{};
    ub_rw_lock_t b{};
    EXPECT_EQ(switch_local_lock_binding(nullptr, &b), nullptr);
    EXPECT_EQ(switch_local_lock_binding(&a, nullptr), nullptr);
}

TEST(UbDistLockSwitchBinding, SameLockRegisteredRebinds)
{
    auto *lk = new ub_rw_lock_t{};
    auto ll = std::make_shared<LocalLock>(lk);
    register_local_lock(lk, ll);
    auto got = switch_local_lock_binding(lk, lk);
    EXPECT_EQ(got, ll);
    EXPECT_EQ(got->ub_lock_ptr_, lk);
    (void)unregister_local_lock(lk);
    delete lk;
}

TEST(UbDistLockSwitchBinding, SameLockNotRegisteredReturnsNull)
{
    auto *lk = new ub_rw_lock_t{};
    EXPECT_EQ(switch_local_lock_binding(lk, lk), nullptr);
    delete lk;
}

TEST(UbDistLockSwitchBinding, MoveOldToNewWhenNewAbsent)
{
    auto *old_lock = new ub_rw_lock_t{};
    auto *new_lock = new ub_rw_lock_t{};
    auto ll = std::make_shared<LocalLock>(old_lock);
    register_local_lock(old_lock, ll);
    auto got = switch_local_lock_binding(old_lock, new_lock);
    EXPECT_EQ(got, ll);
    EXPECT_EQ(got->ub_lock_ptr_, new_lock);
    EXPECT_EQ(lookup_local_lock(old_lock), nullptr);
    EXPECT_EQ(lookup_local_lock(new_lock), ll);
    (void)unregister_local_lock(new_lock);
    delete old_lock;
    delete new_lock;
}

TEST(UbDistLockSwitchBinding, OldAbsentNewPresentReturnsNewBinding)
{
    auto *old_lock = new ub_rw_lock_t{};
    auto *new_lock = new ub_rw_lock_t{};
    auto ll = std::make_shared<LocalLock>(new_lock);
    register_local_lock(new_lock, ll);
    auto got = switch_local_lock_binding(old_lock, new_lock);
    EXPECT_EQ(got, ll);
    EXPECT_EQ(got->ub_lock_ptr_, new_lock);
    (void)unregister_local_lock(new_lock);
    delete old_lock;
    delete new_lock;
}

TEST(UbDistLockSwitchBinding, BothAbsentReturnsNull)
{
    auto *old_lock = new ub_rw_lock_t{};
    auto *new_lock = new ub_rw_lock_t{};
    EXPECT_EQ(switch_local_lock_binding(old_lock, new_lock), nullptr);
    delete old_lock;
    delete new_lock;
}

TEST(UbDistLockSwitchBinding, BothPresentMovesOldAndOverwritesNew)
{
    auto *old_lock = new ub_rw_lock_t{};
    auto *new_lock = new ub_rw_lock_t{};
    auto old_ll = std::make_shared<LocalLock>(old_lock);
    auto new_ll = std::make_shared<LocalLock>(new_lock);
    register_local_lock(old_lock, old_ll);
    register_local_lock(new_lock, new_ll);
    auto got = switch_local_lock_binding(old_lock, new_lock);
    EXPECT_EQ(got, old_ll);
    EXPECT_EQ(got->ub_lock_ptr_, new_lock);
    EXPECT_EQ(lookup_local_lock(new_lock), old_ll);
    EXPECT_EQ(lookup_local_lock(old_lock), nullptr);
    (void)unregister_local_lock(new_lock);
    delete old_lock;
    delete new_lock;
}

// ---------------------------------------------------------------------------
// DistributedLock 成员方法：query_holder / unlock_sx / recover / rebuild
// ---------------------------------------------------------------------------

class UbDistLockMemberTest : public ::testing::Test {
public:
    void SetUp() override
    {
        // 其他测试文件（如 api 测试）可能对 query_holder/verify_param 等打桩后未及时清理，
        // 这里先重置全局 mock 状态，保证本 fixture 的用例调用到真实实现，与执行顺序无关。
        mockcpp::GlobalMockObject::reset();
        shm_ = new ub_rw_lock_t{};
        for (uint8_t i = 0; i < UB_MAX_NODES; ++i) {
            shm_->node_registry[i] = 0;
        }
        lock_ = new DistributedLock(shm_);
    }
    void TearDown() override
    {
        (void)unregister_local_lock(shm_);
        delete lock_;
        lock_ = nullptr;
        delete shm_;
        shm_ = nullptr;
        mockcpp::GlobalMockObject::verify();
    }

protected:
    ub_rw_lock_t *shm_{};
    DistributedLock *lock_{};
};

TEST_F(UbDistLockMemberTest, QueryHolderInvalidNodeReturnsError)
{
    ub_location_t loc{1, 0xFF};
    ub_lock_query_result_t res{};
    EXPECT_EQ(lock_->query_holder(loc, res), UB_LOCK_ERROR);
}

TEST_F(UbDistLockMemberTest, QueryHolderNoLocalLockReturnsIdle)
{
    ub_location_t loc{1, 3};
    ub_lock_query_result_t res{};
    EXPECT_EQ(lock_->query_holder(loc, res), UB_LOCK_SUCCESS);
    EXPECT_EQ(res.node_id, 3);
    EXPECT_EQ(res.held_mode, UB_LOCK_I);
    EXPECT_FALSE(res.has_shared_ref);
    EXPECT_EQ(res.reserve_mode, UB_LOCK_I);
}

TEST_F(UbDistLockMemberTest, QueryHolderXHolder)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    ll->hold_global.store(true, std::memory_order_release);
    ll->lock_x_owner.store(42, std::memory_order_release);
    ll->x_recursive_.store(3, std::memory_order_release);

    ub_location_t loc{1, 3};
    ub_lock_query_result_t res{};
    EXPECT_EQ(lock_->query_holder(loc, res), UB_LOCK_SUCCESS);
    EXPECT_EQ(res.held_mode, UB_LOCK_X);
    EXPECT_EQ(res.holder_tid, 42);
    EXPECT_EQ(res.recursive_count, 3u);
}

TEST_F(UbDistLockMemberTest, QueryHolderXZeroRecursiveBumpsToOne)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    ll->hold_global.store(true, std::memory_order_release);
    ll->lock_x_owner.store(8, std::memory_order_release);
    ll->x_recursive_.store(0, std::memory_order_release);

    ub_location_t loc{1, 3};
    ub_lock_query_result_t res{};
    EXPECT_EQ(lock_->query_holder(loc, res), UB_LOCK_SUCCESS);
    EXPECT_EQ(res.held_mode, UB_LOCK_X);
    EXPECT_EQ(res.recursive_count, 1u);
}

TEST_F(UbDistLockMemberTest, QueryHolderSxHolder)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    ll->hold_global.store(true, std::memory_order_release);
    ll->lock_x_owner.store(0, std::memory_order_release);
    ll->lock_sx_owner.store(7, std::memory_order_release);
    ll->sx_recursive_.store(0, std::memory_order_release);

    ub_location_t loc{1, 3};
    ub_lock_query_result_t res{};
    EXPECT_EQ(lock_->query_holder(loc, res), UB_LOCK_SUCCESS);
    EXPECT_EQ(res.held_mode, UB_LOCK_SX);
    EXPECT_EQ(res.holder_tid, 7);
    EXPECT_EQ(res.recursive_count, 1u);
}

TEST_F(UbDistLockMemberTest, QueryHolderSharedRef)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    ll->hold_global.store(false, std::memory_order_release);
    ll->lock_x_owner.store(0, std::memory_order_release);
    ll->lock_sx_owner.store(0, std::memory_order_release);
    ll->global_read_ref_count_.store(2, std::memory_order_release);
    ll->local_is_reserve_lock.store(UB_LOCK_S, std::memory_order_release);

    ub_location_t loc{1, 3};
    ub_lock_query_result_t res{};
    EXPECT_EQ(lock_->query_holder(loc, res), UB_LOCK_SUCCESS);
    EXPECT_TRUE(res.has_shared_ref);
    EXPECT_EQ(res.held_mode, UB_LOCK_S);
    EXPECT_EQ(res.reserve_mode, UB_LOCK_S);
}

TEST_F(UbDistLockMemberTest, UnlockSxInvalidNodeReturnsError)
{
    ub_lock_policy_t policy{1000, false, false};
    ub_location_t loc{1, 0xFF};
    EXPECT_EQ(lock_->unlock_sx(policy, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistLockMemberTest, UnlockSxNoLocalLockReturnsError)
{
    ub_lock_policy_t policy{1000, false, false};
    ub_location_t loc{1, 3};
    EXPECT_EQ(lock_->unlock_sx(policy, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistLockMemberTest, UnlockSxOwnerMismatchReturnsError)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    ub_lock_policy_t policy{1000, false, false}; // allow_delay_release=false
    ub_location_t loc{1, 3};
    shm_->lock_owner_sx.store(make_global_owner(5, 999), std::memory_order_release);
    EXPECT_EQ(lock_->unlock_sx(policy, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistLockMemberTest, UnlockSxOwnerMatchReleasesGlobal)
{
    auto ll = std::make_shared<LocalLock>(shm_);
    register_local_lock(shm_, ll);
    ub_location_t loc{CurTid(), 3};
    ll->lock_sx_owner.store(loc.tid, std::memory_order_release);
    shm_->lock_owner_sx.store(make_global_owner(loc.node_id, loc.tid), std::memory_order_release);
    shm_->lock_word.store(X_LOCK_HALF_DECR, std::memory_order_release);
    shm_->waiting_count.store(0, std::memory_order_release);

    ub_lock_policy_t policy{1000, false, false}; // 非递归 -> release_global
    (void)lock_->unlock_sx(policy, loc);

    EXPECT_EQ(shm_->lock_owner_sx.load(), LOCK_INVALID_OWNER);
    EXPECT_EQ(shm_->sx_recursive.load(), 0u);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_HALF_DECR + X_LOCK_HALF_DECR);
}

TEST_F(UbDistLockMemberTest, RecoverInvalidProcessIdReturnsError)
{
    ub_location_t loc{CurTid(), 1};
    EXPECT_EQ(lock_->recover(UB_MAX_NODES, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistLockMemberTest, RecoverSharedSxSInvalidOwnerResets)
{
    // lock_word 落在 (0, X_LOCK_HALF_DECR) -> recover_shared_sx_s
    shm_->lock_word.store(X_LOCK_HALF_DECR - 1, std::memory_order_release);
    shm_->lock_owner_sx.store(LOCK_INVALID_OWNER, std::memory_order_release);
    ub_location_t loc{CurTid(), 1};
    EXPECT_EQ(lock_->recover(1, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->sx_recursive.load(), 0u);
    EXPECT_EQ(shm_->reserve_lock_owner.load(), LOCK_INVALID_OWNER);
}

TEST_F(UbDistLockMemberTest, RecoverSharedSxSValidOwnerSamePidResets)
{
    shm_->lock_word.store(X_LOCK_HALF_DECR - 1, std::memory_order_release);
    shm_->lock_owner_sx.store(make_global_owner(1, 1234), std::memory_order_release); // pid==1
    ub_location_t loc{CurTid(), 1};
    EXPECT_EQ(lock_->recover(1, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->sx_recursive.load(), 0u);
}

TEST_F(UbDistLockMemberTest, RecoverSharedSxSValidOwnerDifferentPidRestores)
{
    shm_->lock_word.store(X_LOCK_HALF_DECR - 2, std::memory_order_release);
    const uint64_t owner = make_global_owner(5, 1234); // pid==5 != process_id(1)
    shm_->lock_owner_sx.store(owner, std::memory_order_release);
    ub_location_t loc{CurTid(), 1};
    EXPECT_EQ(lock_->recover(1, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_owner_sx.load(), owner);
}

TEST_F(UbDistLockMemberTest, RebuildRejectsNullOldLock)
{
    ub_lock_rebuild_info_t info{};
    ub_location_t loc{CurTid(), 1};
    EXPECT_EQ(lock_->rebuild(nullptr, info, loc), UB_LOCK_ERROR);
}

TEST_F(UbDistLockMemberTest, RebuildRejectsInvalidNodeId)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_lock_query_result_t entry = MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I);
    ub_lock_rebuild_info_t info{&entry, 1};
    ub_location_t loc{CurTid(), 0xFF};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsZeroOrTooManyResults)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_lock_query_result_t entry = MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I);
    ub_location_t loc{CurTid(), 1};

    ub_lock_rebuild_info_t zero{&entry, 0};
    EXPECT_EQ(lock_->rebuild(old_lock, zero, loc), UB_LOCK_ERROR);

    ub_lock_rebuild_info_t too_many{&entry, UB_MAX_NODES + 1};
    EXPECT_EQ(lock_->rebuild(old_lock, too_many, loc), UB_LOCK_ERROR);

    ub_lock_rebuild_info_t null_results{nullptr, 1};
    EXPECT_EQ(lock_->rebuild(old_lock, null_results, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsMissingLocalResult)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_lock_query_result_t entry = MakeEntry(2, UB_LOCK_I, 0, 0, false, UB_LOCK_I);
    ub_lock_rebuild_info_t info{&entry, 1};
    ub_location_t loc{CurTid(), 1}; // 本地 node_id=1 不在结果中
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsInvalidEntry)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_location_t loc{CurTid(), 1};
    // X 模式但 holder_tid==0 -> is_valid_query_result_entry 失败
    ub_lock_query_result_t bad = MakeEntry(1, UB_LOCK_X, 0, 1, false, UB_LOCK_I);
    ub_lock_rebuild_info_t info{&bad, 1};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsDuplicateNode)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entries[2] = {
        MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I),
        MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I), // 重复 node_id
    };
    ub_lock_rebuild_info_t info{entries, 2};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsConflictingXHolder)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entries[2] = {
        MakeEntry(1, UB_LOCK_X, 5, 1, false, UB_LOCK_I),
        MakeEntry(2, UB_LOCK_X, 6, 1, false, UB_LOCK_I), // 第二个 X 冲突
    };
    ub_lock_rebuild_info_t info{entries, 2};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsConflictingSxHolder)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entries[2] = {
        MakeEntry(1, UB_LOCK_SX, 5, 1, false, UB_LOCK_I),
        MakeEntry(2, UB_LOCK_SX, 6, 1, false, UB_LOCK_I), // 第二个 SX 冲突
    };
    ub_lock_rebuild_info_t info{entries, 2};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsXCoexistWithShared)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entries[2] = {
        MakeEntry(1, UB_LOCK_X, 5, 1, false, UB_LOCK_I),
        MakeEntry(2, UB_LOCK_S, 0, 0, true, UB_LOCK_I), // X 与 shared 共存
    };
    ub_lock_rebuild_info_t info{entries, 2};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsInvalidDelayedReleaseCombination)
{
    auto *old_lock = new ub_rw_lock_t{};
    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entries[2] = {
        MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_X), // reserve X
        MakeEntry(2, UB_LOCK_X, 6, 1, false, UB_LOCK_I), // 同时存在 X holder -> 非法组合
    };
    ub_lock_rebuild_info_t info{entries, 2};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildRejectsMissingOldLocalLock)
{
    auto *old_lock = new ub_rw_lock_t{}; // 未注册 local lock
    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entry = MakeEntry(1, UB_LOCK_I, 0, 0, false, UB_LOCK_I);
    ub_lock_rebuild_info_t info{&entry, 1};
    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_ERROR);
    delete old_lock;
}

TEST_F(UbDistLockMemberTest, RebuildSucceedsWithXHolder)
{
    auto *old_lock = new ub_rw_lock_t{};
    auto ll = std::make_shared<LocalLock>(old_lock);
    register_local_lock(old_lock, ll);

    ub_location_t loc{CurTid(), 1};
    ub_lock_query_result_t entry = MakeEntry(1, UB_LOCK_X, loc.tid, 2, false, UB_LOCK_I);
    ub_lock_rebuild_info_t info{&entry, 1};
    shm_->is_inited.store(0, std::memory_order_release);

    EXPECT_EQ(lock_->rebuild(old_lock, info, loc), UB_LOCK_SUCCESS);
    EXPECT_EQ(shm_->lock_word.load(), 0);
    EXPECT_EQ(shm_->lock_owner_x.load(), make_global_owner(1, loc.tid));
    EXPECT_EQ(shm_->x_recursive.load(), 2u);
    EXPECT_EQ(shm_->is_inited.load(), 1);

    // switch_local_lock_binding 已将 ll 从 old_lock 迁移到 shm_，old_lock 不再持有登记
    EXPECT_EQ(lookup_local_lock(old_lock), nullptr);
    delete old_lock;
}

// ---------------------------------------------------------------------------
// C ABI 包装 ub_rw_lock_recover
// ---------------------------------------------------------------------------

TEST(UbDistLockRecoverApi, NullArgsReturnError)
{
    ub_location_t loc{1, 1};
    EXPECT_EQ(ub_rw_lock_recover(nullptr, 1, &loc), UB_LOCK_ERROR);
    ub_rw_lock_t lock{};
    EXPECT_EQ(ub_rw_lock_recover(&lock, 1, nullptr), UB_LOCK_ERROR);
}

TEST(UbDistLockRecoverApi, ValidCallDelegatesToImpl)
{
    ub_rw_lock_t lock{};
    lock.lock_word.store(X_LOCK_DECR, std::memory_order_release); // 无人持锁
    ub_location_t loc{1, 1};
    EXPECT_EQ(ub_rw_lock_recover(&lock, 1, &loc), UB_LOCK_SUCCESS);
}

} // namespace ut
} // namespace ublock
