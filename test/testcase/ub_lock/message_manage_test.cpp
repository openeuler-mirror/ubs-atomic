/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/
#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#undef private
#include "UBShmTransport.h"

using ub_comm_queue::UBShmTransport;
extern UBShmTransport *g_transport;

#define MOCKER_CPP(api, TT) MOCKCPP_NS::mockAPI(#api, reinterpret_cast<TT>(api))

namespace ublock {
namespace ut {

class MessageManageTest : public ::testing::Test {
public:
    void SetUp() override
    {
        shm_ = new ub_rw_lock_t{};
        ResetLocalLockEntries();
        lock_ = new DistributedLock(shm_);
        if (g_transport == nullptr) {
            g_transport = new UBShmTransport();
        }
    }

    void TearDown() override
    {
        for (uint8_t i = 0; i < UB_MAX_NODES; ++i) {
            if (shm_->node_registry[i]) {
                delete reinterpret_cast<LocalLock *>(shm_->node_registry[i]);
                shm_->node_registry[i] = 0;
            }
        }
        delete lock_;
        delete shm_;
        lock_ = nullptr;
        shm_ = nullptr;
        delete g_transport;
        g_transport = nullptr;
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

TEST_F(MessageManageTest, CreateMassageTest)
{
    ub_location_t loc{1, 2};
    local_msg_body_t body = {.tid = 1, .addr = nullptr, .type = UB_RELEASE, .mode = UB_LOCK_SX};
    message_t *msg = lock_->create_message(loc, 1, body);
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->header.src_node_id, 1);
    delete[] msg->body;
    delete msg;
}

TEST_F(MessageManageTest, NotifyWaitersTest)
{
    ub_waiter_t waiter{};
    ub_location_t loc{1, 2};
    ub_lock_result_t res;

    waiter.location = ub_location_t{.tid = 9, .node_id = 3};
    waiter.mode = UB_LOCK_S;
    MOCKER_CPP(&UBShmTransport::send, int (*)(const message_t *)).defaults().will(returnValue(-1));
    res = lock_->notify_waiters(waiter, loc);
    EXPECT_EQ(res, UB_LOCK_ERROR);

    MOCKER_CPP(&UBShmTransport::send, int (*)(const message_t *)).stubs().will(returnValue(0));
    res = lock_->notify_waiters(waiter, loc);
    EXPECT_EQ(res, UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::create_message,
               message_t * (*)(const ub_location_t &, uint8_t, const local_msg_body_t &))
        .stubs()
        .will(returnValue((message_t *)nullptr));
    res = lock_->notify_waiters(waiter, loc);
    EXPECT_EQ(res, UB_LOCK_ERROR);

    waiter.location.node_id = loc.node_id;
    res = lock_->notify_waiters(waiter, loc);
    EXPECT_EQ(res, UB_LOCK_ERROR);
    GlobalMockObject::verify();
}

TEST_F(MessageManageTest, MessageProcessReleaseWithNullLocalLock)
{
    local_msg_body_t body{};
    body.tid = 0;
    body.addr = nullptr;
    body.type = UB_RELEASE;
    body.mode = UB_LOCK_S;

    message_t msg{};
    msg.header.dest_node_id = 1;
    msg.header.body_length = sizeof(local_msg_body_t);
    msg.body = new char[sizeof(local_msg_body_t)];
    std::memcpy(msg.body, &body, sizeof(local_msg_body_t));

    message_process_thread_func(&msg, nullptr);

    delete[] msg.body;
}

TEST_F(MessageManageTest, MessageProcessRejectsShortBody)
{
    char short_body[sizeof(local_msg_body_t) - 1] = {};
    message_t msg{};
    msg.header.body_length = sizeof(short_body);
    msg.body = short_body;

    message_process_thread_func(&msg, nullptr);
}

TEST_F(MessageManageTest, MessageProcessReleaseWithNullUbLock)
{
    auto *local_lock = new LocalLock(nullptr);
    local_lock->ub_lock_ptr_ = nullptr;

    local_msg_body_t body{};
    body.addr = local_lock;
    body.type = UB_RELEASE;
    body.mode = UB_LOCK_S;

    message_t msg{};
    msg.header.dest_node_id = 1;
    msg.header.body_length = sizeof(local_msg_body_t);
    msg.body = new char[sizeof(local_msg_body_t)];
    std::memcpy(msg.body, &body, sizeof(local_msg_body_t));

    message_process_thread_func(&msg, nullptr);

    delete[] msg.body;
    delete local_lock;
}

TEST_F(MessageManageTest, MessageProcessReleaseWhenRemoteReleaseInProgress)
{
    auto *local_lock = new LocalLock(shm_);
    local_lock->remote_release_in_progress_.store(true, std::memory_order_release);

    local_msg_body_t body{};
    body.addr = local_lock;
    body.type = UB_RELEASE;
    body.mode = UB_LOCK_S;

    message_t msg{};
    msg.header.dest_node_id = 1;
    msg.header.body_length = sizeof(local_msg_body_t);
    msg.body = new char[sizeof(local_msg_body_t)];
    std::memcpy(msg.body, &body, sizeof(local_msg_body_t));

    message_process_thread_func(&msg, nullptr);

    delete[] msg.body;
    delete local_lock;
}

TEST_F(MessageManageTest, MessageProcessReleaseWhenLocalLockHeld)
{
    auto *local_lock = new LocalLock(shm_);
    local_lock->lock_word.store(1, std::memory_order_release);

    local_msg_body_t body{};
    body.addr = local_lock;
    body.type = UB_RELEASE;
    body.mode = UB_LOCK_S;

    message_t msg{};
    msg.header.dest_node_id = 1;
    msg.header.body_length = sizeof(local_msg_body_t);
    msg.body = new char[sizeof(local_msg_body_t)];
    std::memcpy(msg.body, &body, sizeof(local_msg_body_t));

    message_process_thread_func(&msg, nullptr);

    delete[] msg.body;
    delete local_lock;
}

TEST_F(MessageManageTest, MessageProcessReleaseTriggersDelayUnlock)
{
    auto *local_lock = new LocalLock(shm_);
    local_lock->local_is_reserve_lock.store(UB_LOCK_S, std::memory_order_release);
    shm_->reserve_lock_owner.store(3, std::memory_order_release);
    shm_->lock_word.store(X_LOCK_DECR - 1, std::memory_order_release);

    local_msg_body_t body{};
    body.addr = local_lock;
    body.type = UB_RELEASE;
    body.mode = UB_LOCK_S;

    message_t msg{};
    msg.header.dest_node_id = 3;
    msg.header.body_length = sizeof(local_msg_body_t);
    msg.body = new char[sizeof(local_msg_body_t)];
    std::memcpy(msg.body, &body, sizeof(local_msg_body_t));

    message_process_thread_func(&msg, nullptr);

    EXPECT_EQ(local_lock->local_is_reserve_lock.load(), UB_LOCK_S);
    EXPECT_EQ(shm_->reserve_lock_owner.load(), UB_LOCK_I);
    EXPECT_EQ(shm_->lock_word.load(), X_LOCK_DECR - 1);

    delete[] msg.body;
    delete local_lock;
}

TEST_F(MessageManageTest, NotifyUnlockTest)
{
    local_msg_body_t msg_body{};
    ub_location_t loc{};
    ub_lock_result_t res;

    MOCKER_CPP(&UBShmTransport::send, int (*)(const message_t *)).defaults().will(returnValue(-1));
    res = lock_->notify_unlock(loc, 0, msg_body);
    EXPECT_EQ(res, UB_LOCK_ERROR);

    MOCKER_CPP(&UBShmTransport::send, int (*)(const message_t *)).stubs().will(returnValue(0));
    res = lock_->notify_unlock(loc, 0, msg_body);
    EXPECT_EQ(res, UB_LOCK_SUCCESS);

    MOCKER_CPP(&DistributedLock::create_message,
               message_t * (*)(const ub_location_t &, uint8_t, const local_msg_body_t &))
        .stubs()
        .will(returnValue((message_t *)nullptr));
    res = lock_->notify_unlock(loc, 0, msg_body);
    EXPECT_EQ(res, UB_LOCK_ERROR);
    GlobalMockObject::verify();
}

TEST_F(MessageManageTest, MessageProcessThreadTest)
{
    ub_location_t loc{1, 2};
    local_msg_body_t body = {.tid = 1, .addr = nullptr, .type = UB_GRANT, .mode = UB_LOCK_SX};
    message_t *msg = lock_->create_message(loc, 1, body);
    ASSERT_NE(msg, nullptr);

    message_process_thread_func(nullptr, nullptr);
    message_process_thread_func(msg, nullptr);
    delete[] msg->body;
    delete msg;

    body.type = UB_RELEASE;
    msg = lock_->create_message(loc, 1, body);
    ASSERT_NE(msg, nullptr);
    message_process_thread_func(msg, nullptr);
    delete[] msg->body;
    delete msg;

    body.type = UB_UNKNOWN;
    msg = lock_->create_message(loc, 1, body);
    ASSERT_NE(msg, nullptr);
    message_process_thread_func(msg, nullptr);
    delete[] msg->body;
    delete msg;
}

TEST_F(MessageManageTest, MessageProcessGrantNotifiesCtx)
{
    ub_location_t loc{1, 2};
    local_wait_ctx_t ctx{};
    local_msg_body_t body = {.tid = 1, .addr = &ctx, .type = UB_GRANT, .mode = UB_LOCK_S};
    message_t *msg = lock_->create_message(loc, 1, body);
    ASSERT_NE(msg, nullptr);

    message_process_thread_func(msg, nullptr);
    EXPECT_FALSE(ctx.notified);

    delete[] msg->body;
    delete msg;
}

TEST_F(MessageManageTest, RegisterMessageProcessTest)
{
    register_message_process_func();
    delete g_transport;
    g_transport = nullptr;
    register_message_process_func();
}

} // namespace ut
} // namespace ublock
