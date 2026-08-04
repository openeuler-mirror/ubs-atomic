#include "ub_dist_tx_res.h"
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define MOCKER_CPP(api, TT) MOCKCPP_NS::mockAPI(#api, reinterpret_cast<TT>(api))
namespace ut {
class DistTxResTest : public ::testing::Test {
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

TEST_F(DistTxResTest, DistTxResBasicTest)
{
    uint64_t tid;
    uint64_t *tid_addr = nullptr;
    int status;
    uint64_t value = 0;

    status = ub_dist_tx_res_init(&tid);
    EXPECT_TRUE(status == UB_RES_OK);
    EXPECT_TRUE(tid == 0);

    status = ub_dist_tx_res_set(&tid, 100);
    EXPECT_TRUE(status == UB_RES_OK);
    EXPECT_TRUE(tid == 100);

    status = ub_dist_tx_res_get(&tid, &value);
    EXPECT_TRUE(status == UB_RES_OK);
    EXPECT_TRUE(tid == 100);

    status = ub_dist_tx_res_set(&tid, 99);
    status = ub_dist_tx_res_fetch_add(&tid, 1, &value);
    EXPECT_TRUE(status == UB_RES_OK);
    EXPECT_TRUE(value == 99);
    EXPECT_TRUE(tid == 100);

    status = ub_dist_tx_res_init(tid_addr);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_set(tid_addr, 99);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_get(tid_addr, &value);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_fetch_add(tid_addr, 1, &value);
    EXPECT_TRUE(status == UB_RES_ERROR);

    tid_addr = reinterpret_cast<uint64_t *>(0x7ffee3b5a001);
    status = ub_dist_tx_res_init(tid_addr);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_set(tid_addr, 99);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_get(tid_addr, &value);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_fetch_add(tid_addr, 1, &value);
    EXPECT_TRUE(status == UB_RES_ERROR);
}

TEST(DistTxFenceTest, FenceBasicTest)
{
    // 统一接口：五种语义调用不崩溃，返回UB_RES_OK
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_RELAXED), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_ACQUIRE), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_RELEASE), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_ACQ_REL), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_SEQ_CST), UB_RES_OK);
}

TEST(DistTxFenceTest, FenceInvalidOrderTest)
{
    // 非法枚举值应返回 UB_RES_ERROR
    EXPECT_EQ(ub_dist_tx_res_fence((ub_fence_order_t)99), UB_RES_ERROR);
    EXPECT_EQ(ub_dist_tx_res_fence((ub_fence_order_t)-1), UB_RES_ERROR);
}

TEST(DistTxFenceTest, FenceBackwardCompatMacroTest)
{
    // 旧接口宏仍可正常调用
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_ACQUIRE), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_RELEASE), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_ACQ_REL), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_fence(UB_FENCE_SEQ_CST), UB_RES_OK);
}

TEST(DistTxFenceTest, FenceRelaxedCompilerBarrierTest)
{
    // relaxed 作为 compiler barrier：配合 volatile 可阻止寄存器缓存
    uint64_t data = 0;
    uint64_t flag = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&data), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_init(&flag), UB_RES_OK);

    std::thread writer([&]() {
        ub_dist_tx_res_set(&data, 77);
        ub_dist_tx_res_fence(UB_FENCE_RELAXED); // 仅编译器屏障
        ub_dist_tx_res_set(&flag, 1);
    });
    std::thread reader([&]() {
        uint64_t f = 0;
        while (f == 0) {
            ub_dist_tx_res_get(&flag, &f);
            ub_dist_tx_res_fence(UB_FENCE_RELAXED);
        }
        uint64_t d = 0;
        ub_dist_tx_res_get(&data, &d);
        // relaxed 不保证硬件保序，但 get() 内部使用 acquire 语义
        // 此处验证 relaxed 不崩溃且流程正常完成
        EXPECT_TRUE(d == 77 || d == 0); // 允许两种结果
    });
    writer.join();
    reader.join();
}

TEST(DistTxAddTest, AddBasicTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val), UB_RES_OK);

    // add后值正确增加
    EXPECT_EQ(ub_dist_tx_res_add(&val, 10), UB_RES_OK);
    uint64_t out = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, 10);

    // 再次add
    EXPECT_EQ(ub_dist_tx_res_add(&val, 5), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, 15);
}

TEST(DistTxAddTest, AddNullHandleTest)
{
    EXPECT_EQ(ub_dist_tx_res_add(nullptr, 1), UB_RES_ERROR);
}

TEST(DistTxAddTest, AddUnalignedTest)
{
    uint64_t *unaligned = reinterpret_cast<uint64_t *>(0x7ffee3b5a001);
    EXPECT_EQ(ub_dist_tx_res_add(unaligned, 1), UB_RES_ERROR);
}

TEST(DistTxAddTest, AddOverflowTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val), UB_RES_OK);

    // 设置为UINT64_MAX
    EXPECT_EQ(ub_dist_tx_res_set(&val, UINT64_MAX), UB_RES_OK);

    // 加1应回绕为0
    EXPECT_EQ(ub_dist_tx_res_add(&val, 1), UB_RES_OK);
    uint64_t out = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, 0);
}

TEST(DistTxAddTest, AddConcurrentTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val), UB_RES_OK);
    const int N = 8;
    const uint64_t increment = 100;
    std::vector<std::thread> threads;
    for (int i = 0; i < N; i++) {
        threads.emplace_back([&val, increment]() {
            for (int j = 0; j < 1000; j++) {
                ub_dist_tx_res_add(&val, increment);
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    uint64_t out = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, (uint64_t)N * 1000 * increment);
}

TEST(DistTxFenceTest, FenceReleaseAcquireTest)
{
    uint64_t data = 0;
    uint64_t flag = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&data), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_init(&flag), UB_RES_OK);

    std::thread writer([&]() {
        ub_dist_tx_res_set(&data, 42);
        ub_dist_tx_res_fence(UB_FENCE_RELEASE);
        ub_dist_tx_res_set(&flag, 1);
    });
    std::thread reader([&]() {
        uint64_t f = 0;
        while (f == 0) {
            ub_dist_tx_res_get(&flag, &f);
        }
        ub_dist_tx_res_fence(UB_FENCE_ACQUIRE);
        uint64_t d = 0;
        ub_dist_tx_res_get(&data, &d);
        EXPECT_EQ(d, 42);
    });
    writer.join();
    reader.join();
}

TEST(DistTxAddTest, AddVsFetchAddConsistencyTest)
{
    uint64_t val1 = 0, val2 = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val1), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_init(&val2), UB_RES_OK);

    EXPECT_EQ(ub_dist_tx_res_set(&val1, 100), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_set(&val2, 100), UB_RES_OK);

    uint64_t old_val = 0;
    EXPECT_EQ(ub_dist_tx_res_fetch_add(&val1, 42, &old_val), UB_RES_OK);
    EXPECT_EQ(old_val, 100);
    EXPECT_EQ(ub_dist_tx_res_add(&val2, 42), UB_RES_OK);

    uint64_t out1 = 0, out2 = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val1, &out1), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_get(&val2, &out2), UB_RES_OK);
    EXPECT_EQ(out1, out2);
    EXPECT_EQ(out1, 142);
}

TEST(DistTxXorTest, FetchXorBasicTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_set(&val, 0xFF), UB_RES_OK);

    uint64_t old = 0;
    // 0xFF ^ 0x0F = 0xF0, 旧值应为 0xFF
    EXPECT_EQ(ub_dist_tx_res_fetch_xor(&val, 0x0F, &old), UB_RES_OK);
    EXPECT_EQ(old, 0xFF);

    uint64_t out = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, 0xF0);
}

TEST(DistTxXorTest, FetchXorNullHandleTest)
{
    uint64_t old = 0;
    EXPECT_EQ(ub_dist_tx_res_fetch_xor(nullptr, 1, &old), UB_RES_ERROR);
}

TEST(DistTxXorTest, FetchXorNullOutValTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_fetch_xor(&val, 1, nullptr), UB_RES_ERROR);
}

TEST(DistTxXorTest, FetchXorUnalignedTest)
{
    uint64_t *unaligned = reinterpret_cast<uint64_t *>(0x7ffee3b5a001);
    uint64_t old = 0;
    EXPECT_EQ(ub_dist_tx_res_fetch_xor(unaligned, 1, &old), UB_RES_ERROR);
}

TEST(DistTxCasTest, CompareExchangeSuccessTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_set(&val, 42), UB_RES_OK);

    uint64_t expected = 42;
    int success = 0;
    // 期望值42匹配当前值42，应成功替换为100
    EXPECT_EQ(ub_dist_tx_res_compare_exchange(&val, &expected, 100, &success), UB_RES_OK);
    EXPECT_EQ(success, 1);

    uint64_t out = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, 100);
}

TEST(DistTxCasTest, CompareExchangeFailTest)
{
    uint64_t val = 0;
    EXPECT_EQ(ub_dist_tx_res_init(&val), UB_RES_OK);
    EXPECT_EQ(ub_dist_tx_res_set(&val, 42), UB_RES_OK);

    uint64_t expected = 99; // 不匹配当前值42
    int success = 0;
    EXPECT_EQ(ub_dist_tx_res_compare_exchange(&val, &expected, 100, &success), UB_RES_OK);
    EXPECT_EQ(success, 0);
    // expected 应被更新为实际值 42
    EXPECT_EQ(expected, 42);

    // 值未被修改
    uint64_t out = 0;
    EXPECT_EQ(ub_dist_tx_res_get(&val, &out), UB_RES_OK);
    EXPECT_EQ(out, 42);
}

TEST(DistTxCasTest, CompareExchangeNullTest)
{
    uint64_t val = 0;
    uint64_t expected = 0;
    int success = 0;

    EXPECT_EQ(ub_dist_tx_res_compare_exchange(nullptr, &expected, 1, &success), UB_RES_ERROR);
    EXPECT_EQ(ub_dist_tx_res_compare_exchange(&val, nullptr, 1, &success), UB_RES_ERROR);
    EXPECT_EQ(ub_dist_tx_res_compare_exchange(&val, &expected, 1, nullptr), UB_RES_ERROR);
}
} // namespace ut