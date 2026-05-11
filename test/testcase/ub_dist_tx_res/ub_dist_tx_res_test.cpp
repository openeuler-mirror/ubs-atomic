#include "ub_dist_tx_res.h"
#include <string>
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

    tid_addr = reinterpret_cast<uint64_t*>(0x7ffee3b5a001);
    status = ub_dist_tx_res_init(tid_addr);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_set(tid_addr, 99);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_get(tid_addr, &value);
    EXPECT_TRUE(status == UB_RES_ERROR);

    status = ub_dist_tx_res_fetch_add(tid_addr, 1, &value);
    EXPECT_TRUE(status == UB_RES_ERROR);
}
} // namespace ut