#include "gtest/gtest.h"
#include "mockcpp/mokc.h"

#define private public
#include "inner_distribute_lock.h"
#undef private

#define MOCKER_CPP(api, TT) MOCKCPP_NS::mockAPI(#api, reinterpret_cast<TT>(api))

namespace ublock {
namespace ut {

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

} // namespace ut
} // namespace ublock
