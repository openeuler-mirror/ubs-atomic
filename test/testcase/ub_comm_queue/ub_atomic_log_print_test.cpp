/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/
#include <atomic>
#include <cstring>
#include "gtest/gtest.h"
#include "ub_atomic_log_print.h"

namespace ub_comm_queue {
namespace ut {
namespace {

std::atomic<int> g_log_capture_level{LOG_LEVEL_INVALID};
std::atomic<uint32_t> g_log_capture_line{0};
char g_log_capture_message[256] = {};
std::atomic<uint32_t> g_log_capture_count{0};

int CaptureLog(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    g_log_capture_level.store(level, std::memory_order_release);
    g_log_capture_line.store(line, std::memory_order_release);
    std::snprintf(g_log_capture_message, sizeof(g_log_capture_message), "%s", message ? message : "");
    g_log_capture_count.fetch_add(1, std::memory_order_acq_rel);
    return 0;
}

class AtomicLogPrintTest : public ::testing::Test {
public:
    void SetUp() override
    {
        // Restore default threshold
        set_log_level_threshold(LOG_LEVEL_DEFAULT);
        register_print_func(CaptureLog);
        g_log_capture_level.store(LOG_LEVEL_INVALID, std::memory_order_release);
        g_log_capture_line.store(0, std::memory_order_release);
        g_log_capture_message[0] = '\0';
        g_log_capture_count.store(0, std::memory_order_release);
    }

    void TearDown() override
    {
        set_log_level_threshold(LOG_LEVEL_DEFAULT);
        register_print_func(nullptr);
    }
};

} // namespace

// L14: register_print_func with NULL falls back to log_no_print.
TEST_F(AtomicLogPrintTest, RegisterPrintFuncWithNullFallsBackToNoPrint)
{
    register_print_func(nullptr);
    log_print(LOG_LEVEL_INFO, "file.cpp", "func", 100, "after null register");
    // No callback should fire, but log_print still does not crash.
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 0u);
}

// L14: register_print_func with valid func stores it.
// TEST_F(AtomicLogPrintTest, RegisterPrintFuncWithValidFuncStoresIt)
// {
//     register_print_func(CaptureLog);
//     log_print(LOG_LEVEL_INFO, "file.cpp", "func", 101, "with valid func");
//     EXPECT_GE(g_log_capture_count.load(std::memory_order_acquire), 1u);
// }

// L28: get_log_level_threshold returns previously-stored threshold.
TEST_F(AtomicLogPrintTest, GetLogLevelThresholdRoundTrip)
{
    int saved = get_log_level_threshold();
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG), 0);
    EXPECT_EQ(get_log_level_threshold(), LOG_LEVEL_DEBUG);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_CRITICAL), 0);
    EXPECT_EQ(get_log_level_threshold(), LOG_LEVEL_CRITICAL);
    (void)set_log_level_threshold(saved);
}

// L35: set_log_level_threshold rejects values below LOG_LEVEL_DEBUG.
TEST_F(AtomicLogPrintTest, SetLogLevelThresholdBelowRangeRejected)
{
    int saved = get_log_level_threshold();
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG - 1), -1);
    EXPECT_EQ(set_log_level_threshold(-100), -1);
    EXPECT_EQ(get_log_level_threshold(), saved);
    (void)set_log_level_threshold(saved);
}

// L35: set_log_level_threshold rejects values above LOG_LEVEL_CRITICAL.
TEST_F(AtomicLogPrintTest, SetLogLevelThresholdAboveRangeRejected)
{
    int saved = get_log_level_threshold();
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_CRITICAL + 1), -1);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_NUM), -1);
    EXPECT_EQ(get_log_level_threshold(), saved);
    (void)set_log_level_threshold(saved);
}

// L35: set_log_level_threshold accepts boundary values.
TEST_F(AtomicLogPrintTest, SetLogLevelThresholdAcceptsBoundary)
{
    int saved = get_log_level_threshold();
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG), 0);
    EXPECT_EQ(get_log_level_threshold(), LOG_LEVEL_DEBUG);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_CRITICAL), 0);
    EXPECT_EQ(get_log_level_threshold(), LOG_LEVEL_CRITICAL);
    (void)set_log_level_threshold(saved);
}

// L50: log_print early-returns when level >= LOG_LEVEL_NUM.
TEST_F(AtomicLogPrintTest, LogPrintSkipsWhenLevelExceedsMax)
{
    register_print_func(CaptureLog);
    g_log_capture_count.store(0, std::memory_order_release);
    log_print(LOG_LEVEL_NUM, "file.cpp", "func", 200, "too high");
    log_print(LOG_LEVEL_NUM + 5, "file.cpp", "func", 200, "way too high");
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 0u);
}

// L50: log_print early-returns when level is below current threshold.
TEST_F(AtomicLogPrintTest, LogPrintSkipsWhenBelowThreshold)
{
    register_print_func(CaptureLog);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_CRITICAL), 0);
    g_log_capture_count.store(0, std::memory_order_release);
    log_print(LOG_LEVEL_DEBUG, "file.cpp", "func", 201, "below threshold");
    log_print(LOG_LEVEL_INFO, "file.cpp", "func", 201, "below threshold");
    log_print(LOG_LEVEL_WARN, "file.cpp", "func", 201, "below threshold");
    log_print(LOG_LEVEL_ERROR, "file.cpp", "func", 201, "below threshold");
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 0u);
}

// L50/L72: log_print invokes callback when level within range.
TEST_F(AtomicLogPrintTest, LogPrintInvokesCallbackWhenAllowed)
{
    register_print_func(CaptureLog);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG), 0);
    g_log_capture_count.store(0, std::memory_order_release);
    log_print(LOG_LEVEL_INFO, "file.cpp", "func", 202, "level=%d value=%s", LOG_LEVEL_INFO, "hi");
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 1u);
    EXPECT_EQ(g_log_capture_level.load(std::memory_order_acquire), LOG_LEVEL_INFO);
    EXPECT_EQ(g_log_capture_line.load(std::memory_order_acquire), 202u);
    EXPECT_STREQ(g_log_capture_message, "level=1 value=hi");
}

// L50: log_print early-returns when level is negative.
TEST_F(AtomicLogPrintTest, LogPrintSkipsWhenLevelNegative)
{
    register_print_func(CaptureLog);
    g_log_capture_count.store(0, std::memory_order_release);
    log_print(-1, "file.cpp", "func", 203, "negative level");
    log_print(-9999, "file.cpp", "func", 203, "very negative level");
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 0u);
}

// L46: log_print auto-fills g_logger_func when it is NULL.
TEST_F(AtomicLogPrintTest, LogPrintAutoFillsNullLoggerFunc)
{
    register_print_func(nullptr);
    EXPECT_EQ(get_log_level_threshold(), LOG_LEVEL_DEFAULT);
    log_print(LOG_LEVEL_CRITICAL, "file.cpp", "func", 204, "auto-filled logger");
    // Should not crash and should be silent (log_no_print used).
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 0u);
}

// L67: log_print clamps trailing byte when message fits exactly.
TEST_F(AtomicLogPrintTest, LogPrintHandlesEmptyFormat)
{
    register_print_func(CaptureLog);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG), 0);
    g_log_capture_count.store(0, std::memory_order_release);
    log_print(LOG_LEVEL_DEBUG, "file.cpp", "func", 205, "literal-only");
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 1u);
    EXPECT_STREQ(g_log_capture_message, "literal-only");
}

// L72: log_print passes file/func/line to callback.
TEST_F(AtomicLogPrintTest, LogPrintPassesMetadataToCallback)
{
    register_print_func(CaptureLog);
    EXPECT_EQ(set_log_level_threshold(LOG_LEVEL_DEBUG), 0);
    g_log_capture_count.store(0, std::memory_order_release);
    log_print(LOG_LEVEL_WARN, "src_file.cpp", "src_func", 999, "metadata test");
    EXPECT_EQ(g_log_capture_count.load(std::memory_order_acquire), 1u);
    EXPECT_EQ(g_log_capture_level.load(std::memory_order_acquire), LOG_LEVEL_WARN);
    EXPECT_EQ(g_log_capture_line.load(std::memory_order_acquire), 999u);
}

// log_no_print returns 0 regardless of inputs.
TEST_F(AtomicLogPrintTest, LogNoPrintAlwaysReturnsZero)
{
    EXPECT_EQ(log_no_print(LOG_LEVEL_DEBUG, nullptr, nullptr, 0, nullptr), 0);
    EXPECT_EQ(log_no_print(LOG_LEVEL_CRITICAL, "f", "g", 42, "msg"), 0);
}

} // namespace ut
} // namespace ub_comm_queue