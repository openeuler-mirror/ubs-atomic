#include <atomic>
#include <cstdint>
#if defined(__aarch64__) || defined(__arm__)
#include <arm_neon.h>
#endif
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <stdexcept>
#include <string.h>

// 获取缓存行大小，通常为 64 字节
constexpr uint64_t CACHELINE_SIZE = 64;


static inline void cpu_relax_arm() {
#if defined(__aarch64__)
  asm volatile("yield" ::: "memory");   // ARM hint, not OS yield
#else
  asm volatile("" ::: "memory");
#endif
}

// 【写屏障】对应 x86: sfence
static inline void arm_sfence() {
#if defined(__aarch64__) || defined(__arm__)
    asm volatile("dsb ish" ::: "memory");
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

// 【读屏障】对应 x86: lfence
static inline void arm_lfence() {
#if defined(__aarch64__) || defined(__arm__)
    asm volatile("dsb ish" ::: "memory");
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}


static void pin_this_thread_to_cpu(int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    if (rc != 0) {
        throw std::runtime_error(std::string("pthread_setaffinity_np failed: ") + strerror(rc));
    }
}

static int get_cpu() {
    return sched_getcpu();
}

static uint32_t get_online_cpu_num() {
    long cpu_num = sysconf(_SC_NPROCESSORS_ONLN);
    if (cpu_num <= 0) {
        throw std::runtime_error(std::string("sysconf get cpu num failed: ") + strerror(errno));
    }
    return static_cast<uint32_t>(cpu_num);
}

// 校验cpuid是否为有效编号
static bool check_cpu_id_valid(int cpu) {
    uint32_t cpu_num = get_online_cpu_num();
    if (cpu < 0 || static_cast<uint32_t>(cpu) >= cpu_num) {
        return false;
    }
    return true;
}
