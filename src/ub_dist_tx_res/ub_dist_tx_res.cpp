#include "ub_dist_tx_res.h"
#include <atomic>
#include <stdexcept>
#include "ub_atomic_log_print.h"

#define UINT64_ALIGN (alignof(uint64_t))
class DistributeTxResource {
private:
    std::atomic<uint64_t> *ptr;

public:
    explicit DistributeTxResource(std::atomic<uint64_t> *address) : ptr(address)
    {
        if (!ptr)
            throw std::invalid_argument("shm_address is null");
    }

    void init()
    {
        ptr->store(0, std::memory_order_relaxed);
    }
    void set(uint64_t value)
    {
        ptr->store(value, std::memory_order_release);
    }
    uint64_t get()
    {
        return ptr->load(std::memory_order_acquire);
    }
    uint64_t fetch_add(uint64_t value)
    {
        return ptr->fetch_add(value, std::memory_order_acq_rel);
    }
    void add(uint64_t value)
    {
        // release语义：无需acquire（不读旧值），release保证之前的写可见
        ptr->fetch_add(value, std::memory_order_release);
    }

    uint64_t fetch_xor(uint64_t value)
    {
        return ptr->fetch_xor(value, std::memory_order_acq_rel);
    }

    bool compare_exchange(uint64_t *expected, uint64_t desired)
    {
        return ptr->compare_exchange_strong(*expected, desired,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire);
    }
};

extern "C" {

int ub_dist_tx_res_init(uint64_t *handle)
{
    if (!handle) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    res.init();
    return UB_RES_OK;
}

int ub_dist_tx_res_set(uint64_t *handle, uint64_t value)
{
    if (!handle) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    res.set(value);
    return UB_RES_OK;
}

int ub_dist_tx_res_get(uint64_t *handle, uint64_t *out_val)
{
    if (!handle || !out_val) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    *out_val = res.get();
    return UB_RES_OK;
}

int ub_dist_tx_res_fetch_add(uint64_t *handle, uint64_t value, uint64_t *out_val)
{
    if (!handle || !out_val) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    *out_val = res.fetch_add(value);
    return UB_RES_OK;
}

int ub_dist_tx_res_fence(ub_fence_order_t order)
{
    switch (order) {
    case UB_FENCE_RELAXED:
        // 仅编译器屏障：阻止寄存器缓存和编译器重排，不生成硬件 fence
        asm volatile("" ::: "memory");
        break;
    case UB_FENCE_ACQUIRE:
#if defined(__aarch64__) || defined(__arm__)
        asm volatile("dmb ishld" ::: "memory");
#else
        std::atomic_thread_fence(std::memory_order_acquire);
#endif
        break;
    case UB_FENCE_RELEASE:
#if defined(__aarch64__) || defined(__arm__)
        asm volatile("dmb ishst" ::: "memory");
#else
        std::atomic_thread_fence(std::memory_order_release);
#endif
        break;
    case UB_FENCE_ACQ_REL:
#if defined(__aarch64__) || defined(__arm__)
        asm volatile("dmb ish" ::: "memory");
#else
        std::atomic_thread_fence(std::memory_order_acq_rel);
#endif
        break;
    case UB_FENCE_SEQ_CST:
#if defined(__aarch64__) || defined(__arm__)
        asm volatile("dsb ish" ::: "memory");
#else
        std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
        break;
    default:
        return UB_RES_ERROR;
    }
    return UB_RES_OK;
}

int ub_dist_tx_res_add(uint64_t *handle, uint64_t value)
{
    if (!handle) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    res.add(value);
    return UB_RES_OK;
}

int ub_dist_tx_res_fetch_xor(uint64_t *handle, uint64_t value, uint64_t *out_val)
{
    if (!handle || !out_val) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    *out_val = res.fetch_xor(value);
    return UB_RES_OK;
}

int ub_dist_tx_res_compare_exchange(uint64_t *handle, uint64_t *expected,
                                     uint64_t desired, int *success)
{
    if (!handle || !expected || !success) {
        return UB_RES_ERROR;
    }
    if (reinterpret_cast<uintptr_t>(handle) % UINT64_ALIGN != 0) {
        ATOMIC_LOG(LOG_LEVEL_ERROR, "SHM data ptr is NOT 8-byte aligned!\n");
        return UB_RES_ERROR;
    }
    DistributeTxResource res(reinterpret_cast<std::atomic<uint64_t> *>(handle));
    *success = res.compare_exchange(expected, desired) ? 1 : 0;
    return UB_RES_OK;
}

} // extern "C"
