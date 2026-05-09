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

} // extern "C"