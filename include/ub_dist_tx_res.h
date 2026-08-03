/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * rmrs is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef UB_DIST_TX_RES_H
#define UB_DIST_TX_RES_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// 定义日志级别常量
#ifndef LOG_LEVEL_DEBUG
#define LOG_LEVEL_DEBUG 0
#define LOG_LEVEL_INFO 1
#define LOG_LEVEL_WARN 2
#define LOG_LEVEL_ERROR 3
#define LOG_LEVEL_CRITICAL 4
#endif

/* Operation result: execution succeeded */
#define UB_RES_OK 0
/* Operation result: execution failed/error occurred */
#define UB_RES_ERROR -1

/**
 * @brief Initialize a distributed transaction resource object.
 * @param[out] handle : pointer to store the handle of the initialized resource object
 * @return Operation result status (UB_RES_OK for success, UB_RES_ERROR for failure)
 */
int ub_dist_tx_res_init(uint64_t *handle);

/**
 * @brief Set a specific value to the distributed transaction resource.
 * @param[in] handle : pointer to the handle of the target resource object
 * @param[in] value  : the value to be set to the distributed transaction resource
 * @return Operation result status (UB_RES_OK for success, UB_RES_ERROR for failure)
 */
int ub_dist_tx_res_set(uint64_t *handle, uint64_t value);

/**
 * @brief Get the current value of the distributed transaction resource.
 * @param[in]  handle   : pointer to the handle of the target resource object
 * @param[out] out_val  : pointer to store the retrieved value of the resource
 * @return Operation result status (UB_RES_OK for success, UB_RES_ERROR for failure)
 */
int ub_dist_tx_res_get(uint64_t *handle, uint64_t *out_val);

/**
 * @brief Atomically add a value to the distributed transaction resource and get the original value.
 * @param[in]  handle   : pointer to the handle of the target resource object
 * @param[in]  value    : the value to be atomically added to the resource
 * @param[out] out_val  : pointer to store the original value of the resource before the addition
 * @return Operation result status (UB_RES_OK for success, UB_RES_ERROR for failure)
 */
int ub_dist_tx_res_fetch_add(uint64_t *handle, uint64_t value, uint64_t *out_val);

/**
 * @brief 内存屏障语义枚举，对应 C11/C++11 memory_order。
 */
typedef enum
{
    UB_FENCE_RELAXED = 0, /**< 仅编译器屏障，不生成硬件 fence 指令 */
    UB_FENCE_ACQUIRE = 1, /**< Acquire: 后续读不可前移 (ARM64: dmb ishld) */
    UB_FENCE_RELEASE = 2, /**< Release: 先前写不可后移 (ARM64: dmb ishst) */
    UB_FENCE_ACQ_REL = 3, /**< Acquire-Release: 双向 (ARM64: dmb ish) */
    UB_FENCE_SEQ_CST = 4, /**< Sequential Consistency: 全局一致序 (ARM64: dsb ish) */
} ub_fence_order_t;

/**
 * @brief 统一内存屏障接口。
 *        根据 order 参数插入对应强度的编译器屏障和硬件屏障。
 *        所有变体均包含 compiler barrier（"memory" clobber），
 *        可阻止编译器寄存器缓存和跨点重排。
 * @param[in] order : 屏障语义，取值范围 UB_FENCE_RELAXED ~ UB_FENCE_SEQ_CST
 * @return UB_RES_OK     操作成功
 * @return UB_RES_ERROR  order 超出合法范围
 * @note 线程安全：是（fence 仅约束调用线程自身的内存访问序）
 * @note 幂等性：连续多次调用等价于单次对应屏障
 */
int ub_dist_tx_res_fence(ub_fence_order_t order);

/**
 * @brief 对分布式事务资源执行原子加法（无fetch版本）。
 *        将value原子地加到handle指向的共享内存位置，不返回旧值。
 *        使用memory_order_release语义。
 * @param[in] handle : 指向目标共享内存位置的指针，必须8字节对齐
 * @param[in] value  : 要累加的64位无符号整数值
 * @return UB_RES_OK     操作成功
 * @return UB_RES_ERROR  handle为NULL或地址未8字节对齐
 * @note 与 ub_dist_tx_res_fetch_add 的区别：不返回旧值，可优化为更轻量指令
 */
int ub_dist_tx_res_add(uint64_t *handle, uint64_t value);

/**
 * @brief 对分布式事务资源执行原子异或并返回旧值。
 *        将value与handle指向的共享内存位置原子地进行XOR操作，返回旧值。
 *        使用memory_order_acq_rel语义。
 * @param[in]  handle  : 指向目标共享内存位置的指针，必须8字节对齐
 * @param[in]  value   : 要异或的64位无符号整数值
 * @param[out] out_val : 指针，用于存储操作前的旧值
 * @return UB_RES_OK     操作成功
 * @return UB_RES_ERROR  handle或out_val为NULL，或地址未8字节对齐
 */
int ub_dist_tx_res_fetch_xor(uint64_t *handle, uint64_t value, uint64_t *out_val);

/**
 * @brief 对分布式事务资源执行原子比较并交换（CAS）。
 *        如果handle指向的值等于*expected，则原子地将其替换为desired，返回成功；
 *        否则将当前值写入*expected，返回失败。
 *        使用memory_order_acq_rel（成功）/ acquire（失败）语义。
 * @param[in]     handle   : 指向目标共享内存位置的指针，必须8字节对齐
 * @param[in,out] expected : 指针，输入为期望值，失败时输出当前实际值
 * @param[in]     desired  : 期望匹配时要写入的新值
 * @param[out]    success  : 指针，输出CAS是否成功（1=成功，0=失败）
 * @return UB_RES_OK     操作成功（注意：success的值表示CAS是否匹配，而非函数调用是否成功）
 * @return UB_RES_ERROR  handle/expected/success为NULL，或地址未8字节对齐
 */
int ub_dist_tx_res_compare_exchange(uint64_t *handle, uint64_t *expected, uint64_t desired, int *success);

#ifndef UB_ATOMIC_LOG_FUNC_TYPEDEF
#define UB_ATOMIC_LOG_FUNC_TYPEDEF
/*
* @brief log func
* @param level [in] : LogLevel
* @param file [in] : source file name
* @param func [in] : function name
* @param line [in] : line number
* @param message [in] : formatted message
*/
typedef int (*ub_atomic_log_func)(int level, const char *file, const char *func, uint32_t line, const char *message);

/*
* @brief register log function
* @param func [in] : user-defined log function pointer
*/
void ub_atomic_register_log_func(ub_atomic_log_func func);

/*
* @brief set log level threshold
* @param level [in] : log level threshold (LOG_LEVEL_DEBUG ~ LOG_LEVEL_CRITICAL)
* @return 0 on success, -1 on invalid level
*/
int ub_atomic_set_log_level(int level);
#endif
#ifdef __cplusplus
}
#endif

#endif /* UB_DIST_TX_RES_H */
