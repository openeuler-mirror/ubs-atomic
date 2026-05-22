# UBS Atomic 对外接口文档

本文档根据 `include` 目录下的公开头文件整理，面向调用方说明 UBS Atomic 共享内存原子能力的 C ABI 接口。当前对外能力包括：

- 分布式共享内存通信队列：`include/ub_dist_comm_queue.h`
- 分布式读写锁：`include/ub_dist_lock.h`
- 分布式事务资源：`include/ub_dist_tx_res.h`
- 通用日志接口：三个头文件均暴露同一组 `ub_atomic_*` 日志接口

接口默认支持 C/C++ 调用；C++ 场景下头文件使用 `extern "C"` 保持 C ABI。

## 1. 通用日志接口

### 1.1 日志级别

| 名称 | 值 | 说明 |
| --- | ---: | --- |
| `LOG_LEVEL_DEBUG` | 0 | 调试日志 |
| `LOG_LEVEL_INFO` | 1 | 普通信息日志 |
| `LOG_LEVEL_WARN` | 2 | 告警日志 |
| `LOG_LEVEL_ERROR` | 3 | 错误日志 |
| `LOG_LEVEL_CRITICAL` | 4 | 严重错误日志 |

### 1.2 `ub_atomic_log_func`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_atomic_log_func` |
| 接口描述 | 用户自定义日志函数类型，库内部产生日志时调用。 |
| 接口类型 | 回调函数类型 |
| 函数原型 | `typedef int (*ub_atomic_log_func)(int level, const char *file, const char *func, uint32_t line, const char *message);` |
| 返回参数 | `int`，由调用方自定义；当前库不依赖该返回值表达业务结果。 |

**输入参数列表与参数有效性规格**

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `level` | `int` | 日志级别 | 建议为 `LOG_LEVEL_DEBUG` 到 `LOG_LEVEL_CRITICAL` |
| `file` | `const char *` | 源文件名 | 由库传入，回调内不应修改 |
| `func` | `const char *` | 函数名 | 由库传入，回调内不应修改 |
| `line` | `uint32_t` | 行号 | 由库传入 |
| `message` | `const char *` | 格式化后的日志内容 | 由库传入，回调内不应修改或长期持有裸指针 |

**约束与注意事项**

- 回调函数应避免阻塞过久，否则可能影响调用接口的执行路径。
- 回调中如需保存日志内容，应自行拷贝字符串。

### 1.3 `ub_atomic_register_log_func`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_atomic_register_log_func` |
| 接口描述 | 注册用户自定义日志函数。 |
| 接口类型 | 函数 |
| 函数原型 | `void ub_atomic_register_log_func(ub_atomic_log_func func);` |
| 返回参数 | 无 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `func` | `ub_atomic_log_func` | 日志回调函数指针 | 可以传入有效函数指针；传入空指针表示取消或不使用用户日志函数，具体行为取决于内部日志模块 |

### 1.4 `ub_atomic_set_log_level`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_atomic_set_log_level` |
| 接口描述 | 设置日志输出阈值，低于阈值的日志不会输出。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_atomic_set_log_level(int level);` |
| 返回参数 | 成功返回 `0`；非法日志级别返回 `-1`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `level` | `int` | 日志级别阈值 | 必须在 `LOG_LEVEL_DEBUG` 到 `LOG_LEVEL_CRITICAL` 范围内 |

**使用样例**

```c
#include "ub_dist_tx_res.h"
#include <stdio.h>

static int app_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    printf("[%d] %s:%u %s: %s\n", level, file, line, func, message);
    return 0;
}

void setup_log(void)
{
    ub_atomic_register_log_func(app_logger);
    (void)ub_atomic_set_log_level(LOG_LEVEL_INFO);
}
```

## 2. 分布式共享内存通信队列

### 2.1 模块说明

通信队列提供基于共享内存的多节点消息通道。每个节点在自己的 Ring 区域中创建本地收包 Ring，其他节点通过共享内存写入目标节点对应优先级的 Ring。主要收包路径为后台分发线程轮询本节点 Ring 并派发回调。

**全局约束**

- 当前最大节点数为 16。
- 当前最大优先级级别个数为 8，即 `priority` 取值范围为 `0~7`，数值越小级别越高，。
- `priority == 0` 为内部分布式锁 Ring 和系统消息保留，业务 Ring 配置不得使用 0。
- 系统保留消息类型包括 `0xFF`、`0xFE`、`0xFD`，业务侧不应直接发送或注册这些消息类型。
- `ring_capacity` 必须是 2 的幂。
- `UB_COMM_SEND_CONGESTED` 的值为 `1`，表示消息已经入队但目标 Ring 达到拥塞阈值，不是失败。

### 2.2 公开数据结构

#### `ub_shm_area_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `size` | `size_t` | 共享内存区域大小，单位字节 | 初始化公告牌区域时必须足够容纳内部公告牌；Ring 区域必须足够放置本节点所有 Ring |
| `ptr` | `void *` | 共享内存首地址 | 必须指向调用方已分配并映射好的共享内存 |

#### `ub_ring_region_info_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `region` | `ub_shm_area_t` | 某个节点的 Ring 共享内存区域 | `region.ptr` 非空，`region.size` 满足该节点 Ring 空间需求 |
| `node_id` | `uint8_t` | 节点 ID | 必须在集群内唯一 |

#### `ub_ring_region_map_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `entries` | `ub_ring_region_info_t *` | 所有节点 Ring 区域数组 | 非空；各节点看到的数组内容和顺序应保持一致 |
| `count` | `uint8_t` | 数组元素个数 | 非 0，且不超过 16；应等于 `ub_comm_conf_t.max_nodes` |

#### `ub_ring_desc_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `ring_capacity` | `uint32_t` | Ring 容量，按消息条目计 | 必须是 2 的幂 |
| `max_msg_size` | `uint32_t` | 单条消息最大大小，单位字节 | 必须能容纳消息头和业务消息体 |
| `priority` | `uint8_t` | Ring 优先级 | 业务 Ring 必须为 `1~7`，`0` 保留 |

#### `ub_comm_conf_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `cpu_id` | `int32_t` | 分发线程绑核 CPU ID | 小于 0 表示不绑核；非负值应为有效 CPU ID |
| `max_nodes` | `uint8_t` | 集群最大节点数 | `1~16` |
| `current_node_id` | `uint8_t` | 当前节点 ID | 必须存在于 `ring_regions.entries` 中 |
| `num_rings` | `uint8_t` | 当前节点创建的业务 Ring 数量 | `1~7`，且不包含内部保留 Ring |
| `ring_descs` | `ub_ring_desc_t *` | Ring 配置数组 | 非空，长度至少为 `num_rings` |

#### `message_header_t` 与 `message_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `src_thread_id` | `uint64_t` | 发送线程或逻辑线程 ID | 调用方自定义 |
| `body_length` | `uint32_t` | 消息体长度 | 应与 `message_t.body` 指向的数据长度一致；消息头加消息体不得超过目标 Ring 的 `max_msg_size` |
| `dest_node_id` | `uint8_t` | 目标节点 ID | 必须存在于节点映射中，且目标节点已初始化 |
| `src_node_id` | `uint8_t` | 源节点 ID | 必须等于当前通信实例的 `current_node_id` |
| `msg_type` | `uint8_t` | 业务消息类型 | 不应使用系统保留值 `0xFF`、`0xFE`、`0xFD` |
| `priority` | `uint8_t` | 目标 Ring 优先级 | 业务发送应使用已配置的 `1~7` |
| `body` | `char *` | 消息体地址 | 当 `body_length > 0` 时必须指向有效内存 |

#### `ub_func_type_t` 与 `ub_callback_t`

| 名称 | 类型 | 说明 |
| --- | --- | --- |
| `ub_callback_t` | `void (*)(const message_t *msg, void *ctx)` | 消息处理回调类型 |
| `UB_FUNC_SYNC` | 枚举值 0 | 同步回调 |
| `UB_FUNC_ASYNC` | 枚举值 1 | 异步回调，投递到内部线程池执行 |

#### `ub_comm_queue_status_t`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `used` | `uint64_t` | 估算占用条目数 |
| `total` | `uint64_t` | Ring 总容量 |
| `free` | `uint64_t` | 估算空闲条目数 |
| `state` | `ub_comm_queue_state_t` | 队列状态 |
| `congestion_threshold` | `uint64_t` | 当前拥塞阈值对应的条目数 |
| `max_depth` | `uint64_t` | Ring 初始化以来观测到的最大深度 |

若编译时定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS`，结构体还包含 `full_fail_count`、`cas_fail_count`、`congestion_enter_ts_us`、`congestion_exit_ts_us`。

| `state` 名称 | 说明 |
| --- | --- |
| `UB_COMM_QUEUE_IDLE` | `used == 0` |
| `UB_COMM_QUEUE_NORMAL` | `0 < used < congestion_threshold` |
| `UB_COMM_QUEUE_CONGESTED` | `used >= congestion_threshold && used < total` |
| `UB_COMM_QUEUE_FULL` | `used >= total` |

#### `ub_comm_queue_heartbeat_config_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `heartbeat_interval_ms` | `uint32_t` | 本节点消费者心跳序号刷新周期，单位毫秒；会发布给其他节点用于超时窗口计算 | 必须大于 0 |
| `check_interval_ms` | `uint32_t` | 本节点生产者心跳监控线程轮询周期，单位毫秒 | 必须大于 0 |
| `timeout_ms` | `uint32_t` | 本节点观察 peer 的最小超时阈值，单位毫秒 | 必须大于 0，且不小于 `2 * check_interval_ms` |

#### `ub_comm_queue_heartbeat_status_t`

| 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `timeout_ms` | `uint32_t` | 本节点对被查询节点使用的实际心跳超时阈值，单位毫秒 | 输出字段 |
| `last_observed_seq` | `uint64_t` | 本节点最近观察到的目标节点心跳序号 | 输出字段 |
| `last_change_age_ms` | `uint64_t` | 从本节点最后一次观察到序号变化到现在的本地单调时间差，单位毫秒 | 输出字段；`UINT64_MAX` 表示尚未观察到有效心跳 |
| `node_id` | `uint8_t` | 被查询节点 ID | 输出字段 |
| `alive` | `uint8_t` | 本节点对目标节点消费者是否存活的本地判断 | 输出字段；`1` 表示存活，`0` 表示超时或未知 |

### 2.3 `ub_comm_queue_init`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_init` |
| 接口描述 | 初始化共享内存通信实例，创建本地 Ring，发布节点状态，并启动后台分发线程。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_init(ub_shm_comm_t *handle, ub_shm_area_t *init_region, ub_ring_region_map_t *ring_regions, ub_comm_conf_t *conf);` |
| 返回参数 | 成功返回 `0`；失败返回负数错误码，如 `-EINVAL`、`-ENOMEM`、`-EFAULT` 或内部初始化错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 输出通信实例句柄 | 非空；成功后写入不透明句柄 |
| `init_region` | `ub_shm_area_t *` | 全局初始化公告牌共享内存 | 非空；`ptr` 指向共享内存，`size` 足够容纳公告牌 |
| `ring_regions` | `ub_ring_region_map_t *` | 所有节点 Ring 区域映射 | 非空；`entries` 非空；`count` 为 `1~16` |
| `conf` | `ub_comm_conf_t *` | 当前节点通信配置 | 非空；字段满足 2.2 中规格 |

**约束与注意事项**

- 调用方负责分配和管理共享内存生命周期。
- 所有节点应使用一致的 `ring_regions` 节点集合。
- 第一个初始化成功的通信实例会被内部标记为分布式锁使用实例。
- 初始化成功后必须调用 `ub_comm_queue_deinit` 释放实例。

### 2.4 `ub_comm_queue_deinit`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_deinit` |
| 接口描述 | 反初始化通信实例，停止后台线程并释放实例对象。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_deinit(ub_shm_comm_t *handle);` |
| 返回参数 | 成功返回 `0`；`handle == NULL` 或 `*handle == NULL` 返回 `-EINVAL`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 为初始化成功返回的有效句柄 |

**约束与注意事项**

- 成功后接口会将 `*handle` 置为 `NULL`。
- 调用该接口后，不得继续使用原句柄调用发送、查询或注册接口。

### 2.5 `ub_comm_queue_send`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_send` |
| 接口描述 | 向目标节点指定优先级 Ring 发送业务消息。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_send(ub_shm_comm_t *handle, const message_t *msg);` |
| 返回参数 | 成功返回 `0`；消息已入队但目标 Ring 达到拥塞阈值返回 `UB_COMM_SEND_CONGESTED`；失败返回负数错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `msg` | `const message_t *` | 待发送消息 | 非空；消息头字段满足 2.2 中规格 |

**常见返回值**

| 返回值 | 说明 |
| --- | --- |
| `0` | 发送成功，目标 Ring 未处于稳定拥塞 |
| `UB_COMM_SEND_CONGESTED` | 发送成功，但目标 Ring 已达到拥塞阈值，调用方可降速 |
| `-EINVAL` | 参数错误、优先级非法等 |
| `-EOPNOTSUPP` | 发送系统保留消息类型 |
| `-EPERM` | `src_node_id` 与当前节点不一致 |
| `-EMSGSIZE` | 消息大小超过目标 Ring 限制 |
| 私有负数错误码 | Ring 满、远端未就绪、目标 Ring 不存在、CAS 重试超限等内部错误 |

**约束与注意事项**

- `UB_COMM_SEND_CONGESTED` 表示消息已经入队，不是失败。
- 业务侧不应使用 `msg_type` 为 `0xFF`、`0xFE`、`0xFD` 的消息。
- 发送到远端前可使用 `ub_comm_queue_check_ready` 确认目标节点已就绪。

### 2.6 `ub_comm_queue_get_status`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_get_status` |
| 接口描述 | 查询指定节点、指定优先级 Ring 的流控状态快照。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_get_status(ub_shm_comm_t *handle, uint8_t node_id, uint8_t priority, ub_comm_queue_status_t *status);` |
| 返回参数 | 成功返回 `0`；失败返回负数错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `node_id` | `uint8_t` | 待查询节点 ID | 必须存在于当前节点映射表 |
| `priority` | `uint8_t` | 待查询 Ring 优先级 | `0~7`；业务 Ring 为 `1~7` |
| `status` | `ub_comm_queue_status_t *` | 输出状态快照 | 非空 |

**约束与注意事项**

- 返回的是原子快照，适合维测和流控估计，不提供强一致队列长度语义。
- 查询不存在的 Ring 或未就绪节点会返回负数错误码。

### 2.7 `ub_comm_queue_set_congestion_threshold`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_set_congestion_threshold` |
| 接口描述 | 设置当前节点本地指定业务 Ring 的拥塞阈值。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_set_congestion_threshold(ub_shm_comm_t *handle, uint8_t priority, uint32_t congestion_threshold_percent);` |
| 返回参数 | 成功返回 `0`；失败返回负数错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `priority` | `uint8_t` | 本地 Ring 优先级 | 必须为 `1~7`；`0` 为内部保留，不能配置 |
| `congestion_threshold_percent` | `uint32_t` | 拥塞阈值百分比 | `0~100`；`0` 表示所有非满状态都按拥塞处理 |

**约束与注意事项**

- 只更新当前节点本地 Ring。远端生产者写入该 Ring 时会从共享 Ring 对象读取到新阈值。
- 默认阈值语义可用 `80` 恢复到常见的 80% 水位。

### 2.8 `ub_comm_queue_config_heartbeat`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_config_heartbeat` |
| 接口描述 | 设置或查询当前通信实例的本地心跳配置。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_config_heartbeat(ub_shm_comm_t *handle, const ub_comm_queue_heartbeat_config_t *request, ub_comm_queue_heartbeat_config_t *effective);` |
| 返回参数 | 成功返回 `0`；失败返回负数错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `request` | `const ub_comm_queue_heartbeat_config_t *` | 请求设置的心跳配置 | 可为空；非空时字段满足 2.2 中规格 |
| `effective` | `ub_comm_queue_heartbeat_config_t *` | 输出最终生效配置 | 可为空 |

**约束与注意事项**

- `request == NULL && effective != NULL` 表示只查询当前配置。
- `request != NULL && effective == NULL` 表示只设置配置。
- `request != NULL && effective != NULL` 表示设置后返回最终生效配置。
- `request` 和 `effective` 不能同时为 `NULL`。
- 该接口只影响后台心跳线程，不修改收发热路径。
- 本节点发布 `heartbeat_interval_ms` 后，其他节点会用它放大对本节点的实际超时窗口，避免节点间心跳配置不一致导致误判。
- 本节点观察某个 peer 的实际超时窗口为 `max(timeout_ms, peer heartbeat_interval_ms * 3, check_interval_ms * 2)`。

**使用样例**

```c
ub_comm_queue_heartbeat_config_t req = {
    .heartbeat_interval_ms = 100,
    .check_interval_ms = 100,
    .timeout_ms = 1000,
};
ub_comm_queue_heartbeat_config_t eff = {0};
int ret = ub_comm_queue_config_heartbeat(&handle, &req, &eff);
```

### 2.9 `ub_comm_queue_get_heartbeat_status`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_get_heartbeat_status` |
| 接口描述 | 查询本节点对指定节点消费者心跳的本地观察状态。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_get_heartbeat_status(ub_shm_comm_t *handle, uint8_t node_id, ub_comm_queue_heartbeat_status_t *status);` |
| 返回参数 | 成功返回 `0`；失败返回负数错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `node_id` | `uint8_t` | 待查询节点 ID | 必须存在于当前节点映射表 |
| `status` | `ub_comm_queue_heartbeat_status_t *` | 输出心跳观察状态 | 非空 |

**约束与注意事项**

- 该接口只读本节点本地观察状态，不直接读取远端共享内存。
- `last_change_age_ms` 基于本节点本地单调时钟计算，不依赖跨节点时钟同步。
- `timeout_ms` 返回本节点对 `node_id` 使用的实际超时窗口；若 peer 声明了更大的心跳刷新周期，该值会大于本节点配置的最小超时窗口。

**使用样例**

```c
ub_comm_queue_heartbeat_status_t st = {0};
int ret = ub_comm_queue_get_heartbeat_status(&handle, 1, &st);
```

### 2.10 `ub_comm_queue_check_ready`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_check_ready` |
| 接口描述 | 查询指定节点是否已完成通信队列初始化。 |
| 接口类型 | 函数 |
| 函数原型 | `bool ub_comm_queue_check_ready(ub_shm_comm_t *handle, const uint8_t node_id);` |
| 返回参数 | 节点就绪返回 `true`；参数无效、节点未就绪或查询失败返回 `false`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `node_id` | `uint8_t` | 待查询节点 ID | 应存在于节点映射表 |

**约束与注意事项**

- 当前不建议业务依赖该接口完成收包；请优先使用 `ub_comm_queue_register_process_func` 注册回调。

### 2.11 `ub_comm_queue_recv`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_recv` |
| 接口描述 | 主动接收消息接口。当前主要收包路径为后台分发线程和回调派发。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_recv(ub_shm_comm_t *handle, void *buffer, uint32_t length);` |
| 返回参数 | 当前实现返回 `0`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `buffer` | `void *` | 接收缓冲区 | 当前实现未使用 |
| `length` | `uint32_t` | 接收缓冲区长度 | 当前实现未使用 |

**约束与注意事项**

- 当前不建议业务依赖该接口完成收包；请优先使用 `ub_comm_queue_register_process_func` 注册回调。

### 2.12 `ub_comm_queue_register_process_func`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_comm_queue_register_process_func` |
| 接口描述 | 为指定业务消息类型注册处理回调。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_comm_queue_register_process_func(ub_shm_comm_t *handle, uint8_t msg_type, ub_func_type_t func_type, ub_callback_t func, void *ctx);` |
| 返回参数 | 成功返回 `0`；失败返回负数错误码。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `ub_shm_comm_t *` | 通信实例句柄指针 | 非空，且 `*handle` 有效 |
| `msg_type` | `uint8_t` | 消息类型 | 不应为系统保留值 `0xFF`, `0xFE`或 `0xFD` |
| `func_type` | `ub_func_type_t` | 回调执行方式 | `UB_FUNC_SYNC` 或 `UB_FUNC_ASYNC` |
| `func` | `ub_callback_t` | 回调函数 | 非空 |
| `ctx` | `void *` | 用户上下文 | 可为空；回调时原样传回 |

**约束与注意事项**

- 同一个 `msg_type` 重复注册会覆盖旧回调。
- 已经被分发线程取出或已投递线程池的消息可能仍使用旧回调执行。

**通信队列使用样例**

```c
#include "ub_dist_comm_queue.h"
#include <stdint.h>
#include <stdlib.h>

static void on_msg(const message_t *msg, void *ctx)
{
    (void)ctx;
    /* 处理 msg->body，长度为 msg->header.body_length */
}

int comm_example(void)
{
    void *init_mem = calloc(1, 4096);
    void *ring_mem0 = calloc(1, 1 << 20);
    void *ring_mem1 = calloc(1, 1 << 20);
    ub_ring_region_info_t entries[2] = {
        {.region = {.size = 1 << 20, .ptr = ring_mem0}, .node_id = 0},
        {.region = {.size = 1 << 20, .ptr = ring_mem1}, .node_id = 1},
    };
    ub_ring_region_map_t map = {.entries = entries, .count = 2};
    ub_ring_desc_t desc = {.ring_capacity = 1024, .max_msg_size = 256, .priority = 1};
    ub_comm_conf_t conf = {.cpu_id = -1, .max_nodes = 2, .current_node_id = 0, .num_rings = 1, .ring_descs = &desc};
    ub_shm_area_t init_region = {.size = 4096, .ptr = init_mem};
    ub_shm_comm_t handle = NULL;

    int ret = ub_comm_queue_init(&handle, &init_region, &map, &conf);
    if (ret != 0) {
        return ret;
    }
    ret = ub_comm_queue_register_process_func(&handle, 1, UB_FUNC_SYNC, on_msg, NULL);
    if (ret != 0) {
        (void)ub_comm_queue_deinit(&handle);
        return ret;
    }

    char body[] = "hello";
    message_t msg = {
        .header = {.src_thread_id = 1001, .body_length = (uint32_t)sizeof(body), .dest_node_id = 1,
                   .src_node_id = 0, .msg_type = 1, .priority = 1},
        .body = body,
    };
    ret = ub_comm_queue_send(&handle, &msg);
    (void)ub_comm_queue_deinit(&handle);
    return ret == UB_COMM_SEND_CONGESTED ? 0 : ret;
}
```

## 3. 分布式读写锁

### 3.1 模块说明

分布式读写锁通过调用方提供的共享内存对象保存锁状态，支持共享锁 `S`、共享排他锁 `SX`、排他锁 `X`，并提供故障恢复、持有者查询和重建能力。

**全局约束**

- 共享内存中需为锁对象预留 `UB_RW_LOCK_SIZE` 字节，当前为 `640` 字节。
- `ub_rw_lock_t` 是不透明共享内存布局类型，调用方不应依赖其内部字段。
- `ub_location_t` 用于标识锁调用者，`node_id` 和 `tid` 应在业务集群内能唯一定位调用线程或逻辑线程。

### 3.2 公开数据结构

| 名称 | 类型 | 说明 |
| --- | --- | --- |
| `time_ms_t` | `uint64_t` | 毫秒时间戳类型 |
| `ub_rw_lock_t` | 不透明结构体 | 分布式读写锁共享内存对象 |

#### `ub_lock_mode_t`

| 名称 | 值 | 说明 |
| --- | ---: | --- |
| `UB_LOCK_S` | 0 | 共享锁，读锁 |
| `UB_LOCK_SX` | 1 | 共享排他锁，升级意图锁 |
| `UB_LOCK_X` | 2 | 排他锁，写锁 |
| `UB_LOCK_I` | 3 | 无效锁类型 |

#### `ub_lock_result_t`

| 名称 | 值 | 说明 |
| --- | ---: | --- |
| `UB_LOCK_SUCCESS` | 0 | 操作成功 |
| `UB_LOCK_TIMEOUT` | 1 | 加锁或等待超时 |
| `UB_LOCK_CONFLICT` | 2 | 锁冲突 |
| `UB_LOCK_ERROR` | 3 | 参数错误或内部错误 |

#### 锁参数结构

| 结构体 | 字段 | 类型 | 说明 | 参数有效性规格 |
| --- | --- | --- | --- | --- |
| `ub_location_t` | `tid` | `int32_t` | 线程 ID 或业务逻辑线程 ID | 同一节点内应能唯一标识调用者 |
| `ub_location_t` | `node_id` | `uint8_t` | 逻辑节点 ID | 应与通信/锁集群节点 ID 一致 |
| `ub_lock_policy_t` | `timeout_ts` | `time_ms_t` | 绝对超时时间戳，单位毫秒 | 传入 `NULL` 策略时使用默认值 `10000` |
| `ub_lock_policy_t` | `allow_delay_release` | `bool` | 是否允许延迟释放 | 传入 `NULL` 策略时默认 `false` |
| `ub_lock_policy_t` | `recursive` | `bool` | 是否允许递归加锁 | 传入 `NULL` 策略时默认 `false` |
| `ub_lock_config_t` | `lease_time` | `time_ms_t` | 分布式锁租约时长 | 传入 `NULL` 配置时默认 `60000` |
| `ub_lock_config_t` | `heartbeat_timeout` | `time_ms_t` | 心跳超时阈值 | 传入 `NULL` 配置时默认 `500` |
| `ub_lock_query_result_t` | `node_id` | `uint8_t` | 被查询节点 ID | 由查询接口填充 |
| `ub_lock_query_result_t` | `held_mode` | `ub_lock_mode_t` | 该节点可恢复的持锁模式 | 输出字段 |
| `ub_lock_query_result_t` | `holder_tid` | `int32_t` | `X/SX` 模式下的持有者线程 ID | 输出字段 |
| `ub_lock_query_result_t` | `recursive_count` | `uint32_t` | `X/SX` 模式下递归计数 | 输出字段 |
| `ub_lock_query_result_t` | `reserve_mode` | `ub_lock_mode_t` | 延迟释放保留模式 | 输出字段 |
| `ub_lock_rebuild_info_t` | `query_results` | `const ub_lock_query_result_t *` | 各节点查询结果数组 | 重建时非空，长度为 `query_result_count` |
| `ub_lock_rebuild_info_t` | `query_result_count` | `uint32_t` | 查询结果数量 | 重建时应与数组元素数量一致 |

### 3.3 `ub_rw_lock_create`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_create` |
| 接口描述 | 初始化共享内存中的分布式读写锁对象。 |
| 接口类型 | 函数 |
| 函数原型 | `void ub_rw_lock_create(ub_rw_lock_t *lock, const ub_lock_config_t *config, const ub_location_t *location);` |
| 返回参数 | 无。参数无效时直接返回。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `lock` | `ub_rw_lock_t *` | 共享内存锁对象地址 | 非空；指向至少 `UB_RW_LOCK_SIZE` 字节共享内存 |
| `config` | `const ub_lock_config_t *` | 锁配置 | 可为空；为空时使用默认配置 |
| `location` | `const ub_location_t *` | 调用者位置 | 非空 |

### 3.4 `ub_rw_lock_free`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_free` |
| 接口描述 | 释放调用节点与该锁关联的本地资源。 |
| 接口类型 | 函数 |
| 函数原型 | `void ub_rw_lock_free(ub_rw_lock_t *lock, const ub_location_t *location);` |
| 返回参数 | 无。参数无效时直接返回。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `lock` | `ub_rw_lock_t *` | 共享内存锁对象地址 | 非空 |
| `location` | `const ub_location_t *` | 调用者位置 | 非空 |

### 3.5 加锁接口

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_s_lock`、`ub_rw_lock_x_lock`、`ub_rw_lock_sx_lock` |
| 接口描述 | 分别申请共享锁、排他锁、共享排他锁。 |
| 接口类型 | 函数 |
| 函数原型 | `ub_lock_result_t ub_rw_lock_s_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);` |
| 函数原型 | `ub_lock_result_t ub_rw_lock_x_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);` |
| 函数原型 | `ub_lock_result_t ub_rw_lock_sx_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);` |
| 返回参数 | 返回 `ub_lock_result_t`。参数无效返回 `UB_LOCK_ERROR`；策略为空时使用默认策略。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `lock` | `ub_rw_lock_t *` | 共享内存锁对象地址 | 非空，且已通过 `ub_rw_lock_create` 初始化 |
| `policy` | `const ub_lock_policy_t *` | 本次加锁策略 | 可为空；为空使用默认策略 |
| `location` | `const ub_location_t *` | 调用者位置 | 非空 |

**约束与注意事项**

- `recursive == false` 时，同一调用者重复申请同类锁可能按冲突或错误处理。
- 调用方应使用与加锁模式匹配的解锁接口释放。

### 3.6 解锁接口

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_s_unlock`、`ub_rw_lock_x_unlock`、`ub_rw_lock_sx_unlock` |
| 接口描述 | 分别释放共享锁、排他锁、共享排他锁。 |
| 接口类型 | 函数 |
| 函数原型 | `ub_lock_result_t ub_rw_lock_s_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);` |
| 函数原型 | `ub_lock_result_t ub_rw_lock_x_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);` |
| 函数原型 | `ub_lock_result_t ub_rw_lock_sx_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);` |
| 返回参数 | 返回 `ub_lock_result_t`。参数无效返回 `UB_LOCK_ERROR`；策略为空时使用默认策略。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `lock` | `ub_rw_lock_t *` | 共享内存锁对象地址 | 非空，且已初始化 |
| `policy` | `const ub_lock_policy_t *` | 本次释放策略 | 可为空；为空使用默认策略 |
| `location` | `const ub_location_t *` | 调用者位置 | 非空，且应与持锁者匹配 |

**约束与注意事项**

- 释放未持有的锁、释放模式不匹配或调用者不匹配可能返回 `UB_LOCK_ERROR` 或 `UB_LOCK_CONFLICT`。

### 3.7 `ub_rw_lock_recover`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_recover` |
| 接口描述 | 对异常失败进程持有或半写入的锁状态进行恢复清理。 |
| 接口类型 | 函数 |
| 函数原型 | `ub_lock_result_t ub_rw_lock_recover(ub_rw_lock_t *lock, const uint32_t process_id, const ub_location_t *location);` |
| 返回参数 | 成功返回 `UB_LOCK_SUCCESS`；参数无效或恢复失败返回 `UB_LOCK_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `lock` | `ub_rw_lock_t *` | 共享内存锁对象地址 | 非空，且已初始化 |
| `process_id` | `uint32_t` | 待恢复的进程或节点标识 | 应与故障持有者的标识匹配 |
| `location` | `const ub_location_t *` | 执行恢复的调用者位置 | 非空 |

### 3.8 `ub_rw_lock_query_holder`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_query_holder` |
| 接口描述 | 查询本节点用于锁重建的最小持有者状态。 |
| 接口类型 | 函数 |
| 函数原型 | `ub_lock_result_t ub_rw_lock_query_holder(ub_rw_lock_t *lock, const ub_location_t *location, ub_lock_query_result_t *result);` |
| 返回参数 | 成功返回 `UB_LOCK_SUCCESS`；参数无效返回 `UB_LOCK_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `lock` | `ub_rw_lock_t *` | 共享内存锁对象地址 | 非空，且已初始化 |
| `location` | `const ub_location_t *` | 调用者位置 | 非空 |
| `result` | `ub_lock_query_result_t *` | 输出查询结果 | 非空 |

### 3.9 `ub_rw_lock_rebuild`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_rw_lock_rebuild` |
| 接口描述 | 根据集群各节点查询结果，在新的共享内存锁对象上重建锁状态。 |
| 接口类型 | 函数 |
| 函数原型 | `ub_lock_result_t ub_rw_lock_rebuild(ub_rw_lock_t *old_lock, ub_rw_lock_t *new_lock, const ub_lock_rebuild_info_t *rebuild_info, const ub_location_t *location);` |
| 返回参数 | 成功返回 `UB_LOCK_SUCCESS`；参数无效或重建失败返回 `UB_LOCK_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `old_lock` | `ub_rw_lock_t *` | 旧共享内存锁对象地址 | 非空 |
| `new_lock` | `ub_rw_lock_t *` | 新共享内存锁对象地址 | 非空，指向可写共享内存 |
| `rebuild_info` | `const ub_lock_rebuild_info_t *` | 聚合后的查询结果 | 非空；`query_results` 和 `query_result_count` 应匹配 |
| `location` | `const ub_location_t *` | 执行重建的调用者位置 | 非空 |

**分布式锁使用样例**

```c
#include <stdbool.h>
#include "ub_dist_lock.h"
#include <stdlib.h>

void lock_example(void)
{
    ub_rw_lock_t *lock = (ub_rw_lock_t *)calloc(1, UB_RW_LOCK_SIZE);
    ub_location_t loc = {.tid = 1001, .node_id = 1};
    ub_lock_config_t config = {.lease_time = 60000, .heartbeat_timeout = 500};
    ub_lock_policy_t policy = {.timeout_ts = 10000, .allow_delay_release = false, .recursive = false};

    ub_rw_lock_create(lock, &config, &loc);
    if (ub_rw_lock_x_lock(lock, &policy, &loc) == UB_LOCK_SUCCESS) {
        /* 临界区 */
        (void)ub_rw_lock_x_unlock(lock, &policy, &loc);
    }
    ub_rw_lock_free(lock, &loc);
    free(lock);
}
```

## 4. 分布式事务资源

### 4.1 模块说明

事务资源接口把调用方传入的 `uint64_t` 存储地址视作一个共享原子值，提供初始化、设置、读取和原子加法能力。

| 名称 | 值 | 说明 |
| --- | ---: | --- |
| `UB_RES_OK` | 0 | 操作成功 |
| `UB_RES_ERROR` | -1 | 操作失败 |

**通用参数有效性规格**

- `handle` 必须非空。
- `handle` 地址必须满足 `uint64_t` 对齐要求，当前实现按 8 字节对齐检查。
- 输出参数如 `out_val` 必须非空。
- 调用方负责保证 `handle` 指向的内存在所有参与方之间可见，并在使用期间保持有效。

### 4.2 `ub_dist_tx_res_init`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_dist_tx_res_init` |
| 接口描述 | 初始化事务资源原子值，将目标值置为 0。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_dist_tx_res_init(uint64_t *handle);` |
| 返回参数 | 成功返回 `UB_RES_OK`；参数无效或地址未对齐返回 `UB_RES_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `uint64_t *` | 共享原子值地址 | 非空，且 8 字节对齐 |

### 4.3 `ub_dist_tx_res_set`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_dist_tx_res_set` |
| 接口描述 | 设置事务资源原子值。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_dist_tx_res_set(uint64_t *handle, uint64_t value);` |
| 返回参数 | 成功返回 `UB_RES_OK`；参数无效或地址未对齐返回 `UB_RES_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `uint64_t *` | 共享原子值地址 | 非空，且 8 字节对齐 |
| `value` | `uint64_t` | 待设置值 | 任意 `uint64_t` 值 |

### 4.4 `ub_dist_tx_res_get`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_dist_tx_res_get` |
| 接口描述 | 读取事务资源当前值。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_dist_tx_res_get(uint64_t *handle, uint64_t *out_val);` |
| 返回参数 | 成功返回 `UB_RES_OK` 并写入 `*out_val`；参数无效或地址未对齐返回 `UB_RES_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `uint64_t *` | 共享原子值地址 | 非空，且 8 字节对齐 |
| `out_val` | `uint64_t *` | 输出读取值 | 非空 |

### 4.5 `ub_dist_tx_res_fetch_add`

| 项目 | 内容 |
| --- | --- |
| 名称 | `ub_dist_tx_res_fetch_add` |
| 接口描述 | 对事务资源执行原子加法，并返回加法前的旧值。 |
| 接口类型 | 函数 |
| 函数原型 | `int ub_dist_tx_res_fetch_add(uint64_t *handle, uint64_t value, uint64_t *out_val);` |
| 返回参数 | 成功返回 `UB_RES_OK` 并将旧值写入 `*out_val`；参数无效或地址未对齐返回 `UB_RES_ERROR`。 |

| 参数名 | 参数类型 | 参数类型说明 | 参数有效性规格 |
| --- | --- | --- | --- |
| `handle` | `uint64_t *` | 共享原子值地址 | 非空，且 8 字节对齐 |
| `value` | `uint64_t` | 原子增加值 | 任意 `uint64_t` 值，溢出按无符号整数规则回绕 |
| `out_val` | `uint64_t *` | 输出加法前旧值 | 非空 |

**使用样例**

```c
#include "ub_dist_tx_res.h"
#include <stdint.h>

int tx_res_example(void)
{
    uint64_t value = 0;
    uint64_t old_value = 0;

    if (ub_dist_tx_res_init(&value) != UB_RES_OK) {
        return -1;
    }
    if (ub_dist_tx_res_set(&value, 99) != UB_RES_OK) {
        return -1;
    }
    if (ub_dist_tx_res_fetch_add(&value, 1, &old_value) != UB_RES_OK) {
        return -1;
    }
    /* old_value == 99, value == 100 */
    return ub_dist_tx_res_get(&value, &old_value);
}
```

## 5. 接入建议

- 通信队列和分布式锁均依赖调用方提供共享内存，调用方应统一管理共享内存创建、映射、清零和生命周期。
- 业务消息类型和 Ring 优先级建议集中定义，避免与系统保留值冲突。
- 对外接口多以指针参数表达共享状态，调用前应显式校验非空、对齐、长度和节点 ID 范围。
- 对于发送接口，调用方应区分“发送失败”和“发送成功但拥塞提示”：`UB_COMM_SEND_CONGESTED` 不应被当作失败重发。
- 多进程场景下，所有参与方应使用相同版本的头文件和库文件，避免共享内存布局不一致。
