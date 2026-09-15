# ubs-atomic 接口说明

## 说明

ubs-atomic 对外提供 C ABI 接口，当前对外能力包括：

- 分布式读写锁（S/SX/X 模式）、分布式互斥锁、分布式自旋锁
- 分布式共享内存通信队列
- 分布式事务资源原子操作（含内存屏障）

### 头文件列表

开发代码时所需的头文件如[表 1](#table01)所示。

**表 1 <a id='table01'></a>** 头文件列表

| 头文件名称 | 用途 |
| --------------- | ------------------ |
| ub_dist_lock.h | 定义分布式读写锁、互斥锁、自旋锁对外接口，以及通用日志接口。 |
| ub_dist_comm_queue.h | 定义分布式共享内存通信队列对外接口，以及通用日志接口。 |
| ub_dist_tx_res.h | 定义分布式事务资源原子操作和内存屏障对外接口，以及通用日志接口。 |

三个头文件均使用 `extern "C"` 声明，C/C++ 程序均可直接引用；同一组通用日志接口（`ub_atomic_register_log_func`、`ub_atomic_set_log_level`）在三个头文件中均有暴露，链接时指向同一实现，任一头文件引入即可使用。

### 设置进程运行环境

- 库和头文件的安装路径取决于软件包格式：

| 产物 | RPM | 源码安装（默认） |
| ---- | -------------------------- | ------------------------------ |
| 共享库 | /usr/lib64/libubs-atomic.so | dist/release/lib/libubs-atomic.so |
| 头文件 | /usr/include/ | include/ |

- RPM 安装产物位于系统库目录，通常无需额外设置 `LD_LIBRARY_PATH`；源码构建部署时，需将产物目录加入 `LD_LIBRARY_PATH`：

  ```bash
  export LD_LIBRARY_PATH=/path/to/lib:$LD_LIBRARY_PATH
  ```

- 编译链接示例：

  ```bash
  gcc my_app.c -I/usr/include -lubs-atomic -lpthread -lrt -o my_app
  ```

- 多进程/多节点场景下，所有参与方应使用相同版本的 libubs-atomic.so 和头文件，避免共享内存布局不一致导致的未定义行为。
- 组件本身不创建、不销毁共享内存对象，调用方需自行创建并映射共享内存（可参考 `sample_code/share_mem`），并保证在接口使用期间内存保持有效。
- 事务资源接口要求目标地址 8 字节对齐。

## 接口

### 通用日志

#### ub_atomic_register_log_func

**接口功能**

注册用户自定义日志函数。库内部产生日志时会调用该函数，未注册时使用内部默认日志行为。

**接口格式**

```C
void ub_atomic_register_log_func(ub_atomic_log_func func);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----- | ---- | ---- | --------------- |
| func | ub_atomic_log_func | 入参 | 日志回调函数指针，原型为 `int (*)(int level, const char *file, const char *func, uint32_t line, const char *message)`；传入空指针表示取消用户日志函数。 |

**返回值**

无。

#### ub_atomic_set_log_level

**接口功能**

设置日志输出阈值，低于阈值的日志不会输出。

**接口格式**

```C
int ub_atomic_set_log_level(int level);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----- | ---- | ---- | --------------- |
| level | int | 入参 | 日志级别阈值，取值 `LOG_LEVEL_DEBUG`（0）~`LOG_LEVEL_CRITICAL`（4）。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| -1 | 日志级别非法。 |

### 分布式读写锁

#### ub_rw_lock_create

**接口功能**

初始化分布式读写锁。在调用方提供的共享内存上构建锁对象，锁配置在创建时固定。

**接口格式**

```C
void ub_rw_lock_create(ub_rw_lock_t *lock, const ub_lock_config_t *config, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_rw_lock_t \* | 入参 | 共享内存锁对象地址，需指向至少 `UB_RW_LOCK_SIZE`（640 字节）的共享内存。 |
| config | const ub_lock_config_t \* | 入参 | 锁配置（租约时间、心跳超时），字段说明参见[配置说明](ubs_atomic_configuration_instructions.md)。 |
| location | const ub_location_t \* | 入参 | 调用者位置（节点 ID + 线程 ID）。 |

**返回值**

无。参数无效时直接返回。

#### ub_rw_lock_free

**接口功能**

释放分布式读写锁关联的本节点资源。

**接口格式**

```C
void ub_rw_lock_free(ub_rw_lock_t *lock, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_rw_lock_t \* | 入参 | 共享内存锁对象地址。 |
| location | const ub_location_t \* | 入参 | 调用者位置。 |

**返回值**

无。

#### ub_rw_lock_s_lock / ub_rw_lock_x_lock / ub_rw_lock_sx_lock

**接口功能**

分别获取共享（S）锁、独占（X）写锁、共享排他（SX）锁。

**接口格式**

```C
ub_lock_result_t ub_rw_lock_s_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);
ub_lock_result_t ub_rw_lock_x_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);
ub_lock_result_t ub_rw_lock_sx_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_rw_lock_t \* | 入参 | 共享内存锁对象地址，且已初始化。 |
| policy | const ub_lock_policy_t \* | 入参 | 本次加锁策略（超时时间、是否允许延迟释放、是否允许递归），可为空，为空时使用默认策略。 |
| location | const ub_location_t \* | 入参 | 调用者位置，非空。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 加锁成功。 |
| UB_LOCK_TIMEOUT | 等待超时。 |
| UB_LOCK_ERROR | 参数无效或内部错误。 |

#### ub_rw_lock_s_unlock / ub_rw_lock_x_unlock / ub_rw_lock_sx_unlock

**接口功能**

分别释放共享（S）锁、独占（X）写锁、共享排他（SX）锁。释放模式应与加锁模式匹配。

**接口格式**

```C
ub_lock_result_t ub_rw_lock_s_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);
ub_lock_result_t ub_rw_lock_x_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);
ub_lock_result_t ub_rw_lock_sx_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_rw_lock_t \* | 入参 | 共享内存锁对象地址，且已初始化。 |
| policy | const ub_lock_policy_t \* | 入参 | 本次释放策略，可为空，为空时使用默认策略。 |
| location | const ub_location_t \* | 入参 | 调用者位置，应与持锁者匹配。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 解锁成功。 |
| UB_LOCK_ERROR | 参数无效、未持锁或调用者不匹配。 |

#### ub_rw_lock_recover

**接口功能**

对异常失败进程持有或半写入的锁状态进行恢复清理。

**接口格式**

```C
ub_lock_result_t ub_rw_lock_recover(ub_rw_lock_t *lock, const uint32_t process_id, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_rw_lock_t \* | 入参 | 共享内存锁对象地址，且已初始化。 |
| process_id | uint32_t | 入参 | 待恢复的故障进程 ID。 |
| location | const ub_location_t \* | 入参 | 执行恢复的调用者位置。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 恢复成功。 |
| UB_LOCK_ERROR | 参数无效或恢复失败。 |

#### ub_rw_lock_query_holder

**接口功能**

查询本节点用于锁重建的最小持有者状态。

**接口格式**

```C
ub_lock_result_t ub_rw_lock_query_holder(ub_rw_lock_t *lock, const ub_location_t *location, ub_lock_query_result_t *result);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_rw_lock_t \* | 入参 | 共享内存锁对象地址，且已初始化。 |
| location | const ub_location_t \* | 入参 | 调用者位置。 |
| result | ub_lock_query_result_t \* | 出参 | 输出本节点归一化的持有者快照（持锁模式、持有者线程 ID、递归计数等）。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 查询成功。 |
| UB_LOCK_ERROR | 参数无效。 |

#### ub_rw_lock_rebuild

**接口功能**

根据集群各节点的查询结果，在新的共享内存锁对象上重建锁状态。

**接口格式**

```C
ub_lock_result_t ub_rw_lock_rebuild(ub_rw_lock_t *old_lock, ub_rw_lock_t *new_lock, const ub_lock_rebuild_info_t *rebuild_info, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| old_lock | ub_rw_lock_t \* | 入参 | 旧共享内存锁对象地址。 |
| new_lock | ub_rw_lock_t \* | 入参 | 新共享内存锁对象地址，指向可写共享内存。 |
| rebuild_info | const ub_lock_rebuild_info_t \* | 入参 | 集群各节点 `ub_rw_lock_query_holder` 结果的聚合。 |
| location | const ub_location_t \* | 入参 | 执行重建的调用者位置。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 重建成功。 |
| UB_LOCK_ERROR | 参数无效或重建失败。 |

### 分布式互斥锁

#### ub_mutex_lock_create

**接口功能**

初始化分布式互斥锁。

**接口格式**

```C
void ub_mutex_lock_create(ub_mutex_lock_t *lock);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_mutex_lock_t \* | 入参 | 共享内存互斥锁对象地址，需指向至少 `UB_MUTEX_LOCK_SIZE`（384 字节）的共享内存。 |

**返回值**

无。参数无效时直接返回。

#### ub_mutex_lock_free

**接口功能**

释放分布式互斥锁关联资源。

**接口格式**

```C
void ub_mutex_lock_free(ub_mutex_lock_t *lock);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_mutex_lock_t \* | 入参 | 共享内存互斥锁对象地址。 |

**返回值**

无。

#### ub_mutex_lock

**接口功能**

获取分布式互斥锁。

**接口格式**

```C
ub_lock_result_t ub_mutex_lock(ub_mutex_lock_t *lock, time_ms_t timeout_ms, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_mutex_lock_t \* | 入参 | 共享内存互斥锁对象地址，且已初始化。 |
| timeout_ms | time_ms_t | 入参 | 超时时间，单位毫秒；0 表示使用默认值 10000ms。 |
| location | const ub_location_t \* | 入参 | 调用者位置。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 加锁成功。 |
| UB_LOCK_TIMEOUT | 等待超时。 |
| UB_LOCK_ERROR | 参数无效。 |

#### ub_mutex_unlock

**接口功能**

释放分布式互斥锁。

**接口格式**

```C
ub_lock_result_t ub_mutex_unlock(ub_mutex_lock_t *lock, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_mutex_lock_t \* | 入参 | 共享内存互斥锁对象地址，且已初始化。 |
| location | const ub_location_t \* | 入参 | 调用者位置，应与持锁者匹配。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 解锁成功。 |
| UB_LOCK_ERROR | 参数无效或调用者不匹配。 |

### 分布式自旋锁

#### ub_spin_lock_init

**接口功能**

初始化分布式自旋锁。

**接口格式**

```C
void ub_spin_lock_init(ub_spin_lock_t *lock);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_spin_lock_t \* | 入参 | 共享内存自旋锁对象地址，需指向至少 `UB_SPIN_LOCK_SIZE`（64 字节）的共享内存。 |

**返回值**

无。参数无效时直接返回。

#### ub_spin_lock

**接口功能**

获取分布式自旋锁。等待期间不阻塞线程，持续尝试获取，适用于极短临界区。

**接口格式**

```C
ub_lock_result_t ub_spin_lock(ub_spin_lock_t *lock, time_ms_t timeout_ms, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_spin_lock_t \* | 入参 | 共享内存自旋锁对象地址，且已初始化。 |
| timeout_ms | time_ms_t | 入参 | 超时时间，单位毫秒；0 表示使用默认值 10000ms。 |
| location | const ub_location_t \* | 入参 | 调用者位置。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 加锁成功。 |
| UB_LOCK_TIMEOUT | 等待超时。 |
| UB_LOCK_ERROR | 参数无效。 |

#### ub_spin_unlock

**接口功能**

释放分布式自旋锁。

**接口格式**

```C
ub_lock_result_t ub_spin_unlock(ub_spin_lock_t *lock, const ub_location_t *location);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| lock | ub_spin_lock_t \* | 入参 | 共享内存自旋锁对象地址，且已初始化。 |
| location | const ub_location_t \* | 入参 | 调用者位置，应与持锁者匹配。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_LOCK_SUCCESS | 解锁成功。 |
| UB_LOCK_ERROR | 参数无效或调用者不匹配。 |

### 分布式通信队列

#### ub_comm_queue_init

**接口功能**

初始化共享内存通信实例：创建本地 Ring，发布节点状态，并启动后台分发线程。

**接口格式**

```C
int ub_comm_queue_init(ub_shm_comm_t *handle, ub_shm_area_t *init_region, ub_ring_region_map_t *ring_regions, ub_comm_conf_t *conf);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 出参 | 输出通信实例句柄。 |
| init_region | ub_shm_area_t \* | 入参 | 全局初始化公告牌共享内存区域，`size` 需足够容纳内部公告牌（建议不小于 4096 字节）。 |
| ring_regions | ub_ring_region_map_t \* | 入参 | 所有节点 Ring 区域映射数组，各节点看到的数组内容和顺序应保持一致。 |
| conf | ub_comm_conf_t \* | 入参 | 当前节点通信配置（绑核 CPU、节点数、Ring 配置等），字段说明参见[配置说明](ubs_atomic_configuration_instructions.md)。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| 负数错误码 | 参数错误（如 -EINVAL）、内存不足（-ENOMEM）等。 |

说明：

- 第一个初始化成功的通信实例会被内部标记为分布式锁使用的实例。
- 集群最大节点数为 16；`priority` 取值 0~7，其中 0 为内部保留，业务 Ring 配置不得使用。
- 初始化成功后必须调用 `ub_comm_queue_deinit` 释放实例。

#### ub_comm_queue_deinit

**接口功能**

反初始化通信实例，停止后台线程并释放实例对象。

**接口格式**

```C
int ub_comm_queue_deinit(ub_shm_comm_t *handle);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄指针，且 `*handle` 为初始化成功返回的有效句柄。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功，成功后 `*handle` 被置为 NULL。 |
| 负数错误码 | handle 为空（-EINVAL）等。 |

#### ub_comm_queue_send

**接口功能**

向目标节点指定优先级 Ring 发送业务消息。

**接口格式**

```C
int ub_comm_queue_send(ub_shm_comm_t *handle, const message_t *msg);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| msg | const message_t \* | 入参 | 待发送消息，`header` 中各字段需满足约束：`dest_node_id` 必须存在于节点映射中；`src_node_id` 必须等于当前节点 ID；`msg_type` 不得使用系统保留值 0xFF、0xFE、0xFD；`priority` 使用已配置的 1~7；消息头加消息体不得超过目标 Ring 的 `max_msg_size`。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 发送成功，目标 Ring 未处于拥塞状态。 |
| UB_COMM_SEND_CONGESTED（1） | 发送成功，但目标 Ring 已达到拥塞阈值，调用方可据此降速。 |
| 负数错误码 | 参数错误（-EINVAL）、消息超长（-EMSGSIZE）、保留消息类型（-EOPNOTSUPP）、源节点不匹配（-EPERM）、Ring 满、远端未就绪等。 |

#### ub_comm_queue_recv

**接口功能**

接收消息。当前主推的收包方式是注册回调（见 `ub_comm_queue_register_process_func`），不建议业务直接依赖本接口收包。

**接口格式**

```C
int ub_comm_queue_recv(ub_shm_comm_t *handle, void *buffer, uint32_t length);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| buffer | void \* | 出参 | 接收消息的缓冲区。 |
| length | uint32_t | 入参 | 缓冲区长度，应不小于单条消息最大长度。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| 负数错误码 | 参数错误、无消息等。 |

#### ub_comm_queue_get_status

**接口功能**

查询指定节点、指定优先级 Ring 的流控状态快照。

**接口格式**

```C
int ub_comm_queue_get_status(ub_shm_comm_t *handle, uint8_t node_id, uint8_t priority, ub_comm_queue_status_t *status);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| node_id | uint8_t | 入参 | 待查询节点 ID，必须存在于当前节点映射表。 |
| priority | uint8_t | 入参 | 待查询 Ring 优先级，取值 0~7。 |
| status | ub_comm_queue_status_t \* | 出参 | 输出状态快照（used/total/free、队列状态、拥塞阈值、最大深度等）。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| 负数错误码 | Ring 不存在、节点未就绪、参数错误等。 |

说明：返回的是原子快照，适合维测和流控估计，不提供强一致队列长度语义。

#### ub_comm_queue_set_congestion_threshold

**接口功能**

设置当前节点本地指定业务 Ring 的拥塞阈值百分比。远端生产者写入该 Ring 时会通过共享 Ring 对象读取到新阈值。

**接口格式**

```C
int ub_comm_queue_set_congestion_threshold(ub_shm_comm_t *handle, uint8_t priority, uint32_t congestion_threshold_percent);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| priority | uint8_t | 入参 | 本地 Ring 优先级，必须为 1~7；0 为内部保留，不能配置。 |
| congestion_threshold_percent | uint32_t | 入参 | 拥塞阈值百分比，取值 0~100；0 表示所有非满状态都按拥塞处理；默认值为 80。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| 负数错误码 | 参数错误等。 |

#### ub_comm_queue_config_heartbeat

**接口功能**

设置和/或查询当前通信实例的本地心跳配置。

**接口格式**

```C
int ub_comm_queue_config_heartbeat(ub_shm_comm_t *handle, const ub_comm_queue_heartbeat_config_t *request, ub_comm_queue_heartbeat_config_t *effective);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| request | const ub_comm_queue_heartbeat_config_t \* | 入参 | 请求设置的心跳配置；为 NULL 表示仅查询。 |
| effective | ub_comm_queue_heartbeat_config_t \* | 出参 | 输出最终生效配置；为 NULL 表示不需要回读。request 与 effective 不能同时为 NULL。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| 负数错误码 | 参数错误等。 |

说明：本节点观察某个 peer 的实际超时窗口为 `max(timeout_ms, peer heartbeat_interval_ms * 3, check_interval_ms * 2)`，用于避免节点间心跳配置不一致导致误判。

#### ub_comm_queue_check_ready

**接口功能**

查询指定节点是否已完成通信队列初始化。

**接口格式**

```C
bool ub_comm_queue_check_ready(ub_shm_comm_t *handle, const uint8_t node_id);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| node_id | uint8_t | 入参 | 待查询节点 ID。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| true | 节点已就绪。 |
| false | 参数无效或节点未就绪。 |

#### ub_comm_queue_register_process_func

**接口功能**

为指定业务消息类型注册处理回调。

**接口格式**

```C
int ub_comm_queue_register_process_func(ub_shm_comm_t *handle, uint8_t msg_type, ub_func_type_t func_type, ub_callback_t func, void *ctx);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | ub_shm_comm_t \* | 入参 | 通信实例句柄。 |
| msg_type | uint8_t | 入参 | 消息类型，不得使用系统保留值 0xFF、0xFE、0xFD。 |
| func_type | ub_func_type_t | 入参 | 回调执行方式：UB_FUNC_SYNC（同步回调）或 UB_FUNC_ASYNC（异步回调，投递到内部线程池执行）。 |
| func | ub_callback_t | 入参 | 回调函数，原型为 `void (*)(const message_t *msg, void *ctx)`，非空。 |
| ctx | void \* | 入参 | 用户上下文，可为空，回调时原样传回。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| 0 | 操作成功。 |
| 负数错误码 | 参数错误等。 |

说明：同一个 `msg_type` 重复注册会覆盖旧回调；已被分发线程取出或已投递线程池的消息可能仍使用旧回调执行。

### 分布式事务资源

#### ub_dist_tx_res_init

**接口功能**

初始化分布式事务资源原子值，将目标值置为 0。

**接口格式**

```C
int ub_dist_tx_res_init(uint64_t *handle);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK（0） | 操作成功。 |
| UB_RES_ERROR（-1） | 参数无效或地址未 8 字节对齐。 |

#### ub_dist_tx_res_set

**接口功能**

设置分布式事务资源的值。

**接口格式**

```C
int ub_dist_tx_res_set(uint64_t *handle, uint64_t value);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |
| value | uint64_t | 入参 | 待设置的值。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 操作成功。 |
| UB_RES_ERROR | 参数无效或地址未对齐。 |

#### ub_dist_tx_res_get

**接口功能**

读取分布式事务资源当前值。

**接口格式**

```C
int ub_dist_tx_res_get(uint64_t *handle, uint64_t *out_val);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |
| out_val | uint64_t \* | 出参 | 输出读取值。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 操作成功。 |
| UB_RES_ERROR | 参数无效或地址未对齐。 |

#### ub_dist_tx_res_fetch_add

**接口功能**

对分布式事务资源执行原子加法，并返回加法前的旧值。使用 acq_rel 语义。

**接口格式**

```C
int ub_dist_tx_res_fetch_add(uint64_t *handle, uint64_t value, uint64_t *out_val);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |
| value | uint64_t | 入参 | 原子增加值，溢出按无符号整数规则回绕。 |
| out_val | uint64_t \* | 出参 | 输出加法前的旧值。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 操作成功。 |
| UB_RES_ERROR | 参数无效或地址未对齐。 |

#### ub_dist_tx_res_add

**接口功能**

对分布式事务资源执行原子加法（无 fetch 版本），不返回旧值。使用 release 语义，可优化为更轻量指令，适用于仅需累加不需旧值的场景。

**接口格式**

```C
int ub_dist_tx_res_add(uint64_t *handle, uint64_t value);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |
| value | uint64_t | 入参 | 原子增加值，溢出按无符号整数规则回绕。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 操作成功。 |
| UB_RES_ERROR | 参数无效或地址未对齐。 |

#### ub_dist_tx_res_fetch_xor

**接口功能**

对分布式事务资源执行原子异或并返回旧值。使用 acq_rel 语义。

**接口格式**

```C
int ub_dist_tx_res_fetch_xor(uint64_t *handle, uint64_t value, uint64_t *out_val);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |
| value | uint64_t | 入参 | 要异或的值。 |
| out_val | uint64_t \* | 出参 | 输出异或前的旧值。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 操作成功。 |
| UB_RES_ERROR | 参数无效或地址未对齐。 |

#### ub_dist_tx_res_compare_exchange

**接口功能**

对分布式事务资源执行原子比较并交换（CAS）。若 handle 指向的值等于 `*expected`，则原子替换为 desired；否则将当前值写入 `*expected`。使用 acq_rel（成功）/ acquire（失败）语义，strong 语义，不发生伪失败。

**接口格式**

```C
int ub_dist_tx_res_compare_exchange(uint64_t *handle, uint64_t *expected, uint64_t desired, int *success);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| handle | uint64_t \* | 入参 | 共享原子值地址，非空且 8 字节对齐。 |
| expected | uint64_t \* | 入参/出参 | 输入为期望值；CAS 失败时输出当前实际值，可据此重试。 |
| desired | uint64_t | 入参 | 期望匹配时要写入的新值。 |
| success | int \* | 出参 | 输出 CAS 是否匹配（1=成功，0=失败）。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 函数调用成功（注意：CAS 是否匹配以 `*success` 为准）。 |
| UB_RES_ERROR | 参数无效或地址未对齐。 |

#### ub_dist_tx_res_fence

**接口功能**

统一内存屏障接口。根据 order 参数插入对应强度的编译器屏障和硬件屏障，所有变体均包含 compiler barrier。

**接口格式**

```C
int ub_dist_tx_res_fence(ub_fence_order_t order);
```

**参数说明**

| 参数名 | 数据类型 | 参数类型 | 描述 |
| ----------------- | --------------------- | ---- | ------------ |
| order | ub_fence_order_t | 入参 | 屏障语义，取值 UB_FENCE_RELAXED（0，仅编译器屏障）~ UB_FENCE_SEQ_CST（4，全局一致序，ARM64 为 dsb ish）。 |

**返回值**

| 返回值 | 描述 |
| --- | ------------------------------- |
| UB_RES_OK | 操作成功。 |
| UB_RES_ERROR | order 超出合法范围。 |

## 错误码

### 通用日志接口返回码

| 返回码 | 描述 |
| --- | --- |
| 0 | 操作成功。 |
| -1 | 日志级别非法（ub_atomic_set_log_level）。 |

### 分布式锁返回码

| 返回码 | 值 | 描述 |
| --- | ---: | --- |
| UB_LOCK_SUCCESS | 0 | 操作成功。 |
| UB_LOCK_TIMEOUT | 1 | 等待超时。 |
| UB_LOCK_CONFLICT | 2 | 锁冲突。 |
| UB_LOCK_ERROR | 3 | 参数错误或内部异常。 |

### 通信队列返回码

| 返回码 | 描述 |
| --- | --- |
| 0 | 操作成功。 |
| UB_COMM_SEND_CONGESTED（1） | 消息已入队，但目标 Ring 达到拥塞阈值（非失败）。 |
| -EINVAL | 参数错误。 |
| -EOPNOTSUPP | 发送系统保留消息类型。 |
| -EPERM | 消息源节点与当前节点不一致。 |
| -EMSGSIZE | 消息大小超过目标 Ring 限制。 |
| -ENOMEM | 内存不足。 |
| 其他负数错误码 | Ring 满、远端未就绪、CAS 重试超限等内部错误。 |

### 事务资源返回码

| 返回码 | 值 | 描述 |
| --- | ---: | --- |
| UB_RES_OK | 0 | 操作成功。 |
| UB_RES_ERROR | -1 | 操作失败（参数无效、地址未 8 字节对齐、order 非法等）。 |

## 使用样例

完整的可编译样例参见仓库 `sample_code/` 目录：

| 样例 | 说明 |
| --- | --- |
| sample_code/ub_lock/ub_dist_lock_func_test.cpp | 分布式锁（含故障恢复与重建）功能验证。 |
| sample_code/ub_comm_queue/pingpong.cpp | 双节点通信队列收发验证。 |
| sample_code/ub_dist_tx_res/ub_dist_tx_res_fence_semantic_test.cpp | 事务资源原子操作与内存屏障语义验证。 |
| sample_code/share_mem/ubsm_shm_creator.cpp | 锁和队列所需共享内存对象的创建/删除工具。 |
