# ubs-atomic 配置说明

本文档说明 ubs-atomic 的构建配置项和运行时配置项，以及各配置项的取值范围、默认值和配置建议。

## 构建配置

### 构建类型

通过 `build.sh -T <type>` 指定构建类型，默认为 Release。

**表 1** 构建类型说明

| 构建类型 | 说明 | 构建目录 |
| ---- | ---- | ---- |
| Release | 默认。`-O2` 优化，开启 `-D_FORTIFY_SOURCE=2`，无调试信息 | dist/release |
| Debug | `-O0 -ggdb3`，带完整调试信息，UT/覆盖率默认使用该类型 | dist/debug |
| RelWithDebInfo | `-O2` 优化并保留调试信息 | relwithdebinfo |
| MinSizeRel | 最小体积优化 | minsizerel |

### build.sh 常用参数

**表 2** build.sh 参数说明

| 参数 | 说明 |
| ---- | ---- |
| `-D` / `--debug` | Debug 构建 |
| `-T <type>` | 指定构建类型（Debug/Release/RelWithDebInfo/MinSizeRel） |
| `-t <target>` | 指定构建目标：`all`（默认）、`package`（RPM 打包）、`test`（构建并运行 UT） |
| `-V <ver>` | 指定发布版本，格式 `1.0.0-1` 或纯 release 号（如 `2`） |
| `-c` | 清理构建目录 |
| `-j <N>` | 指定并行任务数，默认为 CPU 线程数（本地构建自动减 2） |
| `-v` | 输出详细构建日志 |
| `--ninja` / `--make` | 强制指定构建生成器（默认自动检测，有 Ninja 时优先使用） |
| `--asan` / `--lsan` / `--tsan` / `--ubsan` | 分别启用 Address/Leak/Thread/UndefinedBehavior Sanitizer |
| `--std <N>` | 指定 C++ 标准版本，默认 17 |

### CMake 缓存变量

**表 3** CMake 变量说明

| 变量 | 默认值 | 说明 |
| ---- | ---- | ---- |
| UBS_ATOMIC_VERSION | 1.0.0 | RPM 包版本号 |
| UBS_ATOMIC_PACKAGE_RELEASE | 1 | RPM 包 release 号 |
| BUILD_TESTS | OFF | 是否构建测试目录 |
| ENABLE_COVERAGE | OFF | 是否生成覆盖率报告（依赖 `lcov` + `genhtml`） |

### 平台相关编译选项

- ARM64（aarch64）平台自动附加 `-march=armv8-a+lse`，启用 LSE 原子指令。
- x86_64 平台不传入该选项，可直接本地编译（不在默认配套范围）。

### 安全编译选项

Release 与 Debug 构建均默认启用安全编译选项（详见[安全说明](ubs_atomic_security_instructions.md)）。

## 资源规格配置

### 锁对象共享内存大小

锁对象直接构建在调用方提供的共享内存上，各锁对象的最小共享内存需求如下。

**表 4** 锁对象共享内存需求

| 资源类型 | 宏定义 | 大小 |
| ---- | ---- | ----: |
| 分布式读写锁 | UB_RW_LOCK_SIZE | 640 字节 |
| 分布式互斥锁 | UB_MUTEX_LOCK_SIZE | 384 字节 |
| 分布式自旋锁 | UB_SPIN_LOCK_SIZE | 64 字节 |
| 分布式事务资源 | - | 8 字节（1 个 uint64_t，需 8 字节对齐） |

建议为每个锁对象预留独立且对齐的共享内存区域，所有节点使用相同的地址布局。

### 共享内存规划

ubs-atomic 不管理共享内存生命周期，调用方负责创建、映射、清零和销毁。共享内存域配置可参考 `sample_code/share_mem/ubsm_region.conf`：

```ini
request_size_mb=1024
hosts=computer01,computer02
```

| 配置项 | 说明 |
| ---- | ---- |
| request_size_mb | 每个共享内存对象大小，单位 MB。 |
| hosts | 参与共享内存域的主机名列表，必须和执行环境中的节点主机名一致。 |

## 运行时配置

### 分布式读写锁配置

#### 锁配置（ub_lock_config_t，创建时固定）

**表 5** ub_lock_config_t 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| ---- | ---- | ----: | ---- |
| lease_time | time_ms_t | 60000 | 锁租约时长，单位毫秒。建议设置为业务临界区最长执行时间的 2~3 倍。 |
| heartbeat_timeout | time_ms_t | 500 | 心跳超时阈值，单位毫秒。超时未收到持锁者心跳则触发故障检测。 |

#### 加锁策略（ub_lock_policy_t，每次加锁可变）

**表 6** ub_lock_policy_t 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| ---- | ---- | ----: | ---- |
| timeout_ts | time_ms_t | 10000 | 本次加锁的绝对超时时间戳，单位毫秒。 |
| allow_delay_release | bool | false | 是否允许延迟释放。开启后解锁时延迟通知等待者，可减少跨节点通知开销，但会延迟锁释放时机，适合高吞吐、低实时性场景。 |
| recursive | bool | false | 是否允许同一线程递归加锁。 |

#### 锁模式兼容性

**表 7** 锁模式兼容矩阵

| 已持有 \ 请求 | S（共享读） | SX（共享排他） | X（独占写） |
| ---- | ---- | ---- | ---- |
| S（共享读） | 兼容 | 兼容 | 互斥 |
| SX（共享排他） | 兼容 | 互斥 | 互斥 |
| X（独占写） | 互斥 | 互斥 | 互斥 |

#### 互斥锁/自旋锁超时

`ub_mutex_lock`、`ub_spin_lock` 的 `timeout_ms` 参数为 0 时使用默认值 10000ms。自旋锁仅建议用于极短临界区，超时时间不宜设置过大。

### 通信队列配置

#### 全局通信配置（ub_comm_conf_t）

**表 8** ub_comm_conf_t 配置项

| 配置项 | 类型 | 约束 | 说明 |
| ---- | ---- | ---- | ---- |
| cpu_id | int32_t | -1 或有效 CPU ID | 后台分发线程绑核 CPU ID；-1 表示不绑核。 |
| max_nodes | uint8_t | 1~16 | 集群最大节点数。 |
| current_node_id | uint8_t | 必须存在于节点映射中 | 当前节点 ID，集群内唯一。 |
| num_rings | uint8_t | 1~7 | 当前节点创建的业务 Ring 数量（不含内部保留 Ring）。 |
| ring_descs | ub_ring_desc_t \* | 非空，长度不小于 num_rings | Ring 配置数组。 |

#### Ring 配置（ub_ring_desc_t）

**表 9** ub_ring_desc_t 配置项

| 配置项 | 类型 | 约束 | 说明 |
| ---- | ---- | ---- | ---- |
| ring_capacity | uint32_t | 必须为 2 的幂 | Ring 容量，按消息条目计。 |
| max_msg_size | uint32_t | 不小于消息头 + 消息体 | 单条消息最大大小，单位字节。 |
| priority | uint8_t | 业务 Ring 取 1~7 | Ring 优先级，数值越小级别越高；0 为内部分布式锁 Ring 和系统消息保留。 |

#### 拥塞阈值

通过 `ub_comm_queue_set_congestion_threshold` 配置本地 Ring 拥塞阈值百分比：

- 取值范围 0~100，默认 80（即 80% 水位）。
- 0 表示所有非满状态都按拥塞处理。
- 只影响当前节点本地 Ring，远端生产者通过共享 Ring 对象读取新阈值。
- 达到阈值后发送返回 `UB_COMM_SEND_CONGESTED`，消息仍成功入队。

#### 队列状态

**表 10** 队列状态说明

| 状态 | 判定条件 |
| ---- | ---- |
| UB_COMM_QUEUE_IDLE | used == 0 |
| UB_COMM_QUEUE_NORMAL | 0 < used < congestion_threshold |
| UB_COMM_QUEUE_CONGESTED | used >= congestion_threshold 且 used < total |
| UB_COMM_QUEUE_FULL | used >= total |

#### 心跳配置（ub_comm_queue_heartbeat_config_t）

**表 11** 心跳配置项

| 配置项 | 类型 | 约束 | 说明 |
| ---- | ---- | ---- | ---- |
| heartbeat_interval_ms | uint32_t | 大于 0 | 本节点消费者心跳序号刷新周期，单位毫秒，会发布给其他节点用于超时窗口计算。 |
| check_interval_ms | uint32_t | 大于 0 | 本节点生产者心跳监控线程轮询周期，单位毫秒。 |
| timeout_ms | uint32_t | 大于 0 且不小于 2 × check_interval_ms | 本节点观察 peer 的最小超时阈值，单位毫秒。 |

说明：本节点观察某个 peer 的实际超时窗口为 `max(timeout_ms, peer heartbeat_interval_ms × 3, check_interval_ms × 2)`。

### 日志配置

通过 `ub_atomic_set_log_level` 设置日志输出阈值，通过 `ub_atomic_register_log_func` 注册自定义日志回调（回调内不应阻塞过久，如需保存日志内容应自行拷贝）。

**表 12** 日志级别

| 级别 | 值 | 说明 |
| ---- | ---: | ---- |
| LOG_LEVEL_DEBUG | 0 | 调试日志 |
| LOG_LEVEL_INFO | 1 | 普通信息日志 |
| LOG_LEVEL_WARN | 2 | 告警日志 |
| LOG_LEVEL_ERROR | 3 | 错误日志 |
| LOG_LEVEL_CRITICAL | 4 | 严重错误日志 |

## 保留值与全局约束

**表 13** 保留值与约束汇总

| 项目 | 约束 |
| ---- | ---- |
| Ring 优先级 0 | 内部分布式锁 Ring 和系统消息保留，业务不得配置和发送。 |
| 消息类型 0xFF / 0xFE / 0xFD | 系统保留消息类型，业务侧不应发送或注册。 |
| 集群最大节点数 | 16。 |
| Ring 优先级范围 | 0~7，共 8 级，数值越小级别越高。 |
| ring_capacity | 必须为 2 的幂。 |
| 事务资源地址 | 必须 8 字节对齐。 |
| 消息大小 | 消息头 + 消息体不得超过目标 Ring 的 max_msg_size。 |
| ring_regions 一致性 | 各节点看到的节点映射数组内容和顺序应保持一致。 |
| 版本一致性 | 所有参与方应使用相同版本的库和头文件，避免共享内存布局不一致。 |

## 样例配置参考

锁功能验证样例的节点/共享内存配置可参考 `sample_code/ub_lock/dw_lock.conf`：

```ini
self=NodeA
nodes=2
lock_shm=shm_ub_lock
shm.NodeA=shm_node1_export
shm.NodeB=shm_node2_export
ip.NodeA=192.168.100.100
ip.NodeB=192.168.100.101
```

| 配置项 | 说明 |
| ---- | ---- |
| `self` | 本节点身份标识。 |
| `nodes` | 节点总数。 |
| `lock_shm` | 存放读写锁对象的全局共享内存名。 |
| `shm.<NodeX>` | 各节点通信队列导出共享内存名。 |
| `ip.<NodeX>` | 各节点 IP，用于 query/rebuild 测试交换查询结果。 |

共享内存创建方法参见 `sample_code/share_mem/README.md`。
