# libubs-atomic API Notes

## 通信队列心跳配置与查询

心跳功能用于让发送端在后台感知目标节点消费者是否还在推进。发送热路径不读取共享心跳字段，只读取本地 `peer_alive_` 快照。

### 为什么不用时间戳

共享内存中的心跳字段不保存 wall clock 时间戳，而保存单调递增的 `consumer_heartbeat_seq`。原因是跨节点机器时间可能不同步，也可能被 NTP 调整或回拨。如果节点 B 写入自己的时间戳，节点 A 用本地时间做差，可能误判。

当前模型是：

1. 消费者后台线程周期性递增 `consumer_heartbeat_seq`。
2. 生产者后台监控线程轮询远端序号。
3. 序号变化时，生产者用本地 `CLOCK_MONOTONIC` 记录观察时间。
4. 序号超过本地超时阈值未变化时，生产者把本地 `peer_alive_` 标记为 false。

### 配置接口

```c
typedef struct {
    uint32_t size;
    uint32_t heartbeat_interval_ms;
    uint32_t check_interval_ms;
    uint32_t timeout_ms;
} ub_comm_queue_heartbeat_config_t;

int ub_comm_queue_config_heartbeat(
    ub_shm_comm_t *handle,
    const ub_comm_queue_heartbeat_config_t *request,
    ub_comm_queue_heartbeat_config_t *effective);
```

调用方式：

- `request == NULL && effective != NULL`：查询当前配置。
- `request != NULL && effective == NULL`：设置配置。
- `request != NULL && effective != NULL`：设置并返回最终生效配置。

字段说明：

- `size`：调用方填 `sizeof(ub_comm_queue_heartbeat_config_t)`。
- `heartbeat_interval_ms`：本节点消费者心跳序号刷新周期。
- `check_interval_ms`：本节点生产者心跳监控轮询周期。
- `timeout_ms`：本节点观察到 peer 心跳序号不变化的超时阈值。

默认值：

```text
heartbeat_interval_ms = 100
check_interval_ms     = 100
timeout_ms            = 1000
```

校验规则：

- 三个时间参数必须大于 0。
- `timeout_ms >= max(3 * heartbeat_interval_ms, 2 * check_interval_ms)`。

### 状态查询接口

```c
typedef struct {
    uint32_t size;
    uint32_t timeout_ms;
    uint64_t last_observed_seq;
    uint64_t last_change_age_ms;
    uint8_t node_id;
    uint8_t alive;
    uint16_t reserved;
} ub_comm_queue_heartbeat_status_t;

int ub_comm_queue_get_heartbeat_status(
    ub_shm_comm_t *handle,
    uint8_t node_id,
    ub_comm_queue_heartbeat_status_t *status);
```

字段说明：

- `size`：调用方填 `sizeof(ub_comm_queue_heartbeat_status_t)`。
- `timeout_ms`：当前本节点使用的心跳超时阈值。
- `last_observed_seq`：本节点最近观察到的目标节点心跳序号。
- `last_change_age_ms`：距离上一次观察到序号变化的本地单调时间；`UINT64_MAX` 表示尚未观察到有效心跳。
- `node_id`：被查询节点。
- `alive`：当前本地 `peer_alive_` 快照。
- `reserved`：对齐字段，调用方忽略。

### 关于 `size` 和 reserved

`size` 是 C ABI 演进字段。结构体一旦作为公共 API 发布，后续如果直接修改已有字段布局，会破坏老版本二进制兼容。调用方传入 `size` 后，库可以判断调用方使用的结构体版本，未来在结构体尾部追加字段时仍能兼容旧调用方。

本方案没有大量预留字段。配置结构体不额外保留 reserved；状态结构体只有一个 `uint16_t reserved`，用于显式对齐并避免尾部 padding 语义不清。调用方不应读取或依赖 reserved。

### 示例

查询当前配置：

```c
ub_comm_queue_heartbeat_config_t cfg = {
    .size = sizeof(ub_comm_queue_heartbeat_config_t),
};
int ret = ub_comm_queue_config_heartbeat(&handle, NULL, &cfg);
```

设置并确认配置：

```c
ub_comm_queue_heartbeat_config_t req = {
    .size = sizeof(ub_comm_queue_heartbeat_config_t),
    .heartbeat_interval_ms = 100,
    .check_interval_ms = 100,
    .timeout_ms = 1000,
};
ub_comm_queue_heartbeat_config_t eff = {
    .size = sizeof(ub_comm_queue_heartbeat_config_t),
};
int ret = ub_comm_queue_config_heartbeat(&handle, &req, &eff);
```

查询节点状态：

```c
ub_comm_queue_heartbeat_status_t st = {
    .size = sizeof(ub_comm_queue_heartbeat_status_t),
};
int ret = ub_comm_queue_get_heartbeat_status(&handle, 1, &st);
```
