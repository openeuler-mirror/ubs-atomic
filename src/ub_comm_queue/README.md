# UB Comm Queue Implementation Guide

本文面向维护者和接入排障人员，说明 `ub_comm_queue` 当前实现的核心流程、接口语义和关键内部状态。对外 API 定义在 `include/ub_dist_comm_queue.h`，主要实现位于本目录的 `ub_dist_comm_queue.cpp`、`UBShmTransport.*`、`MPSCRingBuffer.*`。

## 1. 模块定位

通信队列提供基于共享内存的多节点消息通道。每个节点在自己的 Ring 区域内创建本地收包 Ring，其他节点通过共享内存地址直接写入该 Ring。整体模型是：

- 生产者：多线程并发调用 `ub_comm_queue_send`，写入目标节点指定优先级 Ring。
- 消费者：每个 `UBShmTransport` 实例启动一个分发线程，轮询本节点本地 Ring 并派发回调。
- Ring 模型：`MPSCRingBuffer` 是多生产者单消费者队列，生产者通过 CAS 递增 `tail_` 抢占槽位，消费者单线程递增 `head_`。
- 共享公告牌：`Billboard` 存放各节点初始化状态和各优先级 Ring 相对本节点 Ring 区域的偏移，供其他节点发现远端 Ring。

## 2. 对外接口

### `ub_comm_queue_init`

入口在 `ub_dist_comm_queue.cpp`。该接口创建 `UBShmTransport` 对象并调用 `UBShmTransport::init`。初始化成功后，返回的不透明句柄就是该 `UBShmTransport` 指针。

特殊点：

- 第一个创建成功的实例会写入全局 `g_transport`，并标记为分布式锁使用实例。
- 后续实例会被标记为非锁实例。
- 当前 C API 会拦截业务侧直接发送或注册系统保留消息类型：`MSG_TYPE_DIST_LOCK` 和 `MSG_TYPE_SYS_PEER_EXIT`。

### `ub_comm_queue_deinit`

删除 `UBShmTransport` 对象，并把调用方句柄置空。析构函数会进入 `deinit_and_broadcast`，完成停止分发线程、伪造满环、清公告牌、群发下线消息和释放线程池。

如果该实例是 `g_transport` 指向的锁实例，`g_transport` 会被置空。

### `ub_comm_queue_send`

发送业务消息。成功返回有两种：

- `UB_COMM_OK`：消息写入成功，目标 Ring 未处于稳定拥塞。
- `UB_COMM_SEND_CONGESTED`：消息写入成功，但目标 Ring 达到拥塞阈值。

失败返回负数，例如：

- `-EINVAL`：参数错误或优先级非法。
- `-EPERM`：消息头的 `src_node_id` 与当前节点不一致。
- `-EMSGSIZE`：消息头加消息体超过 Ring 最大消息大小。
- `UB_COMM_ERR_RING_FULL`：Ring 已满，消息未写入。
- `UB_COMM_ERR_RING_BUSY`：远端 CAS 重试超过上限。
- `UB_COMM_ERR_PEER_NOT_READY` / `UB_COMM_ERR_RING_NOT_FOUND`：远端节点或目标 Ring 不可用。

### `ub_comm_queue_register_process_func`

注册业务消息回调。内部调用 `UBShmTransport::register_func`，以 `msg_type` 为数组下标写入 `callbacks_`。

这是热注册接口：

- 注册时持有 `cb_mu_` 写锁。
- 分发时在 `dispatch_internal` 中持有 `cb_mu_` 读锁，复制一份 `cb_info_t` 后释放锁，再执行回调或投递异步任务。
- 同一个 `msg_type` 重复注册会覆盖旧回调；后续新到消息使用新回调，已经复制出来或已经投递到线程池的消息仍按旧回调执行。

### `ub_comm_queue_check_ready`

查询节点是否就绪。本节点直接返回 `init_complete_`；远端节点通过 `try_populate_cache(node_id, 0)` 尝试加载锁 Ring 缓存，成功则认为远端已初始化。

### `ub_comm_queue_get_status`

查询指定节点、指定优先级 Ring 的流控和统计快照。查询本节点时直接读 `local_rings_`；查询远端时通过 `get_remote_ring` 找到远端 Ring 后读取其中的原子状态。

返回内容包括：

- `used` / `total` / `free`
- `state`
- `congestion_threshold`
- `max_depth`
- 定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 时额外包含失败计数和拥塞时间戳

该接口读取的是原子快照，适合维测和本地估计，不提供强一致长度语义。

### `ub_comm_queue_set_congestion_threshold`

运行期调整本节点本地 Ring 的拥塞阈值。阈值只作用于指定 `priority` 的本地 Ring。远端生产者写入该 Ring 时会读到 Ring 对象里的新阈值。

`0~100` 为合法百分比。`0` 表示阈值为 0，所有非满状态（包含空 Ring）都按拥塞状态上报；恢复默认阈值时显式设置 `80`。

### `ub_comm_queue_config_heartbeat`

设置和查询本节点心跳配置。该接口不修改 `ub_comm_conf_t`，因此不破坏已有初始化 ABI。

调用方式：

- `request == NULL && effective != NULL`：只查询当前配置。
- `request != NULL && effective == NULL`：只设置。
- `request != NULL && effective != NULL`：设置后返回最终生效配置，推荐用于确认参数。

配置项均为毫秒：

- `heartbeat_interval_ms`：本节点消费者心跳序号刷新周期。
- `check_interval_ms`：本节点生产者心跳监控线程轮询周期。
- `timeout_ms`：本节点观察到某个 peer 心跳序号超过该时间不变化时，认为 peer 消费者超时。

校验规则：

- `heartbeat_interval_ms`、`check_interval_ms`、`timeout_ms` 均必须大于 0。
- `timeout_ms >= max(3 * heartbeat_interval_ms, 2 * check_interval_ms)`，避免过小阈值造成抖动误判。

结构体中保留 `size` 字段是为了 ABI 演进。调用方需要将 `size` 填为 `sizeof(ub_comm_queue_heartbeat_config_t)`；未来若结构体尾部追加字段，库可以通过 `size` 判断调用方使用的新旧版本。

### `ub_comm_queue_get_heartbeat_status`

查询本节点对某个节点消费者心跳的本地观察状态。该接口只读本地状态，不读远端共享内存，不影响发送热路径。

返回内容包括：

- `node_id`：被查询节点。
- `alive`：当前本地 `peer_alive_` 快照。
- `last_observed_seq`：本节点最近观察到的远端心跳序号。
- `last_change_age_ms`：从本节点最后一次观察到序号变化到现在的本地单调时间差；`UINT64_MAX` 表示尚未观察到有效心跳。
- `timeout_ms`：当前本节点使用的心跳超时阈值。

状态结构体同样带 `size` 字段，用于 ABI 演进。`reserved` 只是显式对齐字段，调用方应忽略。

### `ub_comm_queue_recv`

当前 `UBShmTransport::recv` 实现返回 `0`，主要收包路径是后台分发线程加回调派发。主动拉取接口保留但尚未实现完整读取逻辑。

## 3. 初始化流程

`UBShmTransport::init` 是明确的流水线，目前按 11 步执行。

### 1. 基础校验与配置拷贝

`setup_config_and_validate` 检查：

- `init_area`、`ring_map`、`conf` 及其内部指针非空。
- `max_nodes` 在 `1~MAX_NODES_LIMIT` 内。
- `init_area->size` 至少容纳 `Billboard`。
- `ring_map->count` 非 0，且不超过内部上限。
- `num_rings` 非 0，且不超过内部最大优先级数。
- `ring_descs` 非空。
- `cpu_id` 合法；负数表示不绑核。

随后保存 `init_region_ptr_` 并浅拷贝 `conf_`。真正的 Ring 描述符数组会在后续步骤深拷贝。

### 2. 构建节点 ID 映射

`build_node_mapping` 从 `ring_map->entries` 收集所有 node id，排序后生成：

- `node_id_to_idx_`：逻辑 node id 到紧凑下标。
- `idx_to_node_id_`：紧凑下标到逻辑 node id。

排序的目的是让所有节点基于同一份 `ring_map` 得到一致的 `Billboard` 下标。

### 3. 构建内存基址表

`build_region_bases` 生成：

- `region_bases_[compact_idx]`
- `region_sizes_[compact_idx]`

并根据当前节点 ID 设置 `ring_region_ptr_`，后续本节点 Ring 都在这块区域内 placement new。

### 4. 内存容量预检

`check_memory_capacity` 在真正构造 Ring 前计算本节点所需 Ring 区域大小：

- 先算内置锁 Ring：`priority=0`，容量 `4096`，消息大小 `128`。
- 再遍历用户配置的 Ring。
- 每个 Ring 起始地址都按 `CACHELINE_SIZE` 对齐。
- 用户 Ring 要求 `priority != 0`、`priority < MAX_PRIORITY_LEVELS`、`ring_capacity` 是 2 的幂。

预检失败会在写共享内存前返回错误。

### 5. 深拷贝 Ring 描述符

`deep_copy_ring_descs` 把调用方传入的 `ring_descs` 拷贝到 `ring_descs_storage_`，并让 `conf_.ring_descs` 指向内部 vector。这样初始化后不依赖调用方数组生命周期。

### 6. 初始化线程池

`init_thread_pool` 按硬件并发度的 `WORKER_POOL_RATIO` 和 `WORKER_POOL_MIN_SIZE` 计算异步工作线程数，创建 `ThreadPool`。同步回调不使用线程池。

### 7. 创建本地 Ring

`create_local_rings` 先创建内置锁 Ring，再创建用户 Ring：

- 对当前地址做 cache line 对齐。
- 使用 `MPSCRingBuffer::CalculateMemorySize` 计算大小。
- 触碰首尾字节做可访问性预检。
- 使用 placement new 在共享内存上构造 `MPSCRingBuffer`。
- 将指针放入 `local_rings_[priority]`。
- 将优先级放入 `local_active_priorities_`，供分发线程轮询。
- 将相对 `ring_region_ptr_` 的偏移写入 `out_offsets[priority]`，后续发布到公告牌。

### 8. 发布公告牌

`publish_to_billboard` 将本节点可用 Ring 发布到 `Billboard`：

1. 先把所有 `ring_offsets` 写成 `UINT64_MAX`。
2. 再把已创建 Ring 的实际 offset 写入对应优先级槽位。
3. 最后写 `initialized=true`。

写入使用 `ub_nt_store64` / `ub_nt_store8`，并配合 `arm_sfence`，目标是保证共享内存可见性，避免其他节点先看到 ready 但 offset 未落盘。

### 9. 等待集群就绪

`wait_for_cluster_ready` 遍历所有远端节点，最多等待 10 秒。读取远端 `initialized` 前会执行伪 CAS 和 `force_refresh_whole_struct`，用于刷新可能的 NC/CC 可见性。

超时只打印 WARN，不阻止当前节点继续初始化。

### 10. 预热远程表

`preload_remote_table` 初始化 `remote_lookup_table_`，并尽可能预加载远端 Ring：

- 自己的 compact index 直接指向 `local_rings_`，形成本地快捷路径。
- 远端节点先刷新 `NodeBoardInfo`，确认 initialized，再读取 `ring_offsets`。
- 通过 `region_bases_[idx] + offset` 得到远端 `MPSCRingBuffer *`。
- 调用 `try_populate_cache` 填充 `RemoteRingCache`。

### 11. 启动分发线程

`start_dispatcher` 创建 `dispatcher_thread_`。线程主循环在 `run_dispatcher_loop` 中：

- 如果配置了 CPU，则先绑核。
- 使用 `max_msg_size_global_` 分配接收缓冲区。
- 反复调用 `poll_and_dispatch_once`。
- 没有处理到消息时执行 `cpu_relax_arm`。

## 4. Ring 内存布局

`MPSCRingBuffer` 对象本体放在共享内存起始位置，数据区从 `GetDataOffset()` 开始，按 64 字节对齐。

每个 Entry 由：

- `ready_seq`：生产者写完数据后写入 `tail + 1`，消费者仅在 `ready_seq == head + 1` 时读取该槽。
- `data[]`：连续存放 `message_header_t` 和 body。

队列大小要求为 2 的幂，因此槽位下标使用 `idx & index_mask_` 计算，避免取模开销。

`ready_seq` 同时承担提交标志和序号校验两个作用。它替代简单的 `is_ready=1` 标志，避免消费者跳过半写槽后，原生产者晚提交旧槽位，后续环绕时被误认为新消息。

### Ring 区预留大小公式

当前每个节点的 Ring 区按如下顺序预留和构造：

1. 内置锁 Ring：`priority=0`，`capacity=4096`，`max_msg_size=128`。
2. 调用方配置的用户 Ring：`priority=1..7`。
3. 每个 Ring 构造前，起始地址按 `CACHELINE_SIZE` 对齐。

单个 Ring 大小公式与 `MPSCRingBuffer::CalculateMemorySize` 保持一致：

```text
align64(x) = ceil(x / 64) * 64

ring_header = align64(sizeof(MPSCRingBuffer))
entry_stride = align64(sizeof(MPSCRingBuffer::Entry header) + max_msg_size)
ring_size = ring_header + entry_stride * ring_capacity
```

当前默认编译布局下：

```text
sizeof(MPSCRingBuffer) = 192
sizeof(MPSCRingBuffer::Entry header) = 8
ring_header = 192
entry_stride = align64(8 + max_msg_size)
```

因此某节点的 Ring 区最小预留为：

```text
ring_region_size_per_node =
    align_pad(lock_ring_start) + ring_size(4096, 128)
  + sum(align_pad(user_ring_i_start) + ring_size(capacity_i, max_msg_size_i))
```

如果 `ring_region.ptr` 本身已经 64B 对齐，则第一个 `align_pad(lock_ring_start)` 为 0。后续 `align_pad` 由前一个 Ring 结束地址决定，取值范围为 `0..63`。

仓内提供脚本按同一规则输出明细：

```bash
python3 tools/calc_ub_comm_ring_region.py \
  --nodes 2 \
  --ring 1:65536:1024 \
  --ring 2:4096:4096 \
  --formula
```

脚本默认包含内置锁 Ring，并输出单节点 Ring 区大小、所有节点 Ring 区合计、公告牌 `init_region` 最小值以及总计。若后续修改了 `MPSCRingBuffer` 或 `Entry` 布局，可先通过 `--object-size`、`--entry-header-size` 覆盖布局参数，再同步更新本节默认值。

## 5. 发送路径

### 公共校验

`UBShmTransport::send` 先做：

- `msg` 非空检查。
- `msg->header.src_node_id` 必须等于 `conf_.current_node_id`，防止伪造源节点。
- 系统消息类型走内部优先级 0；业务消息不能使用优先级 0。
- 优先级必须小于 `MAX_PRIORITY_LEVELS`。

### 本地发送

当 `dest_node_id == current_node_id` 时走本地路径：

1. 直接从 `local_rings_[prio]` 获取目标 Ring。
2. 调用 `ring->enqueue_local(&msg->header, msg->body, body_len)`。

`enqueue_local` 的关键逻辑：

- 检查消息总长度不超过 `max_msg_size_`。
- 读取 `tail_`。
- 使用 `local_producer_cached_head_` 作为本地生产者影子 head，避免每次发送都读消费者 `head_`。
- 只有影子 head 判断可能满时，才读取真实 `head_` 并刷新影子值。
- 如果仍满，debug 模式下增加 `full_fail_count_`，并返回 `UB_COMM_ERR_RING_FULL`。
- 通过 CAS 递增 `tail_` 抢占槽位。
- CAS 失败时 debug 模式下增加 `cas_fail_count_`，然后 `yield`。
- 把 header/body 拷贝到槽位。
- `ready_seq.store(curr_tail + 1, release)` 提交给消费者。
- 采样流控状态，返回 `UB_COMM_OK` 或 `UB_COMM_SEND_CONGESTED`。

### 远端发送

当目标是其他节点时走远端路径：

1. 持有 `cache_mutex_` 读锁。
2. 使用逻辑 `dest_id` 和 `prio` 找到 `ring_caches_[dest_id][prio]`。
3. 如果 cache 未初始化，调用 `try_populate_cache`。
4. 调用静态 `MPSCRingBuffer::enqueue_remote`。

`RemoteRingCache` 缓存了远端 Ring 的不变量：

- `raw_ptr`
- `mask`
- `stride`
- `max_size`

同时持有运行时影子变量：

- `shadow_head`
- `cached_threshold`
- `cached_threshold_version`

远端 `enqueue_remote` 与本地路径类似，但尽量使用缓存好的常量，减少远端共享内存读取：

- 用 `shadow_head` 做判满的本地估计。
- 只有可能满时才读取远端真实 `head_`，并更新 `shadow_head`。
- 用 `cached_threshold` 和 `cached_threshold_version` 缓存目标 Ring 的流控阈值。
- CAS 操作直接作用在远端 Ring 的 `tail_`。
- CAS 超过 `MAX_CAS_RETRIES` 返回 `UB_COMM_ERR_RING_BUSY`。
- 写入远端 Entry 后，写 `ready_seq=curr_tail+1` 提交。

这个设计的核心是：发送快路径以本地估计为主，远端同步为辅。

### 本地 Cache 更新逻辑

通信队列里有两层本地 cache：

| Cache | 作用 | 更新位置 |
| --- | --- | --- |
| `remote_lookup_table_[compact_idx][priority]` | 缓存远端 Ring 指针，避免每次查公告牌 | `preload_remote_table` / `get_remote_ring` / `remove_node_cache` |
| `ring_caches_[node_id][priority]` | 缓存发送快路径所需字段 | `try_populate_cache` / `enqueue_remote` / `flow_result_after_enqueue_cached` / `remove_node_cache` |

`remote_lookup_table_` 使用 compact index，来源于初始化时构建的 node id 映射。`ring_caches_` 当前仍使用逻辑 `node_id` 直接下标，代码中已有 TODO；因此稀疏 node id 场景需要进一步统一索引体系。

#### 初始化预热

`preload_remote_table` 在初始化末尾执行：

- 先为 `remote_lookup_table_` 分配 `[node_count][MAX_PRIORITY_LEVELS]`。
- 本节点直接写入 `local_rings_[priority]`，形成本地快捷路径。
- 远端节点先通过 `force_refresh_whole_struct` 刷新公告牌，再读取 `initialized` 和 `ring_offsets[priority]`。
- offset 有效时，用 `region_bases_[compact_idx] + offset` 算出远端 `MPSCRingBuffer *`。
- 随后调用 `try_populate_cache` 填充 `RemoteRingCache`。

#### Lazy Populate

远端发送时，`send` 持有 `cache_mutex_` 读锁读取 `ring_caches_[dest_id][prio]`。如果 `initialized == false`，调用 `try_populate_cache`：

```text
try_populate_cache
  -> get_remote_ring
       -> hit remote_lookup_table_ 直接返回
       -> miss 时刷新 Billboard，读取 ring_offsets，回填 remote_lookup_table_
  -> 读取 Ring 不变量并写入 RemoteRingCache
  -> shadow_head = 0
  -> cached_threshold = ring->get_congestion_threshold()
  -> cached_threshold_version = ring->get_congestion_threshold_version()
  -> initialized.store(true, release)
```

这里的 `initialized.store(true, release)` 保证其他发送线程 acquire 读到 true 后，可以看到此前写入的 cache 字段。

#### shadow_head 更新

`shadow_head` 是生产者侧缓存的消费者 `head_`，只用于发送侧判满和流控估计：

- 初始化或下线清理后置 0。
- 每次远端发送先用 `shadow_head` 和当前 `tail_` 做本地判满估计。
- 只有估计“可能满”时，才读取远端真实 `head_` 并更新 `shadow_head`。
- 入队成功后的流控采样如果估计达到阈值，也会读取真实 `head_` 并更新 `shadow_head`。

这个字段允许滞后。滞后的方向是保守的：消费者已经释放的槽位可能暂时没有体现在 `shadow_head` 里，因此生产者会更早进入慢路径刷新真实 `head_`。

#### 流控阈值 Cache 更新

目标 Ring 的阈值存放在共享内存里的 `congestion_threshold_` 和 `congestion_threshold_version_`。本节点调用 `ub_comm_queue_set_congestion_threshold` 时，会更新本地 Ring 的阈值并递增版本号。

远端生产者不会每次发送都读取共享阈值，而是在以下时机刷新 `cached_threshold`：

- `cached_threshold == 0`，通常是 cache 初始或被清理后的防御路径。
- `new_tail & 1023 == 0`，即每 1024 次入队采样一次版本号。
- 本地估计 `used >= cached_threshold`，即可能返回拥塞提示时立即刷新，降低旧阈值造成的误判。

刷新逻辑先读远端 `congestion_threshold_version_`。版本不同才更新本地 `cached_threshold` 和 `cached_threshold_version`。

#### 下线清理

收到 `MSG_TYPE_SYS_PEER_EXIT` 后，`remove_node_cache` 持有 `cache_mutex_` 写锁清理该节点所有优先级：

- `cache.raw_ptr = nullptr`
- `cache.shadow_head = 0`
- `cache.initialized = false`
- `remote_lookup_table_[compact_idx][priority] = nullptr`

写锁会等待正在发送的读锁释放，因此能避免下线清理与已进入发送路径的线程同时改同一份 cache。

#### 并发与性能影响

正常远端发送快路径只读本地 cache，避免查公告牌和读取远端不变量；远端共享内存操作主要集中在 `tail_` CAS、Entry 写入和 `ready_seq` 提交。

会进入较重路径的场景包括：

- 首次发送或下线后重建 cache：需要刷新公告牌并填充 `RemoteRingCache`。
- `shadow_head` 估计可能满：需要读取远端真实 `head_`。
- 流控阈值采样或达到阈值：需要读取远端阈值版本，必要时读取阈值并更新本地 cache。
- 多生产者同时发送同一个 Ring：远端 `tail_` CAS 是主要竞争点，`shadow_head` 和阈值 cache 也可能有轻微 cache line 抖动。

当前实现中，`send` 在持有 `cache_mutex_` 读锁时可能调用 `try_populate_cache` 写 `RemoteRingCache`。多个线程首次同时发送同一个目标 Ring 时，可能并发写入同一份 cache 字段。由于写入值通常相同，实际行为大概率可用，但严格并发语义上建议后续改成 miss 后升级到写锁或增加 per-cache 初始化保护。

## 6. 流控与维测

流控状态存放在每个 `MPSCRingBuffer` 内，属于 Ring 属性。

配置来源：

- 初始化时所有 Ring 使用默认 `80%`。
- 运行期可通过 `ub_comm_queue_set_congestion_threshold` 调整本节点指定 Ring。
- setter 参数 `0~100` 合法；`0` 表示所有非满状态均为拥塞，恢复默认阈值时显式设置 `80`。

判定规则：

- 队列满：返回 `UB_COMM_ERR_RING_FULL`，消息未写入。
- 使用量达到阈值：消息已写入，返回 `UB_COMM_SEND_CONGESTED`。
- 未达到阈值：消息已写入，返回 `UB_COMM_OK`。

统计字段：

- `max_depth_`：启动以来最大估计深度。

Debug 统计：

- `full_fail_count_`：因为 Ring 满导致发送失败的次数。
- `cas_fail_count_`：生产者 CAS 抢占失败次数。
- `congestion_enter_ts_us_` / `congestion_exit_ts_us_`：最近拥塞进入/退出时间。

除 `max_depth_` 外，Debug 统计字段仅在定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 时编译进 `MPSCRingBuffer` 和 `ub_comm_queue_status_t`。普通模式不维护这些计数，以减少快路径原子写。

日志策略：

- 默认开启。
- 只在拥塞状态边缘触发：首次进入拥塞、从拥塞恢复。
- 普通模式日志字段包含事件、触发点、Ring 地址、时间戳、使用量、容量和阈值。
- 定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 后，日志额外包含历史最大深度、满环失败次数和 CAS 失败次数。

`approximate_used` 通过读取 `tail_` 和 `head_` 得到本地估计值。它不承诺强一致长度，但足够用于流控提示和维测观察。

### 悲观估计策略对流控的影响

发送侧流控使用的是“先本地估计，必要时刷新真实 head”的策略。

本地发送使用 `local_producer_cached_head_`，远端发送使用 `RemoteRingCache::shadow_head`。这两个缓存都可能落后于真实 `head_`，也就是消费者已经出队释放空间，但生产者侧还不知道。因此估计出来的 `used = new_tail - cached_head` 可能偏大。

这种偏大的估计会带来两个结果：

- 更早进入慢路径：当估计 `used >= threshold` 时，生产者会刷新真实 `head_`，再用 fresh head 重新计算。
- 拥塞提示偏保守：如果刷新后仍达到阈值，才返回 `UB_COMM_SEND_CONGESTED`。如果只是缓存滞后，刷新会消除误判。

阈值配置也采用保守刷新：

- 正常路径使用 `cached_threshold`，避免每条消息读取远端配置。
- 每 1024 次入队或即将返回拥塞时刷新版本。
- 因此阈值调整不是对所有远端生产者“下一条消息立即可见”，但在高水位路径会更快收敛。

流控提示只影响返回值，不影响消息是否已经入队。`UB_COMM_SEND_CONGESTED` 表示消息已写入，调用方可以据此降速或批处理。

### 悲观估计策略对判满的影响

判满使用 `cached_head + capacity <= curr_tail` 作为第一层估计。如果命中，必须读取真实 `head_` 再判断一次：

```text
if cached_head + capacity <= curr_tail:
    fresh_head = head_.load(acquire)
    cached_head = fresh_head
    if fresh_head + capacity <= curr_tail:
        return RING_FULL
```

因此缓存滞后不会直接导致错误满环返回；最多导致额外读取一次真实 `head_`。只有 fresh head 仍然说明容量已满时，才返回 `UB_COMM_ERR_RING_FULL`，且消息不会写入。

这个设计把满环判断放在安全侧：

- 不因为消费者释放空间但缓存未更新而直接失败。
- 不因为缓存过新而绕过容量检查；缓存更新只来自真实 `head_` 读取。
- 满环失败会触发 `sample_flow_state(entry_num_, "enqueue_full")`，用于更新拥塞状态和 debug 统计。

### 悲观估计策略对状态查询的影响

`ub_comm_queue_get_status` 不使用 `shadow_head` 或 `local_producer_cached_head_`，而是直接读取 Ring 内的 `tail_` 和 `head_`：

```text
tail = tail_.load(acquire)
head = head_.load(acquire)
used = tail >= head ? tail - head : 0
used = min(used, entry_num_)
```

查询里的估计仍不是强一致快照，因为生产者和消费者可能在两次 load 之间并发推进。实现选择先读 `tail_` 再读 `head_`，倾向于避免低估生产者刚抢占的槽位。

这对查询结果的影响是：

- `used` 和 `free` 适合作为监控和限流参考，不适合作为业务精确队列长度。
- `state == FULL` 表示查询瞬间估计已满或超过容量上限，不代表下一次发送一定失败；发送路径仍会 fresh head 后二次确认。
- `state == CONGESTED` 依赖内部稳定拥塞标记 `congested_`，该标记只在发送、满环失败、出队或配置阈值时采样更新，查询本身不会推进状态。
- 如果没有新的发送或出队动作，查询不会主动刷新拥塞边缘，也不会产生日志。

## 7. 环状态查询接口

环状态查询接口对应对外 API `ub_comm_queue_get_status`，用于在不进入发送快路径、不加重锁的前提下拿到指定 Ring 的当前估计水位和关键统计。

### 设计目标

- 维测优先：用于日志、监控、排障和上层限流策略参考。
- 线程安全：读取对象内部原子变量，不读取非稳定的裸数据结构。
- 轻量低侵入：查询不阻塞生产者 CAS 入队，也不阻塞消费者出队。
- 兼容远端：既能查询本节点本地 Ring，也能查询已映射共享内存中的远端 Ring。
- 明确语义：返回的是瞬时原子快照和本地估计，不承诺强一致队列长度。

### 调用链路

外部调用路径如下：

```text
ub_comm_queue_get_status
  -> UBShmTransport::get_status
       -> MPSCRingBuffer::get_status
```

`ub_comm_queue_get_status` 只做句柄和 `status` 指针校验，然后把请求转给 `UBShmTransport`。

`UBShmTransport::get_status` 根据 `node_id` 分两条路径：

- 查询本节点：直接从 `local_rings_[priority]` 获取 Ring 指针。
- 查询远端节点：持有 `cache_mutex_` 读锁，通过 `get_remote_ring(node_id, priority, &ring)` 找到远端 Ring。

远端查询不要求发送缓存已经初始化。`get_remote_ring` 会优先查 `remote_lookup_table_`，未命中时刷新 `Billboard`，读取目标节点的 `ring_offsets[priority]`，再通过 `region_bases_[idx] + offset` 计算远端 Ring 地址。

### 快照内容

`MPSCRingBuffer::get_status` 填充 `ub_comm_queue_status_t`：

| 字段 | 来源 | 说明 |
| --- | --- | --- |
| `used` | `approximate_used()` | 估计已占用元素数 |
| `total` | `entry_num_` | Ring 总容量 |
| `free` | `total - used` | 估计空闲元素数，满时为 0 |
| `state` | `used` 和内部 `congested_` 推导 | `IDLE` / `NORMAL` / `CONGESTED` / `FULL` |
| `congestion_threshold` | `get_flow_threshold()` | 当前阈值对应的元素数量 |
| `max_depth` | `max_depth_` | 进程启动以来最大估计深度 |

定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 后，结构体还会包含以下字段：

| 字段 | 来源 | 说明 |
| --- | --- | --- |
| `full_fail_count` | `full_fail_count_` | 因 Ring 满导致发送失败次数 |
| `cas_fail_count` | `cas_fail_count_` | 多生产者 CAS 抢占失败次数 |
| `congestion_enter_ts_us` | `congestion_enter_ts_us_` | 最近一次进入拥塞的微秒时间戳 |
| `congestion_exit_ts_us` | `congestion_exit_ts_us_` | 最近一次退出拥塞的微秒时间戳 |

### `used` 的估计方式

`approximate_used` 按以下方式估算水位：

```text
tail = tail_.load(acquire)
head = head_.load(acquire)
used = tail >= head ? tail - head : 0
used = min(used, entry_num_)
```

这里先读 `tail_` 再读 `head_`，目的是在生产者并发抢占槽位时尽量避免低估水位。由于生产者和消费者可能同时推进，`used` 是估计值，不是线性一致读数。

这和发送侧的水位判断是一致的思路：以本地估计为主，必要时通过刷新 head 降低误判。查询接口不参与判满或抢占，所以不会为了“更准”去打扰快路径。

### `state` 推导规则

状态由快照里的 `used` 和内部稳定拥塞标记推导：

```text
used == 0          -> UB_COMM_QUEUE_IDLE
used >= total      -> UB_COMM_QUEUE_FULL
internal congested -> UB_COMM_QUEUE_CONGESTED
otherwise          -> UB_COMM_QUEUE_NORMAL
```

这里 `FULL` 优先级高于 `CONGESTED`。也就是说，一个满 Ring 在状态查询里会显示为 `UB_COMM_QUEUE_FULL`，即使内部稳定拥塞标记仍为 true。

### 与流控日志的关系

状态查询本身不会触发拥塞进入/退出日志。边缘日志只在发送入队、满环失败或消费者出队后的 `sample_flow_state` 中触发。

这避免了“监控查询越频繁，日志越多”的问题。查询接口只读已有状态：

- 如果此前发送触发过拥塞，`state` 可反映为 `UB_COMM_QUEUE_CONGESTED`。
- 如果此后出队触发恢复，`state` 会恢复为 `UB_COMM_QUEUE_NORMAL` 或 `UB_COMM_QUEUE_IDLE`。
- 定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 后，还能读取拥塞进入/退出时间戳。
- 如果只查询不发送、不出队，状态不会因为查询动作本身被推进。

### 错误返回

常见失败场景：

- `-EINVAL`：句柄、`status` 指针或优先级非法。
- `UB_COMM_ERR_RING_NOT_FOUND`：本地或远端指定优先级 Ring 不存在。
- `UB_COMM_ERR_PEER_NODE_NOT_FOUND`：`node_id` 不在当前节点映射表中。
- `UB_COMM_ERR_PEER_NOT_READY`：远端节点未初始化或公告牌不可用。

### 使用建议

- 适合周期性采样上报，但不要把 `used` 当作严格队列长度做业务一致性判断。
- `max_depth` 适合用于观察启动以来的水位峰值；debug 模式下的 `full_fail_count`、`cas_fail_count` 更适合用于定位长期压力和竞争热点。
- 查询远端 Ring 依赖当前进程已经映射对端 Ring 区域；映射失效或对端下线时应按错误码处理。
- 如果上层只关心本节点发送压力，优先查询目标节点对应 Ring 的状态，而不是本节点所有 Ring。

## 8. 接收与分发

分发线程轮询 `local_active_priorities_` 中的本地 Ring。`poll_and_dispatch_once` 的策略是：

- 从当前优先级开始尝试 `dequeue`。
- 如果读到消息，立即派发，并把轮询索引重置为 0。
- 如果当前优先级无消息，才进入下一个优先级。
- 单次轮询最多处理 `MAX_BATCH_PER_POLL` 条，避免高优先级消息无限占用线程。

`MPSCRingBuffer::dequeue` 是单消费者逻辑：

1. 读取当前 `head_`。
2. 找到对应 Entry。
3. 计算期望提交序号 `expected_seq = head + 1`。
4. 如果 `ready_seq != expected_seq`，返回 0 表示当前 head 槽尚不可读，并可能进入半写槽慢路径。
5. 根据 header 中的 `body_length` 计算真实消息长度。
6. 拷贝到分发线程缓冲区。
7. 清 `ready_seq`。
8. `head_.store(expected_seq, release)` 推进消费者位置。
9. 采样流控状态，可能触发拥塞恢复日志。

### 半写槽恢复

半写槽指生产者已经通过 CAS 抢占 `tail_`，但进程在提交 `ready_seq` 前退出或长时间卡住。旧的单纯 `is_ready` 方案下，消费者看到 head 槽未 ready 会一直返回 0，即使后续槽位已经有完整消息，也会被这个脏槽阻塞。

当前恢复方案遵循两个原则：

- 生产者热路径不新增抢占时间戳写入，也不新增任何远端共享内存读写。
- 消费者正常 ready 路径只做一次 `ready_seq` acquire load，不进入超时判断。

具体逻辑：

1. 生产者抢占槽位后只写消息数据，最后用 `ready_seq.store(curr_tail + 1, release)` 提交。这是原提交标志的等价替换，不额外增加远端写。
2. 消费者读取当前 `head_` 后，只在 `ready_seq != head + 1` 时认为当前槽不可读。
3. 为降低空转开销，不是每次不可读都做恢复判断，而是通过 `STALE_RESERVED_PROBE_MASK` 做采样，默认每 1024 次不可读检查一次慢路径。
4. 慢路径先读 `tail_`。只有 `tail_ > head_` 时，才说明确实存在已经被生产者抢占但未提交的 head 槽；如果 `tail_ <= head_`，这是普通空队列。
5. 消费者使用本线程本地状态记录该 Ring、该 `head_` 第一次被观测为“已抢占但未提交”的时间，时间源为本节点 `CLOCK_MONOTONIC`。
6. 同一个 `head_` 连续超过 `HALF_WRITE_TIMEOUT_US` 后，消费者再次确认 `ready_seq != head + 1`，然后本地执行跳过：清该槽 `ready_seq`，并 `head_.store(head + 1, release)`。
7. 跳过后下一轮轮询即可继续检查后续槽位，避免单个半写槽长期阻塞队列。

这个方案不使用生产者写入的 wall clock 时间戳。跨节点时钟可能不同步，发送节点 A 写时间戳、接收节点 B 用本地时间判断会产生误判；因此超时窗口完全由接收节点的本地单调时钟观测得到。

`ready_seq` 还用于防止跳过后的 ABA：

- 如果消费者已经跳过 `head=N`，原生产者之后才恢复并把旧槽提交出来，写入的是 `ready_seq=N+1`。
- 当环形队列未来再次绕回这个槽时，新的期望序号已经不是 `N+1`，消费者不会把旧提交误读成新消息。

注意：`HALF_WRITE_TIMEOUT_US` 必须大于生产者正常写入最大耗时。阈值过小会把慢写误判为半写并丢弃该消息；阈值过大则半写槽恢复更慢。

### 消费者心跳感知

消费者存活感知不使用跨节点时间戳。公告牌 `NodeBoardInfo` 中保存的是 `consumer_heartbeat_seq`，即消费者后台线程周期性递增的序号：

- 消费者本地后台线程定时执行 `consumer_heartbeat_seq.store(++local_seq, release)`。
- 生产者本地后台监控线程轮询远端 `consumer_heartbeat_seq`。
- 如果监控线程看到远端序号变化，就用本节点 `CLOCK_MONOTONIC` 记录 `last_seen_us`。
- 如果序号长时间不变化，且本地 `now_us - last_seen_us > heartbeat_timeout_us_`，则把本地 `peer_alive_[node_id]` 标记为 false。
- `heartbeat_interval_us_`、`heartbeat_check_interval_us_`、`heartbeat_timeout_us_` 可通过 `ub_comm_queue_config_heartbeat` 运行期调整。
- 发送热路径只读取本地 `peer_alive_` 快照，不读取共享心跳字段。

这样共享内存心跳字段只表达“对端消费者线程还在推进”，不表达任何时间语义。发送节点不会拿接收节点写入的 wall clock 与本地时间做差，因此不受跨节点时钟不同步、NTP 调整或系统时间回拨影响。

`dispatch_internal` 根据 `msg_type` 查找回调：

- 同步回调：直接在分发线程中执行。
- 异步回调：复制 body 到 `std::string`，再投递到 `ThreadPool`。

同步回调应避免长时间阻塞；异步回调可处理较重业务，但线程池队列当前没有硬性背压。

## 9. 热注册细节

回调表是 `std::array<cb_info_t, 256> callbacks_`。

注册路径：

- `register_func` 校验 `msg_type` 和 `func_type`。
- 持有 `cb_mu_` 写锁。
- 覆盖 `callbacks_[msg_type]`。

分发路径：

- 持有 `cb_mu_` 读锁。
- 复制 `callbacks_[msg_type]` 到局部变量。
- 释放锁后执行。

因此注册可以与消息分发并发发生。切换边界是分发线程复制回调信息的时刻：复制前看到新注册，复制后继续使用旧回调。

## 10. 下线与去初始化

`UBShmTransport::~UBShmTransport` 调用 `deinit_and_broadcast`。

流程如下：

1. 设置 `stop_flag_`，等待分发线程退出。
2. 如果实例完成过初始化，遍历所有本地 Ring 调用 `trigger_force_full`。
3. 清公告牌：所有 `ring_offsets` 写成 `UINT64_MAX`，`initialized=false`，并写 `cache_probe_pad` 刷新可见性。
4. 群发 `MSG_TYPE_SYS_PEER_EXIT` 到其他已知活跃节点的 priority 0 锁 Ring。
5. 短暂 sleep 20ms，给下线消息留出写入和传播时间。
6. 删除线程池。

收到 `MSG_TYPE_SYS_PEER_EXIT` 后，内部回调 `on_peer_exit` 调用 `remove_node_cache`：

- 持有 `cache_mutex_` 写锁。
- 清目标节点所有优先级的 `RemoteRingCache`。
- 将 `remote_lookup_table_` 对应项置空。

这样后续发送会重新查公告牌；如果对端已下线，就会返回未就绪或 Ring 不存在。

## 11. 共享内存可见性设计

代码中有几类 ARM/共享内存相关处理：

- `ub_nt_store64` / `ub_nt_store8`：发布公告牌时使用 release store。
- `arm_sfence`：确保公告牌 offset 和 initialized 的写入顺序。
- `force_refresh_whole_struct`：读取远端公告牌前通过伪写刷新 cache line。
- `ready_seq.store(..., release)` / `load(..., acquire)`：保证 Entry 数据先于可读序号对消费者可见。
- `tail_` CAS：多生产者抢占槽位的核心同步点。

## 12. 已知约束

- `MAX_NODES_LIMIT` 当前为 8。
- `MAX_PRIORITY_LEVELS` 当前为 8，priority 0 被内部锁 Ring 和系统消息保留。
- 用户 Ring 容量必须是 2 的幂。
- `ub_comm_queue_recv` 主动接收接口当前未实现完整收包逻辑。
- `ring_caches_` 当前仍直接使用逻辑 `node_id` 作为数组下标，代码中已有 TODO；稀疏 node id 场景下需要进一步收敛到 compact index。
- `wait_for_cluster_ready` 对远端超时只告警，不阻断初始化。
- 异步线程池当前没有严格的队列长度限制，`max_async_queue_len` 和 `async_wait_timeout_us` 尚未参与实际限流。

## 13. 文件职责

| 文件 | 职责 |
| --- | --- |
| `ub_dist_comm_queue.cpp` | C ABI 包装、参数拦截、全局锁实例指针维护 |
| `UBShmTransport.h/.cpp` | 初始化流水线、公告牌、远端缓存、发送路由、分发线程、回调注册、下线广播 |
| `MPSCRingBuffer.h/.cpp` | 共享内存 MPSC Ring、入队/出队、流控统计、边缘日志 |
| `ThreadPool.h` | 异步回调线程池 |
| `ub_comm_errno.h` | 模块返回码 |
| `ub_atomic_log_print.*` | 日志注册、级别过滤和格式化输出 |
