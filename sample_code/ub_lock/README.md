# UB Distributed RWLock Test README

本目录包含两份用于测试 `ub_rw_lock_t` 分布式读写锁（S/SX/X）的 C++ 测试程序：

* **ub_dist_lock_func_test：功能测试**

  * 特点：CLI用户交互测试，模拟线程加解锁行为
* **ub_dist_lock_perf_test：性能测试**

  * 特点：多线程并发持锁，统计时延。

> 测试程序包含的功能：
> 1. 初始化通信队列（跨节点消息通道）
> 2. 映射共享内存锁对象并 `ub_rw_lock_create`
> 3. 启动多线程读/写压测，统计 lock latency
> 4. 支持交互命令 加解锁 / query / rebuild / 退出
> 5. 结束时释放锁资源并卸载 shm

---

## 1. 环境依赖

需要依赖以下组件（实际路径按环境调整）：

* `ubs_mem`（ubsmem SDK）

  * 头文件：`ubs_mem_def.h`, `ubs_mem.h`
  * 库：`-lubsm_sdk`
* 分布式通信队列：

  * 头文件：`ub_dist_comm_queue.h`
  * 库：在 `-lubturbo_tdsql`
* 分布式锁：

  * 头文件：`ub_dist_lock.h`
  * 库：在 `-lubturbo_tdsql`

---

## 2. 共享内存模式说明

程序支持 shm 映射模式：

* 使用 `ubsmem_shmem_map()` 映射共享内存区域
* 依赖 ubsmem SDK
* 适用于：集群/多进程共享内存由 ubsm 管理的场景

---

## 3. 编译

典型编译命令如下（按实际库名调整）：

```bash
g++ -O2 -g -std=c++17 -o ub_dist_lock_func_test ub_dist_lock_func_test.cpp \
  -I/usr/local/include \
  -I/usr/local/ubs_mem/include \
  -L/usr/local/ubs_mem/lib \
  -lubsm_sdk \
  -L/usr/lib64 \
  -lubturbo_tdsql \
  -lpthread \
  -Wl,-rpath,/usr/local/ubs_mem/lib
```

---

## 4. 运行前准备

### 创建共享内存

运行锁样例前，必须先用 `sample_code/share_mem/ubsm_shm_creator` 创建锁对象和通信队列使用的共享内存。下面以两节点 `computer01`、`computer02` 为例，主机名请按实际环境替换。

```bash
cd ../share_mem
vi ubsm_region.conf
```

`ubsm_region.conf` 示例：

```ini
request_size_mb=1024
hosts=computer01,computer02
```

编译并创建默认共享内存：

```bash
g++ -std=c++17 ubsm_shm_creator.cpp -I/usr/local/ubs_mem/include -L/usr/local/ubs_mem/lib -lubsm_sdk -o ubsm_shm_creator
export LD_LIBRARY_PATH=/usr/local/ubs_mem/lib:$LD_LIBRARY_PATH

./ubsm_shm_creator create computer01 shm_ub_lock
./ubsm_shm_creator create computer01 shm_node1_export
./ubsm_shm_creator create computer02 shm_node2_export
```

这三个名字要和 `dw_lock.conf` 保持一致：

```ini
lock_shm=shm_ub_lock
shm.NodeA=shm_node1_export
shm.NodeB=shm_node2_export
```

如果 `dw_lock.conf` 中改了共享内存名，需要先创建修改后的名字，再启动测试程序。

---

## 5. 运行方式

功能测试程序通过 `dw_lock.conf` 中的 `self=NodeA/NodeB` 区分节点身份；性能测试程序仍通过命令行的 `master/slave` 参数区分节点。

### 5.1 启动命令格式

功能测试程序：

```bash
./ub_dist_lock_func_test [slot=origin|slot=rebuild] [delay=0|1] [recursive=0|1]
```

性能测试程序：

```bash
./ub_dist_lock_perf_test <master|slave> <shm_lock_name> <count> <Tpercent> <RWpercent> <delay> <setaffinity> [shm_total_size(MB)] [shm_queueA_name] [shm_queueB_name]
```

参数解释见下文。

---

## 6. 参数说明

功能测试参数：

| 参数 | 含义 | 示例 |
| --- | --- | --- |
| `slot=origin/rebuild` | 启动时选择当前锁使用原始槽位还是重建槽位 | `slot=origin` |
| `delay=0/1` | 是否允许延迟释放 | `delay=0` |
| `recursive=0/1` | 是否允许递归加锁 | `recursive=0` |

性能测试参数：

| 参数 | 含义 | 示例 |
| --- | --- | --- |
| master/slave | 进程角色 | master |
| shm_lock_name | 存放锁的 shm 名字 | ub_lock |
| count | 总线程数（逻辑总数，用于按比例拆到 master/slave） | 512 |
| Tpercent | master 占用线程比例（0~1） | 0.8 |
| RWpercent | 当前节点读线程比例（0~1），写线程=剩余 | 0.7 |
| delay | 是否允许延迟释放（0/1） | 0 |
| setaffinity | CPU/NUMA 绑定策略：-1 不绑；>=0 代表 socket id | 0 |
| shm_total_size | 共享内存导出大小，默认不填写 1024MB | 1024MB 或 1073741824B |
| shm_queueA_name | 通信队列节点1的 shm 名字 | shm_sender |
| shm_queueB_name | 通信队列节点2的 shm 名字 | shm_receiver |
> 线程数拆分规则：

* master 线程数 = round(count * Tpercent)
* slave 线程数 = round(count * (1 - Tpercent))
  每个节点内部：
* reader = round(node_threads * RWpercent)
* writer = node_threads - reader
* `[]`可以不用填写，内部会使用默认的
---

## 7. 典型运行示例

### 性能测试程序

**终端 1（master）：**

```bash
./ub_dist_lock_perf_test master ub_lock 512 0.8 0.7 0 -1 [1024MB] [shm_sender] [shm_receiver]
```

**终端 2（slave）：**

```bash
./ub_dist_lock_perf_test slave ub_lock 512 0.8 0.7 0 -1 [1024MB] [shm_sender] [shm_receiver]
```
启动后程序会提示输入 `c` 开始压测：

```text
Type 'c' to continue...
```


---

## 8. 交互命令

`ub_dist_lock_func_test` 启动后进入 `ub_lock_cli>` 交互环：

* `owner 0..3`：切换后续命令由哪个 worker 线程执行，用于模拟不同线程加解锁。
* `s+` / `s-`：获取 / 释放读锁。
* `sx+` / `sx-`：获取 / 释放 SX 锁。
* `x+` / `x-`：获取 / 释放写锁。
* `recover <node_id>`：对当前锁调用 `ub_rw_lock_recover`，恢复指定故障节点持有的锁状态。
* `query` / `rebuild` / `queryrebuild`：执行锁状态查询和重建验证。
* `jobs`：查看当前 worker 队列和运行状态。
* `q` 或 `quit`：退出并释放资源

---

## 9. 输出说明

程序会打印三份统计：

* READ lock：读锁获取延迟统计（ns）
* WRITE lock：写锁获取延迟统计（ns）
* ALL lock：总体延迟统计（ns）

统计项包括：

* samples
* mean
* p50 / p95 / p99

---

## 10. 查询 / 重建接口测试说明

当前 `sample_code/ub_lock/ub_dist_lock_func_test.cpp` 已经补充了查询、恢复和重建接口的测试能力，用于验证：

- 旧共享内存锁地址上的本地锁状态查询
- 指定故障节点的 `recover`
- 两节点汇总查询结果后，在新的锁地址上执行 `rebuild`
- 进程重启后，从重建后的偏移地址继续执行加解锁

### 10.1 共享内存槽位布局

测试程序只演示一把逻辑锁，内部预留两个共享内存槽位：

- 原始槽位：`lock[0]`
- 重建槽位：`lock[1]`

其中：

- `slot=origin` 表示程序启动后默认从原始槽位运行
- `slot=rebuild` 表示程序启动后默认从重建槽位运行

每次执行 `queryrebuild` 或 `rebuild` 时，程序都会把这把逻辑锁从当前活跃槽位切到另一个槽位。

### 10.2 配置文件要求

`dw_lock.conf` 除原有配置外，还需要增加固定 IP：

```conf
self=NodeA
nodes=2
lock_shm=shm_ub_lock
shm.NodeA=shm_node1_export
shm.NodeB=shm_node2_export
ip.NodeA=10.10.10.1
ip.NodeB=10.10.10.2
```

说明：

- `ip.NodeA` / `ip.NodeB` 用于两节点交换 `query` 结果
- 当前样例的自动交换流程只支持 `2` 节点
- 查询结果文件固定写入当前工作目录下的 `test.txt`

### 10.3 新增命令

程序交互模式新增如下命令：

- `query`
  对当前锁执行本地查询，并把结果写到 `test.txt`

- `recover <node_id>`
  对当前锁调用 `ub_rw_lock_recover`，`node_id` 与配置中的节点顺序一致：`NodeA=0`，`NodeB=1`，`NodeC=2`，`NodeD=3`

- `rebuild`
  从 `test.txt` 读取**已经拼接好的结果**，构造 `rebuild_info`，然后执行 `rebuild`

- `queryrebuild`
  自动执行：
   1. 本地 `query`
   2. 将结果写到 `test.txt`
   3. 两节点互发查询结果
   4. 拼接结果并覆盖写回 `test.txt`
  5. 调用 `rebuild`
  6. 将当前锁切换到新的槽位

### 10.4 查询结果格式

`query` 写入文件的每一行格式如下：

```text
<node_id> <held_mode> <holder_tid> <recursive_count> <has_shared_ref> <reserve_mode>
```

例如：

```text
0 2 1284196 1 0 3
```

表示：

- `node_id=0`
- `held_mode=2`，即 `UB_LOCK_X`
- `holder_tid=1284196`
- `recursive_count=1`
- `has_shared_ref=0`，表示该节点没有持有可恢复的全局 S 引用
- `reserve_mode=3`，即 `UB_LOCK_I`

如果执行 `rebuild`，则 `test.txt` 中必须已经包含该锁的**所有节点结果**，一行一个节点。

例如两节点场景下：

```text
0 2 1284196 1 0 3
1 3 0 0 0 3
```

### 10.5 推荐验证流程

#### 场景 A：自动查询并重建到偏移槽位

两节点分别启动：

```bash
./ub_dist_lock_func_test slot=origin
```

先在某个节点上制造持锁状态，例如：

```text
x+
```

然后两节点都执行：

```text
queryrebuild
```

执行成功后，当前锁会从原始槽位切换到重建槽位。

后续再执行：

```text
x-
s+
s-
```

都会落到新的共享内存锁地址上。

#### 场景 B：共享内存损坏 + 进程退出后，从偏移地址继续验证

1. 两节点先使用原始槽位启动：

```bash
./ub_dist_lock_func_test slot=origin
```

2. 执行一次：

```text
queryrebuild
```

3. 模拟：

- 原始共享内存锁地址失效
- 原进程退出

4. 两节点重新启动程序，但改为从重建槽位组启动：

```bash
./ub_dist_lock_func_test slot=rebuild
```

5. 重新执行：

```text
s+
s-
x+
x-
```

如果这些操作都能正常执行，说明：

- 进程重启后已经从偏移后的新锁地址继续运行
- 原故障进程退出后，幸存状态恢复链路可继续工作

### 10.6 NodeA / NodeB 的交换角色

自动交换 `query` 结果时：

- `NodeA` 作为监听端
- `NodeB` 主动连接 `NodeA`

默认端口为：

```text
39091
```

请确保两节点之间该端口可连通。

### 10.7 注意事项

- `queryrebuild` 需要两节点都执行，否则自动交换会阻塞或失败
- `rebuild` 不会自动拼接文件，只有 `queryrebuild` 才会自动拼接
- 当前样例主要用于验证恢复链路，不等价于生产级故障恢复编排
- 样例当前未额外处理文件半写、网络异常重试、重建超时接管等问题

---

---

## 11. `readloop` 读锁性能压测输出说明

`ub_dist_lock_perf_test` 支持使用 `readloop` 模式压测同一节点内多线程并发读锁性能：

```bash
./ub_dist_lock_perf_test master <shm_lock_name> <readers> 1 1 0 -1 readloop duration=10
```

示例输出：

```text
================== READ LOOP BENCH ==================
readers       : 16
duration_sec  : 10
success_ops   : 4400993
fail_ops      : 0
elapsed_sec   : 10.0032
ops_per_sec   : 439959
avg_lock_ns   : 19079
avg_unlock_ns : 16809
=====================================================
elapsed_ms     : 10005
shared_var     : 0
```

字段含义：

| 字段 | 含义 |
| --- | --- |
| `readers` | 本次压测启动的读线程数量。所有线程都对同一把锁循环执行 `S lock/unlock`。 |
| `duration_sec` | 目标压测时长，单位秒。 |
| `success_ops` | 成功完成的读锁加锁 + 解锁次数。一次完整的 `s_lock` + `s_unlock` 计为 1 次。 |
| `fail_ops` | 加锁或解锁失败次数。正常纯读压测期望为 0。 |
| `elapsed_sec` | 实际压测耗时，包含线程停止和收尾带来的微小误差。 |
| `ops_per_sec` | 每秒完成的成功操作数，核心吞吐指标。数值越高表示读锁并发性能越好。 |
| `avg_lock_ns` | 成功操作中，平均 `ub_rw_lock_s_lock` 耗时，单位纳秒。 |
| `avg_unlock_ns` | 成功操作中，平均 `ub_rw_lock_s_unlock` 耗时，单位纳秒。 |
| `elapsed_ms` | 整个 readloop 模式从开始到结束的外层耗时，单位毫秒。 |
| `shared_var` | 写线程修改的共享变量。`readloop` 为纯读压测，通常保持为 0。 |

推荐对比方式：

```bash
./ub_dist_lock_perf_test master <shm_lock_name> 1 1 1 0 -1 readloop duration=10
./ub_dist_lock_perf_test master <shm_lock_name> 8 1 1 0 -1 readloop duration=10
./ub_dist_lock_perf_test master <shm_lock_name> 16 1 1 0 -1 readloop duration=10
./ub_dist_lock_perf_test master <shm_lock_name> 32 1 1 0 -1 readloop duration=10
```

判断读锁优化是否有效，重点看：

- `ops_per_sec` 是否随 `readers` 增加明显提升。
- `avg_lock_ns` 是否没有随着线程数增加而线性恶化。
- `fail_ops` 是否保持为 0。

该模型主要验证“同节点多读线程并发持有本地 `S` 锁，并复用同一份全局 `S` 锁”的性能收益。

---

## 12. `pureread` 纯读性能基准测试说明

`pureread` 是验证本地 4-lane reader lock 优化的专项模型。它只启动同节点读线程，循环执行同一把锁的 `S lock/unlock`，用固定时长统计吞吐，并按间隔采样延迟，避免每次操作都调用 `clock_gettime` 干扰结果。线程内统计先用本地变量累加，退出后再汇总，避免测试程序自己的 stats 数组伪共享放大噪声。

### 12.1 最推荐模型

最容易体现多路加读锁优化收益的线程数是：

```text
1, 4, 16, 32
```

原因：

- `1`：单线程基线，确认优化没有明显退化。
- `4`：等于 lane 数，观察 lane 分散后的基础扩展。
- `16`：平均每 lane 约 4 个线程，旧单计数 CAS 热点会明显放大，通常最能看出收益。
- `32`：平均每 lane 约 8 个线程，继续放大本地 reader count 竞争，但通常还未被调度/NUMA 噪声完全淹没。

`64` 线程可作为扩展观察项，但更容易受调度、CPU 绑定、全局合流和 NUMA 影响，不建议作为首要判断模型。

### 12.2 启动方式

推荐先跑固定专项矩阵：

```bash
./ub_dist_lock_perf_test master <shm_lock_name> 32 1.0 1.0 0 0 pureread duration=15 threads=1,4,16,32 sample_interval=2048 lane_stats=1
```

若要绕过全局读锁合流，只观察本地 `LocalLock::lock_s/unlock_s`，使用 `localonly`：

```bash
./ub_dist_lock_perf_test master <shm_lock_name> 32 1.0 1.0 0 0 localonly duration=15 threads=1,4,16,32 sample_interval=2048 lane_stats=1
```

参数说明：

| 参数 | 建议值 | 含义 |
| --- | --- | --- |
| `count` | `32` | 未指定 `threads=` 时作为默认矩阵上限；指定 `threads=` 后以显式矩阵为准 |
| `Tpercent` | `1.0` | master 单节点压测时使用全部线程 |
| `RWpercent` | `1.0` | 纯读模型，保持全读 |
| `delay` | `0` | 关闭延迟释放，尽量隔离本地 reader lane 效果 |
| `setaffinity` | `0` 或 `-1` | `0` 绑定 socket0；环境不稳定时可用 `-1` 关闭绑定 |
| `duration=15` | `10~30` | 每个线程数单次运行时长 |
| `threads=1,4,16,32` | 推荐固定值 | 最小化测试矩阵，突出 lane-CAS 收益窗口 |
| `sample_interval=2048` | `1024~4096` | 每 N 次操作采样一次延迟；降低采样开销 |
| `lane_stats=1` | `1` | 打印每个 lane 的线程数和操作数分布 |
| `localonly` | 隔离本地锁时使用 | 不走 API 全局合流，只测本地 `S lock/unlock` |

不传 `threads=` 时，`pureread` 默认使用 `1,4,16,32`，并过滤掉超过 `count` 的线程数。

### 12.3 示例输出

```text
=====================================================
          PURE READ LOCK BENCHMARK (Lanes Test)
=====================================================
Configuration:
  - Duration per run:    15s
  - Warmup iterations:   1
  - Test iterations:     3
  - Latency sampling:    every 2048 ops
  - Delay release:       OFF
  - CPU pinning:         ON
  - Socket ID:           0
-----------------------------------------------------
Results:
Threads     Throughput (M ops/s) Sample Avg (us)  Min (ns)  Max (ns)  Samples     Fail Ops    Total Ops
--------  --------------------  -----------------  ----------  ----------  ------------  ------------  ------------
1         ...
4         ...
16        ...
32        ...
```

### 12.4 字段含义

| 字段 | 含义 |
| --- | --- |
| `Threads` | 本次测试的并发读线程数 |
| `Throughput (M ops/s)` | 每秒成功 `S lock + S unlock` 操作数，核心指标 |
| `Sample Avg (us)` | 按 `sample_interval` 采样到的平均加锁+解锁延迟 |
| `Min (ns)` / `Max (ns)` | 采样到的最小/最大加锁延迟 |
| `Samples` | 延迟采样次数 |
| `Fail Ops` | 加锁或解锁失败次数；正常纯读压测应为 0 |
| `Total Ops` | 所有正式迭代的成功操作数总和 |

- `global_s_acquire/delay`：真正获取全局 S 的次数，以及命中延迟释放保留锁的次数。

### 12.5 判断标准

优先看同一环境下旧实现和新实现的对比：

- `16`、`32` 线程的 `Throughput (M ops/s)` 是否明显提升。
- `16`、`32` 线程的 `Sample Avg (us)` 是否低于旧实现，或至少没有随线程数线性恶化。
- `Lane Distribution` 中 4 个 lane 的 `threads` 和 `ops` 是否大体均匀；如果某个 lane 明显偏斜，结果会低估多路 CAS 收益。
- `fail_ops` 正常应为 0；若出现失败，优先排查超时、CPU 绑定、对端进程和共享内存初始化。

### 12.6 技术原理

4-lane reader lock 通过以下方式减少本地读锁竞争：

1. `tid` hash 到 4 个 reader lane。
2. `S lock` 快路径只 CAS 自己的 16-bit lane reader count。
3. `X lock` 仍 CAS 整个本地 word，并通过所有 lane 的 sentinel 阻止新 reader。
4. 全局读锁仍按同节点合流复用，不是“同 lane 复用全局锁”。

该模型把写锁、远端竞争和业务临界区尽量拿掉，最适合观察本地 `S lock/unlock` 多路 CAS 的收益。

---
