# ub_dist_tx_res 功能验证 Demo

本目录包含 `ub_dist_tx_res`（分布式事务资源）模块的全场景交互式功能验证程序。覆盖 `init` / `set` / `get` / `add` / `fetch_add` 及全部 `fence` 变体，并验证 NULL 指针、未对齐地址、`UINT64_MAX` 溢出回绕等边界场景，以及多线程并发原子性和 fence 可见性。

---

## 覆盖场景列表

| 编号 | 场景名称 | 验证目标 |
|------|---------|---------|
| 1 | 基础操作验证 | `init` → `get` 为 0；`set` / `get` 读写一致性；`fetch_add` 返回旧值 |
| 2 | add 原子加法验证 | `add` 累加正确性；`add(0)` 不变 |
| 3 | fetch_add 与 add 对比 | 两者最终值一致；`fetch_add` 额外返回旧值 |
| 4 | 多线程并发 add 原子性 | 8 线程 × 10000 次 `add(1)`，最终值 == 80000 |
| 5 | fence release + acquire 可见性 | 本端写-本端读，fence 保证写可见性 |
| 6 | fence seq_cst 全局序验证 | 100 轮迭代，不出现两者均读到 0 |
| 7 | fence acq_rel 双向同步 | 双 payload 写入后 fence_acq_rel，reader 读到正确值 |
| 8 | add + fence 组合场景 | 模拟本端写远端读：多次 add 后 fence_release 确保远端可见 |
| 9 | 溢出回绕验证 | `UINT64_MAX + 1` 回绕为 0；`fetch_add` 溢出旧值正确 |
| 10 | 错误处理验证 | NULL 指针返回 `UB_RES_ERROR`；未对齐地址返回 `UB_RES_ERROR`；fence 均返回 `UB_RES_OK` |
| 11 | 多线程并发 fetch_add 原子性 | 8 线程 × 10000 次 `fetch_add(1)`，最终值和旧值之和均正确 |
| 12 | fence 可见性压测 | 50 轮高频 writer/reader fence 同步，零失败 |

> 输入 **99** 可一键运行全部 12 个场景。

---

## 前置准备

先编译项目库，生成 `build/lib/libubs-atomic.so`：

```bash
cd /path/to/ubs-atomic
sh build.sh
```

确认产物存在：

```bash
ls build/lib/libubs-atomic.so
```

---

## 编译

```bash
cd sample_code/ub_dist_tx_res

g++ -O2 -g -std=c++17 \
    -o ub_dist_tx_res_func_test ub_dist_tx_res_func_test.cpp \
    -I../../include \
    -L../../build/lib \
    -lubs-atomic -lpthread \
    -Wl,-rpath,'$ORIGIN/../../build/lib'
```

---

## 运行方式

```bash
./ub_dist_tx_res_func_test
```

程序启动后显示交互式菜单：

```
============================================================
  ub_dist_tx_res 全场景功能验证 Demo
============================================================
   1. 基础操作验证 (init / set / get / fetch_add)
   2. add 原子加法验证
   3. fetch_add 与 add 对比
   4. 多线程并发 add 原子性验证
   5. fence release + acquire 可见性验证 (本端写-本端读)
   6. fence seq_cst 全局序验证
   7. fence acq_rel 双向同步验证
   8. add + fence 组合场景 (模拟本端写远端读)
   9. 溢出回绕验证 (UINT64_MAX + 1)
  10. 错误处理验证 (NULL / 未对齐地址)
  11. 多线程并发 fetch_add 原子性验证
  12. 多线程 fence release+acquire 可见性压测
  99. 运行全部场景
   0. 退出
============================================================
请输入场景编号:
```

输入数字选择场景，输入 `0` 退出。

---

## 输出示例

### 场景1：基础操作验证

```
============================================================
  场景1: 基础操作验证 (init / set / get)
============================================================
  [PASS] init 返回 OK                               (ret=0)
  [PASS] init 后 get == 0                            (值=0)
  [PASS] set(100) 返回 OK                            (ret=0)
  [PASS] set(100) 后 get == 100                      (值=100)
  [PASS] set(UINT64_MAX) 返回 OK                     (ret=0)
  [PASS] set(UINT64_MAX) 后 get == UINT64_MAX        (值=18446744073709551615)
  [PASS] fetch_add(1) 返回 OK                        (ret=0)
  [PASS] fetch_add 旧值 == 99                        (值=99)
  [PASS] fetch_add 后 get == 100                     (值=100)
------------------------------------------------------------
[PASS] 场景1: 基础操作验证 全部通过 (9/9)
============================================================
```

### 场景4：多线程并发 add 原子性验证

```
============================================================
  场景4: 多线程并发 add 原子性验证
============================================================
  线程数: 8, 每线程 add 次数: 10000
  期望最终值: 80000
  实际最终值: 80000  (耗时 3.21 ms)
  [PASS] 并发 add 最终值 == 80000                    (值=80000)
------------------------------------------------------------
[PASS] 场景4: 多线程并发 add 原子性验证 全部通过 (1/1)
============================================================
```

### 场景10：错误处理验证

```
============================================================
  场景10: 错误处理验证 (NULL / 未对齐地址)
============================================================
  ---- NULL 指针测试 ----
  [PASS] init(NULL) 返回 ERROR                       (ret=-1)
  [PASS] set(NULL, 1) 返回 ERROR                     (ret=-1)
  [PASS] get(NULL, &out) 返回 ERROR                  (ret=-1)
  [PASS] fetch_add(NULL, 1, &out) 返回 ERROR         (ret=-1)
  [PASS] add(NULL, 1) 返回 ERROR                     (ret=-1)
  [PASS] get(valid, NULL) 返回 ERROR                 (ret=-1)
  [PASS] fetch_add(valid, 1, NULL) 返回 ERROR        (ret=-1)

  ---- 未对齐地址测试 ----
  [PASS] init(未对齐) 返回 ERROR                     (ret=-1)
  [PASS] set(未对齐, 1) 返回 ERROR                   (ret=-1)
  [PASS] get(未对齐, &out) 返回 ERROR                (ret=-1)
  [PASS] fetch_add(未对齐, 1, &out) 返回 ERROR       (ret=-1)
  [PASS] add(未对齐, 1) 返回 ERROR                   (ret=-1)

  ---- fence 各变体返回值测试 ----
  [PASS] fence_acquire() 返回 OK                     (ret=0)
  [PASS] fence_release() 返回 OK                     (ret=0)
  [PASS] fence_acq_rel() 返回 OK                     (ret=0)
  [PASS] fence_seq_cst() 返回 OK                     (ret=0)
------------------------------------------------------------
[PASS] 场景10: 错误处理验证 全部通过 (16/16)
============================================================
```

---

## 场景详细说明

### 场景1：基础操作验证

验证 `init` 将值初始化为 0，`set` / `get` 读写一致，`set(UINT64_MAX)` 边界值正确，以及 `fetch_add` 返回旧值并正确累加。

### 场景2：add 原子加法验证

验证 `add` 不返回旧值但正确累加，包括 `add(0)` 不变的情况。

### 场景3：fetch_add 与 add 对比

两个变量初始值相同（100），分别执行 `fetch_add(42)` 和 `add(42)`，验证最终值一致（142），`fetch_add` 额外返回旧值。

### 场景4：多线程并发 add 原子性验证

8 个线程各执行 10000 次 `add(1)`，验证最终值恰好为 80000。这是验证原子操作在高并发下不丢失更新的核心场景。

### 场景5：fence release + acquire 可见性

模拟经典的"写-发布"模式：writer 先写 data，`fence_release` 后写 flag；reader 自旋等 flag，`fence_acquire` 后读 data。验证 reader 能看到 writer 的数据。

### 场景6：fence seq_cst 全局序验证

100 轮迭代，每轮两个线程分别 `set` 一个变量后 `fence_seq_cst`，再读另一个变量。seq_cst 保证全局全序，不会出现两个线程都读到 0 的情况。

### 场景7：fence acq_rel 双向同步

writer 写入两个 payload 后 `fence_acq_rel` 再写 flag；reader 看到 flag 后 `fence_acq_rel` 再读 payload。acq_rel 双向同步确保所有写操作对 reader 可见。

### 场景8：add + fence 组合场景

模拟"本端写远端读"的典型 DSM 场景：本端执行 5 次 `add(10)` 后 `fence_release`，模拟远端 `fence_acquire` 后读取，验证能看到完整的累加结果（50）。

### 场景9：溢出回绕验证

验证 `UINT64_MAX + 1` 回绕为 0，`(UINT64_MAX - 1) + 2` 回绕为 0，`UINT64_MAX + UINT64_MAX` 为 `UINT64_MAX - 1`，以及 `fetch_add` 溢出时旧值返回正确。

### 场景10：错误处理验证

- **NULL 指针**：`init`/`set`/`get`/`fetch_add`/`add` 传入 NULL handle，均返回 `UB_RES_ERROR`
- **未对齐地址**：传入 `0x7ffee3b5a001`（非 8 字节对齐），均返回 `UB_RES_ERROR`
- **fence 返回值**：四个 fence 变体均无参数，始终返回 `UB_RES_OK`

### 场景11：多线程并发 fetch_add 原子性验证

8 线程各执行 10000 次 `fetch_add(1)`，不仅验证最终值为 80000，还验证所有线程返回的旧值之和等于 `N*(N-1)/2`（N = 80000），从数学上证明没有操作被丢失或重复。

### 场景12：多线程 fence 可见性压测

50 轮高频重复场景5的 writer/reader 模式，每轮使用不同的 magic 值，验证 fence 在高频调度下始终保证可见性，零失败。

---

## 注意事项

1. **内存序平台差异**：场景 5/6/7/8/12 依赖线程调度，在弱内存序平台（ARM64）上验证更有意义；x86_64 天然 TSO（Total Store Order），fence 效果在这些场景下通常也正确但较难暴露问题。

2. **不依赖共享内存**：本 demo 使用栈上分配的 `uint64_t` 变量进行验证，无需 `ubsmem` SDK 或创建共享内存，降低运行门槛。

3. **跨节点场景**：本 demo 仅验证本端线程间行为；真实跨节点 NC（Non-Coherent）场景需配合 `ubsmem` 使用分布式共享内存。

4. **日志级别**：默认日志级别为 `WARN`，错误处理场景中的未对齐警告会被打印到 stdout。如需抑制，可修改源码中 `ub_atomic_set_log_level` 的级别参数。

---

## 跨节点 NC 环境验证（ub_dist_tx_res_nc_test）

本目录额外提供基于 ubsmem 真实共享内存映射的跨节点 NC（Non-Coherent）环境验证程序 `ub_dist_tx_res_nc_test.cpp`，用于验证 writer（本端CC/export端）与 reader（远端NC/import端）之间通过各类 fence 保证数据可见性的正确性。

### 功能简介

| 编号 | 场景名称 | 验证目标 |
|------|---------|----------|
| 1 | 本端写-远端读 | `set` + `fence_release` → reader `fence_acquire` + `get` |
| 2 | 本端add-远端读 | 5次 `add(100)` + `fence_release` → reader 验证 counter==500 |
| 3 | 全局序验证 | 100轮 `fence_seq_cst` 双向，不出现两端均读到0 |
| 4 | 双向同步 | `fence_acq_rel` + 双payload验证 |
| 5 | 并发fetch_add | 双端各执行10000次 `fetch_add(1)`，最终值==20000 |
| 6 | 多轮压测 | 50轮连续 fence 可见性压测，零失败 |

> 输入 **7** 可一键运行全部 6 个场景。

### 工作原理

```
  writer (NodeA, 本端CC)                  reader (NodeB, 远端NC)
  ──────────────────────                  ──────────────────────
  1. set(data, MAGIC)
  2. fence_release()
  3. set(ready_flag, 1)  ──────────────►  4. 自旋等 ready_flag==1
                                          5. fence_acquire()
                                          6. get(data) → 验证 == MAGIC
                                          7. set(done_flag, 1)
  8. 自旋等 done_flag==1  ◄─────────────
  ──────────────────────────────────────────────────────────────────
  说明：writer 在本端执行写操作后发 fence_release，确保写操作对外可见；
        reader 在远端看到 ready_flag 后发 fence_acquire，确保能看到 writer 的数据。
```

### 共享内存布局

```
  共享内存 "shm_tx_res_nc"
  ┌─────────────────────────────────────────────┐
  │  Control Area (64B, cache line aligned)     │ offset 0
  │  ├─ ready_flag  : uint64_t                 │
  │  ├─ done_flag   : uint64_t                 │
  │  ├─ scenario_id : uint64_t                 │
  │  └─ padding[40]                            │
  ├─────────────────────────────────────────────┤
  │  Data Area                                  │ offset 64
  │  ├─ data        : uint64_t (场景1数据)      │ offset 64
  │  ├─ counter     : uint64_t (场景2计数器)    │ offset 72
  │  ├─ x, y        : uint64_t (场景3全局序)    │ offset 80/88
  │  ├─ payload[2]  : uint64_t (场景4载荷)      │ offset 96
  │  ├─ fetch_counter: uint64_t(场景5竞争计数)   │ offset 112
  │  └─ result      : uint64_t (结果回写)       │ offset 120
  └─────────────────────────────────────────────┘
```

### 前置准备

**1. 编译项目库**

```bash
cd /path/to/ubs-atomic
sh build.sh
ls build/lib/libubs-atomic.so
```

**2. 创建共享内存**

使用 `sample_code/share_mem/ubsm_shm_creator` 创建 NC 验证所需的共享内存。以两节点 `computer01`、`computer02` 为例：

```bash
cd sample_code/share_mem

# 编辑 ubsm_region.conf，设置集群主机列表
cat > ubsm_region.conf <<EOF
request_size_mb=64
hosts=computer01,computer02
EOF

# 编译 ubsm_shm_creator
g++ -std=c++17 ubsm_shm_creator.cpp \
    -I/usr/local/ubs_mem/include \
    -L/usr/local/ubs_mem/lib -lubsm_sdk \
    -o ubsm_shm_creator

export LD_LIBRARY_PATH=/usr/local/ubs_mem/lib:$LD_LIBRARY_PATH

# 创建共享内存（只需在一个节点上执行一次）
./ubsm_shm_creator create computer01 shm_tx_res_nc
```

共享内存名称 `shm_tx_res_nc` 需与 `tx_res_nc.conf` 中的配置一致。

### 编译

```bash
cd sample_code/ub_dist_tx_res

g++ -O2 -g -std=c++17 \
    -o ub_dist_tx_res_nc_test ub_dist_tx_res_nc_test.cpp \
    -I../../include \
    -I/usr/local/ubs_mem/include \
    -L../../build/lib -lubs-atomic \
    -L/usr/local/ubs_mem/lib -lubsm_sdk \
    -lpthread \
    -Wl,-rpath,/usr/local/ubs_mem/lib \
    -Wl,-rpath,'$ORIGIN/../../build/lib'
```

> 编译需依赖 `ubsmem SDK`（`/usr/local/ubs_mem/`），请确保目标机器已安装。

### 运行方式

两台节点分别执行，角色通过 `--role` 参数指定：

**NodeA（writer，本端CC）：**

```bash
./ub_dist_tx_res_nc_test --role writer --config tx_res_nc.conf
```

**NodeB（reader，远端NC）：**

```bash
./ub_dist_tx_res_nc_test --role reader --config tx_res_nc.conf
```

> 两节点的 `tx_res_nc.conf` 中 `shm_name` 需一致，指向同一块共享内存。
> 启动顺序无严格要求，但通常先启动 writer 再启动 reader，或同时启动。

程序启动后显示交互式菜单：

```
============================================================
  ub_dist_tx_res 跨节点 NC 环境验证
============================================================
  角色: writer (本端CC)
============================================================
  1. 本端写-远端读 (set + fence_release + fence_acquire)
  2. 本端add-远端读 (add + fence_release + fence_acquire)
  3. 全局序验证 (fence_seq_cst 双向)
  4. 双向同步 (fence_acq_rel + payload验证)
  5. 并发fetch_add竞争 (双端各执行N次)
  6. 多轮连续fence可见性压测
  7. 运行全部场景
  0. 退出
============================================================
请输入场景编号:
```

两端需输入**相同的场景编号**以协同执行。例如两端都输入 `1` 执行场景1，或都输入 `7` 运行全部。

### 各场景预期输出

**场景1：本端写-远端读**

```
[writer] 场景1: 本端写-远端读
  [writer] 写入 data = 0xdeadbeef
  [writer] reader 已确认读取完成

[reader] 场景1: 本端写-远端读
  [reader] 等待 writer 就绪...
  [reader] 读到 data = 0xdeadbeef
  [PASS] reader 读到 data == 0xDEADBEEF              (值=0xdeadbeef)
[PASS] 场景1: 本端写-远端读 全部通过 (1/1)
```

**场景5：并发fetch_add**

```
[writer] 场景5: 并发fetch_add竞争
  [writer] 开始执行 10000 次 fetch_add(1)...
  [writer] 10000 次 fetch_add 完成, 耗时 X.XX ms
  [writer] 最终 fetch_counter = 20000 (期望 20000)

[reader] 场景5: 并发fetch_add竞争
  [reader] 开始执行 10000 次 fetch_add(1)...
  [reader] 10000 次 fetch_add 完成, 耗时 X.XX ms
  [reader] 最终 fetch_counter = 20000 (期望 20000)
  [PASS] 并发 fetch_add 最终值 == 20000               (值=0x4e20)
```

### 配置文件说明

`tx_res_nc.conf`：

```ini
# 本节点身份: NodeA(writer) 或 NodeB(reader)
self=NodeA

# 共享内存配置
shm_name=shm_tx_res_nc
shm_size_mb=64
```

| 字段 | 说明 |
|------|------|
| `self` | 本节点身份标识，NodeA 或 NodeB |
| `shm_name` | 共享内存名称，两端必须一致 |
| `shm_size_mb` | 共享内存大小（MB），默认 64 |

### 注意事项

1. **平台差异**：NC 场景在弱内存序平台（ARM64）上验证更有意义。x86_64 天然 TSO，fence 效果在本地场景下通常也正确，但跨节点 NC 映射下仍可验证 DSM 框架的正确性。

2. **依赖 ubsmem SDK**：本 demo 依赖 `ubsmem SDK` 进行真实共享内存映射，编译和运行均需 SDK 环境。若当前环境缺少 SDK，可先使用 `ub_dist_tx_res_func_test`（本地版）验证 API 功能。

3. **共享内存必须预先创建**：运行前需使用 `ubsm_shm_creator` 创建指定名称的共享内存，否则 `ubsmem_shmem_map` 会失败。

4. **双端协调**：两端通过 `ready_flag` / `done_flag` 握手机制同步，无需额外网络通信。两端必须同时运行并输入相同场景编号。

5. **不依赖通信队列**：`ub_dist_tx_res` 模块独立于通信队列（`ub_comm_queue`），仅需共享内存和 fence 即可完成跨节点数据可见性验证。
