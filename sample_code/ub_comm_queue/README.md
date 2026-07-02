# PingPong Demo

基于 `ub_dist_comm_queue` 的跨节点 Ping-Pong 延迟测试工具。通过共享内存通信队列，在两个节点间发送/回复消息，测量端到端 RTT（Round-Trip Time）及各阶段耗时。

## 工作原理

```
  Node A (Ping)                          Node B (Pong)
  ┌──────────┐      PING (ring_A)       ┌──────────┐
  │          │ ───────────────────────► │          │
  │  Role A  │                          │  Role B  │
  │          │ ◄─────────────────────── │          │
  └──────────┘      PONG (ring_B)       └──────────┘
```

- **Role A（Pinger）**：发送 PING 消息到 Node B，等待 PONG 回复，统计 RTT。
- **Role B（Ponger）**：接收 PING 消息后立即回复 PONG，统计处理耗时。

两个角色编译为同一个二进制文件，通过 `--role` 参数区分。

## 共享内存布局

| 共享内存 | 内容 | 默认名称 |
|---------|------|---------|
| Node 0 SHM (`-0`) | init_area (1MB) + ring_A | `shm_node0_export` |
| Node 1 SHM (`-1`) | ring_B | `shm_node1_export` |

两个节点必须映射同一组共享内存（名称相同），由 `--role` 决定自身角色。

### 创建共享内存

运行 pingpong 前，先用 `sample_code/share_mem/ubsm_shm_creator` 创建两个默认共享内存。下面以两节点 `computer01`、`computer02` 为例，主机名请按实际环境替换。

```bash
cd ../share_mem
vi ubsm_region.conf
```

`ubsm_region.conf` 示例：

```ini
request_size_mb=1024
hosts=computer01,computer02
```

编译并创建共享内存：

```bash
g++ -std=c++17 ubsm_shm_creator.cpp -I/usr/local/ubs_mem/include -L/usr/local/ubs_mem/lib -lubsm_sdk -o ubsm_shm_creator
export LD_LIBRARY_PATH=/usr/local/ubs_mem/lib:$LD_LIBRARY_PATH

./ubsm_shm_creator create computer01 shm_node0_export
./ubsm_shm_creator create computer02 shm_node1_export
```

如果运行 pingpong 时通过 `-0`、`-1` 指定了其他共享内存名，需要提前创建对应名字。

如果同一环境中已经按锁样例创建过 `shm_node1_export`，建议给 pingpong 使用独立名字，例如先创建 `shm_ping_node0_export`、`shm_ping_node1_export`，启动时增加 `-0 shm_ping_node0_export -1 shm_ping_node1_export`。

## 依赖

本 demo 依赖以下库：

| 依赖 | 说明 |
|------|------|
| **libubs-atomic.so** | ubs-atomic 通信队列库（本项目编译产物） |
| **ubs_mem_stub** | 共享内存抽象层，提供 `ubs_mem.h` / `ubs_mem_def.h` 头文件及 `libubs_mem_stub.so` 库 |

> `ubs_mem_stub` 提供共享内存的初始化（`ubsmem_init_attributes`、`ubsmem_initialize`）、区域查找（`ubsmem_lookup_regions`）和映射（`ubsmem_shmem_map`）等接口，是 pingpong demo 与底层共享内存交互的必要桥梁。

### 安装 ubs_mem_stub

请按照 ubs_mem_stub 项目自身的构建说明进行编译安装。安装完成后需确保：

- 头文件 `ubs_mem.h`、`ubs_mem_def.h` 位于系统 include 路径或可通过 `-I` 指定
- 共享库 `libubs_mem_stub.so` 位于系统 lib 路径或可通过 `-L` 指定

## 编译

### 1. 编译 ubs-atomic 主库

```bash
cd /path/to/ubs-atomic
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
```

编译产物：
- `build/lib/libubs-atomic.so` — 通信队列共享库

### 2. 编译 pingpong demo

```bash
cd /path/to/ubs-atomic/sample_code/ub_comm_queue

g++ -g -O3 -fno-omit-frame-pointer \
    -o pingpong pingpong.cpp \
    -l./ -L./ -lubs-atomic -lubsm_sdk -lpthread \
    -l/usr/local/ubs_mem/include/ -L/usr/local/ubs_mem/lib \
    -Wl,-rpath,'/usr/local/ubs_mem/lib' -Wl,-rpath,'$ORIGIN'
```

> 请根据实际安装路径替换上述 `-I` 和 `-L` 参数。

### 3. 设置运行时库路径

```bash
export LD_LIBRARY_PATH=/path/to/ubs-atomic/build/lib:/path/to/ubs_mem/lib:$LD_LIBRARY_PATH
```

## 运行

### 基本用法

在同一台机器的两个终端中分别运行：

```bash
# 终端 1：先启动 B（接收端）
./pingpong --role B

# 终端 2：再启动 A（发送端），A 启动后会等待 3 秒让 B 就绪
./pingpong --role A
```

> **提示**：A 启动后会自动等待 3 秒，确保 B 已完成初始化。如需调整等待时间，可修改源码中 `g_wait_b_ready_s` 变量。

### 完整参数

```
Usage: ./pingpong --role A|B [options]

Required:
  --role A|B         运行角色 (A=ping发送端, B=pong接收端)

Common options:
  --cpu-id <N>       绑定 CPU ID (默认 4)
  --msg-size <bytes> 消息总长度含消息头 (支持: 64, 4096, 8192，默认 64)
  -0 <shm_name>      Node 0 共享内存名 (默认 shm_node0_export)
  -1 <shm_name>      Node 1 共享内存名 (默认 shm_node1_export)
  -h                 显示帮助

Role A (ping) options:
  -n <count>         消息总数 (默认 10000)
  -t <threads>       发送线程数 (默认 1)
  -d <N>             打印前 N 条详情 (默认 20)
  -w <N>             丢弃前 N 条预热样本 (默认 1)
  -i <us>            每条消息间隔微秒 (默认 0)

Role B (pong) options:
  -n <count>         期望消息数 (0=永远运行，默认 0)
```

### 示例

```bash
# 发送 50000 条 4KB 消息，2 个发送线程，丢弃前 5 条预热
./pingpong --role A -n 50000 --msg-size 4096 -t 2 -w 5

# B 端指定接收 50000 条后退出
./pingpong --role B -n 50000 --msg-size 4096
```

## 输出说明

### A 端输出

```
===== A Node Stats (msg_size=64) =====
Expected: 10000 | Received PONG: 10000

[RTT ns] avg=1234.56 min=800 p50=1200 p99=2500 max=5000

===== First 20 message details (ID 0..19) =====
[ID=0]
  A_send_time : 1234567890
  B_recv_time : 1234568500
  B_send_time : 1234568700
  A_recv_time : 1234569200
  RTT(ns)     : 1310
  B_process(ns): 200
  send_cost(ns): 150
  recv_cb_cost(ns): 50
```

各字段含义：

| 指标 | 说明 |
|------|------|
| **RTT** | 消息往返总耗时（A_send → A_recv） |
| **B_process** | B 端处理耗时（B_recv → B_send） |
| **send_cost** | A 端 `ub_comm_queue_send` 调用耗时 |
| **recv_cb_cost** | A 端接收回调处理耗时 |

### B 端输出

```
[B] pong_sent=1000 avg_b_process(ns)=180.50

===== B Summary (msg_size=64) =====
ping_recv=1000 pong_sent=1000
avg_b_process(ns)=180.50
```

## 注意事项

1. **启动顺序**：先启动 B，再启动 A。A 启动后会等待 3 秒等待 B 就绪。
2. **消息大小**：仅支持 64、4096、8192 三种，需大于等于 `HEADER_SIZE(16B) + PingPongMsg(48B) = 64B`。
3. **CPU 绑定**：可通过 `--cpu-id` 调整。
4. **跨节点部署**：两个节点必须能访问同名共享内存区域。
