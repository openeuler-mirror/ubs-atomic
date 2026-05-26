
---

# UB Distributed COMM_QUEUE Test README

本目录包含几份用于测试 `ub_comm_queue` 分布式通信队列的 C++ 测试程序：

* **ub_dist_comm_queue_test：功能测试**

  * 特点：CMD用户交互测试，模拟消息发送接收

* **integration_queue_test：集成测试**

  * 特点：同一个程序支持 `A/B` 两种角色，覆盖：
  * `A` 发 `A` 收，`SYNC/ASYNC` 两种回调
  * `A` 发 `B` 收，`SYNC/ASYNC` 两种回调
  * 多线程混发，不同消息类型和不同优先级混合发送，并校验优先级隔离

* **flow_control_queue_test：流控验证**

  * 特点：同一个程序支持 `A/B` 两种角色，通过脚本拉起双端，覆盖：
  * 流控 API 参数校验
  * 阈值配置与环状态查询
  * `UB_COMM_OK` / `UB_COMM_SEND_CONGESTED` / `UB_COMM_ERR_RING_FULL` 返回码
  * 拥塞恢复、`max_depth` 历史峰值保持
  * 状态查询线程并发采样下的发送性能对比

* **flow_control_status_smoke：流控状态冒烟测试**

  * 特点：同一个程序支持 `A/B` 两种角色，覆盖：
  * 单节点初始化、状态查询、阈值设置、去初始化后查询
  * 双节点初始化、远端状态查询、远端阈值设置、远端下线后查询
  * 多 Ring 阈值隔离查询
  * 7 Ring 阈值矩阵：`0% / 65% / 80% / 100% / 默认`

> 测试程序包含的功能：
> 1. 映射共享内存
> 2. 初始化通信队列（跨节点消息通道）
> 3. 解析运行参数启动线程发送消息，接收消息
> 4. 结束时释放锁资源并卸载 shm

---

## 1. 环境依赖

需要依赖以下组件（实际路径按环境调整）：

* `ubs_mem`（ubsmem SDK）

  * 头文件：`ubs_mem_def.h`, `ubs_mem.h`
  * 库：`-lubsm_sdk`
* 分布式通信队列：

  * 头文件：`ub_dist_comm_queue.h`
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
g++ -O2 -g -std=c++17 -o ub_dist_comm_queue_send_test ub_dist_comm_queue_send_test.cpp \
  -I/usr/local/include \
  -I/usr/local/ubs_mem/include \
  -L/usr/local/ubs_mem/lib \
  -lubsm_sdk \
  -L/usr/lib64 \
  -lubturbo_tdsql \
  -lpthread \
  -Wl,-rpath,/usr/local/ubs_mem/lib
```
```bash
g++ -O2 -g -std=c++17 -o ub_dist_comm_queue_recv_test ub_dist_comm_queue_recv_test.cpp \
  -I/usr/local/include \
  -I/usr/local/ubs_mem/include \
  -L/usr/local/ubs_mem/lib \
  -lubsm_sdk \
  -L/usr/lib64 \
  -lubturbo_tdsql \
  -lpthread \
  -Wl,-rpath,/usr/local/ubs_mem/lib
```
```bash
g++ -O2 -g -std=c++17 -o integration_queue_test integration_queue_test.cpp \
  -I/usr/local/include \
  -I/usr/local/ubs_mem/include \
  -L/usr/local/ubs_mem/lib \
  -lubsm_sdk \
  -L/usr/lib64 \
  -lubturbo_tdsql \
  -lpthread \
  -Wl,-rpath,/usr/local/ubs_mem/lib
```

流控验证程序也可以按同样方式编译：

```bash
g++ -O2 -g -std=c++17 -o flow_control_queue_test flow_control_queue_test.cpp \
  -I/usr/local/include \
  -I/usr/local/ubs_mem/include \
  -L/usr/local/ubs_mem/lib \
  -lubsm_sdk \
  -L/usr/lib64 \
  -lubturbo_tdsql \
  -lpthread \
  -Wl,-rpath,/usr/local/ubs_mem/lib
```

流控状态冒烟程序：

```bash
g++ -O2 -g -std=c++17 -o flow_control_status_smoke flow_control_status_smoke.cpp \
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

* ubsmem daemon/服务已启动
* 映射的共享内存名称/路径可被 `ubsmem_shmem_map` 正确识别
* 创建`shm_sender`、`shm_receiver` ubsm共享内存用于消息队列（名字可在代码中修改）
---

## 5. 运行方式

程序分为接收端和发送端，需要两个节点分别运行，且两个程序的相同的参数需要保持一致。
* ub_dist_comm_queue_send_test
* ub_dist_comm_queue_recv_test
* integration_queue_test

### 5.1 启动命令格式

```bash
./ub_dist_comm_queue_send_test <num_threads> <num_messages> <num_rings_flag (0 for 1 ring, 1 for 3 rings)> <callback_type_flag (0 for SYNC, 1 for ASYNC)> <self_send_flag (0 to enable self-send, 1 to disable)> <message_body_length (1-1024)> <sender_shm_name> <receiver_shm_name>

```

```bash
./ub_dist_comm_queue_recv_test <num_threads> <num_messages> <num_rings_flag (0 for 1 ring, 1 for 3 rings)> <callback_type_flag (0 for SYNC, 1 for ASYNC)> <sender_shm_name> <receiver_shm_name>
```

```bash
./integration_queue_test --role B -s test_send -r test_recv -n 64 -t 4 -m 64
./integration_queue_test --role A -s test_send -r test_recv -n 64 -t 4 -m 64
```

流控验证推荐使用脚本一键执行：

```bash
bash sample_code/ub_comm_queue/run_flow_control_validation.sh
```

脚本默认使用已经拷贝到当前环境的 `flow_control_queue_test` 二进制，创建两段 ubsm 共享内存，后台启动 `B` 端，再以前台 `A` 端执行验证用例。常用环境变量：

| 环境变量 | 含义 | 默认值 |
|----------|------|--------|
| `BIN` | 已从编译机拷贝过来的 `flow_control_queue_test` 路径 | `sample_code/ub_comm_queue/flow_control_queue_test` |
| `BUILD_LOCAL` | 是否在当前环境本地编译，`1` 表示开启 | `0` |
| `CASE_NAME` | 运行用例：`all` / `param` / `status` / `flow` / `recover` / `hotupdate` / `perf` | `all` |
| `CAPACITY` | 验证 Ring 容量，必须为 2 的幂 | `64` |
| `THRESHOLD` | 流控阈值百分比 | `50` |
| `PERF_MESSAGES` | 性能对比发送消息数 | `20000` |
| `SENDER_HOST` | 发送端共享内存导出节点 hostname | `slot1` |
| `RECEIVER_HOST` | 接收端共享内存导出节点 hostname | `slot2` |
| `RUN_MATRIX` | 是否执行容量/消息数矩阵验证，`1` 表示开启 | `0` |
| `CAPACITY_MATRIX` | 矩阵模式下的 Ring 容量列表 | `64 256 2048` |
| `PERF_MESSAGES_MATRIX` | 矩阵模式下每组性能消息数列表 | `4096 8192 4096` |
| `COMM_LIB_DIR` | `libubturbo_tdsql` 所在目录 | `/usr/lib64` |
| `UBSM_INCLUDE` | ubsmem 头文件目录 | `/usr/local/ubs_mem/include` |
| `UBSM_LIB_DIR` | `libubsm_sdk` 所在目录 | `/usr/local/ubs_mem/lib` |
| `SHM_CREATOR` | `node_ubsm_shm_creator` 路径，调用格式为 `create/delete hostname shm_name` | `./node_ubsm_shm_creator` |

示例：

```bash
CASE_NAME=flow CAPACITY=128 THRESHOLD=60 bash sample_code/ub_comm_queue/run_flow_control_validation.sh
```

如果二进制不在默认位置：

```bash
BIN=/path/to/flow_control_queue_test SHM_CREATOR=./node_ubsm_shm_creator bash sample_code/ub_comm_queue/run_flow_control_validation.sh
```

覆盖不同环大小、不同消息数量以及流控阈值热更新缓存刷新间隔的矩阵验证：

```bash
RUN_MATRIX=1 bash sample_code/ub_comm_queue/run_flow_control_validation.sh
```

其中 `hotupdate` 用例建议使用 `CAPACITY >= 2048`：它会先让发送侧使用缓存阈值，再把远端阈值热更新到 `1%`，随后发送超过 `FLOW_CONFIG_REFRESH_INTERVAL` 的消息，校验阈值版本同步在有界窗口内生效。

如果需要验证真实两节点路径，也就是 `slot1` 上运行 `A`、`slot2` 上运行 `B`，使用两节点脚本：

```bash
sample_code/ub_comm_queue/run_flow_control_validation_2node.sh
```

该脚本不会自动 ssh 到另一台机器，需要把脚本和二进制分别放到两个节点。推荐流程：

```bash
# 1. 在编译机编译二进制
MODE=build OUT=/tmp/flow_control_queue_test bash sample_code/ub_comm_queue/run_flow_control_validation_2node.sh

# 2. 拷贝二进制和脚本到两个节点
scp /tmp/flow_control_queue_test slot1:/path/to/flow_control_queue_test
scp /tmp/flow_control_queue_test slot2:/path/to/flow_control_queue_test
scp sample_code/ub_comm_queue/run_flow_control_validation_2node.sh slot1:/path/to/
scp sample_code/ub_comm_queue/run_flow_control_validation_2node.sh slot2:/path/to/

# 3. 在任意可执行 node_ubsm_shm_creator 的节点创建两段共享内存
TEST_ID=t001 MODE=create SHM_CREATOR=./node_ubsm_shm_creator bash run_flow_control_validation_2node.sh

# 4. 在 slot2 启动 B 端，保持运行
TEST_ID=t001 MODE=run-b BIN=/path/to/flow_control_queue_test bash run_flow_control_validation_2node.sh

# 5. 在 slot1 启动 A 端执行用例
TEST_ID=t001 MODE=run-a BIN=/path/to/flow_control_queue_test CASE_NAME=all bash run_flow_control_validation_2node.sh

# 6. 清理共享内存
TEST_ID=t001 MODE=cleanup SHM_CREATOR=./node_ubsm_shm_creator bash run_flow_control_validation_2node.sh
```

两节点脚本默认共享内存导出节点为 `SENDER_HOST=slot1`、`RECEIVER_HOST=slot2`，创建/删除命令格式为：

```bash
./node_ubsm_shm_creator create slot1 flow_sender_t001
./node_ubsm_shm_creator delete slot1 flow_sender_t001
```

验证阈值热更新缓存刷新间隔时，使用大环：

```bash
TEST_ID=t002 MODE=create CAPACITY=2048 bash run_flow_control_validation_2node.sh
TEST_ID=t002 MODE=run-b CAPACITY=2048 BIN=/path/to/flow_control_queue_test bash run_flow_control_validation_2node.sh
TEST_ID=t002 MODE=run-a CAPACITY=2048 CASE_NAME=hotupdate BIN=/path/to/flow_control_queue_test bash run_flow_control_validation_2node.sh
TEST_ID=t002 MODE=cleanup bash run_flow_control_validation_2node.sh
```

流控状态冒烟测试同样需要在真实两节点上执行。case1 只需要 A 节点，case2~case4 需要先在 B 节点启动 `--role B`，再在 A 节点启动 `--role A`：

```bash
# case1: 仅 slot1
./flow_control_status_smoke --role A --case 1 -s flow_sender_smoke1 -r flow_receiver_smoke1

# case2: slot2
./flow_control_status_smoke --role B --case 2 -s flow_sender_smoke2 -r flow_receiver_smoke2
# case2: slot1
./flow_control_status_smoke --role A --case 2 -s flow_sender_smoke2 -r flow_receiver_smoke2

# case3: slot2
./flow_control_status_smoke --role B --case 3 -s flow_sender_smoke3 -r flow_receiver_smoke3
# case3: slot1
./flow_control_status_smoke --role A --case 3 -s flow_sender_smoke3 -r flow_receiver_smoke3

# case4: slot2
./flow_control_status_smoke --role B --case 4 -s flow_sender_smoke4 -r flow_receiver_smoke4
# case4: slot1
./flow_control_status_smoke --role A --case 4 -s flow_sender_smoke4 -r flow_receiver_smoke4
```

参数解释见下文。

---

## 6. 参数说明

| 参数                 | 含义                                   | 示例                 |
|---------------------|----------------------------------------|---------------------|
| num_threads         | 并发线程数量                           | 1 / 128 / 512         |
| num_messages        | 每个线程每个环发送的消息数量           | 1 / 10                  |
| num_rings_flag      | 每个实例的环数量                       | 1 / 3                 |
| callback_type_flag  | 注册的回调函数类型                     | SYNC / ASYNC        |
| self_send_flag      | 是否开启向本节点发送（0=开启/1=关闭）  | 0 / 1               |
| message_body_length | 发送的消息体长度（字节）               | 64 / 1024           |
| sender_shm_name     | 发送端共享内存名称                     | test_sender         |
| receiver_shm_name   | 接收端共享内存名称                     | test_receiver       |

---

## 7. 典型运行示例

### 功能测试程序

**终端 1（发送端）：**

```bash
./ub_dist_comm_queue_send_test 1 1 0 0 1 64 test_send test_recv
```

**终端 2（接收端）：**

```bash
./ub_dist_comm_queue_recv_test 1 1 0 0 test_send test_recv
```

## 8. 输出说明

**终端 1（发送端）：**
```text
向对方节点（NodeB）发送完成
跳过向本节点发送，self-send已禁用
发送端程序正常退出
```
**终端 2（接收端）：**
```text
将在接收到 1 条消息后自动退出
已接收到预期的消息数量(1)，正在退出
接收端程序正常退出
```
---

## 9. 发送侧流控说明

通信队列在每个 Ring 上维护轻量级流控状态。生产者发送时会先判断是否已满，再判断是否拥塞：

| 返回值 | 含义 | 调用方建议 |
|--------|------|------------|
| `UB_COMM_OK` | 消息写入成功，Ring 未拥塞 | 正常继续发送 |
| `UB_COMM_SEND_CONGESTED` | 消息写入成功，但 Ring 已达到拥塞阈值 | 发送端可降速、批量、让出 CPU 或触发上层反压 |
| `UB_COMM_ERR_RING_FULL` | Ring 已满，消息未写入 | 按失败处理，可重试或丢弃 |
| 其他负数 | 参数错误、远端未就绪、Ring 不存在等错误 | 按具体错误码处理 |

默认拥塞阈值为 Ring 容量的 `80%`。初始化接口不暴露阈值配置：

```cpp
ub_ring_desc_t ring_desc{};
ring_desc.ring_capacity = 1024;
ring_desc.max_msg_size = 1024;
ring_desc.priority = 1;
```

如需调整，在运行期修改本节点某个本地 Ring：

```cpp
int ret = ub_comm_queue_set_congestion_threshold(&handle, 1, 90);
```

查询状态示例：

```cpp
ub_comm_queue_status_t status{};
int ret = ub_comm_queue_get_status(&handle, 1, 1, &status);
if (ret == UB_COMM_OK) {
    printf("state=%d used=%llu total=%llu free=%llu threshold=%llu max_depth=%llu\n",
           status.state,
           (unsigned long long)status.used,
           (unsigned long long)status.total,
           (unsigned long long)status.free,
           (unsigned long long)status.congestion_threshold,
           (unsigned long long)status.max_depth);
}
```

流控日志默认开启，只在状态边缘打印：首次进入拥塞、从拥塞恢复。普通模式日志字段包含 Ring 地址、微秒时间戳、使用量、总容量、阈值和历史最大深度。定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 后，状态结构和日志会额外包含环满失败次数、CAS 失败次数、拥塞进入/退出时间戳。防抖采样由内部策略闭环处理，不需要接入方配置。
用法变成：

# B 节点/接收端，长期服务
./reliability_eer_demo --role B --case service -s shm_node0_export -r shm_node1_export -l 1
A 节点按单个用例跑：

# 半写/发送端压力验证
./reliability_eer_demo --role A --case send -s shm_node0_export -r shm_node1_export -n 512 -t 4 -l 1

# 心跳：默认配置查询后验证
./reliability_eer_demo --role A --case rcv-default -s shm_node0_export -r shm_node1_export -l 1

# 心跳：设置 100/100/1500 后验证
./reliability_eer_demo --role A --case rcv-normal -s shm_node0_export -r shm_node1_export -l 1

# 心跳配置接口 001/002/003
./reliability_eer_demo --role A --case if -s shm_node0_export -r shm_node1_export -l 1

# 生产端被 kill/stop 后，恢复后单独探测
./reliability_eer_demo --role A --case probe -s shm_node0_export -r shm_node1_export -l 1
对于 rcv-default / rcv-normal，A 端会停在类似提示：

手动故障注入点：请现在暂停接收端 B。
在 B 节点执行：kill -STOP <B端pid>
确认操作完成后按 Enter 继续...
感知超时后还会再次停住，提示你恢复：

已感知 B 端不可用。请现在恢复接收端 B。
在 B 节点执行：kill -CONT <B端pid>
确认操作完成后按 Enter 继续...
另外我保留了同机自动注入：

./reliability_eer_demo --role A --case rcv-normal \
  --peer-pid <B端pid> --fault-after-ms 2000 --resume-after-ms 3000