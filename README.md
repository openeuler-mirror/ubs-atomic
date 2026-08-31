# ubs-atomic

Ubs-atomic supports distributed atomic services such as distributed locks and queues based on shared memory.
- [项目简介](##项目简介)
- [功能特性](##功能特性)
- [适用场景](##适用场景)
- [模块架构](##模块架构)
- [目录结构](##目录结构)
- [环境与依赖](##环境与依赖)
- [快速开始](##快速开始)
- [使用示例](##使用示例)
- [API说明](##API说明)
- [样例说明](##样例说明)
- [测试说明](##测试说明)
- [注意事项](##注意事项)
- [常见问题](##常见问题)
- [License](##License)
## 项目简介
基于共享内存的轻量级分布式基础组件，当前仓库主要提供以下几种能力：

- 分布式读写锁：支持 `S` / `SX` / `X` 三种锁模式，适合跨节点并发控制。
- 分布式互斥锁：轻量级排他锁，适合简单的互斥访问场景。
- 分布式自旋锁：基于 CAS 的自旋锁，适合极短临界区场景。
- 分布式通信队列：基于共享内存 Ring Buffer 的点对点消息通道，支持同步/异步回调。
- 分布式事务资源：面向共享内存 `uint64_t` 资源的原子初始化、读写与自增能力。

项目代码以 C ABI 为对外接口，核心实现使用 C++17。调用方负责共享内存的创建、映射和生命周期管理；组件本身负责在这块共享内存上组织锁、队列和原子资源。

## 功能特性

- 对外头文件清晰，适合 C / C++ 混合项目直接接入。
- 分布式读写锁支持 `S`、`SX`、`X` 三种模式，并提供超时、延迟释放、可选递归和故障恢复接口。
- 分布式互斥锁提供轻量级排他访问能力，适合简单的互斥场景。
- 分布式自旋锁基于 CAS 实现，适合极短临界区的高性能场景。
- 通信队列支持多节点、多优先级 Ring、同步/异步回调、就绪状态检查，并提供发送侧流控、拥塞状态查询和边缘触发维测日志。
- 事务资源直接封装共享内存上的 `std::atomic<uint64_t>`，适合计数器、状态位、序号分配等场景。
- 仓库内附带 `sample_code/` 和 `test/`，便于快速验证接口行为。

## 适用场景

- 多进程 / 多节点场景下的共享资源互斥与读写协调。
- 基于共享内存的低延迟消息传递与跨节点通知。
- 分布式事务中的共享计数器、位点分配、状态同步。
- 依赖灵衢共享内存（`ubsmem`）进行实际部署的集群场景。

## 模块架构

| 模块 | 对外头文件 | 核心能力 | 典型场景 |
| --- | --- | --- | --- |
| 分布式读写锁 | `include/ub_dist_lock.h` | 共享内存读写锁，支持 `S/SX/X`、超时、递归、故障恢复 | 元数据锁、事务并发控制 |
| 分布式互斥锁 | `include/ub_dist_lock.h` | 轻量级排他锁，适合简单互斥访问 | 资源保护、简单临界区 |
| 分布式自旋锁 | `include/ub_dist_lock.h` | CAS 自旋锁，极低延迟 | 极短临界区、高频访问 |
| 通信队列 | `include/ub_dist_comm_queue.h` | 共享内存消息队列，支持多 Ring、同步/异步回调 | 节点间通知、事件投递、消息流控 |
| 事务资源 | `include/ub_dist_tx_res.h` | 基于共享内存的原子 `uint64_t` 资源访问 | 计数器、状态位、全局序号 |

### 内部关系

- `ub_dist_comm_queue` 是分布式消息基础设施。
- `ub_dist_lock` 内部会借助通信队列完成跨节点唤醒和释放通知。
- `ub_dist_tx_res` 不依赖通信队列，只要求调用方提供 8 字节对齐的共享内存地址。

### 安全假设和约束

UBS Atomic基于UB共享内存池实现，假设应用场景的安全威胁模型和UB共享内存池一致，比如共享内存池内的内存访问是可信的。

UBS Atomic 本身仅接收共享内存地址并对其中的数据结构进行操作，不负责共享内存池的创建、生命周期管理、访问控制。因此，UBS Atomic 的正确性和安全性依赖于调用方提供的UB共享内存池，调用方需至少保证：
1) 共享内存池可被所有参与节点一致访问；
2) 共享内存仅授权给必要的用户，不会被非授权实体提权读写；
3) 共享内存状态对所有节点保持一致可见；
4) 底层原子操作（CAS 等）满足全局线性化语义。

UBS Atomic安全假设和DSM DB、CXL Shared Lock等业界类似，依赖共享内存安全性，共同的依赖有：
1）通过共享状态和原子操作实现资源仲裁；
2）系统正确性依赖于共享内存上共享状态的可信性；
3）需要保证共享状态的一致可见性以及原子操作的正确性。

### 安全威胁分析

根据攻击路径模型和韧性控制点梳理，针对高风险架构元素威胁分析和消减措施如下：
共享内存的威胁风险和消减措施如下：

| 架构元素 | 威胁分析说明 | 消减措施 | 备注 |
| --- | --- | --- | --- |
| shared memory | 仿冒：恶意进程伪装成合法的通信方，通过猜测或窃听获知共享内存的名称和结构，从而链接到管道，冒充发送方或接收方。 | 共享内存创建时指定权限为 `600`，只有属主进程能够访问，避免低权限或其他用户的进程访问共享内存; <br>共享内存名称 `Name` 随机生成，避免被攻击者猜测到，避免被提前创建或恶意接入共享内存。<br>共享内存名称 `Name` 通过可信通信如 TCP/TLS 通信交换，避免被攻击者获取到，避免被提前创建或恶意接入共享内存。 | 通过权限访问控制和通信协议身份认证保证合法接入共享内存。<br>由上层应用负责共享内存的创建 |
|  | 篡改：恶意进程任意篡改共享内存的数据，接收方无法区分数据是来自真实的发送方还是被中间人篡改过。 | 共享内存发送方和接收方使用密钥对通信消息进行加密和解密，防止数据被篡改。 | 由上层应用负责消息完整性保护。<br>UBS Atomic 只提供消息的发送和接收能力。 |
|  | 信息泄露：恶意进程通过访问共享内存读取包含敏感信息的通信内容。 |  | 由上层应用负责消息加密和解密。 |
|  | 拒绝服务-资源耗尽：恶意进程可以疯狂写入数据，占满共享内存空间，导致合法通信无法进行。 | UBS Atomic 提供消息流控机制，支持配置消息队列拥塞阈值，当目标节点队列环消息堆积超过阈值时返回消息拥塞；当目标节点环满时返回发送消息失败。 | UBS Atomic 本身无法完全防止 DoS 攻击。 |
|  | 拒绝服务-破坏同步原语：攻击者可以破坏用于保护共享内存的锁或信号量，使通信双方陷入死锁或活锁。 | UBS Atomic 基于 UB 共享内存实现无锁多生产者单消费者消息队列，增强系统容错与高可用能力，支持消息队列故障恢复机制，避免死锁。 | 通过上层业务的权限访问控制和身份认证保证合法接入。 |
|  | 拒绝服务-销毁资源：恶意进程可以故意调用解除共享内存映射或销毁共享内存等操作，导致共享内存被提前分离或销毁。 | 通过共享内存的权限控制和身份认证保证只有合法用户能直接操作共享内存。 |

## 目录结构

```text
ubs-atomic/
├── 3rdparty/                  # 三方依赖与子模块
├── build/                     # CMake 辅助脚本
├── doc/                       # 补充文档
├── include/                   # 对外公开头文件
├── sample_code/               # 示例代码（锁 / 队列 / 事务资源 / 共享内存）
├── src/                       # 核心实现
│   ├── ub_lock/               # 分布式锁实现
│   ├── ub_comm_queue/         # 分布式通信队列实现
│   └── ub_dist_tx_res/        # 分布式事务资源实现
├── test/                      # 单元测试与覆盖率脚本
├── CMakeLists.txt
└── build.sh
```

## 环境与依赖

### 编译环境

- Linux 环境。
- Bash、Git、CMake >= 3.22。
- GCC / G++，C 语言标准至少 C11，C++ 标准至少 C++17。

### 平台要求

仓库顶层 `CMakeLists.txt` 默认启用了：

```cmake
-march=armv8-a+lse
```

这意味着默认面向支持 LSE 原子指令的 ARMv8 平台。如果你的目标环境不是 AArch64/LSE，请按实际工具链调整编译选项后再构建。

### 依赖库

基础构建 / 运行依赖：

- `pthread`
- `librt`
- `libboundscheck.so`（代码和测试都显式依赖）

> **非 openEuler 系统（如 Ubuntu）获取 libboundscheck**：
> 1. 从 https://gitcode.com/openeuler/libboundscheck 克隆源码
> 2. 编译安装：`cd libboundscheck && make && sudo make install`
> 3. Ubuntu 默认不搜索 `/usr/lib64`，需创建符号链接：
>    `sudo ln -sf /usr/lib64/libboundscheck.so /usr/lib/aarch64-linux-gnu/libboundscheck.so && sudo ldconfig`
> 4. 或设置环境变量：`export LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH`

样例与真实共享内存部署额外依赖：

- 灵衢共享内存 SDK：`ubs_mem.h`、`ubs_mem_def.h`
- 对应库：`-lubsm_sdk`
- 可用的 `ubsmem` 服务 / daemon

> **ubsmem SDK 获取方式**：
> - 安装 RPM 包：`ubs-mem`、`ubs-mem-shmem`（从灵衢组件包获取）
> - 安装后头文件位于 `/usr/include/ubs_mem.h`、`/usr/include/ubs_mem_def.h`
> - 库文件位于 `/usr/lib64/libubsm_sdk.so`
> - 启动服务：`systemctl start ubsmem`
> - 样例编译时需指定：`-I/usr/include -L/usr/lib64 -lubsm_sdk`

测试与覆盖率相关依赖：

- `googletest`（手动克隆）、`mockcpp`（手动克隆）
- `lcov`
- `genhtml`
- `dos2unix`

## 快速开始

### 1. 获取代码

```bash
git clone <your-repo-url>
cd ubs-atomic
# googletest 和 mockcpp 需手动克隆到 test/3rdparty/
git clone https://gitcode.com/mirrors/googletest.git test/3rdparty/googletest
git clone https://gitcode.com/mirrors_sinojelly/mockcpp.git test/3rdparty/mockcpp
cd test/3rdparty/mockcpp && git checkout v2.7 && cd ../../..
```

### 2. 编译动态库

```bash
dos2unix build.sh
bash build.sh
```

常用构建方式：

```bash
# Debug 版本
bash build.sh -D

# 指定 Release / RelWithDebInfo / MinSizeRel
bash build.sh -T Release
bash build.sh -T RelWithDebInfo

# 指定并行度
bash build.sh -j 16
```

构建产物默认位于：

- `dist/release/lib/libubs-atomic.so`
- `dist/debug/lib/libubs-atomic.so`

RPM 打包产物通过 `bash build.sh package` 生成，文件名格式为 `ubs-atomic-{version}-{release}.{ARCH}.rpm`，默认从 `ubs-atomic-1.0.0-1.aarch64.rpm` 开始；下个 release 可使用 `bash build.sh package -V 1.0.0-2`。


### 3. 在你的工程中链接

典型链接方式如下：

```bash
g++ -std=c++17 app.cpp \
  -I./include \
  -L./dist/release/lib \
  -lubs-atomic \
  -lpthread -lrt
  -march=armv8-a+lse
```

如果你的系统没有把 `libboundscheck.so` 配置到默认库搜索路径，请一并补充 `-L` 或 `rpath`。

## 使用示例
### 1. 分布式事务资源

适用于共享计数器、全局 ID、状态位等简单原子资源。详细步骤可参考[sample_code](sample_code/ub_dist_tx_res/ub_dist_tx_res_func_test.c)。

```cpp
#include <cstdint>
#include <cstdio>
#include "ub_dist_tx_res.h"

alignas(8) uint64_t shm_counter = 0;

int main()
{
    uint64_t old_val = 0;
    uint64_t cur_val = 0;

    ub_dist_tx_res_init(&shm_counter);
    ub_dist_tx_res_set(&shm_counter, 100);
    ub_dist_tx_res_fetch_add(&shm_counter, 1, &old_val); // old_val = 100
    ub_dist_tx_res_get(&shm_counter, &cur_val);          // cur_val = 101

    std::printf("old=%llu current=%llu\n",
                (unsigned long long)old_val,
                (unsigned long long)cur_val);
    return 0;
}
```

### 2. 分布式读写锁

适用于跨节点读写互斥。锁对象本身必须放在一块所有参与节点都能映射到的共享内存中，并至少预留 `UB_RW_LOCK_SIZE` 字节。分布式读写锁的跨节点唤醒、延迟释放通知依赖 `ub_dist_comm_queue`，因此调用方必须先初始化通信队列，再创建和使用锁。详细步骤可参考[sample_code](sample_code/ub_lock/README.md)。

```cpp
#include <array>
#include <cstddef>
#include "ub_dist_comm_queue.h"
#include "ub_dist_lock.h"

alignas(64) std::array<std::byte, UB_RW_LOCK_SIZE> lock_mem{};
auto *lock = reinterpret_cast<ub_rw_lock_t *>(lock_mem.data());

int main()
{
    ub_location_t self{.tid = 1001, .node_id = 0};
    ub_lock_config_t config{.lease_time = 60000, .heartbeat_timeout = 500};
    ub_lock_policy_t policy{.timeout_ts = 1000, .allow_delay_release = false, .recursive = false};

    // 前置条件：所有参与节点必须先完成通信队列初始化，并保持 handle 存活。
    // init_region、ring_map、comm_conf 需要由调用方按集群节点和共享内存布局提前构造。
    ub_shm_comm_t comm_handle = nullptr;
    ub_shm_area_t init_region{};
    ub_ring_region_map_t ring_map{};
    ub_comm_conf_t comm_conf{};
    comm_conf.current_node_id = self.node_id;
    if (ub_comm_queue_init(&comm_handle, &init_region, &ring_map, &comm_conf) != 0) {
        return 1;
    }

    ub_rw_lock_create(lock, &config, &self);

    if (ub_rw_lock_x_lock(lock, &policy, &self) == UB_LOCK_SUCCESS) {
        // critical section
        ub_rw_lock_x_unlock(lock, &policy, &self);
    }

    ub_rw_lock_free(lock, &self);
    ub_comm_queue_deinit(&comm_handle);
    return 0;
}
```

### 3. 分布式通信队列

适用于跨节点轻量消息通知。以下示例省略共享内存映射过程，只展示接口组织方式；完整可运行示例见 `sample_code/ub_comm_queue/`。详细步骤可参考[sample_code](sample_code/ub_comm_queue/README.md)。

```cpp
#include <cstdint>
#include <cstring>
#include "ub_dist_comm_queue.h"

static void on_msg(const message_t *msg, void *ctx)
{
    (void)ctx;
    // msg->body 仅应在回调执行期间访问
}

int main()
{
    ub_shm_comm_t handle = nullptr;

    // 这些地址需要由调用方通过 shm / ubsmem 等方式提前映射好
    void *init_ptr = nullptr;
    void *local_ring_ptr = nullptr;
    void *peer_ring_ptr = nullptr;
    size_t local_ring_size = 2 * 1024 * 1024;
    size_t peer_ring_size = 2 * 1024 * 1024;

    ub_ring_desc_t rings[] = {
        {.ring_capacity = 1024, .max_msg_size = 256, .priority = 1},
    };

    ub_comm_conf_t conf{};
    conf.cpu_id = -1;            // -1 表示不绑核
    conf.max_nodes = 2;
    conf.current_node_id = 0;
    conf.num_rings = 1;
    conf.ring_descs = rings;

    ub_shm_area_t init_region{.size = 1024, .ptr = init_ptr};

    ub_ring_region_info_t infos[] = {
        {.region = {.size = local_ring_size, .ptr = local_ring_ptr}, .node_id = 0},
        {.region = {.size = peer_ring_size,  .ptr = peer_ring_ptr},  .node_id = 1},
    };
    ub_ring_region_map_t map{.entries = infos, .count = 2};

    ub_comm_queue_init(&handle, &init_region, &map, &conf);
    ub_comm_queue_register_process_func(&handle, 100, UB_FUNC_SYNC, on_msg, nullptr);

    char body[] = "hello";
    message_t msg{};
    msg.header.src_thread_id = 1;
    msg.header.body_length = sizeof(body);
    msg.header.dest_node_id = 1;
    msg.header.src_node_id = 0;
    msg.header.msg_type = 100;
    msg.header.priority = 1;
    msg.body = body;

    ub_comm_queue_send(&handle, &msg);
    ub_comm_queue_deinit(&handle);
    return 0;
}
```

## API说明

### 分布式读写锁 `ub_dist_lock.h`

#### 核心数据结构

| 类型 | 字段 | 说明 |
| --- | --- | --- |
| `ub_location_t` | `tid`, `node_id` | 锁拥有者 / 等待者身份。`node_id + tid` 应在集群中稳定且可区分。 |
| `ub_lock_policy_t` | `timeout_ts` | 实现里按“相对当前时间的超时毫秒数”处理，不是绝对时间戳。 |
| `ub_lock_policy_t` | `allow_delay_release` | 允许延迟释放，用于降低通知和竞争开销。 |
| `ub_lock_policy_t` | `recursive` | 是否允许同线程递归获取 `X` / `SX` 本地锁。 |
| `ub_lock_config_t` | `lease_time` | 锁租约时间，默认使用示例值 `60000ms`。 |
| `ub_lock_config_t` | `heartbeat_timeout` | 心跳超时阈值，默认使用示例值 `500ms`。 |

#### 锁模式

| 枚举值 | 含义 | 典型用途 |
| --- | --- | --- |
| `UB_LOCK_S` | 共享读锁 | 只读访问 |
| `UB_LOCK_SX` | 共享排他锁 | 升级意图、半排他场景 |
| `UB_LOCK_X` | 独占写锁 | 写入或结构修改 |

#### 返回值

| 返回值 | 说明 |
| --- | --- |
| `UB_LOCK_SUCCESS` | 成功 |
| `UB_LOCK_TIMEOUT` | 等待超时 |
| `UB_LOCK_CONFLICT` | 锁冲突 |
| `UB_LOCK_ERROR` | 参数错误、状态错误或内部异常 |

#### 主要接口

| 接口 | 说明 | 使用场景 |
| --- | --- | --- |
| `ub_rw_lock_create` | 初始化共享内存锁对象 | 共享内存首次建锁 |
| `ub_rw_lock_free` | 释放当前节点关联资源 | 进程退出、节点下线 |
| `ub_rw_lock_s_lock` / `ub_rw_lock_s_unlock` | 获取 / 释放读锁 | 并发读 |
| `ub_rw_lock_x_lock` / `ub_rw_lock_x_unlock` | 获取 / 释放写锁 | 独占写 |
| `ub_rw_lock_sx_lock` / `ub_rw_lock_sx_unlock` | 获取 / 释放 SX 锁 | 升级型访问 |
| `ub_rw_lock_recover` | 按进程 ID 恢复异常持锁状态 | 进程崩溃恢复 |

#### 使用建议

- `ub_rw_lock_t` 是不透明类型，调用方只需保证共享内存大小至少为 `UB_RW_LOCK_SIZE`（640 字节）。
- 所有节点必须看到同一把锁对应的同一块共享内存地址。
- `lock`、`policy`、`location` 在加锁和解锁时必须匹配，尤其是 `location.tid` 不能漂移。
- `allow_delay_release=true` 会改变释放时机，适合追求吞吐的场景。
- `recursive=true` 只适用于同一线程重复持锁场景。
#### 参数约束与实现限制
- `location.node_id`、`process_id` 有效范围是 [0,16)。
- 锁会借助通信队列完成跨节点唤醒和释放通知；每个参与节点都必须先完成 `ub_comm_queue_init`，再调用 `ub_rw_lock_create`，并保证锁的生命周期处在通信队列实例生命周期内。
- 通信队列的 `init_region`、节点 Ring 映射、`current_node_id`/`max_nodes` 等配置必须跨节点一致；若队列未初始化或节点间映射不一致，跨节点等待者可能无法收到唤醒/释放消息，表现为加锁超时或失败。
- `ub_rw_lock_recover`接口内部无法判断传入的进程是否异常
- 进程故障发送后，集群服务能获取故障进程ID，集群服务从正常进程中选出一个来调用分布式锁提供`ub_rw_lock_recover`的接口进行故障恢复，恢复流程完成后，集群服务才可重启故障进程。

### 分布式互斥锁 `ub_dist_lock.h`

#### 主要接口

| 接口 | 说明 | 使用场景 |
| --- | --- | --- |
| `ub_mutex_lock_create` | 初始化共享内存互斥锁对象 | 共享内存首次建锁 |
| `ub_mutex_lock_free` | 释放互斥锁关联资源 | 进程退出、节点下线 |
| `ub_mutex_lock` | 获取互斥锁 | 排他访问 |
| `ub_mutex_unlock` | 释放互斥锁 | 退出临界区 |

#### 使用建议

- `ub_mutex_lock_t` 是不透明类型，调用方只需保证共享内存大小至少为 `UB_MUTEX_LOCK_SIZE`（384 字节）。
- 适合简单的排他访问场景，比读写锁更轻量。
- 超时时间参数为 0 时使用默认值 10000ms。

### 分布式自旋锁 `ub_dist_lock.h`

#### 主要接口

| 接口 | 说明 | 使用场景 |
| --- | --- | --- |
| `ub_spin_lock_init` | 初始化共享内存自旋锁对象 | 共享内存首次建锁 |
| `ub_spin_lock` | 获取自旋锁 | 极短临界区排他访问 |
| `ub_spin_unlock` | 释放自旋锁 | 退出临界区 |

#### 使用建议

- `ub_spin_lock_t` 是不透明类型，调用方只需保证共享内存大小至少为 `UB_SPIN_LOCK_SIZE`（64 字节）。
- 适合极短的临界区场景，避免长时间阻塞其他线程。
- 基于 CAS 实现，无操作系统调度开销，但高竞争下会占用 CPU 资源。
- 超时时间参数为 0 时使用默认值 10000ms。


### 分布式通信队列 `ub_dist_comm_queue.h`

#### 核心数据结构

| 类型 | 关键字段 | 说明 |
| --- | --- | --- |
| `ub_shm_area_t` | `size`, `ptr` | 一段共享内存区域描述 |
| `ub_ring_desc_t` | `ring_capacity`, `max_msg_size`, `priority` | 单个 Ring 的容量、消息大小上限、优先级 |
| `ub_comm_conf_t` | `cpu_id`, `max_nodes`, `current_node_id`, `num_rings` | 队列实例全局配置 |
| `message_header_t` | `src_thread_id`, `dest_node_id`, `msg_type`, `priority` | 消息头 |
| `message_t` | `header`, `body` | 完整消息 |
| `ub_comm_queue_status_t` | `used`, `total`, `free`, `state`, `congestion_threshold`, `max_depth` | Ring 状态与流控水位快照 |

#### 发送流控

发送接口按固定优先级判定：

1. Ring 已满：无法写入，返回 `UB_COMM_ERR_RING_FULL`。
2. Ring 已达到拥塞阈值：消息已写入，返回 `UB_COMM_SEND_CONGESTED`。
3. Ring 未拥塞：消息已写入，返回 `UB_COMM_OK`。

默认拥塞阈值为 Ring 容量的 `80%`。初始化接口不暴露阈值配置；如需调整，初始化后调用 `ub_comm_queue_set_congestion_threshold` 修改本节点本地 Ring。防抖采样为内部策略，不作为对外配置项。

流控日志默认开启且为边缘触发，只在首次进入拥塞和从拥塞恢复时打印。普通模式日志包含 `ring`、`ts_us`、`used`、`total`、`threshold`、`max_depth` 等字段；定义 `UB_COMM_QUEUE_ENABLE_DEBUG_STATS` 后会额外包含计数类维测字段。

#### 接口说明

| 接口 | 说明 | 备注 |
| --- | --- | --- |
| `ub_comm_queue_init` | 初始化通信实例 | 需要初始化区和所有节点 Ring 映射 |
| `ub_comm_queue_deinit` | 销毁通信实例 | 释放内部线程和资源 |
| `ub_comm_queue_send` | 发送消息 | 返回 `UB_COMM_OK` / `UB_COMM_SEND_CONGESTED` 表示写入成功，负数表示失败 |
| `ub_comm_queue_get_status` | 查询 Ring 状态 | 返回使用量、容量、空闲数、状态和阈值；debug 宏开启后包含统计计数 |
| `ub_comm_queue_set_congestion_threshold` | 修改本地 Ring 拥塞阈值 | 运行期调整本节点指定优先级 Ring 的水位 |
| `ub_comm_queue_check_ready` | 检查目标节点是否已就绪 | 发消息前可用于探活 / 同步 |
| `ub_comm_queue_register_process_func` | 注册消息处理回调 | 支持 `UB_FUNC_SYNC` 和 `UB_FUNC_ASYNC` |

#### 参数约束与实现限制

- `ring_capacity` 必须是 2 的幂。
- `priority` 建议从 `1` 开始，`0` 被内部锁消息 Ring 保留。
- `max_nodes` 当前实现上限为 `8`。
- 业务 Ring 的优先级级数实际上也受限于内部实现的最大优先级数 `8`。
- 所有节点对 `init_region`、`ring_descs`、Ring 布局的理解必须完全一致，否则会出现互操作错误。
- `msg_type = 0xFF` 、 `0xFE`和 `0xFD` 为内部保留类型，不应作为业务消息类型使用。
- `UB_COMM_SEND_CONGESTED` 是成功返回码，不应按发送失败处理；调用方可据此降低发送速率或触发上层反压。
- 状态查询基于本地原子快照，适合维测和流控策略参考，不应作为强一致队列长度使用。

#### 回调模型

- `UB_FUNC_SYNC`：在分发线程中直接执行回调，回调逻辑应尽量短小，避免阻塞整个收包分发。
- `UB_FUNC_ASYNC`：由内部线程池异步执行，适合较重的业务处理。
- 无论同步还是异步，都不要在回调之外长期持有 `msg->body` 裸指针。

### 分布式事务资源 `ub_dist_tx_res.h`

#### 接口说明

| 接口 | 说明 | 返回 |
| --- | --- | --- |
| `ub_dist_tx_res_init` | 将共享资源初始化为 `0` | `UB_RES_OK` / `UB_RES_ERROR` |
| `ub_dist_tx_res_set` | 写入指定值 | `UB_RES_OK` / `UB_RES_ERROR` |
| `ub_dist_tx_res_get` | 读取当前值 | `UB_RES_OK` / `UB_RES_ERROR` |
| `ub_dist_tx_res_fetch_add` | 原子加并返回旧值 | `UB_RES_OK` / `UB_RES_ERROR` |

#### 使用约束

- `handle` 必须指向可写共享内存中的 `uint64_t` 地址。
- `handle` 必须满足 8 字节对齐，否则接口直接返回失败。
- 适合简单共享原子资源，不负责更复杂的数据结构一致性。

## 样例说明

`sample_code/` 目录给出了更完整的接入方式：

- `sample_code/ub_lock/`：分布式锁功能与压测示例。
- `sample_code/ub_comm_queue/`：消息发送、接收、回调注册示例。
- `sample_code/ub_dist_tx_res/`：单节点、多线程、主从模式下的原子资源示例。
- `sample_code/share_mem/`：共享内存创建和使用示例。

消息队列的内部初始化流程、发送快路径、回调热注册、远端缓存和下线广播实现说明见 `src/ub_comm_queue/README.md`。

这些样例依赖 `ubsmem` SDK，适合在实际共享内存环境中联调。

> **获取 ubsmem**：安装 `ubs-mem` 和 `ubs-mem-shmem` RPM 包后，头文件 `ubs_mem.h` 位于 `/usr/include/`，库 `libubsm_sdk.so` 位于 `/usr/lib64/`。详见上文"依赖库"章节。

## 测试说明

### 1. 初始化子模块

```bash
# googletest 和 mockcpp 需手动克隆到 test/3rdparty/
git clone https://gitcode.com/mirrors/googletest.git test/3rdparty/googletest
git clone https://gitcode.com/mirrors_sinojelly/mockcpp.git test/3rdparty/mockcpp
cd test/3rdparty/mockcpp && git checkout v2.7 && cd ../../..
```

### 2. 执行单元测试

```bash
bash test/run_ut.sh
```

测试脚本会：

- 准备 `mockcpp` 补丁。
- 使用 `test/CMakeLists.txt` 构建 `ubs_atomic_ut`。
- 执行测试并生成覆盖率数据。
- 输出 `gcovr_report` / `coverage.info` 等结果。

### 3. 测试产物位置

常见输出目录：

- `test/build/ubs_atomic_ut`
- `test/build/gcovr_report/`
- `test/build/coverage.info`

## 注意事项

### 1. 共享内存由调用方负责

无论是锁、消息队列还是事务资源，组件本身都不创建业务共享内存对象，只在调用方传入的共享内存上组织内部结构。共享内存的创建、权限、命名和清理由接入方保证。

### 2. 对齐问题不要忽略

- `ub_dist_tx_res_*` 直接要求 `uint64_t` 地址 8 字节对齐。
- `ub_rw_lock_t` 建议放在显式对齐的共享内存首地址上，并预留完整 `UB_RW_LOCK_SIZE` 空间。
- 如果你自己做共享内存布局，务必明确偏移、对齐和总大小，避免裸指针越界。

### 3. 并发访问要保证“身份稳定”

分布式锁依赖 `ub_location_t` 标识持锁者。`node_id` 和 `tid` 在加锁、解锁、恢复过程中必须稳定一致，否则会出现“当前线程无法正确释放自己持有的锁”的问题。

### 4. 延迟释放是优化，不是默认安全选项

`allow_delay_release` 适合减少频繁跨节点通知，但它会改变锁真正完成释放的时机。如果你的业务要求严格的实时可见性，建议保持关闭。

### 5. 通信队列配置必须跨节点一致

通信双方必须共享同一套：

- 节点数量
- 当前节点编号
- Ring 数量与优先级
- Ring 容量
- 单消息最大大小
- 共享内存布局

这类配置只要有一项不一致，就可能出现初始化失败、无法收包或数据错读。

### 6. 回调里不要做重阻塞操作

同步回调跑在分发线程中，执行过慢会拖慢整个消息分发；异步回调虽然有线程池，但仍应避免无限阻塞、长时间持锁、持久保存 `msg->body` 指针等高风险写法。

## 常见问题

### `libboundscheck.so` 找不到

现有 CMake（`src/CMakeLists.txt`）硬编码链接 `/usr/lib64/libboundscheck.so`。openEuler 系统通过 `dnf install libboundscheck` 安装即可。非 openEuler 系统需从源码构建，并在标准库目录创建符号链接，详见上文"依赖库"章节。

### `build.sh` 运行前报脚本格式问题

先执行：

```bash
dos2unix build.sh
```

### `bash build.sh test` 失败

当前仓库里实际存在的是 `test/run_ut.sh`。如果你的分支仍然走到 `test/run_ut.sh`，请直接执行：

```bash
bash test/run_ut.sh
```

### 为什么样例代码依赖 `ubsmem`，而核心库没有直接链接它？

因为核心库只处理“已经映射好的共享内存地址”，不负责共享内存对象的创建与映射；样例为了展示真实部署方式，才接入了 `ubsmem`。

## License

仓库根目录包含 `LICENSE` 文件，使用前请结合实际项目要求确认许可范围。
