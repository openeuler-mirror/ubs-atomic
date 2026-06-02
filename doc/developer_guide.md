# ubs-atomic 开发者指南

## 目录

1. [项目架构说明](#项目架构说明)
2. [环境搭建步骤](#环境搭建步骤)
3. [代码目录结构](#代码目录结构)
4. [构建流程](#构建流程)
5. [部署指南](#部署指南)
6. [API 接口文档](#api-接口文档)
7. [第三方系统接入说明](#第三方系统接入说明)
8. [贡献代码规范](#贡献代码规范)
9. [测试策略与方法](#测试策略与方法)
10. [版本控制流程](#版本控制流程)
11. [内部实现机制](#内部实现机制)

---

## 1. 项目架构说明

### 1.1 整体架构

ubs-atomic 采用分层架构设计，核心组件之间解耦且职责清晰：

```
┌─────────────────────────────────────────────────────────────┐
│                        应用层                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐              │
│  │ 业务代码  │  │ 业务代码  │  │    业务代码   │              │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘              │
└───────┼─────────────┼───────────────┼──────────────────────┘
        │             │               │
        ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                      API 层 (C ABI)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ub_dist_lock.h│  │ub_dist_comm_ │  │ub_dist_tx_res.h  │   │
│  │              │  │queue.h       │  │                  │   │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │
└─────────┼─────────────────┼────────────────────┼────────────┘
          │                 │                    │
          ▼                 ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                    核心实现层 (C++17)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  ub_lock     │  │  ub_comm_    │  │  ub_dist_tx_res  │   │
│  │  (锁实现)    │  │  queue       │  │  (事务资源实现)   │   │
│  │              │  │  (队列实现)  │  │                  │   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    基础设施层                               │
│              共享内存 (ubsmem / POSIX shm)                   │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 模块依赖关系

| 模块 | 依赖模块 | 被依赖模块 |
|-----|---------|-----------|
| ub_lock | ub_comm_queue | 应用层 |
| ub_comm_queue | 无 | ub_lock、应用层 |
| ub_dist_tx_res | 无 | 应用层 |

**关键依赖关系说明：**
- `ub_lock` 内部借助 `ub_comm_queue` 完成跨节点唤醒和释放通知
- `ub_dist_tx_res` 不依赖任何其他模块，只要求调用方提供 8 字节对齐的共享内存地址

### 1.3 核心设计原则

1. **零拷贝设计**: 所有数据结构直接在共享内存上操作，避免数据拷贝
2. **无锁编程**: 内部实现大量使用原子操作，减少锁竞争
3. **C ABI 接口**: 对外提供稳定的 C 接口，便于多语言绑定
4. **不管理共享内存**: 组件本身不创建/销毁共享内存对象，由调用方负责

---

## 2. 环境搭建步骤

### 2.1 编译环境要求

| 软件 | 版本要求 | 说明 |
|-----|---------|------|
| Linux | - | 目标操作系统 |
| Bash | - | 脚本执行环境 |
| Git | - | 版本控制 |
| CMake | >= 3.22 | 构建工具 |
| GCC/G++ | 支持 C11/C++17 | 编译器 |

### 2.2 平台要求

默认编译选项面向 **ARMv8-A + LSE** 平台：

```cmake
-march=armv8-a+lse
```

如需其他平台支持，请修改 CMakeLists.txt 中的编译选项。

### 2.3 依赖库安装

#### 2.3.1 基础依赖

```bash
# CentOS/RHEL
sudo yum install -y git cmake gcc gcc-c++ make

# Ubuntu/Debian
sudo apt-get install -y git cmake gcc g++ make
```

#### 2.3.2 运行时依赖

```bash
# 必须安装
sudo yum install -y glibc-devel libstdc++-devel

# 或
sudo apt-get install -y libc6-dev libstdc++6
```

#### 2.3.3 测试依赖

```bash
# lcov 用于覆盖率统计
sudo yum install -y lcov genhtml dos2unix

# 或
sudo apt-get install -y lcov dos2unix
```

#### 2.3.4 三方库

| 库 | 说明 | 获取方式 |
|-----|------|---------|
| libboundscheck.so | 边界检查库 | 系统安装 |
| googletest | 单元测试框架 | git submodule |
| mockcpp | 模拟测试框架 | git submodule |

### 2.4 开发环境配置

#### 2.4.1 获取代码

```bash
# 克隆仓库
git clone <repository-url>
cd ubs-atomic

# 初始化子模块
git submodule update --init --recursive
```

#### 2.4.2 环境变量配置

```bash
# 可选：设置 CMAKE_PREFIX_PATH
export CMAKE_PREFIX_PATH=/path/to/3rdparty:$CMAKE_PREFIX_PATH

# 可选：设置 LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
```

---

## 3. 代码目录结构

### 3.1 整体结构

```
ubs-atomic/                              # 项目根目录
├── 3rdparty/                            # 三方依赖与子模块
│   ├── googletest/                      # Google Test 框架
│   └── mockcpp/                         # Mock 测试框架
├── build/                               # CMake 辅助脚本
├── doc/                                 # 技术文档
│   ├── user_guide.md                    # 用户指南
│   └── developer_guide.md               # 开发者指南
├── include/                             # 对外公开头文件 (C ABI)
│   ├── ub_dist_lock.h                   # 分布式锁接口
│   ├── ub_dist_comm_queue.h             # 分布式通信队列接口
│   └── ub_dist_tx_res.h                 # 分布式事务资源接口
├── sample_code/                         # 示例代码
│   ├── ub_lock/                         # 锁使用示例
│   ├── ub_comm_queue/                   # 队列使用示例
│   ├── ub_dist_tx_res/                  # 事务资源使用示例
│   └── share_mem/                       # 共享内存示例
├── src/                                 # 核心实现 (C++17)
│   ├── CMakeLists.txt
│   ├── common/                          # 公共组件
│   ├── ub_lock/                         # 分布式锁实现
│   ├── ub_comm_queue/                   # 分布式通信队列实现
│   └── ub_dist_tx_res/                  # 分布式事务资源实现
├── test/                                # 单元测试与覆盖率
│   ├── CMakeLists.txt
│   ├── run_ut.sh                        # 测试执行脚本
│   └── ...                              # 测试用例
├── dist/                                # 构建输出目录
│   ├── release/                         # Release 版本产物
│   └── debug/                           # Debug 版本产物
├── CMakeLists.txt                       # 根 CMake 配置
├── build.sh                             # 构建脚本
└── README.md                            # 项目说明
```

### 3.2 目录职责说明

| 目录 | 职责 | 语言 |
|-----|------|------|
| `3rdparty/` | 存放第三方依赖库（git submodule） | - |
| `build/` | CMake 自定义模块和辅助脚本 | CMake |
| `doc/` | 技术文档（用户指南、开发者指南等） | Markdown |
| `include/` | 对外暴露的 C 接口头文件 | C |
| `sample_code/` | 可运行的示例代码 | C/C++ |
| `src/` | 核心业务逻辑实现 | C++17 |
| `test/` | 单元测试代码和测试脚本 | C++ |
| `dist/` | 构建产物输出目录 | - |

---

## 4. 构建流程

### 4.1 构建脚本使用

项目提供统一的 `build.sh` 脚本进行构建：

```bash
# 查看帮助
sh build.sh -h
```

#### 4.1.1 编译动态库（默认 Release 版本）

```bash
sh build.sh
```

#### 4.1.2 编译 Debug 版本

```bash
sh build.sh -D
```

#### 4.1.3 指定构建类型

```bash
# Release 版本（默认）
sh build.sh -T Release

# Debug 版本
sh build.sh -T Debug

# 带调试信息的 Release
sh build.sh -T RelWithDebInfo

# 最小体积版本
sh build.sh -T MinSizeRel
```

#### 4.1.4 指定并行编译数

```bash
sh build.sh -j 16
```

#### 4.1.5 清理构建

```bash
sh build.sh clean
```

#### 4.1.6 生成 RPM 包

```bash
# 生成默认版本
sh build.sh package

# 生成指定版本
sh build.sh package -V 1.0.0-2
```

### 4.2 构建产物

| 产物 | 路径 | 说明 |
|-----|------|------|
| 动态库 (Release) | `dist/release/lib/libubs-atomic.so` | 发布版本 |
| 动态库 (Debug) | `dist/debug/lib/libubs-atomic.so` | 调试版本 |
| 头文件 | `dist/release/include/` | 公共头文件 |
| RPM 包 | `dist/release/rpm/` | 安装包 |

### 4.3 手动 CMake 构建

```bash
# 创建构建目录
mkdir -p build && cd build

# 配置 (Release)
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-march=armv8-a+lse"

# 配置 (Debug)
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-march=armv8-a+lse"

# 编译
make -j$(nproc)

# 安装
make install DESTDIR=/path/to/install
```

### 4.4 链接示例

```bash
g++ -std=c++17 app.cpp \
  -I./include \
  -L./dist/release/lib \
  -lubs-atomic \
  -lpthread -lrt \
  -march=armv8-a+lse
```

---

## 5. 部署指南

### 5.1 开发环境部署

#### 5.1.1 依赖安装

```bash
# 安装基础依赖
sudo yum install -y libboundscheck

# 安装 UBSM SDK（如果需要运行样例）
```

#### 5.1.2 环境变量

```bash
export LD_LIBRARY_PATH=/path/to/ubs-atomic/dist/release/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/path/to/libboundscheck:$LD_LIBRARY_PATH
```

#### 5.1.3 验证

```bash
# 运行示例代码
./sample_code/ub_dist_tx_res/tx_res_sample
```

### 5.2 测试环境部署

#### 5.2.1 部署步骤

1. 安装测试依赖包
2. 部署 ubsmem 服务
3. 配置共享内存段
4. 部署测试用例
5. 配置监控告警

#### 5.2.2 配置要求

| 配置项 | 要求 |
|-------|------|
| 共享内存大小 | 根据测试场景调整，建议 >= 128MB |
| 锁租约时间 | 测试环境可缩短，如 30 秒 |
| 日志级别 | DEBUG |

### 5.3 生产环境部署

#### 5.3.1 部署步骤

1. **安装 RPM 包**
   ```bash
   rpm -ivh ubs-atomic-1.0.0-1.aarch64.rpm
   ```

2. **配置 ubsmem 服务**
   - 配置共享内存段
   - 设置合理的权限
   - 配置自动重启

3. **配置应用**
   - 配置正确的节点 ID
   - 设置合理的租约时间
   - 配置心跳检测

4. **启动应用**
   ```bash
   systemctl start your-application
   ```

#### 5.3.2 生产环境配置建议

| 配置项 | 建议值 | 说明 |
|-------|-------|------|
| lease_time | 60000ms (60秒) | 锁租约时间 |
| heartbeat_timeout | 500ms | 心跳超时 |
| allow_delay_release | false | 生产环境建议关闭 |
| timeout_ts | 1000-5000ms | 根据业务调整 |

#### 5.3.3 高可用配置

- **多节点部署**: 确保至少 3 个节点，避免单点故障
- **共享内存冗余**: 配置共享内存的备份机制
- **监控告警**: 配置锁状态、队列状态的监控和告警

---

## 6. API 接口文档

### 6.1 分布式读写锁 (ub_dist_lock.h)

#### 6.1.1 核心数据结构

##### ub_location_t

```c
typedef struct {
    uint64_t tid;       // 线程 ID，需全局唯一
    uint32_t node_id;   // 节点 ID，范围 [0, 16)
} ub_location_t;
```

##### ub_lock_config_t

```c
typedef struct {
    uint32_t lease_time;        // 租约时间（毫秒）
    uint32_t heartbeat_timeout; // 心跳超时（毫秒）
} ub_lock_config_t;
```

##### ub_lock_policy_t

```c
typedef struct {
    uint32_t timeout_ts;             // 加锁超时时间（毫秒）
    bool allow_delay_release;        // 是否允许延迟释放
    bool recursive;                  // 是否允许递归加锁
} ub_lock_policy_t;
```

#### 6.1.2 主要接口

| 接口 | 原型 | 说明 |
|-----|------|------|
| `ub_rw_lock_create` | `int ub_rw_lock_create(ub_rw_lock_t *lock, const ub_lock_config_t *config, const ub_location_t *self)` | 创建读写锁 |
| `ub_rw_lock_free` | `int ub_rw_lock_free(ub_rw_lock_t *lock, const ub_location_t *self)` | 销毁读写锁 |
| `ub_rw_lock_s_lock` | `int ub_rw_lock_s_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *self)` | 获取共享读锁 |
| `ub_rw_lock_s_unlock` | `int ub_rw_lock_s_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *self)` | 释放共享读锁 |
| `ub_rw_lock_x_lock` | `int ub_rw_lock_x_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *self)` | 获取独占写锁 |
| `ub_rw_lock_x_unlock` | `int ub_rw_lock_x_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *self)` | 释放独占写锁 |
| `ub_rw_lock_sx_lock` | `int ub_rw_lock_sx_lock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *self)` | 获取共享排他锁 |
| `ub_rw_lock_sx_unlock` | `int ub_rw_lock_sx_unlock(ub_rw_lock_t *lock, const ub_lock_policy_t *policy, const ub_location_t *self)` | 释放共享排他锁 |
| `ub_rw_lock_recover` | `int ub_rw_lock_recover(ub_rw_lock_t *lock, uint64_t process_id)` | 恢复异常持锁状态 |

#### 6.1.3 返回值定义

| 返回值 | 宏定义 | 说明 |
|-------|-------|------|
| 0 | `UB_LOCK_SUCCESS` | 操作成功 |
| -1 | `UB_LOCK_TIMEOUT` | 等待超时 |
| -2 | `UB_LOCK_CONFLICT` | 锁冲突 |
| -3 | `UB_LOCK_ERROR` | 参数错误或内部异常 |

### 6.2 分布式互斥锁

#### 6.2.1 主要接口

| 接口 | 原型 | 说明 |
|-----|------|------|
| `ub_mutex_lock_create` | `int ub_mutex_lock_create(ub_mutex_lock_t *mutex, const ub_lock_config_t *config, const ub_location_t *self)` | 创建互斥锁 |
| `ub_mutex_lock_free` | `int ub_mutex_lock_free(ub_mutex_lock_t *mutex, const ub_location_t *self)` | 销毁互斥锁 |
| `ub_mutex_lock` | `int ub_mutex_lock(ub_mutex_lock_t *mutex, const ub_lock_policy_t *policy, const ub_location_t *self)` | 获取互斥锁 |
| `ub_mutex_unlock` | `int ub_mutex_unlock(ub_mutex_lock_t *mutex, const ub_lock_policy_t *policy, const ub_location_t *self)` | 释放互斥锁 |

### 6.3 分布式自旋锁

#### 6.3.1 主要接口

| 接口 | 原型 | 说明 |
|-----|------|------|
| `ub_spin_lock_init` | `void ub_spin_lock_init(ub_spin_lock_t *lock)` | 初始化自旋锁 |
| `ub_spin_lock` | `int ub_spin_lock(ub_spin_lock_t *lock, uint32_t timeout_ms)` | 获取自旋锁 |
| `ub_spin_unlock` | `void ub_spin_unlock(ub_spin_lock_t *lock)` | 释放自旋锁 |

### 6.4 分布式通信队列 (ub_dist_comm_queue.h)

#### 6.4.1 核心数据结构

##### ub_shm_area_t

```c
typedef struct {
    size_t size;   // 区域大小
    void *ptr;     // 区域指针
} ub_shm_area_t;
```

##### ub_ring_desc_t

```c
typedef struct {
    uint32_t ring_capacity;   // Ring 容量（必须是 2 的幂）
    uint32_t max_msg_size;    // 单消息最大大小
    uint32_t priority;        // 优先级（从 1 开始）
} ub_ring_desc_t;
```

##### ub_comm_conf_t

```c
typedef struct {
    int32_t cpu_id;              // 绑核 CPU ID（-1 表示不绑核）
    uint32_t max_nodes;          // 最大节点数（上限 8）
    uint32_t current_node_id;    // 当前节点 ID
    uint32_t num_rings;          // Ring 数量
    const ub_ring_desc_t *ring_descs; // Ring 描述数组
} ub_comm_conf_t;
```

##### message_t

```c
typedef struct {
    message_header_t header;  // 消息头
    void *body;               // 消息体（仅在回调期间有效）
} message_t;
```

#### 6.4.2 主要接口

| 接口 | 说明 |
|-----|------|
| `ub_comm_queue_init` | 初始化通信队列 |
| `ub_comm_queue_deinit` | 销毁通信队列 |
| `ub_comm_queue_send` | 发送消息 |
| `ub_comm_queue_get_status` | 查询 Ring 状态 |
| `ub_comm_queue_set_congestion_threshold` | 设置拥塞阈值 |
| `ub_comm_queue_check_ready` | 检查节点就绪状态 |
| `ub_comm_queue_register_process_func` | 注册消息处理回调 |

#### 6.4.3 返回值定义

| 返回值 | 宏定义 | 说明 |
|-------|-------|------|
| 0 | `UB_COMM_OK` | 操作成功 |
| 1 | `UB_COMM_SEND_CONGESTED` | 发送成功，但队列拥塞 |
| -1 | `UB_COMM_ERR_RING_FULL` | Ring 已满 |
| -2 | `UB_COMM_ERROR` | 操作失败 |

### 6.5 分布式事务资源 (ub_dist_tx_res.h)

#### 6.5.1 主要接口

| 接口 | 原型 | 说明 |
|-----|------|------|
| `ub_dist_tx_res_init` | `int ub_dist_tx_res_init(uint64_t *handle)` | 初始化事务资源 |
| `ub_dist_tx_res_set` | `int ub_dist_tx_res_set(uint64_t *handle, uint64_t value)` | 设置值 |
| `ub_dist_tx_res_get` | `int ub_dist_tx_res_get(const uint64_t *handle, uint64_t *out)` | 获取值 |
| `ub_dist_tx_res_fetch_add` | `int ub_dist_tx_res_fetch_add(uint64_t *handle, int64_t delta, uint64_t *old)` | 原子加并返回旧值 |

#### 6.5.2 返回值定义

| 返回值 | 宏定义 | 说明 |
|-------|-------|------|
| 0 | `UB_RES_OK` | 操作成功 |
| -1 | `UB_RES_ERROR` | 操作失败 |

---

## 7. 第三方系统接入说明

### 7.1 ubsmem 接入

#### 7.1.1 依赖说明

样例代码依赖 UBSM SDK，但核心库不直接链接它。核心库只处理已映射的共享内存地址。

#### 7.1.2 头文件依赖

```cpp
#include "ubs_mem.h"
#include "ubs_mem_def.h"
```

#### 7.1.3 链接依赖

```bash
-lubsm_sdk -lpthread -lrt
```

#### 7.1.4 使用流程

```
1. 初始化 ubsmem 环境
2. 创建/打开共享内存段
3. 映射共享内存到进程地址空间
4. 将映射地址传递给 ubs-atomic 接口
5. 使用完成后释放映射
6. 清理 ubsmem 资源
```

### 7.2 libboundscheck 接入

#### 7.2.1 依赖配置

确保 `libboundscheck.so` 在系统库路径中：

```bash
# 检查库是否存在
ls -la /usr/lib64/libboundscheck.so

# 如果不存在，复制到系统路径
cp /path/to/libboundscheck.so /usr/lib64/
ldconfig
```

#### 7.2.2 CMake 配置

```cmake
find_library(BOUNDSCHECK_LIB boundscheck REQUIRED)
target_link_libraries(ubs-atomic PRIVATE ${BOUNDSCHECK_LIB})
```

### 7.3 自定义共享内存接入

如果不使用 ubsmem，可以使用 POSIX 共享内存：

```cpp
#include <sys/mman.h>
#include <fcntl.h>

// 创建共享内存
int fd = shm_open("/my_shm", O_CREAT | O_RDWR, 0666);
ftruncate(fd, size);

// 映射
void *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

// 使用 ptr 作为共享内存地址
// ...

// 清理
munmap(ptr, size);
shm_unlink("/my_shm");
```

---

## 8. 贡献代码规范

### 8.1 代码风格

#### 8.1.1 通用规则

- **缩进**: 4 个空格，禁用 Tab
- **行宽**: 每行不超过 120 字符
- **命名规范**:
  - 宏定义: `UB_XXX_YYY`（全大写，下划线分隔）
  - 类型定义: `ub_xxx_t`（小写，下划线分隔）
  - 函数名: `ub_xxx_func_name`（小写，下划线分隔）
  - 变量名: `snake_case`（小写，下划线分隔）
  - 类名: `CamelCase`（首字母大写）
  - 私有成员: `m_memberName`（m_ 前缀 + 驼峰）

#### 8.1.2 C++ 代码规范

- 使用 C++17 特性
- 头文件使用 pragma once 或 include guard
- 智能指针优先于裸指针
- 使用 `nullptr` 而非 `NULL`
- 避免全局变量
- 异常处理：核心库不使用异常，使用返回码

#### 8.1.3 C 接口规范

- 所有对外接口必须是 C ABI
- 导出符号使用 `UB_API` 宏
- 不暴露内部数据结构细节（不透明类型）
- 参数校验必须完整

### 8.2 Git 规范

#### 8.2.1 分支命名

| 分支类型 | 命名规范 | 示例 |
|---------|---------|------|
| 功能分支 | `feature/xxx` | `feature/add-new-lock-type` |
| 修复分支 | `fix/xxx` | `fix/deadlock-issue` |
| 热修复分支 | `hotfix/xxx` | `hotfix/production-crash` |
| 发布分支 | `release/xxx` | `release/1.0.0` |

#### 8.2.2 Commit 信息规范

```
<类型>(<模块>): <简要描述>

<详细描述（可选）>

<相关 Issue/PR 编号（可选）>
```

类型说明：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码风格调整（不影响功能）
- `refactor`: 重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建/工具相关

示例：
```
feat(ub_lock): add recursive lock support

- 添加递归加锁功能
- 更新相关测试用例

Closes #123
```

#### 8.2.3 提交频率

- 每个 commit 应该是一个独立的、可验证的更改
- 避免超大 commit（超过 500 行代码变更）
- 确保每次提交都能通过编译和测试

### 8.3 代码审查规范

#### 8.3.1 PR 提交要求

1. 必须关联相关 Issue
2. 必须通过 CI 检查
3. 必须有至少一个 Reviewer 批准
4. 必须包含单元测试（如果是新功能）
5. 必须更新相关文档

#### 8.3.2 Review 检查清单

- [ ] 代码符合风格规范
- [ ] 没有明显的 bug
- [ ] 性能考虑充分
- [ ] 单元测试覆盖完整
- [ ] 文档已更新
- [ ] 没有引入新的编译警告
- [ ] 没有内存泄漏风险

### 8.4 提交前检查

```bash
# 1. 运行 clang-format
find . -name "*.cpp" -o -name "*.h" | xargs clang-format -i

# 2. 运行 clang-tidy
run-clang-tidy -p build

# 3. 运行单元测试
sh test/run_ut.sh

# 4. 检查编译警告
sh build.sh -Werror
```

---

## 9. 测试策略与方法

### 9.1 测试层次

| 测试层次 | 说明 | 工具 |
|---------|------|------|
| 单元测试 | 测试单个函数/类 | Google Test |
| 集成测试 | 测试模块间交互 | Google Test |
| 压力测试 | 高并发场景测试 | 自定义脚本 |
| 回归测试 | 确保修复不引入新问题 | CI/CD |

### 9.2 测试环境

#### 9.2.1 依赖安装

```bash
# 初始化子模块
git submodule update --init --recursive

# 安装测试工具
sudo yum install -y lcov genhtml dos2unix
```

#### 9.2.2 Mock 配置

项目使用 mockcpp 进行模拟测试：

```cpp
#include <mockcpp/mockcpp.hpp>

// Mock 示例
MOCKER(ub_comm_queue_send)
    .stubs()
    .will(returnValue(UB_COMM_OK));
```

### 9.3 执行测试

#### 9.3.1 运行单元测试

```bash
sh test/run_ut.sh
```

#### 9.3.2 测试脚本执行流程

```
1. 准备 mockcpp 补丁
2. 使用 test/CMakeLists.txt 构建测试用例
3. 执行测试
4. 生成覆盖率数据
5. 输出测试报告
```

#### 9.3.3 测试产物

| 产物 | 路径 | 说明 |
|-----|------|------|
| 测试可执行文件 | `test/build/ubs_atomic_ut` | 单元测试二进制 |
| 覆盖率报告 | `test/build/gcovr_report/` | HTML 格式 |
| 覆盖率数据 | `test/build/coverage.info` | lcov 格式 |

### 9.4 测试覆盖率要求

| 模块 | 行覆盖率要求 | 分支覆盖率要求 |
|-----|-------------|---------------|
| 核心模块 | >= 90% | >= 80% |
| 工具模块 | >= 80% | >= 70% |
| 新增代码 | >= 95% | >= 85% |

### 9.5 测试用例编写规范

#### 9.5.1 测试命名

```cpp
// 测试用例命名：TEST(模块, 场景_期望结果)
TEST(ub_lock, x_lock_success) {
    // ...
}

TEST(ub_lock, x_lock_timeout_when_contended) {
    // ...
}
```

#### 9.5.2 测试结构

```cpp
TEST(模块, 场景) {
    // 1. 设置 (Setup)
    // 准备测试数据和环境
    
    // 2. 执行 (Exercise)
    // 调用被测试的函数
    
    // 3. 断言 (Verify)
    // 验证结果符合预期
    
    // 4. 清理 (Teardown)
    // 清理测试资源（如有需要）
}
```

#### 9.5.3 边界条件测试

必须覆盖的边界条件：
- 参数为 NULL
- 超时时间为 0
- 最大/最小配置值
- 并发竞争场景
- 异常恢复场景

### 9.6 CI/CD 集成

#### 9.6.1 流水线步骤

1. **代码检查**: clang-format, clang-tidy
2. **编译构建**: Debug 和 Release 版本
3. **单元测试**: 运行所有测试用例
4. **覆盖率检查**: 确保达到覆盖率要求
5. **打包发布**: 生成 RPM 包

#### 9.6.2 质量门禁

- 代码检查必须通过
- 测试必须全部通过
- 覆盖率必须达标
- 无新增编译警告

---

## 10. 版本控制流程

### 10.1 版本号规则

采用语义化版本控制（Semantic Versioning）：

```
MAJOR.MINOR.PATCH-RELEASE
```

| 字段 | 说明 | 变更场景 |
|-----|------|---------|
| MAJOR | 主版本号 | 不兼容的 API 变更 |
| MINOR | 次版本号 | 向后兼容的功能新增 |
| PATCH | 修订号 | 向后兼容的问题修复 |
| RELEASE | 发布号 | 同一版本的多次发布 |

示例：`1.0.0-1`, `1.2.3-2`

### 10.2 分支管理

#### 10.2.1 分支模型

```
┌───────────────────────────────────────────────────────────────┐
│                      main (主分支)                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │
│  │  v1.0.0  │  │  v1.1.0  │  │  v2.0.0  │                    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                    │
└───────┼─────────────┼─────────────┼──────────────────────────┘
        │             │             │
        ▼             ▼             ▼
┌───────────────────────────────────────────────────────────────┐
│                release/1.0  release/1.1  release/2.0         │
│                (发布分支)     (发布分支)   (发布分支)          │
└───────────────────────────────────────────────────────────────┘
        │             │
        ▼             ▼
┌───────────────────────────────────────────────────────────────┐
│         feature/xxx      fix/xxx       hotfix/xxx            │
│         (功能分支)       (修复分支)     (热修复分支)          │
└───────────────────────────────────────────────────────────────┘
```

#### 10.2.2 分支管理规则

| 分支 | 保护 | 合并规则 |
|-----|------|---------|
| main | 是 | 必须通过 PR + 2 个 Reviewer 批准 |
| release/* | 是 | 必须通过 PR + 1 个 Reviewer 批准 |
| feature/* | 否 | 无强制要求 |
| fix/* | 否 | 无强制要求 |
| hotfix/* | 否 | 无强制要求 |

### 10.3 发布流程

#### 10.3.1 常规发布

1. **创建发布分支**
   ```bash
   git checkout -b release/1.0 main
   ```

2. **更新版本号**
   - 更新 CMakeLists.txt 中的版本号
   - 更新 README.md 中的版本信息
   - 更新 CHANGELOG.md

3. **测试验证**
   - 运行完整测试套件
   - 验证性能指标
   - 进行回归测试

4. **打标签**
   ```bash
   git tag -a v1.0.0 -m "Release v1.0.0"
   git push origin v1.0.0
   ```

5. **构建发布包**
   ```bash
   sh build.sh package -V 1.0.0-1
   ```

6. **合并回主分支**
   ```bash
   git checkout main
   git merge release/1.0 --no-ff
   ```

7. **清理发布分支**
   ```bash
   git branch -d release/1.0
   git push origin --delete release/1.0
   ```

#### 10.3.2 热修复发布

1. **从标签创建热修复分支**
   ```bash
   git checkout -b hotfix/1.0.1 v1.0.0
   ```

2. **修复问题**
   - 最小化代码变更
   - 编写回归测试

3. **测试验证**

4. **更新版本号并打标签**
   ```bash
   git tag -a v1.0.1 -m "Hotfix v1.0.1"
   ```

5. **合并到主分支和活跃的 release 分支**
   ```bash
   git checkout main
   git merge hotfix/1.0.1
   
   git checkout release/1.1
   git merge hotfix/1.0.1
   ```

6. **清理**
   ```bash
   git branch -d hotfix/1.0.1
   ```

### 10.4 版本归档

- 所有发布版本都必须打标签
- 标签格式：`vX.Y.Z`
- 标签必须包含签名（可选）
- 发布包必须归档保存

---

## 11. 内部实现机制

### 11.1 分布式锁实现机制

#### 11.1.1 锁状态机

```
Free → Locking → Locked → Unlocking → Free
         ↓          ↓
        Timeout    Lease Expired
         ↓          ↓
        Free      Recovering → Free
```

#### 11.1.2 租约机制

```
┌─────────────────────────────────────────────────────────────┐
│                     Lease 机制                              │
├─────────────────────────────────────────────────────────────┤
│  1. 获取锁时，记录持有者身份和租约到期时间                    │
│  2. 持有者定期心跳续约（< heartbeat_timeout）                 │
│  3. 租约到期后，其他节点可抢占                               │
│  4. 支持故障恢复：按进程 ID 强制释放异常持锁                   │
└─────────────────────────────────────────────────────────────┘
```

#### 11.1.3 跨节点通知

分布式锁内部借助通信队列完成：
- 锁释放时通知等待者
- 租约到期时通知持有者
- 故障恢复时通知相关节点

### 11.2 通信队列实现机制

#### 11.2.1 Ring Buffer 设计

```
┌─────────────────────────────────────────────────────────────┐
│                     Ring Buffer                             │
├─────────────────────────────────────────────────────────────┤
│  head → [ ] [ ] [ ] [ ] [ ] [ ] [ ] [ ] ← tail              │
│          ↑           ↑                                      │
│        write        read                                    │
├─────────────────────────────────────────────────────────────┤
│  - 容量必须是 2 的幂（便于位运算取模）                        │
│  - 单生产者/单消费者模型                                     │
│  - 无锁设计，使用原子操作                                    │
└─────────────────────────────────────────────────────────────┘
```

#### 11.2.2 流控机制

```
拥塞判定流程：
1. 写入消息前检查 Ring 使用量
2. 如果使用量 > 阈值（默认 80%），标记拥塞
3. 返回 UB_COMM_SEND_CONGESTED
4. 调用方可据此降低发送速率
```

#### 11.2.3 回调模型

| 模式 | 执行位置 | 适用场景 |
|-----|---------|---------|
| `UB_FUNC_SYNC` | 分发线程 | 轻量快速处理 |
| `UB_FUNC_ASYNC` | 线程池 | 较重的业务处理 |

### 11.3 性能优化策略

#### 11.3.1 内存对齐

- 锁结构 64 字节对齐（避免 false sharing）
- 事务资源 8 字节对齐（保证原子操作）
- 消息结构缓存行对齐

#### 11.3.2 无锁编程

- 使用 CAS 操作替代互斥锁
- 原子变量代替共享状态
- 内存顺序优化（acquire/release）

#### 11.3.3 延迟释放

通过 `allow_delay_release` 选项：
- 减少频繁的跨节点通知
- 提高吞吐量
- 适用于读多写少场景

---

*文档版本: 1.0*  
*最后更新: 2024年*
