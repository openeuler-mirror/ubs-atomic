# ubs-atomic 安装部署

## 环境要求

### 硬件要求

**表 1** 硬件要求

| 服务器名称 | TaiShan 服务器 |
| ----- | ----- |
| 处理器 | 鲲鹏处理器（ARM64/aarch64） |
| CPU | 要求支持 LSE 原子指令集（ARMv8.1-A 及以上）<br>可通过命令 `grep -o 'lse' /proc/cpuinfo \| head -1` 检查，有输出即表示支持 |

> [!NOTE] 说明
>
> - ubs-atomic 默认使用 `-march=armv8-a+lse` 编译选项以获得最优原子操作性能，ARM64 平台为当前配套目标平台。
> - x86_64 平台可编译运行，但不在默认配套范围内，需自行调整编译选项。

### 软件版本

**表 2** 软件要求

| 软件名称 | 软件版本 |
| --------- | ------ |
| OS | openEuler 22.03 LTS、openEuler 24.03 LTS |
| GCC | 7.3.0 及以上（需支持 C11/C++17） |
| CMake | 3.22 及以上 |
| Ninja | 1.10 及以上（可选，未安装时自动回退到 Unix Makefiles） |
| libboundscheck | 1.1.10 及以上（运行时依赖，动态链接） |

## 安装步骤

### 前提条件

前置依赖 libboundscheck（openEuler 开源的安全函数库），可通过以下方式安装。

- 有 openEuler yum/dnf 镜像源时，可以直接安装。

  ```bash
  dnf install -y libboundscheck
  ```

- 源码编译安装。
  1. 下载发行版本，地址：<https://gitee.com/openeuler/libboundscheck/releases>（推荐最新 release 版本）。
  2. 编译。

     ```bash
     make CC=gcc
     ```

  3. 编译后根目录下 lib 目录中存在 libboundscheck.so，拷贝到系统库目录。

     ```bash
     cp lib/libboundscheck.so /usr/lib64
     ```

### 方式一：RPM 包安装

**操作步骤**

若环境上已安装 ubs-atomic，则先卸载再安装，否则直接安装即可。

1. 执行以下命令，卸载 rpm。

   ```bash
   rpm -e ubs-atomic
   ```

2. 执行以下命令，安装 rpm。

   ```bash
   rpm -ivh ubs-atomic-1.0.0-1.aarch64.rpm
   ```

   > [!NOTE] 说明
   > RPM 包同时包含运行库和开发头文件，安装后即可进行二次开发，无需额外安装 devel 包。

3. 执行以下命令，验证安装。

   ```bash
   # 检查库文件
   ls -l /usr/lib64/libubs-atomic.so

   # 检查头文件
   ls -l /usr/include/ub_dist_lock.h /usr/include/ub_dist_comm_queue.h /usr/include/ub_dist_tx_res.h
   ```

**安装产物**

**表 3** RPM 安装产物路径

| 产物 | 安装路径 |
| ---- | ------ |
| 共享库 | /usr/lib64/libubs-atomic.so |
| 头文件 | <ul><li>/usr/include/ub_dist_lock.h</li> <li>/usr/include/ub_dist_comm_queue.h</li> <li>/usr/include/ub_dist_tx_res.h</li></ul> |

RPM 安装产物位于系统库目录和系统头文件目录，编译链接时通常无需额外设置 `LD_LIBRARY_PATH`：

```bash
gcc my_app.c -I/usr/include -lubs-atomic -lpthread -lrt -o my_app
```

### 方式二：源码编译安装

**操作步骤**

1. 安装构建依赖。

   ```bash
   dnf install -y make gcc gcc-c++ cmake ninja-build libboundscheck findutils git
   ```

2. 获取源码并进入源码根目录。

   ```bash
   cd ubs-atomic
   ```

3. 执行以下命令，构建 RPM 包。

   ```bash
   bash build.sh package
   ```

   该命令默认执行 Release 构建，RPM 包生成于构建目录 `dist/release/` 下，文件名形如 `ubs-atomic-1.0.0-1.aarch64.rpm`。

4. 若只需在本机直接使用产物，可执行以下命令完成构建。

   ```bash
   # Release 构建（默认）
   bash build.sh

   # Debug 构建
   bash build.sh -D
   ```

   构建产物路径为 `dist/release/lib/libubs-atomic.so`（Release）或 `dist/debug/lib/libubs-atomic.so`（Debug）。

5. 执行以下命令，将产物拷贝到系统目录（可选）。

   ```bash
   cp dist/release/lib/libubs-atomic.so /usr/lib64/
   cp include/*.h /usr/include/
   ```

**常用构建命令**

| 命令 | 说明 |
| ---- | ---- |
| `bash build.sh` | Release 构建（默认） |
| `bash build.sh -D` | Debug 构建 |
| `bash build.sh -T RelWithDebInfo` | 带调试信息的 Release 构建 |
| `bash build.sh package` | 构建 RPM 包 |
| `bash build.sh test` | 构建并运行全部 UT（自动切换 Debug） |
| `bash build.sh -c` | 清理构建目录 |
| `bash build.sh -v` | 输出详细构建日志 |

### 部署验证

安装完成后，可使用仓库自带样例进行功能验证：

1. 参照 `sample_code/share_mem/README.md` 创建锁和通信队列所需的共享内存对象。
2. 编译并运行样例：
   - 分布式锁：`sample_code/ub_lock/ub_dist_lock_func_test.cpp`
   - 通信队列：`sample_code/ub_comm_queue/pingpong.cpp`
   - 事务资源：`sample_code/ub_dist_tx_res/ub_dist_tx_res_fence_semantic_test.cpp`
3. 样例运行成功即表示安装部署完成。

### 卸载

执行以下命令卸载 ubs-atomic：

```bash
rpm -e ubs-atomic
```

> [!NOTE] 说明
> 卸载不会回收业务已创建的共享内存对象，请通过共享内存管理工具（如 `sample_code/share_mem/ubsm_shm_creator`）单独清理。
