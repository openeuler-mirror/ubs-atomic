# ubs-atomic 安装指南

当前 ubs-atomic 提供两种环境部署方式：RPM 包安装、容器镜像部署。环境部署流程包含以下主要步骤：

1. 环境准备与安装
2. 构建项目与单元测试

## 环境要求

|部件|版本|
|:---|:---|
|操作系统|openEuler 24.03 LTS 或更高版本|
|CPU架构|aarch64（需支持 LSE 原子指令）|
|内存|无特殊要求|
|共享内存服务|运行时依赖 ubsmem 服务（仅业务使用分布式锁/队列/事务资源时需要）|
|用户权限|安装与管理需 <code>root</code> 权限|

## 安装注意事项

  - ubs-atomic 为基础组件库（发布件 <code>libubs-atomic.so</code>），不包含常驻系统服务，安装后无需启动进程。
  - ubs-atomic 基于 C ABI 对外提供接口，业务进程直接链接使用；头文件安装于 <code>/usr/include</code>。
  - 运行期依赖 <code>libboundscheck</code>（系统库，随 RPM 自动安装）。
  - 使用分布式锁、通信队列、事务资源等功能前，需确保共享内存服务（ubsmem）已正确部署并启动，共享内存段权限与业务进程用户一致。

## 执行安装

### RPM 包安装

- 在线安装

  > [!NOTE]说明
  >
  > 在线安装过程中，所需依赖会自动进行安装。

  ```bash
  # 注：需要系统配置了 openEuler 24.03 (LTS) 镜像源
  # 安装基础组件库
  sudo dnf install -y ubs-atomic
  ```

- 离线安装

  > [!WARNING]说明
  >
  > 离线安装需要提前安装所需依赖。
  > ubs-atomic 运行依赖信息记录在 <code>CMakeLists.txt</code> 的 CPack 配置中。
  > 运行依赖所需系统库，通常由包管理器自动安装。

  ```bash
  # 通过 rpm 包安装运行包
  sudo rpm -ivh ubs-atomic-<version>-<release>.aarch64.rpm
  # 如需覆盖安装，可执行如下命令：
  sudo rpm -ivh ubs-atomic-<version>-<release>.aarch64.rpm --force
  ```

### 容器镜像部署（可选）

容器环境部署有两种方式：

- 基于镜像构建容器环境
- 基于 openEuler 基础环境从零安装

#### 方式一：基于镜像构建容器环境

基于镜像构建容器环境，首先需要获取镜像。通过 Dockerfile 预先将全部构建依赖安装进镜像，后续进入容器即可直接编译，无需重复安装工具链，节省环境准备时间。

**步骤 1：获取镜像**

Dockerfile 位于仓库 `docker/ubs-atomic.Dockerfile`，内容如下：

```dockerfile
ARG BASE_IMAGE=openeuler/openeuler:24.03-lts
FROM ${BASE_IMAGE}

RUN dnf install -y \
        make gcc gcc-c++ cmake ninja-build \
        libboundscheck findutils git \
    && dnf clean all \
    && rm -rf /var/cache/dnf

WORKDIR /workspace
CMD ["/bin/bash"]
```

构建镜像：

```bash
cd ubs-atomic
docker build -f docker/ubs-atomic.Dockerfile -t ubs-atomic-build:24.03-lts .
```

> [!NOTE]说明
>
> 构建依赖与仓库 <code>README.md</code> "环境与依赖"章节保持一致；googletest 由 CMake 自动从 src-openeuler 开源仓库下载，mockcpp 需按 <code>README.md</code> 说明手动放置到 <code>test/3rdparty/mockcpp/</code>，均无需在镜像内预装。
> aarch64 主机直接构建即可；x86_64 主机可加 <code>--platform linux/arm64</code> 构建镜像（仅用于验证 Dockerfile，容器内交叉编译极慢，不推荐）。
> 覆盖率报告依赖 lcov/genhtml（openEuler 官方仓库不含，需源码安装），如容器内需生成覆盖率，可在镜像内追加安装。

**步骤 2：创建容器**

以 aarch64 服务器为例，创建容器（源码目录以数据卷方式挂载到 <code>/workspace</code>）：

```bash
docker run -d --name ubs-atomic-build \
    -v /home/workspace/ubs-atomic:/workspace \
    ubs-atomic-build:24.03-lts \
    sleep infinity
```

**步骤 3：进入容器**

```bash
docker exec -it ubs-atomic-build bash
```

#### 方式二：基于 openEuler 基础环境从零安装

不使用镜像时，可在 openEuler 24.03 LTS (aarch64) 环境手动安装全部构建依赖：

```bash
sudo dnf install -y make gcc gcc-c++ cmake ninja-build \
    libboundscheck findutils git
```

该方式每次环境初始化均需联网安装依赖，耗时较长，推荐使用方式一。

## 构建项目与单元测试

进入容器后，在 `/workspace` 下执行：

```bash
cd /workspace
bash build.sh              # Release 构建，产物 dist/release/lib/libubs-atomic.so
bash build.sh -D           # Debug 构建
bash build.sh package      # 打包 RPM
```

单元测试：

```bash
bash build.sh test         # 编译并运行 ubs_atomic_ut，生成覆盖率报告
```

> [!NOTE]说明
>
> 首次运行单元测试前，需将 mockcpp 源码放置到 <code>test/3rdparty/mockcpp/</code>（详见仓内 <code>README.md</code>）；googletest 会由 CMake 自动从 src-openeuler 开源仓库下载。详细用法见仓内 <code>README.md</code> 与 <code>doc/developer_guide.md</code>。

## 运行示例与验证

仓库 `sample_code/` 目录提供共享内存、分布式锁、通信队列、事务资源四类使用示例。安装完成后可编译验证库的可用性：

```bash
# 验证库与头文件安装结果
ls -la /usr/lib64/libubs-atomic.so
ls /usr/include/ub_dist_lock.h /usr/include/ub_dist_comm_queue.h /usr/include/ub_dist_tx_res.h

# 编译链接验证（在任意测试目录执行）
cat > /tmp/verify_ubs_atomic.c <<'EOF'
#include <stdio.h>
#include "ub_dist_tx_res.h"

int main(void)
{
    printf("ubs-atomic linked OK\n");
    return 0;
}
EOF
gcc /tmp/verify_ubs_atomic.c -lubs-atomic -o /tmp/verify_ubs_atomic && /tmp/verify_ubs_atomic
```

运行分布式锁、通信队列等完整示例的前置条件见 `sample_code/README.md`。

## 安装结果

ubs-atomic 基础组件库安装结果：

| 路径                                  | 用途          |
|-------------------------------------| -------------|
| /usr/lib64/libubs-atomic.so          | 动态库        |
| /usr/include/ub_dist_lock.h          | 分布式锁 C ABI 头文件 |
| /usr/include/ub_dist_comm_queue.h    | 通信队列 C ABI 头文件 |
| /usr/include/ub_dist_tx_res.h        | 事务资源 C ABI 头文件 |

## （可选）配置共享内存

使用分布式锁、通信队列、事务资源功能前，需确保共享内存服务已正确配置：

1. 确保 ubsmem 服务已启动。
2. 配置共享内存段的权限。
3. 记录共享内存的访问路径和大小，业务进程据此完成初始化。

详细配置方法见 `doc/user_guide.md` 第 2.3 节。

## 验证部署

### 检查安装结果

```bash
dnf list installed ubs-atomic 2>/dev/null || rpm -q ubs-atomic
```

### 链接验证

```bash
gcc your_app.c -lubs-atomic -o your_app
```

## 卸载与清理

1. 停止并删除容器

```bash
docker ps -a
docker stop ubs-atomic-build
docker rm ubs-atomic-build
```

2. 删除镜像

```bash
docker rmi ubs-atomic-build:24.03-lts
```

3. 卸载 RPM 包

```bash
sudo dnf remove -y ubs-atomic
# 或
sudo rpm -e ubs-atomic
```

> [!NOTE]说明
>
> 卸载 RPM 包会同时移除 <code>/usr/lib64/libubs-atomic.so</code> 与 <code>/usr/include</code> 下的头文件；业务进程如仍依赖该库，请在卸载前确认。
