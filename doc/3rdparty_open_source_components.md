# ubs-atomic 开源三方件清单

> 本文档梳理本仓库依赖的全部开源三方件、来源、许可证及引用方式（静态/动态）。
> 发布件 `libubs-atomic.so` 本体不静态链接任何三方件；静态引用的三方件仅存在于
> UT 可执行文件 `ubs_atomic_ut` 中，不随产品分发。

## 1. 依赖总览

| 三方件 | 版本 | 许可证 | 来源 | 引用方式 | 使用范围 |
|--------|------|--------|------|----------|----------|
| googletest | 1.10.0 | BSD-3-Clause | openEuler 社区开源件 [src-openeuler/googletest](https://gitcode.com/src-openeuler/googletest)（humble 分支，`ros-humble-gtest-vendor_1.10.9004.orig.tar.gz`） | **静态链接**（`libgtest.a`、`libgtest_main.a`） | 仅 UT（`test/`） |
| mockcpp | 上游版本 | Apache-2.0 | 开源项目 mockcpp，维持原有获取方式（git submodule / 手动放置于 `test/3rdparty/mockcpp`），src-openeuler 中暂无对应件 | **静态链接**（`libmockcpp.a`） | 仅 UT（`test/`） |
| boundscheck | 系统安装版本 | openEuler 社区开源（securec 同源） | openEuler 发行版软件包 `libboundscheck`（`yum install libboundscheck`） | 动态链接（`libboundscheck.so`） | 主库与 UT |
| pthread / rt / dl | glibc | LGPL（glibc） | 操作系统自带 | 动态链接 | 主库与 UT |

## 2. 静态引用的开源三方件明细

### 2.1 googletest（静态）

- **引入位置**：`test/3rdparty/CMakeLists.txt` 中 `EXTERNALPROJECT_ADD(gtest ...)`
- **获取方式**：构建 UT 时自动从 openEuler 社区开源仓库
  `https://gitcode.com/src-openeuler/googletest`（humble 分支）下载
  `ros-humble-gtest-vendor_1.10.9004.orig.tar.gz` 源码包并解压构建；
  也可将 googletest 源码手动放置于 `test/3rdparty/googletest/` 优先使用本地源码
- **构建开关**：`-DBUILD_SHARED_LIBS=OFF -DINSTALL_GTEST=ON -DGOOGLETEST_VERSION=1.10.0`
  （vendor 源码包顶层为 ROS 包装，实际构建入口为包内 `CMakeLists.txt.upstream`）
- **链接产物**：`libgtest.a`、`libgtest_main.a` 静态链接进 `ubs_atomic_ut`
- **许可证**：BSD-3-Clause（源码包内 `LICENSE`）
- **合规说明**：BSD-3-Clause 允许静态链接闭源使用，需保留版权声明；
  该静态库仅存在于测试可执行文件，不进入发布件

### 2.2 mockcpp（静态）

- **引入位置**：`test/3rdparty/CMakeLists.txt` 中 `EXTERNALPROJECT_ADD(mockcpp ...)`
- **获取方式**：维持原有静态依赖方式，源码通过 git submodule / 手动放置于
  `test/3rdparty/mockcpp/`（src-openeuler 中暂无 mockcpp 对应开源件）。
  构建时拷贝至 build 目录并应用 `mockcpp_support_arm64.patch`（ARM64 支持补丁）
- **链接产物**：`libmockcpp.a` 静态链接进 `ubs_atomic_ut`
- **许可证**：Apache-2.0（源码内 `COPYING`）
- **合规说明**：Apache-2.0 允许静态链接使用，需随源码保留许可证与 NOTICE；
  该静态库仅存在于测试可执行文件，不进入发布件

## 3. 动态链接三方件

### 3.1 boundscheck

- 来源：openEuler 发行版开源软件包（`yum install libboundscheck`），
  提供 `/usr/lib64/libboundscheck.so`（securec 同源边界检查库）
- 主库 `src/CMakeLists.txt` 与 UT `test/CMakeLists.txt` 均动态链接
- src-openeuler 暂无该件的源码仓，发行版本身即为开源发布件，无需切换

### 3.2 系统库

- `pthread`、`rt`、`dl`：glibc 提供，动态链接，无需管理

## 4. 发布件依赖结论

`libubs-atomic.so`（RPM 发布件）链接关系：

```
libubs-atomic.so
├── libboundscheck.so   (openEuler 开源件，动态)
├── libpthread / librt  (glibc，动态)
└── 无静态链接的三方件
```

即：**发布件中不存在静态引用的开源三方件**，静态引用仅发生在 UT 构建中
（googletest、mockcpp），不产生对外分发的合规义务传递。

## 5. 变更记录

| 日期 | 变更内容 |
|------|----------|
| 2026-08 | googletest 切换为 openEuler 社区开源件 src-openeuler/googletest（1.10.9004），构建时自动拉取 |
| 2026-08 | mockcpp 维持原有静态依赖方式（源码由 git submodule / 手动放置于 `test/3rdparty/mockcpp`），不随仓库内置 |
