# 版本说明书

## 版本配套说明

### 产品版本信息

| 项目 | 内容 |
| ---- | ---- |
| 产品名称 | UBS Atomic |
| 产品版本 | master |
| 版本类型 | 正式版本 |

### 相关产品版本配套说明

**硬件版本配套表**

| 产品名称 | 版本说明 |
| ----- | ----- |
| 服务器名称 | TaiShan 服务器 |
| 处理器 | 鲲鹏处理器（ARM64/aarch64） |
| CPU | 要求支持 LSE 原子指令集（ARMv8.1-A 及以上）<br>可通过命令 `grep -o 'lse' /proc/cpuinfo \| head -1` 检查，有输出即表示支持 |

**软件版本配套表**

| 软件名称 | 软件版本 |
| --------- | ------ |
| OS | <ul><li>openEuler 22.03 LTS</li><li>openEuler 24.03 LTS</li></ul> |
| GCC | 7.3.0 及以上（需支持 C11/C++17） |
| CMake | 3.22 及以上 |
| Ninja | 1.10 及以上（可选，未安装时自动回退到 Unix Makefiles） |
| libboundscheck | 1.1.10 及以上（运行时依赖，动态链接） |

## 版本兼容性说明

无

## 更新说明

### 关键特性变更

UBS Atomic 支持 fence、add 原子操作。

### 接口变更说明

**新增**

- `add`：原子加。
- `fetch_xor`：原子异域。
- `cas`：比较交换。
- `fence`：屏障语义。

### 已解决的问题

无

### 遗留问题

无

## 升级影响

### 升级过程中对现行系统的影响

- 对业务的影响

    软件版本升级过程中会导致业务中断。

- 对网络通信的影响

    对通信无影响。

### 升级后对现行系统的影响

无

## 版本配套文档

|文档名称|内容简介|
|---|---|
|《[安装部署](../zh/ubs_atomic_installation_deployment.md)》|提供安装UBS Atomic的安装部署、卸载等操作。|
|《[API接口](../zh/ubs_atomic_api_description.md)》|提供对外的C ABI接口。|
|《[配置说明](../zh/ubs_atomic_configuration_instructions.md)》|提供UBS Atomic的构建配置项和运行时配置项，以及各配置项的取值范围、默认值和配置建议。|
|《[安全说明](../zh/ubs_atomic_security_instructions.md)》|提供UBS Atomic在构建、部署和运行阶段涉及的安全机制、访问控制要求和安全使用约束。|
