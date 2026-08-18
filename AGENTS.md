# AGENTS.md

## Project Overview

ubs-atomic — 基于共享内存的轻量级分布式基础组件库。提供分布式读写锁、互斥锁、自旋锁、通信队列和事务资源原子操作能力。C ABI 对外接口，C++17 核心实现。目标平台为 openEuler Linux (ARM64)。发布件为 `libubs-atomic.so`。

## Build Commands

```shell
# Release build (default)
bash build.sh

# Debug build
bash build.sh -D

# Release with debug info
bash build.sh -T RelWithDebInfo

# Build and package as RPM
bash build.sh package

# Clean build directory
bash build.sh -c

# Verbose output
bash build.sh -v
```

## Test Commands

```shell
# Build and run all UT tests (auto-switches to Debug for coverage)
bash build.sh test

# Same as above
bash build.sh ut

# Run specific test case directly (after building)
cd test/build && ./ubs_atomic_ut --gtest_filter="UbDistributeLock*"

# Coverage report (requires lcov + genhtml, automatically handled by run_ut.sh)
bash build.sh test
```

## Code Style

- C++17 / C11 standard (no compiler extensions)
- clang-format for formatting (Google-based, 4-space indent, no tabs, ColumnLimit 120, mixed brace style: Allman for functions, K&R for others)
- clang-tidy for static analysis (whitelist strategy: disable all then enable specific checks; WarningsAsErrors: `bugprone-use-after-move`)
- pre-commit hooks enforce: Release build, Test build, clang-format, clang-tidy
- Comments in Chinese or English as appropriate

## Dev Environment Tips

- Default build type is Release; tests auto-switch to Debug for coverage
- Build uses Ninja if available, falls back to Unix Makefiles
- `compile_commands.json` locations: `dist/release/` (Release build), `dist/debug/` (Debug build), `cmake-build-debug/` (CLion, used by clang-tidy pre-commit hook)
- Point your LSP to the correct build directory for your configuration
- Main output: `dist/release/lib/libubs-atomic.so`
- ARM64 builds use `-march=armv8-a+lse` for LSE atomic instructions
- Dependencies: `libboundscheck` (system, dynamic link)
- Test dependencies: googletest (auto-download from src-openeuler), mockcpp (manual placement in `test/3rdparty/mockcpp/`)
- Install build deps: `dnf install -y make gcc gcc-c++ cmake ninja-build libboundscheck findutils git`

## Architecture

```
src/
├── common/           # Shared utilities (log.h, util.h)
├── ub_lock/          # Distributed lock primitives
│   ├── ub_distribute_lock_api.cpp   # C ABI entry for distributed read-write lock
│   ├── ub_distribute_lock.cpp       # Distributed read-write lock (S/SX/X modes)
│   ├── ub_mutex_lock.cpp            # Distributed mutex lock
│   ├── ub_spin_lock.cpp             # CAS-based spin lock
│   ├── lock_optimization.cpp        # Lock performance optimizations
│   ├── message_manage.cpp           # Lock message management
│   └── wait_queue.cpp               # Wait queue for lock contenders
├── ub_comm_queue/    # Distributed communication queue
│   ├── ub_dist_comm_queue.cpp       # Shared-memory ring buffer queue
│   ├── MPSCRingBuffer.cpp           # Multi-producer single-consumer ring buffer
│   ├── UBShmTransport.cpp           # Shared memory transport layer
│   └── ThreadPool.h                 # Thread pool utility
├── ub_dist_tx_res/   # Distributed transaction resource
│   └── ub_dist_tx_res.cpp           # Atomic uint64 resource init/read/write/incr
└── CMakeLists.txt    # Builds libubs-atomic.so

include/
├── ub_dist_lock.h        # C ABI: distributed lock
├── ub_dist_comm_queue.h  # C ABI: communication queue
└── ub_dist_tx_res.h      # C ABI: transaction resource

test/
├── 3rdparty/             # Test framework integration (googletest + mockcpp)
├── testcase/
│   ├── ub_lock/          # Lock unit tests
│   ├── ub_comm_queue/   # Queue unit tests
│   └── ub_dist_tx_res/  # Transaction resource unit tests
├── run_ut.sh             # Test build & run script
└── CMakeLists.txt

sample_code/            # Usage examples
├── share_mem/          # Shared memory setup example
├── ub_lock/            # Lock usage example
├── ub_comm_queue/      # Queue usage example
└── ub_dist_tx_res/     # Transaction resource usage example

doc/
├── developer_guide.md                   # Developer guide
├── user_guide.md                        # User guide
├── api/libubs-atomic.md                 # API reference
└── 3rdparty_open_source_components.md   # Third-party dependency manifest
```

## Security Guidelines

**禁止以下行为（红线规则）：**

1. **禁止提交敏感信息**
   - API 密钥、密码、Token、证书私钥
   - 数据库连接字符串含凭证
   - SSH 私钥、GPG 密钥

2. **禁止硬编码凭证**
   - 用户名/密码
   - Access Key/Secret Key
   - 认证 Token

3. **禁止绕过安全检查**
   - 禁用 SSL/TLS 验证
   - 注释或删除安全相关代码
   - 关闭认证/授权机制

4. **禁止不安全日志**
   - 记录敏感数据（密码、Token、个人信息）
   - 明文记录凭证

**必须遵守：**

- 使用环境变量或配置文件管理凭证（配置文件需 `.gitignore`）
- 敏感操作需代码审查
- 定期轮换密钥和凭证
- 报告安全漏洞不公开披露

## Commit Guidelines

- Run `bash build.sh` before committing (pre-commit hook enforces Release build)
- Run `bash build.sh test` for test-related changes (pre-commit hook enforces test build)
- Ensure clang-format and clang-tidy checks pass (pre-commit hooks enforce both)
- Add or update tests for code changes
- Commit message format: `type(scope): description` (e.g., `feat(ub_lock): add X-mode lock support`)
