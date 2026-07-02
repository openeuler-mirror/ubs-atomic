# share_mem 共享内存创建工具

本目录提供基于 UBSM 的共享内存管理示例，用来创建锁和通信队列样例运行前需要的共享内存对象。当前脚本按两节点部署模型说明。

## 文件说明

| 文件 | 作用 |
| --- | --- |
| `ubsm_region.conf` | 共享内存域配置，包含共享内存大小和参与节点主机名。 |
| `ubsm_shm_creator.cpp` | 创建或删除共享内存域中的共享内存对象。 |

## 1. 修改共享内存域配置

```ini
request_size_mb=1024
hosts=computer01,computer02
```

- `request_size_mb`：每个共享内存对象大小，单位 MB。
- `hosts`：参与共享内存域的主机名列表，必须和执行环境中的节点主机名一致。
- 创建共享内存时传入的 `<expected_export_hostname>` 必须在 `hosts` 列表中。

## 2. 编译工具

```bash
g++ -std=c++17 ubsm_shm_creator.cpp \
  -I/usr/local/ubs_mem/include \
  -L/usr/local/ubs_mem/lib \
  -lubsm_sdk \
  -o ubsm_shm_creator

export LD_LIBRARY_PATH=/usr/local/ubs_mem/lib:$LD_LIBRARY_PATH
```

如果 `ubs_mem` 安装在其他目录，请同步修改 `-I`、`-L` 和 `LD_LIBRARY_PATH`。

## 3. 创建共享内存

命令格式：

```bash
./ubsm_shm_creator create <expected_export_hostname> <shared_memory_name>
```

示例：

```bash
./ubsm_shm_creator create computer01 shm_ub_lock
./ubsm_shm_creator create computer01 shm_node1_export
./ubsm_shm_creator create computer02 shm_node2_export
```

脚本会先按 `<expected_export_hostname>` 创建或查找 `<expected_export_hostname>_affinity` 共享内存域，然后在该域中创建 `<shared_memory_name>`。

## 4. 删除共享内存

命令格式：

```bash
./ubsm_shm_creator delete <expected_export_hostname> <shared_memory_name>
```

示例：

```bash
./ubsm_shm_creator delete computer01 shm_ub_lock
./ubsm_shm_creator delete computer01 shm_node1_export
./ubsm_shm_creator delete computer02 shm_node2_export
```

## 5. 锁和队列样例需要创建哪些共享内存

| 样例 | 默认共享内存名 | 用途 |
| --- | --- | --- |
| `sample_code/ub_lock/ub_dist_lock_func_test` | `shm_ub_lock` | 存放读写锁对象。 |
| `sample_code/ub_lock/ub_dist_lock_func_test` | `shm_node1_export` | NodeA 通信队列共享内存。 |
| `sample_code/ub_lock/ub_dist_lock_func_test` | `shm_node2_export` | NodeB 通信队列共享内存。 |

运行样例时，配置文件或启动参数中的共享内存名必须和这里实际创建的名字一致。创建完成后，可直接运行锁或队列样例验证共享内存是否能被正常映射和初始化。

注意：不同样例的默认共享内存名可能重复，例如锁样例和 pingpong 样例都可能使用 `shm_node1_export`。如果要在同一环境同时保留两套样例，请给其中一套改成独立名字，并在配置文件或启动参数中同步指定。
