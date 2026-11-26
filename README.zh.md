<p align="center">
  <img src="imgs/logo.png" alt="pg_yaap 标志" width="200">
</p>

<h1 align="center">pg_yaap</h1>

<p align="center"><strong>面向 PostgreSQL 的现代分析引擎</strong></p>
<p align="center">现代 OLAP，原生 PostgreSQL 存储。</p>
<p align="center">PostgreSQL 扩展 · C++17 · Apache 2.0 · 实验性项目</p>

<p align="center">
  <a href="README.md">English</a> · 中文 ·
  <a href="#性能基准">性能基准</a> · <a href="#架构">架构</a> ·
  <a href="#快速开始">快速开始</a>
</p>

## 项目概览

pg_yaap 为 PostgreSQL 中受支持的分析型查询提供独立优化器与 C++ 向量化执行引擎。它通过 PostgreSQL 的存储和缓冲区管理接口直接读取原生堆表。

PostgreSQL 负责 SQL 解析、系统目录、事务和客户端协议。YAAP 为接管的查询生成计划，并行执行各条 Pipeline。项目处于实验阶段，适合开发和评估，暂不建议用于生产环境。

## 性能基准

在配备 **24 GB 内存的 Apple M4 Pro** 上，YAAP 记录的 SQL 总耗时为 **SF10 20.5 秒**、**SF100 414.5 秒**。SF100 的 PostgreSQL 数据库在磁盘上约占 **125 GB**，YAAP 完成了全部 22 条查询的执行。

| TPC-H 数据规模 | PostgreSQL | pg_duckdb | pg_yaap |
| --- | ---: | ---: | ---: |
| SF10 | 447.9 秒 | 95.8 秒 | **20.5 秒** |
| SF100 | 3,302.0 秒 | 3,346.6 秒 | **414.5 秒** |

在 SF10 的历史记录中，PostgreSQL 和 pg_duckdb 的总耗时分别是 YAAP 的 21.8 倍和 4.7 倍。各组测试条件存在差异。

| SF100 执行情况 | PostgreSQL | pg_duckdb | pg_yaap |
| --- | ---: | ---: | ---: |
| 完成执行 | 18/22 | 18/22 | 22/22 |
| 超时 | 4 | 4 | 0 |

**SF100 正确性**：Q1 的 `N` / `O` 组中，YAAP 返回的 `sum_charge` 为负数，两组基线均返回正数。每次基线超时按 600 秒计入总耗时，两组基线的 `shared_buffers` 为 2 GB，YAAP 为 4 GB。表格记录耗时与执行完成情况，结果一致性仍待验证。

详见[测试条件、逐查询图表与复现入口](docs/benchmark.md)。

## 核心特性

| 特性 | 实现 |
| --- | --- |
| 独立优化器 | YAAP 自有逻辑与物理计划，支持子查询去关联、谓词下推和基于代价的连接排序。 |
| 向量化执行 | 每个列式 `DataChunk` 最多容纳 2,048 行，算子按批处理。 |
| 并行 Pipeline | 事件 DAG 调度 `RUN`、`COMBINE` 和 `FINALIZE` 阶段。 |
| 多进程运行时 | PostgreSQL 后台进程从共享就绪队列领取任务，通过 DSM 和 DSA 共享状态。 |
| 超内存处理 | 执行引擎按内存预算管理中间数据，分区哈希连接等算子支持溢写。 |
| PostgreSQL 原生存储 | 通过 PostgreSQL 缓冲区管理器和快照读取原生堆表。 |

## 架构

![系统架构图，展示 PostgreSQL 解析与存储如何与 YAAP 规划和执行配合](imgs/arch.png)

规划器 Hook 生成 YAAP 物理计划，执行层将计划转换为多条 Pipeline，再通过 PostgreSQL 的元组接口返回结果。YAAP 接管查询后，规划或执行失败会直接报错。

同一进程内的算子沿 Pipeline 直接传递列式批次。哈希表构建、聚合等 Pipeline breaker 发布共享状态，供下游 Pipeline 读取。Leader 构建事件图并启动 Worker。Worker 激活就绪阶段，从查询级共享队列领取各条 Pipeline 的任务。详见[执行引擎架构与调度设计](docs/pipeline_runtime.md#pipelines-and-event-dependencies)。

运行时将调度事件和各进程耗时导出为 Chrome Trace JSON，可在 Perfetto 中查看。详见[执行追踪](docs/execution_trace.md)。

## 快速开始

### 构建依赖

- **PostgreSQL 18.3** 是 CI 验证版本。使用与目标服务端配套的 `pg_config` 和服务端开发头文件。
- C 和 C++17 编译工具链、Meson、Ninja。
- 若目标 PostgreSQL 启用了 LLVM，需要相容的 LLVM 开发文件与 `llvm-config`。可通过 `-Dllvm_config=/path/to/llvm-config` 指定路径。未启用 LLVM 的 PostgreSQL 采用非 JIT 路径。

### 构建与安装

在仓库根目录执行，安装账号需有目标 PostgreSQL 安装目录的写权限。

```sh
meson setup build -Dpg_config=/path/to/postgresql/bin/pg_config -Dbuildtype=release
meson compile -C build
meson install -C build
```

从 PostgreSQL 源码构建或在离线环境安装，见[离线构建指南](docs/offline-linux-build.md)。

### 运行与验证

使用超级用户通过 `psql` 连接测试数据库，在当前会话加载 YAAP。

```sql
LOAD 'pg_yaap';
SHOW pg_yaap.enabled;
```

对分析查询运行普通 `EXPLAIN`，查看计划是否包含 `Planner: YAAP optimizer` 和 `YAAP Physical Plan`。要确认实际执行路径，可启用 `pg_yaap.trace_execution_path`，执行查询后在服务端日志中查找 `pg_yaap_path: path=pipeline detail=yaap_pipeline`。

## 设计路线

| 设计维度 | PostgreSQL | pg_duckdb | pg_yaap |
| --- | --- | --- | --- |
| 查询优化器与执行引擎 | PostgreSQL | 嵌入式 DuckDB | YAAP |
| 执行模型 | 以元组为单位 | DuckDB 向量化执行 | YAAP 向量化执行 |
| 并行执行 | PostgreSQL 多进程 | DuckDB 多线程，堆表扫描可使用 PostgreSQL Worker | PostgreSQL 后台进程 |
| 查询 PostgreSQL 原生表 | 支持 | 支持 | 支持 |

YAAP 围绕 PostgreSQL 多进程架构实现了自己的优化器、算子和调度器。表中的 pg_duckdb 一列描述上游项目的设计。参见 [pg_duckdb 项目配置](https://github.com/duckdb/pg_duckdb/blob/main/docs/settings.md)。

## 当前状态与演进方向

- **优化器** · 为受支持的分析 SQL 生成逻辑与物理计划。
- **执行引擎** · 并行顺序扫描、哈希连接、哈希聚合、完美哈希聚合、Top-N 和磁盘溢写。
- **运行时** · 按查询启动的后台 Worker、事件 DAG 调度、共享任务队列和执行追踪。
- **当前限制** · SQL 覆盖有限。排序采用单进程 PostgreSQL tuplesort。YAAP 的定点整数 `NUMERIC` 计算可能与 PostgreSQL 精确十进制结果有差异。

可探索的方向包括 TPC-H 结果校验与正确性修复、并行排序、Worker 池化、更广的 SQL 覆盖，以及内存和 I/O 优化。

## 文档

| 主题 | 参考资料 |
| --- | --- |
| Pipeline 调度与 Worker 生命周期 | [Pipeline 运行时](docs/pipeline_runtime.md) |
| 磁盘溢写与分区探测 | [混合哈希连接](docs/hybrid_hash_join.md)、[分区哈希连接探测](docs/hash_join_partition_probe.md) |
| 内存预算 | [DSA 内存限制](docs/dsa_memory_limits.md) |
| 追踪配置与事件类型 | [执行追踪](docs/execution_trace.md) |
| 基准测试方法 | [TPC-H 性能快照](docs/benchmark.md) |
| 离线构建 PostgreSQL 与 YAAP | [离线 Linux 构建](docs/offline-linux-build.md) |

## 致谢与许可证

感谢 PostgreSQL 项目提供的扩展基础设施，以及 DuckDB 对 YAAP 优化器与执行引擎设计的启发。

pg_yaap 采用 [Apache License 2.0](LICENSE)。
