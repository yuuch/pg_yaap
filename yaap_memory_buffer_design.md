# pg_yaap 内存与 Buffer 架构设计

## 1. 结论

本方案不恢复 2026-06-18 之后那套“所有对象都经过 BufferManager 的 block/pin/unpin/accounting”设计，而是在当前 `41eda4a` 快路径上建立明确的两级内存模型：

1. **pipeline 内短生命周期数据保持 backend-private**：`DataChunk`、表达式 scratch、临时 selection vector、当前算子调用的临时数组由 worker 本地 `MemoryContext` 管理，通过对象池复用，不进入 DSM/DSA，不参与 spill，也不进行跨进程 pin/unpin。
2. **只有跨 pipeline、跨进程、可能 spill 的长期中间态进入 shared substrate**：HashAgg、HashJoin、Order、Output 等 pipeline breaker 的持久数据使用 query-scoped DSM 作为生命周期和控制面，使用 DSA 分配可变长 segment，使用 `dsa_pointer`/逻辑 handle 发布，使用 `SharedFileSet` 保存 spill 数据。

核心原则是：

> DataChunk 是流水线运输容器，不是缓存页；segment 才是长期中间态的管理和回收单位。

因此，对“pipeline breaker 下是否应该使用 PostgreSQL DSM”的直接回答是：

- **应该使用 DSM，但 DSM 主要承载固定控制结构、目录、barrier、队列和资源生命周期。**
- **可变长 breaker 数据应通过 DSM 内创建的 DSA 分配，并只在跨进程结构中保存 `dsa_pointer` 或逻辑 handle。**
- **不应该为每个 DataChunk 建 DSM，不应该通过 `shm_mq` 复制大块中间结果，也不应该在每次 tuple/chunk 访问时执行共享 pin/unpin。**

## 2. 当前基线与历史问题

### 2.1 当前快路径

当前工作树位于 buffer 引入前的 `41eda4a`。现有执行路径已经具备可复用的基础：

- `runtime_dsm.cpp` 创建 query-scoped DSM，并在其中放置 `PipelineSharedControl`、任务队列和一个 in-place DSA。
- `pipeline_descriptor.cpp` 通过 DSA descriptor 将 operator/pipeline 元数据发布给 worker。
- `task.cpp` 为每个 backend 的每条 pipeline 维护一个 source chunk 和两个 scratch chunk，同一 pipeline 的 source、operator、sink 在同一个 backend 内连续执行。
- `TupleDataCollection`、HashAgg/HashJoin/Order/Output 的持久状态使用 DSA，因此可以被多个 backend 解析。
- pipeline DAG 的 RUN、COMBINE、FINALIZE event 已经提供了天然的 phase barrier。
- HashJoin 已在 consumer RUN 完成后显式释放 build payload；其他 breaker 的细粒度回收仍不完整。

### 2.2 2026-06-18 之后设计变慢的原因

历史 profile 表明，主要问题不是单个 mutex，而是将原本连续、本地、粗粒度的计算变成大量细粒度 managed block 操作：

```text
allocate block
  -> create shared descriptor
  -> shared accounting
  -> pin
  -> update
  -> dirty/unpin
  -> eviction bookkeeping
```

Q18 的历史数据中：

- `agg_update_local` 从约 5.44s 增长到约 41.20s；
- `agg_combine_global` 从约 2.06s 增长到约 124.69s；
- allocation 约 168,000 次，累计 allocation bytes 约 9.2GB，但 peak resident 仅约 1.25GB；
- 没有显著 promotion copy 或实际内存压力，说明主要成本是分配 churn、DSA/descriptor 元数据、原子记账、pin/unpin 和碎片化 combine。

Q9 的历史 flame graph提供了同样的证据：旧 managed collection 路径中
`YaapManagedTupleCollectionMaterializeSegments` 一度占 worker 样本约 39%，改成 direct resident
segment read 后降到约 1%。因此正常 resident 路径必须做到零 snapshot、零 artifact replay、零
materialize；spill encoding 只能在真实内存压力下出现。

另一个结构性问题是表示反复变化：

```text
local live TDC/AHT
  -> artifact snapshot
  -> segmented collection
  -> replay scratch TDC
  -> global live TDC/AHT
  -> source TDC
```

这里的 publish 实际是 encode/copy，combine 实际是 decode/replay。新设计必须让 publish 成为一次状态转换和 ownership handoff，而不是数据格式转换。

## 3. 目标与非目标

### 3.1 目标

1. 保持当前 pipeline 内 DataChunk 快路径，不为短生命周期数据支付跨进程管理成本。
2. 给所有 pipeline breaker 提供统一的 shared segment 表示。
3. 明确每类内存的 owner、可见范围、回收点和异常清理责任。
4. 让 local writer 无锁写入，seal 后跨 backend 只读。
5. 避免 local -> shared 的常态全量复制。
6. 支持 query memory limit、主动回收和大数据量 spill。
7. 让 HashAgg/HashJoin/Order 能按 operator 语义决定 spill，而不是由通用 BufferManager 随机驱逐活跃状态。
8. 保持 PostgreSQL `MemoryContext`、DSM、DSA、`SharedFileSet` 和 `PG_TRY/PG_CATCH` 的资源纪律。

### 3.2 非目标

1. 不缓存 PostgreSQL heap page；SeqScan 继续使用 PG buffer manager/ReadStream。
2. 不让多个 worker 同时写同一个 segment。
3. 不让每次 row lookup 都经过通用 pin/unpin API。
4. 不通过 `shm_mq` 传输 breaker 的大块数据。
5. 不在第一阶段实现通用异步 spill、压缩或跨查询 cache。
6. 不改变“YAAP 接管后失败即报错”的 no-silent-fallback 原则。

## 4. 内存分类

| 类别 | 示例 | 存储位置 | 是否共享 | 是否 spill | 回收单位 |
|---|---|---|---|---|---|
| Ephemeral chunk | source/scratch `DataChunk` | worker `MemoryContext` | 否 | 否 | chunk lease |
| Operator scratch | hash probe arrays、selection、临时 sort index | worker `MemoryContext` | 否 | 否 | task/event |
| Writable breaker segment | local HashAgg/HashJoin/Order 输入段 | DSA slab | 物理共享、单 writer | 仅 operator seal 后 | segment |
| Sealed breaker segment | published local/global partition | DSA slab | 多 reader | 是 | segment |
| Shared directory/control | descriptor、segment table、event state | DSM/DSA | 是 | 否 | query/operator |
| Spill extent | sealed segment 的外部 backing | `SharedFileSet` | 是 | 已在磁盘 | extent/file |
| Reconstructible state | bloom filter、部分索引目录 | private 或 DSA | 视用途 | 可丢弃重建 | operator phase |

这个分类替代旧设计中按 `COLUMNAR_CHUNK`、`TEMP_PARTITION` 等 block kind 对所有对象套用相同 pin/evict 语义的方式。

## 5. 总体架构

```text
Query leader
  |
  +-- Query DSM ---------------------------------------------------+
  |    PipelineSharedControl                                      |
  |    task queue / event states / worker slots                   |
  |    QueryMemoryCoordinator                                     |
  |    SharedFileSet                                              |
  |    shm_toc -> DSA control region                              |
  |                                                               |
  |    DSA variable storage                                       |
  |      operator directories                                     |
  |      worker slab descriptors                                  |
  |      segment descriptors                                      |
  |      slab payloads                                            |
  |      partition publication lists                              |
  +---------------------------------------------------------------+
         ^                         ^
         | dsa_pointer/handle      | dsa_pointer/handle
         |                         |
  worker backend 0           worker backend N
    LocalChunkPool             LocalChunkPool
    local operator state       local operator state
    local slab cache           local slab cache
    local accounting credit    local accounting credit
```

控制面和数据面必须分离：

- DSM 中的控制面只做任务、phase、owner、目录 root、budget 和错误传播。
- DSA 中的数据面按 slab/segment 粗粒度分配。
- worker 本地保存已解析的指针和 free list cache，但这些指针绝不发布给其他 backend。
- 跨 backend 发布只使用 `dsa_pointer` 或包含 generation 的逻辑 handle。

## 6. DataChunk 生命周期与回收

### 6.1 DataChunk 不进入 BufferManager

当前 `PipelineRunTask` 在同一 backend 中执行：

```text
Source.GetData(src_chunk)
  -> Operator.Execute(src/scratch)
  -> ...
  -> Sink.SinkChunk(final_chunk)
```

这条链没有跨进程边界。把 `DataChunk` 放进 DSM/DSA，只会增加共享分配、原子操作和 cache-line 争用，不会减少复制。

建议增加 worker-local `LocalChunkPool`：

```cpp
struct ChunkLease {
    PipelineChunk *chunk;
    uint32_t slot;
    uint32_t generation;
};

class LocalChunkPool {
public:
    ChunkLease Acquire();
    void Release(ChunkLease lease);
    void TrimStringArenas(uint64_t target_bytes);
};
```

它由 worker query `MemoryContext` 拥有，正常查询结束由 `MemoryContextDelete` 兜底释放。

当前 `PipelineChunk` 将 16 组不同物理类型列、null bitmap 和四套 dictionary index全部内嵌，
单个对象约 1.41MiB；每条已触达 pipeline 常驻 source/scratch A/scratch B 三个对象，约
4.23MiB/backend/pipeline，SeqScan filter 还会额外持有一个 chunk。因此新 pool 不只是减少
allocation，它还必须在 RUN event 真正结束时归还 lease，不能等到 query teardown。

### 6.2 每个 source chunk 的精确生命周期

1. RUN task 从 `LocalChunkPool` 取得 `src_chunk`、`scratch_a`、`scratch_b`。
2. SeqScan 填充 `src_chunk`。其中只保存 YAAP 自己拥有的值：定长值直接复制，字符串写入 chunk 自己的 string arena；不得长期保存 PG buffer page 内地址。
3. operator 只在当前调用链内读取 input，并把输出写入另一个 lease。
4. 如果 operator 返回 `HAVE_MORE_OUTPUT`，其 local state 可以在下一次调用中继续引用当前 input，但该 lease 仍归当前 RUN state 持有，不得回池。
5. 当整个下游返回 `NEED_MORE_INPUT`，表示没有 operator 再持有该 input/scratch 的地址；此时调用 `reset_lightweight()` 并复用 lease。
6. RUN event 完成或 pipeline 不再需要这些 chunk 时，将 lease 归还 worker pool。
7. query 结束时销毁 pool；异常路径依赖 worker `MemoryContext` 清理。

### 6.3 reset 与实际释放

steady state 不对 DataChunk 做 `pfree/new`：

- 定长列数组原地复用；
- selection 和 dictionary metadata reset；
- string arena `clear()` 保留 capacity，以避免每批重新分配；
- 当单个 string arena 曾异常膨胀，或 query memory coordinator 发出 local trim 请求时，才释放/缩小 arena；
- null bitmap 应按实际写入列/行重置，后续可以用 validity generation 优化，但不作为第一阶段前置条件。

local source/operator state可以在 RUN结束时立即释放；breaker local sink state必须保留到该
worker的所有 partition combine任务完成。实现上应为 local sink维护 pending partition计数，
归零后立即释放，而不是把所有 pipeline state拖到 query结束。

### 6.4 SeqScan 与 PG buffer 的关系

SeqScan 从 PostgreSQL `ReadStream`/buffer manager 取得 heap page；PG buffer pin 的生命周期仍由 PG scan API 管理。YAAP 的 DataChunk 只保存 deform 后的数据，不持有 PG buffer pin。

因此：

- “DataChunk 流过 pipeline 后回收”由 `LocalChunkPool::Release/reset` 完成；
- “heap page 什么时候 unpin”由 `ReadStream`/heap scan 完成；
- 二者不应被合并成一个 YAAP BufferManager block。

## 7. Shared Segment Substrate

### 7.1 为什么使用 segment，而不是 flat TDC 或通用 block

flat TDC 扩容需要 allocate + copy + free，且单个 allocation 可达到 GB 级；旧 BufferManager 的 512KB block 又导致 descriptor、pin 和 accounting 数量过多。

新 substrate 使用两层粒度：

- **slab/superblock**：DSA 的实际粗粒度 allocation 和 query budget charge 单位；
- **segment**：算子逻辑上的 append、seal、publish、spill、consume 和 reclaim 单位。

初始参数建议仅作为测量起点：

- slab：8–16MB，lazy allocation；
- tuple segment：1–4MB；
- worker 初始只取得一个小 credit，不预分配 `worker_count × 128 × 512KB`；
- 后续按实际使用几何增长。

slab 在 query 内通常不归还给 DSA，而是把已 retired segment 放回 worker/query free list；query 结束再随 DSA 一次性释放。这样避免 DSA allocator churn，同时仍能在 query 内复用内存。

collection root必须稳定：grow只能 seal current segment并追加 successor，不能替换 root，不能让
descriptor从旧 flat payload切换到新 flat payload。稳定 root是避免跨 backend stale pointer/UAF的
基础。

### 7.2 逻辑 handle

所有长期引用使用稳定 handle：

```cpp
struct SegmentHandle {
    uint32_t arena_id;
    uint32_t segment_id;
    uint32_t generation;
};

struct RowHandle {
    SegmentHandle segment;
    uint32_t row_index;
};
```

segment descriptor 中保存 DSA backing：

```cpp
struct SharedSegmentDesc {
    pg_atomic_uint32 state;
    uint32_t owner_worker;
    uint32_t generation;
    uint32_t layout_id;
    uint32_t row_count;
    uint32_t row_capacity;
    uint32_t heap_used;
    dsa_pointer row_pages_dp;
    dsa_pointer heap_pages_dp;
    SpillExtent spill;
    dsa_pointer next_dp;
};
```

任何 backend 在使用 handle 时，都通过自己的 `dsa_get_address()` 解析。跨进程结构中禁止保存 C/C++ 裸指针。

### 7.3 状态机

```text
FREE
  -> WRITABLE_LOCAL
  -> SEALED_LOCAL
  -> PUBLISHED
  -> CONSUMING
  -> RETIRED
  -> FREE (generation + 1)

PUBLISHED
  -> SPILLING
  -> SPILLED
  -> RELOADING/STREAMING
  -> CONSUMING
```

不变量：

1. `WRITABLE_LOCAL` 只有一个 writer，不需要跨进程锁。
2. segment 只有在写入完成、row/heap metadata 固化后才能 seal。
3. publish 使用 release 语义；consumer 以 acquire 语义观察 `PUBLISHED`。
4. `PUBLISHED` 后 owner 不再修改 payload。
5. reclaim 必须发生在最后一个可能 consumer 的 event barrier 之后。
6. generation 防止 free-list 重用后的 ABA/stale handle。
7. spill/reload 状态转换按 segment 执行，I/O 期间不持有 partition/global spinlock。

### 7.4 pin 的新语义

新设计中的 pin 不是每次访问都修改共享 pin count。

- 同一 event phase 内，pipeline DAG 已证明 segment 不会被 reclaim；consumer 只需在 task 开始时解析一次 handle，获得 process-local read window。
- 只有 spill/reload 可能与 consumer 并发时，才在 segment/window 粒度持有 lease。
- 第一版应禁止同一 segment 的 spill 和 consume 并发，通过 state CAS + event/partition ownership 简化，而不是依赖高频 reader counter。

目标是每个 segment 最多发生 O(1) 次 acquire/release，而不是每 row、每 64/256 行或每 chunk 一次。

## 8. DSM、DSA 与 SharedFileSet 的职责

### 8.1 Query DSM

继续使用当前 query-scoped DSM，并在 `shm_toc` 中加入：

- `QueryMemoryCoordinator`；
- shared arena/segment registry root；
- per-operator breaker directory root；
- `SharedFileSet`；
- retire queue 或 phase epoch；
- profile counters root。

DSM 是生命周期锚点。所有 worker 在使用共享数据前 attach；leader 必须等待 worker 退出后再 detach。

### 8.2 DSA

使用 `dsa_create_in_place(..., control_dsm_segment)`：

- PostgreSQL 会在需要时为 DSA 扩展额外 DSM segment；
- control DSM detach hook 负责 in-place DSA release；
- 所有 participant 都必须正确 attach/detach；
- query memory limit 由 YAAP coordinator 控制，不能把“DSA 还能分配”视为“查询还应继续占内存”。

当前 runtime预留 256MiB in-place DSA，但这不是 hard cap；PostgreSQL 的 in-place DSA默认仍可
按需扩展额外 DSM segment。新设计应从 `dsa_minimum_size()`/小控制区起步，并用
`dsa_set_size_limit()` 设置 shared hard cap。phase级大量回收后可由 leader低频调用
`dsa_trim()`，但禁止每个 segment retire都 trim。

DSA 适合 variable-size shared allocation，但不适合 hot path 上每个小 block 的频繁 allocate/free。所有 DSA allocation 必须被 slab 化或用于低频 descriptor/root。

### 8.3 SharedFileSet

`SharedFileSet` 放在 DSM 中：

- leader 使用 `SharedFileSetInit(fileset, seg)`；
- worker attach DSM 后调用 `SharedFileSetAttach(fileset, seg)`；
- 最后一个 participant detach 时自动删除共享临时目录；
- error path 仍可显式 `SharedFileSetDeleteAll`，但必须避免和 detach callback 重复管理产生竞态。

spill 文件建议按 `(query, operator, partition, writer)` 分文件或独占 append stream，避免所有 worker 争用一个全局 file offset。

如果采用 BufFile fileset接口，writer必须完成 write后 export/close，consumer再按名字只读 open；
extent publication发生在 export成功之后。不能让多个 backend依赖进程私有 `write_offset`向同名
FileSet file追加。

### 8.4 不使用 shm_mq 搬运 breaker 数据

`shm_mq` 适合控制消息或最终 tuple queue，不适合传输数 GB 的 HashJoin/HashAgg/Sort 中间态。YAAP 已有任务队列和 descriptor channel，因此跨进程 handoff 只需发布：

```text
operator_id + partition_id + SegmentHandle list + row_count + state
```

consumer 自己通过 DSA 映射或 SharedFileSet extent 读取数据。

## 9. Query Memory Coordinator

### 9.1 共享状态

```cpp
struct QueryMemoryCoordinator {
    uint64_t hard_limit;
    uint64_t soft_limit;
    pg_atomic_uint64 resident_bytes;
    pg_atomic_uint64 committed_bytes;
    pg_atomic_uint64 spill_bytes;
    pg_atomic_uint64 pressure_epoch;
    WorkerCredit workers[];
};
```

需要区分：

- `resident_bytes`：当前实际驻留、受 YAAP 控制的 shared segment/slab；
- `committed_bytes`：已经授予 worker/operator、尚未全部使用的 credit；
- `private_scratch_bytes`：worker 本地 chunk/scratch 的估算或阶段汇总；
- `spill_bytes`：临时文件当前占用；
- `peak_*`：profile 使用。

### 9.2 分层 credit，避免原子记账污染 hot path

worker 不应为每次 append/分配执行全局 CAS：

1. worker 以较大批次申请 memory credit，例如 8–32MB；
2. slab/segment 在本地 credit 内分配时只更新 backend-local counter；
3. credit 快用完时再访问 shared coordinator；
4. event 结束或大量 reclaim 后批量归还 credit；
5. profile counter 也在 worker 本地累积，event 结束时汇总。

### 9.3 压力处理顺序

当超过 soft limit：

1. 回收已完成 RUN 的 DataChunk lease 和过大的 string arena；
2. 复用 query 内已经 retired 的 shared segment；
3. 让 operator seal 当前可 seal 的 segment；
4. spill 已 sealed、未消费、operator 声明可 spill 的 segment；
5. 对 HashAgg/HashJoin 执行 operator-aware repartition/partition spill；
6. 若仍无法满足请求，`ereport(ERROR)`，不得退回 PG executor。

禁止策略：

- 随机驱逐仍在写的 HashAgg partition；
- pool miss 就 spill 一个小对象；
- 为了凑一个 allocation 连续触发数万次小 spill；
- 在持有 partition spinlock 时做文件 I/O；
- 为低基数 Q1 预分配数百 MB × worker 的固定 pool。

## 10. Breaker 跨进程协议

### 10.1 发布目录

每个 breaker 建立：

```cpp
struct BreakerDirectory {
    uint32_t operator_id;
    uint32_t partition_count;
    dsa_pointer partition_entries_dp;
    pg_atomic_uint32 producers_remaining;
    pg_atomic_uint32 phase;
};
```

每个 `(worker, partition)` 有单 writer publication slot。worker 完成 local sink 后：

1. seal 当前 writable segment；
2. 将 segment chain root 写入自己的 slot；
3. release-store slot state 为 `PUBLISHED`；
4. decrement `producers_remaining`；
5. 最后一个 producer 唤醒 leader/event。

发布不复制 tuple payload，不创建二次 artifact，不 replay 到 staging TDC。

### 10.2 Partition ownership

COMBINE event 以 partition 为任务单位：

- scheduler 将每个 global partition 分配给一个 combine task；
- 该 task 在 combine phase 内独占 global partition 的 mutable state；
- 它顺序消费所有 worker 发布给该 partition 的 segment；
- 不需要 global partition mutex、AHT mutex 或 64-row 锁批次；
- segment 消费完成后立即 retire；
- task 完成时 publish final partition state。

如果需要 work stealing，ownership 可以在 task 尚未开始时转移；开始消费后不得并发转移。

### 10.3 回收协议

优先使用 event barrier，而不是通用 shared refcount：

| 对象 | 最早安全回收点 |
|---|---|
| Source/scratch DataChunk | 下游返回 `NEED_MORE_INPUT` |
| local breaker segment | 对应 partition combine 已消费并确认 |
| HashAgg global segment | 下游 consumer RUN event 完成 |
| HashJoin build segment/hash table | 所有 probe consumer RUN event 完成 |
| Order input run | merge cursor 越过该 run 且无 reader |
| Output segment | leader 已发送最后一行给 `DestReceiver` |
| descriptor/layout | operator 所有 dependent event 完成；通常 query 结束 |
| slab | query 内进入 free list；query 结束随 DSA 释放 |

reclaim 操作将 segment 标记为 `RETIRED`，清除 publication slot，然后放回 arena free list并增加 generation。只有 leader/event owner 负责决定 phase-level reclaim；worker 不得根据本地观察提前 free 共享数据。

## 11. 算子映射

### 11.1 HashAggregate

local sink：

- 每个 worker 的每个 radix partition 有一个 writer-owned segmented collection；
- active AHT 是 worker-local 控制结构，row reference 使用 `RowHandle`；
- row/aggregate payload 直接写入 DSA slab 中的 writable segment，但不走共享 pin/unpin；
- segment 满后 seal，新建下一个 writable segment；
- publish 只交出 sealed segment chain，AHT 本身无需跨进程发布。

PerfectHashAgg应使用每 worker private紧凑 accumulator，RUN结束后显式 merge所有 worker slot，
或按 key range分配唯一 reducer；不能只让固定 worker的 local state进入 COMBINE。当前实现需要在
Phase 0用多 worker结果测试锁住这一点。

combine：

- 每个 global partition 由一个 combine task 独占；
- 直接读取各 worker 的 sealed aggregate rows并合并到 global AHT/segments；
- 消费完一个 local segment 就 retire/reuse；
- global state 不允许多个 worker通过锁并发更新。

spill：

- 活跃 HashAgg page不能被通用 LRU随机驱逐，因为 local AHT仍可能更新其中的 group；
- local AHT directory保持小而固定，entry只保存 `RowHandle`，aggregate row从创建时就位于按 radix
  组织的 spillable page image中；
- directory达到阈值后只 reset directory，旧 aggregate pages留在对应 radix partition并 seal；
- seal后的 page不再包含活跃 group，可由 BufferManager按页直接 spill，不需要整个 partition一起落盘；
- 后续相同 group可能在新的 delta page中再次出现，partition combine阶段统一归并 resident/spilled
  pages；
- 极端 skew或单个 partition超出 merge grant时，再进行 operator-aware recursive repartition，而不是
  让通用 LRU操作 mutable state。

这避免“pool 每缺一页就 spill 一次”的 spill storm。

### 11.2 HashJoin

build：

- 每个 worker 按 radix partition 写 keys/payload segments；
- local segment 单 writer、满后 seal；
- build RUN 结束后发布 segment chain；
- finalize task 可以直接基于所有 published segments构建 partition-local shared hash directory，或保留多 segment row handles，不必先拼接成单个 flat TDC。

probe：

- hash directory 和 build segments 在 probe phase 只读；
- worker 在 task 开始时解析 segment window，批量 probe；
- probe pipeline event 完成后，由当前已有的 post-consumer cleanup 位置统一 retire build payload；
- bloom/filter directory 可标记为 reconstructible，但不能在仍被 probe 使用时释放。

大 build side：

- partition seal 后可直接 spill；
- probe rows按相同 partition 写 spill stream；
- partition task成对读取 build/probe extent；
- 不要求整个 build side重新驻留。

### 11.3 Order

- 小数据：单 worker private sort scratch + shared output segment；
- 中等数据：输入直接写 writer-owned shared segments，private sort index引用 `RowHandle`，排序完成后发布 immutable sorted run；
- 大数据：形成 sorted run后直接写入 `SharedFileSet`，只在 DSA 发布 run descriptor；
- merge task使用每个 run 的 window reader，越过 segment后立即释放该 window；
- 不再构建一个不断倍增复制的 flat payload和单大 `sort_indices` allocation。

### 11.4 OutputSink

- worker 将最终结果写入 output segments并发布；
- leader按稳定顺序 drain；
- 每发完一个 segment立即 retire，从而避免完整结果一直驻留到 query end；
- 如果查询语义允许无序输出，可按 ready segment drain；有 ORDER BY 时按上游 run/order保证顺序。

### 11.5 TopN、CrossProduct、Distinct、Window

- TopN 小 heap/array可以保持 private；最终 N 行一次性写入 shared output segment。
- CrossProduct 的 materialized side使用与 HashJoin build相同的 sealed collection 生命周期。
- Distinct 复用 HashAgg partition substrate。
- Window/其他需要全量 materialize 的算子复用 Order 的 run/segment reader，不再创建第四套 backing。

## 12. Spill 格式与 I/O

### 12.1 跨进程发布不等于序列化

breaker 的 resident 数据如果从一开始就写在 DSA segment 的 canonical layout 中，跨进程
handoff 不需要序列化 tuple payload：

```text
producer 写 DSA segment
  -> seal
  -> release-publish SegmentHandle
  -> consumer acquire-load handle
  -> dsa_get_address
  -> 直接读取相同 payload
```

跨进程只“序列化”少量 descriptor/POD metadata，例如 operator id、partition id、layout id、
segment handle、row count和状态。不同 backend中的虚拟地址可以不同，因此任何裸指针都必须在
consumer侧根据 `dsa_pointer`重新解析，但这不是数据重编码或复制。

只有以下情况需要 resident publish copy：

1. producer故意使用 private memory构建小对象；
2.对象内部包含 backend-local指针或无法跨进程解释的状态；
3.目标 backing不是 DSA，而是 spill file。

这也是为什么本方案让大型 breaker payload直接写 owner-exclusive DSA segment，而 DataChunk仍
保持 private：前者必须跨 phase/backend，后者不会跨越同步 pipeline调用链。

### 12.2 Spill 需要稳定物理格式

spill 必须有稳定物理格式，但不意味着 spill 时必须执行格式转换。推荐让已经 sealed 的 canonical
page image同时成为内存格式和磁盘格式；落盘只写 page bytes：

```text
segment header
row pages
varlen/string heap pages
optional auxiliary pages
checksum
```

定长值使用 YAAP 明确定义的物理格式；NUMERIC继续使用带 scale metadata的 scaled integer；
字符串只能保存 segment heap的相对 offset，不能保存地址。读取时可直接构造 read window，或按
batch gather进 private DataChunk，不应先 materialize整个 flat TDC。

### 12.3 各 breaker 的序列化策略

#### HashAggregate

- aggregate result rows和 group keys直接使用 canonical page image；不写 C++ AHT对象、bucket
  裸指针或 mutex。
- 当前支持的 COUNT/SUM/AVG等 accumulator应定义固定宽度 canonical representation，包括
  `NumericWideInt`、count和 scale。
- reload/combine时从 rows重建 partition AHT；AHT是加速索引，不是持久事实来源。
- 如果未来支持 PostgreSQL任意 aggregate transition state，只有同时提供明确
  `serialize/deserialize` contract的 aggregate才能 spill；否则 admission阶段拒绝该查询，不能
  memcpy backend-local transition pointer。
- active mutable page不能直接 spill；local AHT reset后对应 pages seal，后续输入写新的 delta pages。

#### Order

- 推荐将排序后的行按最终顺序写成 sequential sorted run；这样 spill file本身就是 merge顺序，
  reload时不需要再序列化/恢复大 `sort_indices`数组。
- key和payload可在同一 canonical row中，也可使用 key page + payload row handle；无论哪种形式，
  file中只能出现稳定 ordinal/offset，不能出现地址。
- merge只打开每个 run的小 read window，消费完一个 window即可释放；不完整反序列化整个 run。
- 小 run若在 private memory完成排序，seal时允许一次性写入 DSA或 SharedFileSet；这是明确的小对象
  fast path，不是大型数据的默认 publish路径。

#### HashJoin

- spill build/probe rows和 partition metadata，不序列化内存 hash table。
- reload一个 partition后按 rows重建 hash directory/bloom；它们是 reconstructible auxiliary state。
- 如果 hash directory本身很大且重建代价经 profile证明不可接受，可以定义独立 versioned auxiliary
  page格式，但不能直接 dump包含 `dsa_pointer`或进程地址的结构。

#### Output/其他 breaker

- Output segment已经是 canonical rows，可直接顺序写/读。
- Distinct沿用 HashAgg格式；Window沿用 sorted run/partition格式；CrossProduct沿用 HashJoin build
  row格式。

### 12.4 版本与兼容性

spill数据只在一次 query生命周期内使用，不要求跨 PostgreSQL重启长期兼容，但仍必须带：

- format magic/version；
- layout/schema fingerprint；
- endianness和宽度约束；
- operator/partition/segment generation；
- payload lengths和 checksum。

发现 version、layout或 checksum不一致时必须 `ereport(ERROR)`。即使文件是临时的，这些字段也能
防止错误 extent、stale handle或部分写入被当成有效数据。

每个 spill extent至少包含：

```text
magic/version
query/operator/partition/writer id
layout id + schema fingerprint
segment id + generation
row_count / row_width
row bytes / heap bytes
checksum
```

规则：

1. spill 只接受 sealed segment。
2. spill顺序严格为 write + flush/export -> publish extent/state -> 释放 resident backing。
3. spill 改变 backing，不改变 collection/segment/row 的逻辑 identity。
4. reader 优先流式读取固定大小 window，不要求整个 segment rehydrate。
5. worker 独占写自己的 file/extent offset，避免全局 offset lock。
6. 使用 PG `FileRead/FileWrite` 或 BufFile API并登记 wait event。
7. 第一阶段使用同步 I/O；异步 prefetch只在 correctness和回收协议稳定后加入。
8. temp limit和 resident limit分开；temp limit耗尽应明确报错。

### 12.5 DuckDB 风格的 Canonical Spill Page Image

推荐把“内存格式与磁盘格式几乎一致”定为第一版 spill substrate 的正式方案，而不是后续优化。
目标不是严格意义的 zero-copy I/O，而是：

- 零 row-by-row serialization；
- 零 artifact encode/decode；
- 零中间 staging TDC；
- spill时直接写 sealed page image；
- reload时直接读入最终 resident buffer或单次消费 buffer。

操作系统仍会在用户 buffer、page cache和存储设备之间搬运字节，因此不能声称物理 zero-copy。
消除的是 YAAP 自己的格式转换和额外内存复制。

#### Page image 与 runtime metadata 分离

不能直接 dump整个 DSA allocation，因为 DSA allocator header、锁、atomics、owner状态和
`dsa_pointer`都不是磁盘格式的一部分。每页分成两部分：

```cpp
// 只存在于 shared runtime，不落盘
struct SpillPageRuntimeDesc {
    pg_atomic_uint32 state;
    uint32_t owner_worker;
    uint32_t generation;
    dsa_pointer resident_image_dp;
    SpillExtent extent;
};

// 内存和磁盘完全相同
struct SpillPageImage {
    SpillPageImageHeader header;
    uint8_t payload[PAGE_IMAGE_BYTES - sizeof(SpillPageImageHeader)];
};
```

`SpillPageImageHeader` seal后不可变，至少包含：

```text
magic / format_version / page_kind
layout_id / operator_id / partition_id
page_id / generation
tuple_count / payload_used
linked var-page ids or segment metadata id
header checksum / optional payload checksum
```

runtime descriptor可以在 spill/reload时改变 state、resident pointer和 extent；page image本身不变。

#### 固定页与大对象

参考 DuckDB，steady-state中间态优先使用固定大小页。初始建议对 256KiB、512KiB进行 benchmark，
而不是先硬编码最终值：

-固定页可以放入 grouped spill file，`file_offset = block_index * page_size`；
- resident frame可以直接复用到下一次 reload；
- partial last page仍按完整 page image写入，换取简单 offset、批量 I/O和无 size decode；
-超大 string、hash directory等少数对象可使用 variable-size extent，但不能成为常态。

DSA侧仍按 8–16MiB slab批量申请，再切成固定 page frame；这样保留 DuckDB式固定页 I/O，
同时避免旧 YAAP为每个 512KiB page执行一次 DSA allocate和 descriptor创建。

#### Canonical row/var page 布局

breaker中间态使用 row-major固定宽度 row page；varlen数据使用独立 var page：

```text
ROW_PAGE
  fixed rows:
    null bits
    fixed values
    aggregate states
    string {len, prefix, var_page_id, var_offset}

VAR_PAGE
  append-only bytes
```

与 DuckDB单进程实现不同，YAAP不能在 shared/disk row中保存真实指针，因为各 PostgreSQL backend
的 DSA映射地址不同。推荐始终保存 `(page_id, offset)`；backend在 page/task级 pin后通过本地
page table把它解析为 `base + offset`。可在 process-local cache中保存 swizzled base pointer，
但不得回写 shared page image。

当前 `VecStringRef` 已经采用 length/prefix/offset思路；shared row格式需要把单 arena offset扩展为
`var_page_id + offset`，即可同时满足跨 backend和直接 spill。

#### Direct spill 路径

```text
OWNER_WRITE
  -> seal header/payload
  -> SEALED_RESIDENT
  -> CAS SPILLING
  -> FileWrite/FileWriteV(page image)
  -> publish {file, block_index}
  -> SEALED_SPILLED
  -> resident frame进入 free list
```

这里没有 tuple walker，也没有重新生成 string heap。多个连续 page可用 PostgreSQL
`FileWriteV`合并系统调用。每个 writer拥有自己的 append stream或预分配 extent范围，不能争用一个
进程私有 write offset。

#### 两种 reload 模式

1. `STREAM_ONCE`
   -用于 HashAgg partition combine、Sort merge、Output drain；
   - `FileRead/FileReadV`直接读入 worker-local reusable page frame；
   -消费完立即复用 frame，不重新发布 shared resident page；
   -适合单 owner顺序消费。

2. `REHYDRATE_SHARED`
   -用于 HashJoin build page等会被多个 probe worker随机读取的数据；
   -一个 elected loader分配 DSA page frame并直接读取 page image；
   -release-publish新的 `resident_image_dp`；
   -整个 probe phase持有 phase lease，probe event结束后统一释放。

这两个模式避免“一律 reload成 shared TDC”，并根据最后消费者数量选择最低成本路径。

#### Hash 目录与 sort index 是否一起落盘

- HashAgg local AHT directory不落盘；page中 aggregate rows是事实来源，combine时重建小目录。
- HashJoin build hash directory默认不落盘；如果 rebuild成本经 profile证明显著，再定义独立
  `HASH_INDEX_PAGE`，其中只保存 page/row handles。
- Sort不落盘进程内 pointer index；输出 run按排序顺序写 page image，merge只需要 run descriptor。
- Bloom filter通常重建；只有重建成本大且尺寸稳定时才作为 `AUX_PAGE`直接落盘。

原则是只把“未来读取所必需且本身已经 canonical”的内容写入 page image，避免为了省 rebuild
而重新引入复杂 serialization。

#### 压缩策略

DuckDB当前临时文件路径会根据页面和自适应策略选择直接写未压缩 buffer或压缩后写入。YAAP第一版
建议：

-默认 uncompressed direct page-image spill，优先消除 CPU serialization瓶颈；
-保留可选 adaptive compression，但它属于另一条 backing codec；
-只有预计节省的 I/O时间大于 compression CPU时间时才启用；
-同一 page descriptor明确记录 `RAW_PAGE_IMAGE`或 `COMPRESSED_PAGE_IMAGE`；
-不能把 compression强制放进所有 spill路径，否则会再次使 spill CPU-bound。

#### 与 DuckDB 的相同点和差异

相同点：

-绝大多数临时中间态使用固定大小、可直接 spill的 page；
- row和 varlen数据分页；
- page不再使用时尽早销毁或回收；
- HashAgg使用小型 local pre-aggregation directory和 radix-partitioned spillable pages；
-内存足够时不发生 I/O，内存不足时由 page substrate选择 sealed victim。

YAAP必须做的适配：

- DuckDB worker是同一进程内线程，可以使用并在 reload后修正指针；YAAP是 PostgreSQL多进程，
  canonical image必须使用 page id/offset，不能保存可回写的进程地址；
- DuckDB有进程级统一 buffer pool；YAAP使用 query DSM/DSA和 per-worker local cache，跨进程同步
  必须限制在 page状态和 phase边界；
- spill file必须由 DSM中的 `SharedFileSet`管理，使 leader和 background workers具有共同命名空间
  和异常清理语义。

参考资料：

- [DuckDB Unified Memory Management / Robust External Hash Aggregation](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf)
- [DuckDB StandardBufferManager](https://github.com/duckdb/duckdb/blob/main/src/storage/standard_buffer_manager.cpp)
- [DuckDB TemporaryFileManager](https://github.com/duckdb/duckdb/blob/main/src/storage/temporary_file_manager.cpp)

## 13. 锁与原子操作规则

允许的同步点：

- publication slot 的一次 release/acquire；
- event task counter；
- worker memory credit的低频批量申请/归还；
- segment状态转换；
- SharedFileSet自身内部同步。

禁止的同步点：

- 每 row append 的跨进程 spinlock；
- 每 AHT lookup 的 global mutex；
- 每 DataChunk 的 shared accounting CAS；
- 每 64/256 行 acquire/release global partition lock；
- lock-held spill/reload；
-用 shared descriptor registry hash lookup处理 steady-state row access。

## 14. 正常与异常清理

### 14.1 正常路径

1. source chunk在 RUN 中循环复用。
2. RUN event结束，释放该 pipeline 的 chunk lease/local scratch。
3. COMBINE partition消费完 local segment，立即 retire。
4. consumer RUN结束，调用 operator-specific post-consumer retire。
5. leader输出后 retire output segment。
6. worker收到 shutdown，先释放 local lease/cache，再 detach DSA/DSM。
7. leader等待所有 worker退出，最后 detach query DSM；DSA扩展段和 SharedFileSet由 PG 生命周期机制清理。

### 14.2 ERROR/取消路径

- worker先把错误写入 `PipelineSharedControl`，再退出；
- leader设置 `shutdown_requested`，唤醒并等待所有 worker；
-在任何 shared payload仍可能被 worker访问时不得 detach DSM；
- C++ 对象放在 query `MemoryContext`，避免依赖 `ereport(ERROR)` 跨越的普通栈析构；
- DSM/DSA/SharedFileSet使用 PG detach callback作为最终兜底；
-显式 retire失败不能阻止 query teardown，但必须记录 leaked segment计数用于测试断言。

## 15. 可观测性

统计必须由 worker local累计、event末尾批量合并。至少记录：

- chunk pool：首次 allocation、reuse、peak leases、string arena peak/trim；
- slab：allocation count/bytes、free/reuse count、fragmentation；
- segment：created、sealed、published、consumed、retired、reused；
-每个 operator/partition 的 resident peak；
- memory credit grant/return/denial；
- spilled/reloaded/streamed bytes和 extent数；
- publication等待、partition task执行时间；
- DSA allocation次数；
- stale generation、非法状态转换、query-end live segment数。
- DSA实际 mapped/allocated bytes与 logical payload bytes，避免 allocator fragmentation被遗漏。

profile 必须能证明：

- SeqScan -> pipeline steady state没有 DSA allocation；
- local HashAgg update没有共享 pin/unpin；
- combine没有 global partition锁等待；
-每个 segment至多常数次 publish/acquire/reclaim；
- query结束 live leases/segments为零。

## 16. 实施阶段（本文件仅设计，不实施）

### Phase 0：锁定基线和生命周期测试

- 保存当前 22-query SF10三次中位数基线；
-增加 DataChunk lease、operator input retention、event cleanup的回归测试；
-加入 query-end资源零泄漏断言和 fault injection点；
-不引入新的 BufferManager。
-记录当前已知风险：PerfectHashAgg grow的旧 payload/registry更新、所有 worker local state是否
  完整 combine、Output global flat TDC长期驻留，以及 flat TDC在 `DSA_ALLOC_ZERO` 下的大块清零；
  若未来解除 Output `MaxThreads=1`，还必须先消除并发 grow/root替换风险。迁移时不能把这些行为
  当作新 substrate的正确行为保留。

### Phase 1：worker-local ChunkPool

-将 `ProcessPipelineExecState` 的三个持久 `unique_ptr<PipelineChunk>`改为 lease；
-在 RUN完成/COMBINE边界归还；
-验证 Q1/Q6/Q18和 22-query无回退。

### Phase 2：SharedSegmentArena 基础层

-在现有 runtime DSM/DSA上增加 slab、segment descriptor、generation和 publication slot；
-只做 single writer -> seal -> read -> retire；
-先用 synthetic测试覆盖跨 backend attach和异常清理。

### Phase 3：HashAgg 首个迁移

- local partition直接写 shared segment；
- partition-owned combine；
-默认 publish路径不得 encode artifact；
-先支持 resident，再加入 partition spill。

### Phase 4：HashJoin

-迁移 build keys/rows和 hash row refs；
-保留现有 consumer RUN 后释放位置；
-加入 external partition path。

### Phase 5：Order/Output

-引入 sorted run和 streaming output segment；
-接入 SharedFileSet external merge；
-移除 flat grow/copy路径。

### Phase 6：统一 pressure policy

-接入 query credit、soft/hard limit和 operator-aware spill；
-删除旧 artifact/replay和通用 per-block hot-path管理；
-完成 SF10/SF100、取消、OOM和 worker crash测试。

## 17. 验收标准

功能：

1. TPC-H 22条结果正确，连续多轮无 crash/hang。
2. Q15保持成功。
3.低 memory limit下 HashAgg、HashJoin、Order能 spill并完成。
4.取消、statement timeout、worker ERROR后 DSM/DSA/temp file都被清理。

性能：

1.第一阶段不得让 Q1/Q6相对当前快路径回退超过 5%。
2. Q18不得重现 6.18版本的 allocation/combine放大。
3. steady-state SeqScan每个 chunk零 shared allocation、零 shared pin/unpin。
4. DSA allocation次数应与 slab/segment数量相关，而不是与 tuple/chunk数量相关。
5. 22-query共同成功项的整体几何平均不得显著回退。

内存：

1. query memory hard limit有明确、可复现的执行语义。
2. segment消费后可在同一 query内复用，resident peak不随已完成 pipeline累计。
3. worker数量增加时初始内存不能按固定大 pool线性爆炸。
4. SF100不要求全量中间态驻留。

## 18. 被拒绝的方案

### 所有 DataChunk 都放进 DSM/DSA

拒绝原因：pipeline内部没有跨进程 handoff，shared allocation和同步纯属额外成本。

### local 全部使用进程私有内存，publish 时统一复制

拒绝原因：实现简单，但大型 breaker会产生一次不可忽略的全量 copy和双份 peak memory；只允许作为小结果或过渡路径。

### 多 worker 直接写一个 global hash table

拒绝原因：锁/cache-line争用会重新制造 6.18 的 combine问题；partition ownership更符合现有 event/task模型。

### 固定每 worker 预分配大量 shared page

拒绝原因：低基数查询也会按 worker数支付大额 resident footprint；应 lazy + credit + geometric growth。

### pool miss 立即 spill 一个 partition/page

拒绝原因：历史实验产生约 80,360次 spill，形成 spill storm；压力处理必须按目标释放量批量决策。

### publish 时编码 artifact、combine 时 replay

拒绝原因：把正常 handoff变成 encode/decode/copy；artifact只能作为 spill backing格式，不能是内存内 publish协议。

## 19. Review 时需要确认的决策

建议 review 重点确认以下五项，其他参数可在实现阶段通过 profile调整：

1. 是否接受“DataChunk 永远 backend-private，breaker segment才 shared”的总边界。
2. 是否接受 local breaker payload直接写 DSA slab，以 single-writer ownership换取零 publish copy。
3. 是否接受 COMBINE按 partition独占，而不是多 worker加锁写 global state。
4. 是否接受 HashAgg采用 bounded local AHT + radix page image：通用 manager只驱逐 sealed page，
   recursive repartition仍由 HashAgg决定。
5. 是否接受 DSM作为生命周期/控制面、DSA作为 variable shared data、SharedFileSet作为 spill backing的三层 PG 原生组合。

确认这些边界后，才进入 Phase 0测试设计和分阶段实现；本方案当前不包含任何执行器代码改动。
