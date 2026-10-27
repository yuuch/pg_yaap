# Hybrid Radix Hash Join

`pg_yaap.hash_join_partition_probe=on`（默认）且配置多个worker时，执行器使用分批驻留的Hybrid Hash Join。优化器和Join顺序没有修改。同一驻留批次的多个radix partition共用一张全局hash table。不同worker并行构建该表，批次Probe全部排空后才释放。

## 执行流程

1. **Build Sink / Combine**：worker本地分区，将行写入DSA segment链；Combine发布各分区的共享描述符。此时不为全部分区建表。
2. **选择首批驻留分区**：按整批Build segment容量和共享目录真实大小贪心选取分区。目录包含各独占区间pow2容量之和的bucket heads、links、fingerprints、紧凑行引用和segment目录；多个hybrid共享query级reservation。
3. **并行准备首批**：每个参与worker先对整批key/payload segment取得读lease，完成pin barrier。worker领取partition构建任务，在各自entry范围写入引用，写入同一bucket数组内自己独占的区间。区间按各partition实际行数选取pow2容量，使用offset+hash低位寻址，Build/Probe采用同一映射；不再做原子exchange。建表barrier后才能Probe。没有每行全局互斥锁。
4. **首遍Probe**：属于驻留批次的输入直接匹配并进入下游；其余行按partition写入Probe segment。全部分区驻留时直接使用上游DataChunk，部分驻留时压缩选中行。
5. **后续批次**：所有参与任务到达EOF且排空可恢复输出后，释放各自整批lease。第二个barrier后一个CAS胜者销毁共享表并释放对应Build分区，再选择下一批、并行建一张共享表、读取对应Probe链。每批都等待全部Probe输出完成再销毁，不重扫Probe基表。
6. **下游完成一次**：所有延迟输出都通过原来的下游算子链。下游local sink只保留一份，RUN真正结束后只执行一次Combine/Finalize。

首遍不再无条件将全部Probe输入物化，也不再在Probe输入收尾时将全部Build segment强制Spill。未选中的Build和deferred Probe仍可spill/reload。当前批次的全部Build segment在表存活期间必须驻留，不能使用segment window轮换。

## Pipeline集成与同步

PhysicalHashJoinProbe安装为中间算子；原有source/sink方法作为暂存链发布和deferred pair读取的内部复用接口。PipelineRunTask持久保存source_exhausted和final_op_index，源EOF后依次调用hybrid的FinalExecute，并通过同一个DrainOperatorSuffix发送输出，维持32-chunk执行预算。

多个hybrid串联时从左向右排空。前面的延迟输出仍可进入后面算子的正常Execute；后面算子必须等全部worker都到达自己的EOF后才能释放驻留表。barrier计数使用实际调度的RUN任务数，不能使用配置worker上限；空输入worker也参与。

共享结构只有POD计数器、mask、状态和DSA地址。C++容器、chunk、lease列表及spill reader保持进程私有。atomic计数发布配合显式PG读写屏障，保证ARM下hash目录、Probe chains和ready状态的可见性。新增同步在partition/EOF粒度，不在每行争抢全局锁。

提前结束会发布abandoned，解除其他worker的等待；lease在RUN完成前释放。PG错误清理回调释放当前worker的lease。Query Cancel或worker异常依赖现有query shutdown和DSM生命周期，不添加静默回退。原有预算敏感RUN admission gate也覆盖hybrid中间算子，以免不同重型Pipeline各持预算互相等待。

## 预算与边界

初始预算以min(hard/2,soft)为上界，并扣除固定驻留内存、hard/4预留和其他hybrid的逻辑reservation。reservation不是实际内存分配；实际目录/segment仍通过原有query buffer/DSA预算收费。首批选择按partition顺序贪心，不依赖未知的Probe分布。

首批可为空，随后按批读取暂存Probe。后续单分区可超过soft quota，但必须整体装进剩余hard预算；如果单分区加共享表也无法装入，会明确报需要递归重新分区的错误。目前尚未实现递归radix repartition，不会悄悄回退到逐segment换入。选择器支持至多64个partition，现有规则最多32个。

新增的混合输入chunk及其字符串arena按容量预收费，在EOF释放；原有基础设施的其他私有内存计费口径未重构。没有新的buffer manager或分配器。含Hybrid的Pipeline为每个算子保留稳定输出chunk，避免双scratch轮换覆盖可恢复Join的输入；复用原有LocalChunkPool并对活跃输出的固定容量收费。pool归还lease后仍保留高水位内存，收费不是进程RSS；原有pool字符串arena的计费口径未在本次重构。

单worker或显式关闭partition_probe仍走原有非hybrid路径。这是既有配置选择，不是错误时fallback。

## Hash table的key和value

bucket由完整Join Key的hash定位，16-bit fingerprint过滤后仍比较完整key。冲突以entry索引链连接；同一批次共享一套表及entry数组，bucket区间按partition隔离；没有独立的每分区表对象、分配或释放。

value是8字节`{segment_index:uint32, row_offset:uint32}`，offset相对TDC segment payload基址。共享segment目录保存DSA payload地址；Probe直接解析地址，不经过worker slot/local row映射，不Acquire或reload Build segment。key与payload物理TDC不同则各有一个8字节引用；二者相同时两数组别名，只保留一份。构建转换完成即释放原始selected row-ref表。segment身份/generation在建表时校验；连续访问同一segment时复用描述符校验，逐行保留句柄身份及行边界检查。key/value数组别名时只转换一次。每个reader自己的整批lease保护引用生存期。

## Profile

保留旧计数，并新增：

- `hash_join_batch_tables`：实际创建的批次共享表数。
- `hash_join_batch_partitions`：各批包含的partition数之和。
- `hash_join_batch_direct_refs`：构建的entry数，并非Probe命中数。
- `hash_join_batch_collisions`：插入时bucket已非空的次数，包含重复key。
- `hash_join_hybrid_direct_rows` / `deferred_rows`：首遍直接处理／暂存的输入行数。

Timeline Task active包括等待，不是CPU时间；worker累计时间不能当Query wall time。

## 验证与限制

本轮39项专项、32项内存回归（256MB配置）、17项Timeline及异常路径测试通过。专项覆盖1/4/12 worker、全驻留/部分驻留/无首批、复合键、字符串、重复键、可恢复输出、串联Join、key/value引用别名、取消后复用。全驻留fixture确认8个partition只建1张表；部分驻留建2张，无首批低预算建8张。

异常worker退出由Leader检测并发起shutdown；清理回调不能提前设置全局shutdown，否则会跳过现有退出检测。小型batch同步元数据保留到query teardown，避免等待者访问已释放内存。

现有旧路径的nullable key、LEFT数值NULL、单worker LEFT空Build、展开型串联Join及大LIMIT reload问题未扩大修复。LIMIT测试验证有界结果，未单独证明真实提前EOF。未实测Linux或全部TPC-H。私有chunk pool高水位与RSS计费差异仍存在。

最新Q21测量见`artifacts/batch-q21/report.md`。测试工件：`artifacts/batch-join-verified`、`artifacts/batch-memory-tests`、`artifacts/batch-profile-verified`。

## 独占bucket区间优化验证

42项专项（含单bucket倾斜及空分区）、32项内存回归、17项Timeline/异常检查通过。最新Q21无profile独立三轮6.897/7.327/7.932秒，中位7.327秒；详细profile8.890秒。结果100行及顺序一致。详细数据及测量限制见`artifacts/batch-q21/owned-buckets-report.md`。每个区间至少16个bucket；容量按分区分别向上取整，可能改变批次选择，已按实际分配容量收费。

## Q22：无等值key的标量比较

Hybrid只安装到左右均有等值key的Join。Q22的`c_acctbal > AVG(...)`没有等值key，使用已有YAAP flat Build和残余谓词执行；`NOT EXISTS`的等值ANTI Join继续使用Hybrid。此前无条件安装Hybrid，会将flat单行AVG结果误读为空的radix registry，导致返回0行。现在既在BuildPipelines选择时排除keyless Join，也在Hybrid初始化时断言Build确实为多分区direct-segmented表示，避免再次静默丢行。

新增标量比较及其与ANTI Join串联的回归，在1/4/12 worker与PG逐行比较，专项累计48项。复现与修复证据见`artifacts/q22-hybrid-debug`。

## Q13 / Q18：扫描领取与驻留预算

Q13 customer只有35984页，旧领取量8192页导致约5个worker拥有Probe输入，其余提前等EOF。ReadStream现在按表页数/（4×worker数）向下取pow2限制领取量，至少保留io_combine_limit大小的连续运行；customer使用512页。此处调整的是worker独占的工作量，不调整effective_io_concurrency或合并读取。不能为每个worker提供四段合并读取的小表维持旧领取方式，避免几百页表增加大量producer尾块；256MB独立分支回归覆盖该边界。PG原有原子领取及尾部ramp-down保留。

Q18保留原有Build驻留预算和Probe块满/Combine spill策略。暂存行始终按partition及producer分链。当前批次mask内的每条producer链可独立领取，同一partition可由多个worker并行Probe共享表；next_slot/active_chains保证唯一领取及完全排空。旧hash_join_partition_pairs仍统计partition，新增hash_join_probe_chains统计链。

每个worker仅预取所属链前方至多4个segment、合计4MiB范围，固定私有ring随消费滚动。通过YaapSpillStore::PrefetchBlock复用PG FilePrefetch和原有读文件cache；macOS为F_RDADVISE。它是内核预读提示，不提前分配或pin DSA，不预取非当前batch，不保证IO已完成；正常Acquire/ReadBlock及校验保持权威。hash_join_probe_prefetch的calls为尝试次数、rows为OS接受提示次数。4MiB约束待消费提示窗口，而非OS缓存硬上限。

提高Build预算和保留Probe块的实验未得到稳定收益，均已撤回。最终未更改分配器、scheduler或optimizer。

profile增加hash_join_wait_pin/directory/build/prepared/scan/release/cleanup/drain，分别标记同步等待；hash_join_batch_pin计Acquire（含reload），hash_join_batch_cleanup计单个清理者实际工作。上述阶段与原hybrid_wait等计时嵌套，不可重复累加。无新的全局锁、分配器或调度框架。

最终修改及测量见`artifacts/partition-probe-prefetch/report.md`。48项专项、32项内存、17项异常测试通过，并重跑完整22条、逐行集合与修复前输出比较。
