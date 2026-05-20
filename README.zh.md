# README.zh.md

# pg_yaap — Yet Another Analytic Processing ⚡

<p align="center">
  <img src="imgs/logo.png" alt="pg_yaap logo" width="480" height="480"/>
</p>

<p align="center">
  <strong>把现代 OLAP 优化器和 C++ 列式执行引擎带进 PostgreSQL。</strong>
</p>

<p align="center">
  <a href="README.md">🇺🇸 English</a>
  ·
  <a href="LICENSE">📄 License</a>
  ·
  <a href="#快速开始-">🚀 快速开始</a>
  ·
  <a href="#架构-">🏗️ 架构</a>
  ·
  <a href="#tpc-h-快照-">📊 Benchmark</a>
</p>


---

## pg_yaap 是什么？🚀

`pg_yaap` 是一个 PostgreSQL 扩展。它会在支持的分析型查询上，接管 PostgreSQL 原本的 planner / executor 路径，改用 YAAP 自己的优化器和 C++ 列式执行引擎。

重点是：**YAAP 并不是要替换整个 PostgreSQL。**

PostgreSQL 仍然负责那些“不酷但极其重要”的数据库能力：

* 🧾 SQL 解析
* 🗂️ catalog 访问
* 💾 存储
* 🔒 事务
* 🌐 协议和结果返回
* 🧩 其他数据库服务端运行时

YAAP 接管的是分析型查询的热点路径：

* 🧠 逻辑计划
* 🛠️ 物理计划
* 📦 向量化 / 列式执行
* 🧵 并行 pipeline 执行

简单说：PostgreSQL 还是数据库主体，YAAP 给部分 OLAP 查询换上一条更现代的执行路径。

---

## 为什么做 pg_yaap？⚡

PostgreSQL 是非常优秀的通用数据库，但分析型查询通常需要一种和传统 tuple-at-a-time 执行器完全不同的执行模型。

`pg_yaap` 探索的是这条路线，但不把 PostgreSQL 做成 fork：

* 🐘 保留 PostgreSQL parser、catalog、storage 和事务语义
* 🧠 对支持的分析型查询使用 YAAP 自己的优化器
* 📦 用 columnar batch 处理数据，而不是逐 tuple 执行
* 🏗️ 用 pipeline 组织执行，并显式处理 pipeline breaker
* 🔥 YAAP 一旦接管查询，就不做静默 fallback，而是明确成功或失败

没有魔法。没有偷偷 fallback。YAAP 接管了查询，就由 YAAP 对结果负责。

---

## 核心想法 🧠

* **YAAP 自有优化器**
  支持的分析型查询会进入 YAAP 的逻辑和物理优化器。

* **列式执行引擎**
  执行阶段以 batch / DataChunk 为核心，而不是 PostgreSQL 原生的逐 tuple 执行模型。

* **Pipeline 执行模型**
  使用 pipeline 组织算子执行，并显式处理 join、aggregation、sort 等 pipeline breaker。

* **PostgreSQL 原生接入**
  通过 PostgreSQL hook 接入，最终仍然通过 PostgreSQL 正常的 tuple 接口返回结果。

* **不做静默 fallback**
  YAAP 一旦声明接管某个查询，就应该由 YAAP 执行；不支持或执行失败时明确暴露问题。

---

## 架构 🏗️

![YAAP 架构](imgs/arch.png)

高层说明：

高层说明：

1. `planner_hook` 将被接受的查询路由到 YAAP 的逻辑与物理规划器，生成 YAAP 的物理计划（PhysicalOperator）。
2. 在 `ExecutorStart`/`ExecutorRun` 阶段，已被 YAAP 接管的查询由桥接层交给 YAAP 运行时执行。
3. YAAP 运行时以列式数据块（DataChunk）为单位，通过并行 pipeline 执行，并通过 PostgreSQL 的常规 tuple 接口将结果返回给客户端。

4. 结果再转换回 PostgreSQL 正常的 tuple 接口返回。

---

## TPC-H 快照 📊

![TPC-H median runtime chart](imgs/comparison.svg)

> Benchmark 结果会受到硬件、PostgreSQL 配置、数据规模、查询覆盖范围、YAAP 支持程度等因素影响。这里的图表是当前阶段的性能快照，不代表所有环境下的通用结论。

---

## 构建 🔧

### Meson

```bash
meson setup build -Dpg_config=/path/to/pg_config
meson compile -C build
meson install -C build
```

### PGXS / Makefile

```bash
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

---

## 快速开始 🧪

```sql
LOAD 'pg_yaap';
SET pg_yaap.enabled = on;
SET pg_yaap.parallel = on;
```

之后像平常一样在 PostgreSQL 里执行 YAAP 当前支持的分析型 SQL。

```sql
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price
FROM lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

---

## 当前定位 🚧

`pg_yaap` 主要面向分析型查询处理，不是为了替代 PostgreSQL 成为一个新的通用数据库内核。

适合：

* ✅ scan-heavy 的 OLAP 查询
* ✅ TPC-H 风格分析型负载
* ✅ PostgreSQL 内部的现代优化器 / 执行器实验
* ✅ 列式、向量化、pipeline 执行模型研究
* ✅ pipeline 和并行执行研究

不追求：

* ❌ 替换 PostgreSQL 存储引擎
* ❌ 替换 PostgreSQL parser 或 catalog
* ❌ 做一个通用 PostgreSQL fork
* ❌ 用静默 fallback 掩盖不支持的执行路径
* ❌ 通用 OLTP 加速

---

## 致谢 🙏

感谢 PostgreSQL 项目提供的 hook 架构、扩展生态和长期积累的数据库工程基础。

感谢 DuckDB 在优化器结构、向量化执行和列式分析处理方面带来的启发。

---

## License 📄

Apache License 2.0 — see [LICENSE](LICENSE).

