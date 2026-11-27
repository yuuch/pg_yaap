# 在离线 Linux 上构建和安装 pg_yaap

本文说明如何把当前 pg_yaap 源码带到离线 Linux 主机，在其上用 Meson 构建 Release 扩展并安装到指定 PostgreSQL。GitHub Actions 的构建流程见 [pg_yaap-ci.yml](../.github/workflows/pg_yaap-ci.yml)。

## 制作包含 PostgreSQL 源码的离线包

如果离线机器上还没有 PostgreSQL 安装，可把 PostgreSQL 源码和插件源码一起打包。默认假设 pg_yaap 位于 PostgreSQL checkout 的 `contrib/pg_yaap`：

```bash
./scripts/make_offline_bundle.sh --output /path/to/pg_yaap-offline.tar.gz
```

也可以显式指定 PostgreSQL 源码树：

```bash
./scripts/make_offline_bundle.sh \
  --postgres-src /path/to/postgres \
  --output /path/to/pg_yaap-offline.tar.gz
```

该归档只有源码和构建脚本，不带 `.git` 历史、构建产物或数据集。它包含 PostgreSQL 源码，因此比只带扩展的包大。PostgreSQL 源码版本由你打包时的 checkout 决定；若要复现当前 CI，使用 `REL_18_3` checkout。归档本身不含编译器、系统库或 LLVM 二进制包，这些 Linux 依赖仍需预先放入离线系统/本地包仓库。

在 Linux 上解包并一次性构建安装：

```bash
tar -xzf pg_yaap-offline.tar.gz
cd pg_yaap-offline
./pg_yaap/scripts/build_postgres_and_yaap.sh --prefix "$HOME/pgsql-yaap"
```

此脚本用 Meson `release` 构建 PostgreSQL（LLVM、ICU、Readline 默认启用），安装到指定 prefix，再构建并安装扩展。默认 `ENABLE_LLVM=1`，因此离线机器需有 LLVM 开发工具（`llvm-config`）和 PostgreSQL 的其余构建依赖。若不需要 LLVM/JIT 或目标环境没有 LLVM，可执行 `ENABLE_LLVM=0 ...`；此时 PostgreSQL 和扩展都以无 LLVM 配置构建。可用 `JOBS=8` 控制并行度。构建依赖应按 PostgreSQL 官方构建要求和目标 Linux 发行版准备，包括 C/C++ 工具链、Meson/Ninja、Bison/Flex、Perl/Python、Readline、zlib、OpenSSL、XML/XSLT、ICU、Tcl 等开发包。

运行新安装的 PostgreSQL：

```bash
"$HOME/pgsql-yaap/bin/initdb" -D "$HOME/pgsql-yaap/data"
"$HOME/pgsql-yaap/bin/pg_ctl" -D "$HOME/pgsql-yaap/data" \
  -l "$HOME/pgsql-yaap/server.log" start
"$HOME/pgsql-yaap/bin/psql" -d postgres \
  -c "LOAD 'pg_yaap'; SHOW pg_yaap.enabled;"
```

需要自动加载时，在 `$HOME/pgsql-yaap/data/postgresql.conf` 加入 `shared_preload_libraries = 'pg_yaap'` 并重启。TPC-H 数据仍是可选项，只在执行 TPC-H 查询或基准时需要。

## 先明确需要什么

扩展是针对一个**已经安装好的 PostgreSQL 实例**单独编译的。只构建扩展时不需要 PostgreSQL 内核源码，也不需要 Linux 内核源码；需要的是与目标服务端匹配的 `pg_config`、PostgreSQL server headers（至少有 `pg_config.h`）和 PostgreSQL 安装/运行时。

| 项目 | 是否需要 | 说明 |
|---|---|---|
| pg_yaap 源码 | 必需 | 当前仓库完整源码；带离线机器的架构应与目标 Linux 一致。 |
| PostgreSQL 服务端安装 | 必需 | 必须是 Linux 上运行的 PostgreSQL。扩展链接到 PostgreSQL 提供的库和头文件。 |
| `pg_config` 与 server development headers | 必需 | 使用目标实例对应版本的 `pg_config`。通常由 `postgresql-server-dev-<版本>` 包提供；发行版拆包名称有所不同。 |
| C/C++ 编译器、Meson、Ninja | 必需 | C++17 编译器（GCC 或 Clang）、Meson、Ninja。推荐与目标系统一致的工具链。 |
| PostgreSQL 内核源码 | 仅从源码构建 PostgreSQL 时需要 | CI 下载 PostgreSQL `REL_18_3` 源码并先构建安装 PostgreSQL；这不是构建扩展本身的必要条件。若要复现 CI 的 PostgreSQL 构建，应离线准备该 tag 的源码和 PostgreSQL 自身构建依赖。 |
| LLVM | 条件需要 | `pg_config.h` 中定义 `USE_LLVM 1` 时，本扩展 Meson 配置会要求 `llvm-config` 并链接 LLVM ORC JIT；LLVM 版本/开发库须与 PostgreSQL 构建相容。若 PostgreSQL 未启用 LLVM，插件可不带 LLVM 编译，JIT deform 能力不可用。 |
| TPC-H 数据 | 运行插件不需要 | 只在要复现分析查询或基准时需要。普通加载验证不需要业务数据。 |

CI 用 Ubuntu 24.04，安装构建依赖后尝试安装较新的 LLVM，使用 LLVM 版 PostgreSQL 18.3，再构建插件。其 PostgreSQL 配置包括 `-Dllvm=enabled -Dicu=enabled -Dreadline=enabled -Dbuildtype=release`。如果你已经有 PostgreSQL，不需要重复构建内核；应以该实例的 `pg_config` 为准。扩展 Meson 配置会从 `pg_config` 自动获取头文件和安装目录，并从 PostgreSQL 的 `pg_config.h` 判断是否启用 LLVM。

## 离线准备

在有网络的同架构/同发行版机器上，或从内部镜像准备以下内容并传入离线环境：

1. pg_yaap 的源码归档或 Git checkout（包括完整 `src/`、`meson.build`、`meson_options.txt`、control 和 SQL 文件）。
2. 目标 Linux 上安装的 PostgreSQL，或对应发行版的 PostgreSQL server 与 development headers 包。确认 `pg_config --version` 与运行中的服务端主版本一致。
3. 编译工具链和工具包：`g++`/`clang++`、Meson、Ninja、GNU make/基础构建工具、`grep`、`sed`。Meson/Ninja 可用发行版包离线安装，也可以预装到系统路径。
4. 若 PostgreSQL 的 `pg_config.h` 定义 `USE_LLVM 1`：匹配的 LLVM 开发包（至少包含 `llvm-config`、LLVM headers 与被链接的 ORC/Core/Native 库）。最好使用构建 PostgreSQL 时同一主版本的 LLVM。
5. 目标 PostgreSQL运行需要的动态库也必须在离线机器上存在。若 PostgreSQL 是自编译安装，整个安装前缀和它依赖的运行库都应一起带过去；只拷贝 `pg_yaap.so` 不够。

**架构与 ABI：** 在目标 Linux 上编译最稳妥。仓库当前使用 `-march=native` 和 `-O3`，所以不要在另一台 CPU 上编译后假定二进制可在目标 CPU 上运行。扩展应使用目标 PostgreSQL 的头文件/`pg_config` 构建，且不能跨 PostgreSQL 主版本复用。若同一 PostgreSQL 安装需要在不同 CPU 型号运行，需调整项目编译参数移除 `-march=native` 后再构建通用二进制。

## 一键 Release 构建与安装

从源码根目录执行（或在任意目录用脚本绝对路径）：

```bash
chmod +x scripts/build_install.sh
./scripts/build_install.sh --pg-config /opt/postgres/bin/pg_config
```

若 `pg_config` 已在 `PATH`：

```bash
./scripts/build_install.sh
```

脚本会检查 server header 与必要工具，识别 PostgreSQL 的 LLVM 配置，运行 `meson setup`（`buildtype=release`）、`meson compile` 和 `meson install`。默认构建目录为仓库中的 `build-release-pg<主版本>`；可用 `--build-dir` 或 `BUILD_DIR` 更改。需要管理员权限写入 PostgreSQL 安装目录时，脚本会调用 `sudo`；也可以用安装目录所有者或 root 运行。

也可设置环境变量：

```bash
PG_CONFIG=/opt/postgres/bin/pg_config \
LLVM_CONFIG=/opt/llvm/bin/llvm-config \
BUILD_DIR=/var/tmp/pg_yaap-build \
./scripts/build_install.sh
```

只有目标 PostgreSQL 启用 LLVM 时才需要 `LLVM_CONFIG`。如果扩展目录不可写，`sudo` 需要在离线主机可用；脚本不会下载依赖。

## 让 PostgreSQL 加载 pg_yaap

安装成功后，先用 SQL 会话临时加载并验证：

```bash
/opt/postgres/bin/psql -d postgres
```

```sql
LOAD 'pg_yaap';
SHOW pg_yaap.enabled;
```

`LOAD` 只影响当前会话。YAAP 的 GUC 在模块加载时注册，因此连接后可执行 `LOAD 'pg_yaap'; SET pg_yaap.enabled = on;`。可在查询会话中设置并发相关参数，例如：

```sql
LOAD 'pg_yaap';
SET pg_yaap.enabled = on;
SET pg_yaap.parallel_max_workers = 4;
```

要让所有新连接自动加载，在 `postgresql.conf` 中配置：

```conf
shared_preload_libraries = 'pg_yaap'
```

随后重启 PostgreSQL（修改 `shared_preload_libraries` 必须重启）。`pg_yaap.enabled` 默认是 `on`；需要按工作负载或角色控制时，可在配置文件、数据库、用户或会话级设置。首次启用建议先用 `LOAD` 在单独会话验证，再配置预加载。

基本服务控制示例（路径按实际安装和数据目录替换）：

```bash
/opt/postgres/bin/pg_ctl -D /var/lib/postgresql/data restart
/opt/postgres/bin/psql -d postgres -c "LOAD 'pg_yaap'; SHOW pg_yaap.enabled;"
```

**注意：** `CREATE EXTENSION pg_yaap` 不是必要步骤。控制文件和 SQL 文件由构建安装；本扩展通过加载共享库安装 hooks，当前 SQL 脚本本身执行 `LOAD`。

## TPC-H 数据与验证

TPC-H 数据仅用于跑 TPC-H 查询/性能测试，不是编译或加载插件的前置条件。离线数据准备需把生成器、数据文件、建表/导入 SQL 和查询 SQL 一并传入。数据的 scale factor 决定空间需求；确认磁盘容量后再搬运大规模数据集。

加载数据后，需先创建对应的 TPC-H 表并导入数据，再执行该仓库支持的查询。TPC-H 查询可能触发 YAAP 优化器/执行器的覆盖范围限制；按项目的失败策略，不支持的已接管查询会报错，不应把它当作加载是否成功的唯一判断。最小加载验证是 `LOAD 'pg_yaap'` 和 `SHOW pg_yaap.enabled`。

## 常见问题

- **找不到 `pg_config.h`**：安装与目标 PostgreSQL 同主版本的 server development headers，并确认选中了正确的 `pg_config`。
- **提示找不到 `llvm-config`**：目标 PostgreSQL 的 `pg_config.h` 启用了 LLVM，需准备相容的 LLVM 开发工具并设置 `LLVM_CONFIG`。
- **PostgreSQL 启动时找不到 `pg_yaap`**：确认用同一 `pg_config` 安装；检查 `pg_config --pkglibdir` 下的 `pg_yaap.so`，并查看 PostgreSQL 日志中的缺失动态库信息。
- **运行时找不到共享库**：离线系统须有 PostgreSQL 和 LLVM 链接所需的动态库；检查 `ldd <pkglibdir>/pg_yaap.so` 输出。
- **跨机器 `Illegal instruction`**：构建时 `-march=native` 针对构建机 CPU。移到较老/不同 CPU 时，需在目标机器编译，或改为不使用 native CPU 指令后重新构建。
