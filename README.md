# Cassandra 车辆时序数据压测脚本

`cassandra_vehicle_timeseries_load.py` 面向隔离测试集群持续写入车辆时序数据。一个
`vehicle_id` 会轮流产生 `gps`、`powertrain`、`battery` 三条时间线，每行包含速度、
转速、温度、电压、油量、经纬度、里程和可调大小的随机 payload。

脚本支持：

- 配置文件控制连接、建表 CQL、列与数据生成规则；
- 默认使用 Cassandra mutation write timestamp 作为温度，不回填历史温度，数据随
  `chs_time` 自然由热变冷；
- 可选在同一轮按权重生成冷、热多个业务时间窗口，长压时也不会漂出窗口；
- prepared statement + token-aware 路由 + 有界 `execute_async` pipeline；
- 可选同一车辆分区内的小型 `UNLOGGED BATCH`，不会跨 partition key 组 batch；
- `--concurrency`、`--rows`、`--duration`、`--rate` 覆盖 in-flight、成功写入行数、时长和限速；
- 失败重试、请求/行吞吐、in-flight、P50/P95/P99 统计；
- 写完后按完整主键抽样回读，而不是对大表执行昂贵的 `COUNT(*)`；
- `--check-config` 和 `--dry-run` 不连接 Cassandra；
- 不执行 `DROP`/`DELETE`。只有配置显式打开 truncate 且命令行同时给出
  `--allow-destructive` 才会清表；清表前还会先 prepare 写入与回读 CQL，确认目标表
  和列契约有效。

## 1. 准备

脚本自身只使用 Python 标准库；连接 Cassandra 时只需要 Python Cassandra driver。
优先直接复用目标 Cassandra 的 `cqlsh.py` 运行依赖，不需要另外安装包。

以下命令在本工具目录（独立仓库中即仓库根目录）执行。

实际复制到测试机时只需要两个文件：

```text
cassandra_vehicle_timeseries_load.py
example-config.json
```

README 和 `.gitignore` 都不是运行依赖。

```bash
cp example-config.json /tmp/vehicle-load.json

CASSANDRA_HOME=/path/to/cassandra \
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --check-config
```

加载顺序为：

1. `--cassandra-home`、配置文件的 `connection.cassandra_home` 或环境变量
   `CASSANDRA_HOME`；
2. Linux 发行版路径 `/usr/share/cassandra/lib`；
3. 当前 Python 已经可以导入的 `cassandra` 模块；
4. 以上均不可用时，才考虑额外安装 Python Cassandra driver。

自动发现逻辑与 Cassandra 3.11 `cqlsh.py` 一致：从 `lib/` 加载
`cassandra-driver-internal-only-*.zip`，同时复用其中的 `futures-*.zip` 和
`six-*.zip`。可对照
[Apache Cassandra 3.11 cqlsh.py](https://github.com/apache/cassandra/blob/cassandra-3.11/bin/cqlsh.py)。
脚本不直接依赖 PyYAML、requests、numpy、pandas、gevent 或其他第三方库。

支持 Python 2.7 和 Python 3。Python 2.7 已停止维护，只应在遗留、隔离的测试环境
使用；生产或长期压测优先使用 Python 3。确实没有 cqlsh bundle 时，Python 2.7 可
使用 `cassandra-driver==3.25.0`；Python 3 应选择与本机 Python/Cassandra 版本兼容的
driver。该安装属于最后兜底，不是默认步骤。

先修改 `/tmp/vehicle-load.json`：

- `contact_points`、端口和 `local_dc`；
- 通常保持 `cassandra_home` 为空并使用环境变量；也可填写 Cassandra 安装根目录；
- keyspace、table、建表 CQL 与列；
- `partition_key_columns` 必须与真实表分区键一致；batch 模式据此执行安全校验；
- 示例按隔离的三节点测试集群使用 `SimpleStrategy/rf=3`；正式拓扑应改成
  `NetworkTopologyStrategy` 和真实 DC 名称；
- 若集群启用认证，把 `username_env/password_env` 改为环境变量名，例如
  `CASS_USERNAME`/`CASS_PASSWORD`，凭据本身不要写入 JSON；
- 已有表可将两个 `create_*_if_missing` 设为 `false`；
- `Z06_CHS` 是非标准 Cassandra CQL 扩展，不适用于原生 Apache Cassandra。请按
  目标 Cassandra 分支的真实表参数校准。

示例表保持 CHS 场景的关键布局：分区键是车辆 ID，第一聚簇列是秒级业务时间。
默认 `temperature_source=write_timestamp`，所以建表 CQL 不设置 `chs_column`，
`event_time_s` 只是业务时间列。`sample_seq` 保证高压下主键仍唯一，不会把重复
INSERT 变成覆盖写。示例默认 `ttl_seconds=null`，不会产生 TTL 数据。

## 2. 先检查，再小流量冒烟

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --check-config

python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --dry-run 12

python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --producer-threads 2 \
  --concurrency 4 --rows 1000 --rate 200
```

小流量写入和抽样回读通过后再升压。

## 3. 按数据量或时长运行

### 3.1 写入模式

| `write_mode` | `concurrency` 含义 | 用途 |
| --- | --- | --- |
| `async` | 全局最大 native-protocol in-flight 请求数 | 默认；prepared statement + `execute_async`，通常先用它压吞吐 |
| `unlogged_batch` | 最大 in-flight batch 请求数 | 每个 batch 只包含同一 `vehicle_id` 分区，减少网络往返 |
| `sync` | 同步写线程数 | 兼容和问题定位；不是高吞吐默认值 |

`producer_threads` 只是生成数据、提交异步请求的少量 Python 线程，不应等同于
`concurrency`。默认值是 8 个 producer、256 个 in-flight 请求。

写满 1000 万行，使用异步 pipeline：

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json \
  --write-mode async --producer-threads 8 --concurrency 256 --rows 10m
```

持续 30 分钟，最多 512 个异步请求，限速 5 万行/秒：

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json \
  --write-mode async --producer-threads 8 --concurrency 512 --rows 0 \
  --duration 30m --rate 50000 --summary-json /tmp/vehicle-load-summary.json
```

同一车辆分区内每批 8 行的 UNLOGGED BATCH：

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json \
  --write-mode unlogged_batch --batch-size 8 \
  --producer-threads 8 --concurrency 64 --rows 10m
```

这里的 64 是 batch 请求数；每批 8 行时，最多约 512 行处于未完成状态。脚本要求
`partition_key_columns` 只能使用 `vehicle_id` 或常量生成器，并再次按生成后的
`vehicle_id` 分组。建议从
`batch_size=8` 开始，只比较 1、4、8、16 等小档位，并关注 Cassandra 的
`batch_size_warn_threshold_in_kb`。Batch 不是越大越快，也不应用于跨车辆分区聚合。

如果行数和时长都大于 0，任一限制先到即停止。行数指成功写入量；瞬时失败会按配置
重试。最终仍有失败或抽样回读缺失时，脚本退出码为 1。

### 3.2 三节点 16U64G 建议起点

配置中的 `contact_points` 应包含三个节点，并填写实际 `local_dc`。设置 `local_dc` 时，
脚本使用 `TokenAwarePolicy(DCAwareRoundRobinPolicy)`；prepared statement 的 routing
key 会帮助请求优先发往对应副本。

运行前先用 `DESCRIBE KEYSPACE chs_load_test` 或系统表核对 RF。示例使用
`CREATE KEYSPACE IF NOT EXISTS`，如果 keyspace 已存在，修改 JSON 不会自动改变旧 RF；
需要由测试 DBA 显式调整或重建测试 keyspace。

建议依次测试并记录每档稳定 5–10 分钟的 rows/s、requests/s、P95/P99：

1. `async`: in-flight 128、256、512、1024；
2. 如果单请求网络开销明显，再测同分区 batch 4、8、16；
3. 如果压测机单进程 CPU 已满但集群仍有余量，再启动多个进程，每个进程使用不同
   `--vehicle-id-prefix`，避免写成同一批主键。

压测机最好与 Cassandra 节点分离，同时观察压测机 CPU。如果客户端单核先到 100%，
这不是 Cassandra 容量上限，应使用多进程档位；如果客户端仍有余量而服务端磁盘或
compaction 已饱和，则应降低并发。

不要以“Cassandra CPU 必须跑满”作为唯一目标。如果 rows/s 已不再增长而 P95/P99、
pending compaction、磁盘利用率或网络已经上升，继续加 in-flight 只会扩大排队。

## 4. 前台、nohup 与进度日志

前台运行：

```bash
export CASSANDRA_HOME=/path/to/cassandra
python -u cassandra_vehicle_timeseries_load.py \
  --config example-config.json
```

推荐的 nohup 运行方式：

```bash
export CASSANDRA_HOME=/path/to/cassandra
nohup python -u cassandra_vehicle_timeseries_load.py \
  --config example-config.json \
  > vehicle-load.log 2>&1 &

echo $! > vehicle-load.pid
```

按时长后台压测：

```bash
nohup python -u cassandra_vehicle_timeseries_load.py \
  --config example-config.json \
  --write-mode async --producer-threads 8 --concurrency 512 \
  --rows 0 --duration 24h \
  > vehicle-load.log 2>&1 &
```

如果一个压测进程先成为瓶颈，可启动多个独立进程；每个进程必须使用不同车辆前缀：

```bash
for client in 1 2 3 4; do
  nohup python -u cassandra_vehicle_timeseries_load.py \
    --config example-config.json \
    --write-mode async --producer-threads 8 --concurrency 256 \
    --vehicle-id-prefix "load${client}-vehicle-" \
    --rows 0 --duration 24h \
    > "vehicle-load-${client}.log" 2>&1 &
done
```

查看进度和进程：

```bash
tail -f vehicle-load.log
ps -fp "$(cat vehicle-load.pid)"
```

`workload.progress_interval_seconds` 默认每 5 秒输出一次并立即 flush；设为 `0` 可关闭。
日志包含 UTC 时间、模式、batch 大小、已成功行数/目标、请求/行 in-flight、两种吞吐
和请求 P95，例如：

```text
>>> progress utc=2026-09-04T01:23:45Z elapsed=30.0s mode=async batch=1 rows=150000/1000000 (15.0%) failed=0 inflight_req=256 inflight_rows=256 row_rate=5000.0/s req_rate=5000.0/s p95=8.200ms
```

正常完成后，日志末尾会输出完整 JSON 汇总和 `PASS:`；写入或抽样回读失败则输出
`FAIL:` 并返回非零退出码。`python -u` 与脚本的显式 flush 可以确保重定向日志及时
可见。

## 5. 温度来源与业务时间构造

这两个概念必须分开：

- `schema.temperature_source` 决定 CHS 真正使用哪个温度来源；
- `workload.event_time_mode` 只决定脚本如何生成 `event_time_s` 等业务时间列。

### 5.1 Cassandra write timestamp 作为温度（默认）

```json
"temperature_source": "write_timestamp",
"temperature_column": ""
```

对应建表属性不设置 `chs_column`：

```sql
AND Z06_CHS = {'chs_time':'3600','time_unit':'s'}
```

脚本不使用 `USING TIMESTAMP` 回填历史 mutation timestamp。CHS 按 Cassandra 正常
写入 timestamp 判温，新写入数据先热，超过 `chs_time` 后自然变冷。此时
`event_time_s` 仍会记录业务事件时间，但它不参与 CHS 判温。

该温度来源必须配合自然业务时间模式：

```json
"event_time_mode": "natural_write_time"
```

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --event-time-mode natural_write_time \
  --duration 30m --rows 0
```

### 5.2 自定义 CK 列作为温度

如果要让 `event_time_s` 控制温度，配置必须改为：

```json
"temperature_source": "custom_ck",
"temperature_column": "event_time_s"
```

同时在建表属性中明确指定同一个 CK 列：

```sql
AND Z06_CHS = {
  'chs_column':'event_time_s',
  'chs_time':'3600',
  'time_unit':'s'
}
```

脚本会校验 `temperature_column` 存在于写入列中、使用时间生成器，并与建表 CQL 的
`chs_column` 一致。

自定义 CK 来源可以选择两种业务时间构造方式：

- `natural_write_time`：在 payload 等其他列生成完毕后、真正发起 INSERT 前读取本机
  wall clock，写入 `event_time_s`；
- `weighted_time_windows`：按配置快速构造历史冷窗口和近期热窗口。

窗口模式下，示例预置：

- `cold`：启动前 7 天到前 1 天，`weight=4`；
- `hot`：启动前 5 分钟到启动时刻，`weight=1`。

因此约 80% 行落在冷时间窗、20% 落在热时间窗。到窗口末端后从窗口开头继续，长压
时不会让冷时间漂入热窗口。使用窗口模式前，需要按上面的方式修改
`temperature_source`、`temperature_column` 和 `table_cql`，然后运行：

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json \
  --event-time-mode weighted_time_windows --rows 1m
```

自定义 CK 的自然模式依赖压测机与 Cassandra 节点时钟同步，应先检查 NTP/chrony。
无论使用哪种来源，时间超过阈值都不等于 SST 已经入冷；仍需按目标版本流程执行或
等待 flush、compact、separate 和 move，并用集群指标验证。

## 6. 列生成器

每个 `schema.columns[]` 由 `name` 和 `generator` 定义。支持：

- 身份/时间：`vehicle_id`、`timeline`、`time_window`、`event_time_seconds`、
  `event_time_millis`、`event_time_timestamp`、`event_date`、`sequence`、
  `stream_sequence`；
- 通用值：`random_int`、`random_float`、`choice`、`constant`、
  `linear_float`、`random_blob`、`random_text`、`boolean`。

配置检查会拒绝生成 `null` 的规则，避免 INSERT 在测试表中意外制造 tombstone。
`random_blob.pool_size` 默认 256：脚本用 SHA-256 预生成并复用有限 payload 池，避免
Python 为每一行逐字节调用随机数成为客户端瓶颈；`size * pool_size` 上限为 64 MiB。

## 7. 内置自测

自测不需要 Cassandra driver 或 Cassandra 集群：

```bash
python2.7 -S cassandra_vehicle_timeseries_load.py \
  --config example-config.json --self-test

python3 -S cassandra_vehicle_timeseries_load.py \
  --config example-config.json --self-test
```

真实 Cassandra 连通、DDL 扩展和 CHS 入冷必须在目标隔离集群上验证。

## 8. 实现参考与 Batch 边界

本工具的吞吐模型参考了以下公开实现：

- [Cassandra 3.11 cqlsh COPY](https://github.com/apache/cassandra/blob/cassandra-3.11/pylib/cqlshlib/copyutil.py)：多进程转换、prepared UNLOGGED BATCH、
  `execute_async` callback、in-flight 上限和失败重试；
- [cassandra-stress StressAction](https://github.com/apache/cassandra/blob/cassandra-3.11/tools/stress/src/org/apache/cassandra/stress/StressAction.java)：
  独立控制线程、速率和指标；
- [YCSB CassandraCQLClient](https://github.com/brianfrankcooper/YCSB/blob/master/cassandra/src/main/java/site/ycsb/db/CassandraCQLClient.java)：
  多线程共享 cluster/session，并缓存 prepared statements；
- [DataStax Python driver concurrent API](https://github.com/datastax/python-driver/blob/master/docs/api/cassandra/concurrent.rst)：
  使用受控 concurrency，而不是无限制造同步线程；
- [Apache Cassandra CQL BATCH](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html#batch-statement)：
  Batch 可减少网络往返，但跨分区原子 batch 有额外成本。

因此，本脚本默认使用异步单行写；可选 batch 严格按 `vehicle_id` 分组并使用
`UNLOGGED BATCH`。没有使用跨 partition key 的 logged batch，也没有调用 Python
driver 的私有批量编码字段。
