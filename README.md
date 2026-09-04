# Cassandra 车辆时序数据压测脚本

`cassandra_vehicle_timeseries_load.py` 面向隔离测试集群持续写入车辆时序数据。一个
`vehicle_id` 会轮流产生 `gps`、`powertrain`、`battery` 三条时间线，每行包含速度、
转速、温度、电压、油量、经纬度、里程和可调大小的随机 payload。

脚本支持：

- 配置文件控制连接、建表 CQL、列与数据生成规则；
- 默认使用 Cassandra mutation write timestamp 作为温度，不回填历史温度，数据随
  `chs_time` 自然由热变冷；
- 可选在同一轮按权重生成冷、热多个业务时间窗口，长压时也不会漂出窗口；
- `--concurrency`、`--rows`、`--duration`、`--rate` 覆盖并发、成功写入行数、时长和限速；
- prepared statement、失败重试、延迟与吞吐统计；
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
- 多节点环境不要直接沿用示例的 `SimpleStrategy/rf=1`；
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
  --config /tmp/vehicle-load.json --concurrency 4 --rows 1000 --rate 200
```

小流量写入和抽样回读通过后再升压。

## 3. 按数据量或时长运行

写满 1000 万行，并发 64，不限速：

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --concurrency 64 --rows 10m
```

持续 30 分钟，并发 128，限速 5 万行/秒（`--rows 0` 关闭行数上限）：

```bash
python cassandra_vehicle_timeseries_load.py \
  --config /tmp/vehicle-load.json --concurrency 128 --rows 0 \
  --duration 30m --rate 50000 --summary-json /tmp/vehicle-load-summary.json
```

如果行数和时长都大于 0，任一限制先到即停止。行数指成功写入量；瞬时失败会按配置
重试。最终仍有失败或抽样回读缺失时，脚本退出码为 1。

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
  --rows 0 --duration 24h --concurrency 64 \
  > vehicle-load.log 2>&1 &
```

查看进度和进程：

```bash
tail -f vehicle-load.log
ps -fp "$(cat vehicle-load.pid)"
```

`workload.progress_interval_seconds` 默认每 5 秒输出一次并立即 flush；设为 `0` 可关闭。
日志包含 UTC 时间、已成功行数/目标、失败数、in-flight、实时吞吐和 P95，例如：

```text
>>> progress utc=2026-09-04T01:23:45Z elapsed=30.0s rows=150000/1000000 (15.0%) failed=0 inflight=64 rate=5000.0/s p95=8.200ms
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

## 7. 内置自测

自测不需要 Cassandra driver 或 Cassandra 集群：

```bash
python2.7 -S cassandra_vehicle_timeseries_load.py \
  --config example-config.json --self-test

python3 -S cassandra_vehicle_timeseries_load.py \
  --config example-config.json --self-test
```

真实 Cassandra 连通、DDL 扩展和 CHS 入冷必须在目标隔离集群上验证。
