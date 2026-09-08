# Cassandra 车辆时序压测工具

一个 Python 脚本 + 一个 JSON 配置即可运行。支持 Python 2.7 / Python 3，运行依赖只有现有 Cassandra driver；优先复用 cqlsh 自带依赖，不需要安装 YAML、numpy、gevent 或监控库。

v2 使用独立写入进程、prepared statement、有界异步请求和可选同分区 UNLOGGED BATCH。进程之间只传统计信息，不传逐行数据。普通 Cassandra 可以用于写入性能测试；CHS 冷热迁移只能在支持该扩展的实例验证。

## 快速开始

部署只需要：

```text
cassandra_vehicle_timeseries_load.py
example-config.json
```

修改配置中的 contact_points、local_dc、keyspace、table 和建表 CQL。凭据通过 username_env/password_env 指定环境变量，不写入 JSON。

```bash
export CASSANDRA_HOME=/path/to/cassandra
python cassandra_vehicle_timeseries_load.py --config example-config.json --check-config
python cassandra_vehicle_timeseries_load.py --config example-config.json --dry-run 5

# 少量真实写入；不改表的温度规则
python -u cassandra_vehicle_timeseries_load.py --config example-config.json \
  --processes 1 --producer-threads 1 --concurrency 8 --rows 1000 --duration 0
```

普通 Cassandra 单节点：从 table_cql 中删除末尾的 `AND Z06_CHS = {...}`，把测试 keyspace 的 RF 改为 1。其余默认列和主键不变。不要对已有生产 keyspace 修改 RF；CREATE IF NOT EXISTS 不会修改已有 schema。

支持直接指定 `--cassandra-home`，也支持设置为 cqlsh.py 路径或 Cassandra 的 lib 目录。脚本打印实际加载的 driver 路径、版本、reactor、路由策略和协议版本。Python 要与该 cqlsh bundle 兼容，不是任意旧 bundle 都支持任意新版 Python。

已实测 Python 2.7.18、Python 3.12.13，以及 Cassandra 3.11.19 附带的 driver 3.11.0.post0。Python 2 已停止维护，应限于隔离遗留环境。

## 并发和数量的含义

| 设置 | 含义 |
| --- | --- |
| processes | 独立写进程数；示例为 4 |
| producer_threads | 每个进程的 producer 线程数；建议 1 |
| concurrency | 所有进程合计的最大未回收请求数，不是每进程数量 |
| batch_size | 一个 UNLOGGED BATCH 的行数；async/sync 忽略此设置 |
| total_rows / --rows | 所有进程合计的成功写入目标，0 表示不按行数停止 |
| duration_seconds / --duration | 统一开始发压后的时长；0 表示不按时长停止 |
| rate_limit_rows_per_second / --rate | 所有进程合计的行/秒上限，不是吞吐保证 |
| vehicle_count | 所有进程合计的车辆数；各进程负责不重叠车辆编号范围 |
| verification.sample_size | 全部进程合计的回读样本数 |
| sessions | 每进程 Session 数，默认 1；多个 Session 轮流接收请求 |

concurrency、行数、车辆和限速被分配给各进程，余数也计入分配。进程数不超过 concurrency、vehicle_count 和非零行数目标，因此很小的任务可能实际少开进程。低速限流允许启动时少量进程级突发，不是严格逐毫秒匀速。

示例默认：4 进程，每进程 1 producer，全局 80 个 batch，每批 8 行，最多约 640 行未完成。一个进程加很多 Python 线程可能更慢；不要把 producer_threads 当成请求并发。

### 常用命令

```bash
# 单行异步基线；写够 1000 万行
python -u cassandra_vehicle_timeseries_load.py --config example-config.json \
  --write-mode async --processes 4 --producer-threads 1 \
  --concurrency 256 --rows 10m --duration 0

# 同分区小 batch；持续 30 分钟
python -u cassandra_vehicle_timeseries_load.py --config example-config.json \
  --write-mode unlogged_batch --batch-size 8 \
  --processes 4 --producer-threads 1 --concurrency 80 \
  --rows 0 --duration 30m --summary-json vehicle-load-summary.json
```

行数、时长都大于 0 时，先到的条件停止提交新数据；之后排空已提交请求，因此实际进程退出可以晚于 duration。drain_timeout_seconds 限制停止后的排空等待。

max_batch_bytes 默认 16 KiB，限制 batch 编码体的保守估算大小。每一批检查真实 prepared routing key，跨分区或超限会报错，不会默默发送。batch 不是越大越好；对比 4/8/16 时同时记录请求数、行数和时延。sync 模式仅用于兼容/定位。

sync 使用同步线程，线程总数由 concurrency 决定，忽略 producer_threads；上述“每进程 1 producer”的建议适用于 async/batch。

## nohup、进度与停止

```bash
export CASSANDRA_HOME=/path/to/cassandra
nohup python -u cassandra_vehicle_timeseries_load.py \
  --config example-config.json \
  --processes 4 --producer-threads 1 \
  --write-mode unlogged_batch --batch-size 8 --concurrency 80 \
  --rows 0 --duration 24h --summary-json vehicle-load-summary.json \
  > vehicle-load.log 2>&1 &
echo $! > vehicle-load.pid

tail -f vehicle-load.log

# 给父进程 SIGTERM：停止提交，排空请求，输出 INTERRUPTED 汇总
kill -TERM "$(cat vehicle-load.pid)"
```

父进程输出全局统计并立即 flush，子进程启动时会输出自身连接诊断。不要直接 kill -9 正常停止压测；强制杀死 worker 会使本轮失败，无法确认的写入不能计为成功。

progress 字段：

- row_rate、req_rate、encoded_MBps：相邻两次全局报告的增量速率。各 worker 最新快照可能相差约 1 秒，短报告间隔和起始阶段会有明显波动；最终汇总使用全部 worker 的最终计数。
- avg_row_rate：从统一发压开始计算的累计成功行吞吐。
- driver_pending：尚未进入应用 callback 的请求；包含 driver 内部等待，不是纯服务端队列或 wire in-flight。
- completed_queued：callback 已完成、等待 producer 回收的请求。
- request_errors / retry_attempts / failed：尝试错误、实际应用重试、最终失败行数。
- p95_all：全程请求时延的抽样 P95；不是最近 5 秒 P95。多进程按请求数加权合并样本，不平均各进程 P95。
- callback_queue_mean：回调完成后等待应用回收的平均时间。
- client_cpu_avg：写入进程累计 CPU 使用之和；100% 约为一个逻辑核，四进程可以超过 100%，不含父进程和 Cassandra。
- coordinators：成功/失败回调所报告的 coordinator 累计请求量；旧 driver 可能不提供。
- encoded_MBps：绑定值/批体的编码大小估算，不是抓包字节数或磁盘增长速度。

请求时延包含绑定、提交和等待 callback；不包含之前的数据生成，也不包含之后的完成队列等待。不要再用 `concurrency / req_rate` 推导数据库独立处理时延。

Python 3 优先使用 monotonic 时钟计时；Python 2 无该标准库 API 时回退到系统时钟，运行期间应避免人为跳变时钟，否则时长/速率统计会受影响。

最终 JSON 包含每个进程、全局结果、回读结果和真实 schema 检查结果。PASS 为退出码 0，写入/校验或环境异常非零，正常 SIGTERM/SIGINT 收尾为 INTERRUPTED / 130。

## 数据身份、温度和数据真实性

### 防止误覆盖

run_id 默认 auto，每轮给车辆前缀增加唯一运行标识；所有 worker 使用同一个运行标识，但车辆范围不重叠。每轮仍只生成 vehicle_count 辆车；同表多轮运行会保留不同运行标识的历史车辆 ID。

`--run-id my-run` 可指定标识；`--run-id ''` 可用于刻意重放。重复固定 run_id、固定时间窗口和序列，会覆盖已有主键，此时成功 INSERT 数不等于新增行数。默认主键包含 vehicle_id、时间、timeline、sample_seq；修改 schema 时必须自行保持完整主键唯一性。

### write_timestamp 温度

```json
"temperature_source": "write_timestamp",
"temperature_column": ""
```

建表不指定 chs_column，例：

```sql
AND Z06_CHS = {'chs_time':'3600','time_unit':'s'}
```

业务时间模式使用 natural_write_time。不回填历史 mutation timestamp。已测试 driver 在 protocol v3+ 默认通过协议携带客户端 timestamp，因此该模式也需要压测机/数据库时钟同步；未写 USING TIMESTAMP 不等于必定采用服务端时钟。

### 自定义 CK 温度

```json
"temperature_source": "custom_ck",
"temperature_column": "event_time_s"
```

建表必须指定：

```sql
AND Z06_CHS = {'chs_column':'event_time_s','chs_time':'3600','time_unit':'s'}
```

可使用自然时间，也可使用 weighted_time_windows。窗口设置由 time_windows 的偏移、weight 和 timeline.interval_seconds 控制：

- window_anchor=fixed：固定启动参考时间或 reference_time_utc，适合历史回放；hot 标签会随真实时间推进而老化，不能当作实时热数据比例。
- window_anchor=rolling：每行根据当前时间平移 age 窗口，适合持续构造冷热年龄分布。
- window_sampling=sequential：从窗口起点按 timeline 间隔递进、到末尾回绕。短跑可能只覆盖窗口起点。
- window_sampling=uniform：在窗口内分布采样，用于覆盖时间范围。

启动检查真实分区键、完整验证主键、CK 属性。若实例暴露 Z06_CHS 元数据，还会核对配置值；否则输出 chs_metadata=not_exposed，不伪称验证成功。需要严格拒绝未知状态时设置 require_chs_metadata=true。普通 Cassandra 测试应保持 false。

CREATE IF NOT EXISTS 不会 ALTER 已有表。改变温度来源或表结构前，应由测试人员确认真实表；脚本不会自动 ALTER、DROP 或 DELETE。只有 truncate_before_load=true 且传入 --allow-destructive 才会 TRUNCATE，并在 prepare/schema 检查通过后执行。

### payload 和长压分区

random_blob 默认 mode=pooled、pool_size=256，复用有限随机字节池，降低客户端成本。需要低重复度数据可将该列设为 mode=unique；生成成本和压缩效果会不同。比较版本性能时必须保持此设置一致。

车辆分区持续增长，没有自动时间分桶。设置 vehicle_count、行宽和持续时长时应考虑目标分区密度，不能把短跑结果当作长期 compaction/冷热迁移能力。

## 重试与回读

默认 max_retries=0，避免掩盖压测错误。driver 使用 FallthroughRetryPolicy；协议级 UNPREPARED 等内部恢复不等于应用重试。

async/batch 模式显式打开 max_retries 时，仅对选定的临时 driver 错误使用退避队列，不暂停其他请求回收。sync 保留阻塞重试，只适合定位问题。超时可能已经写入成功，重试会重新生成正常 mutation timestamp；对首写温度敏感的测试应保持 0。TTL 写入要求 max_retries=0，以免重试延长过期时间。

回读保存全程 reservoir 样本，比较配置中的所有写入列，不只检查主键存在。短 TTL 可能在回读前正常到期；不要把此模式用于不区分到期原因的数据一致性验收。LOCAL_ONE 写后 LOCAL_ONE 读也不是所有副本一致性的证明。

CQL timestamp 回读按 Cassandra 的毫秒精度比较，不能要求保留 Python datetime 的微秒精度。

## 自测与性能验证

```bash
python2.7 -S cassandra_vehicle_timeseries_load.py --config example-config.json --self-test
python3 -S cassandra_vehicle_timeseries_load.py --config example-config.json --self-test
```

内置自测使用固定 fixture，不受用户自定义列和窗口名影响。它不连接 Cassandra，不能证明真实吞吐。

实际普通 Cassandra 对照见 [本地性能与回归报告](BENCHMARK.md)。建议内部按 1/2/4/8 进程做扩展曲线，先保持全局 concurrency、车辆数、RF、CL、行宽、payload 模式不变，再单独调整 concurrency 和 batch。同时观察压测机 CPU、数据库 CPU、磁盘、commitlog、compaction、GC 和网络，不能以“数据库 CPU 必须满”为唯一目标。

设计参考：[Python driver 性能说明](https://python-driver.readthedocs.io/en/stable/performance.html)、[Cassandra 3.11 COPY](https://github.com/apache/cassandra/blob/cassandra-3.11/pylib/cqlshlib/copyutil.py)。本工具使用公共 prepared/batch API，没有复制 driver 私有批量编码实现。
