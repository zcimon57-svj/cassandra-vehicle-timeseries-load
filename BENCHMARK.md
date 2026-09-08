# v2 本地性能与回归验证

测试日期：2026-09-08。使用真实 Apache Cassandra，不是 mock 的吞吐数据。

## 测试范围与版本

此次修复重点是客户端发压：独立进程持有各自的 Cluster/Session；每进程一个 producer 驱动多个异步请求；全局并发、行数、车辆数和限速在进程间分配。完成事件优先回收，限速和异步重试不会阻塞整个回收循环。batch 仍使用公共 driver API，并检查真实 routing key 和批体大小。

对照旧版不是重新实现的“慢版本”：旧脚本取自已发布 main 的 `a276956f2dd3262582e54c19ccb131bcfba702e1`，其中脚本最后修改提交为 `7486d135539e6879166b0cc227a686399825dbb5`。旧脚本保持原样。

| 文件 | SHA-256 |
| --- | --- |
| 旧脚本 | `32e4ffc013fed1c0ebc23670a4157e818ef2923c4acd336724cd195875653765` |
| 本次交付脚本 | `90b8c1a9c11d4372678823979f99a25a0ac649e710a4a7abe06bcf2ade804112` |

## 实验条件

- WSL2 Linux，Intel i7-1195G7，8 个逻辑 CPU，约 7.6 GiB 内存。压测客户端和 Cassandra 在同一台机器，争用 CPU/内存；未绑核，非独占物理主机，可能受宿主机调度与背景负载影响。
- Apache Cassandra 3.11.19 官方二进制包，OpenJDK 8，2 GiB heap、400 MiB young generation；单节点，RF=1，LOCAL_ONE，protocol v4，localhost，无 TLS。
- 默认 16 列，1,000 辆车、3 条 timeline；256 字节 payload，pooled 模式、pool_size=256；自然业务时间、不设 TTL、不重试、不限速。
- 每轮使用新建的隔离测试表，去掉 CHS DDL 属性，不覆盖旧数据。所有版本使用相同列、主键和车辆前缀长度。新版仅在性能对照中设 run_id 为空；日常使用默认 auto。
- 主对照使用同一 Python 3.12.13、driver 3.30.1，已具备 Cython 扩展和 Libev reactor。该版本组合是测量条件，不是运行最低要求。
- 先预热 10 秒，再按每组 20 秒、重复 3 轮测量。旧版/新版交错执行，第二轮反转顺序。没有并行跑其他压测。吞吐按成功行数 / 写入阶段耗时计算，包含请求排空，不包含连接、DDL 和结束后的回读。
- async 全局 concurrency=640，每请求 1 行；batch 全局 concurrency=80，每请求最多 8 行。因此两者最多约 640 行未完成，但请求数不同。batch 尾部不足 8 行时仍写入。
- 旧版每轮抽查 20 个主键是否存在；新版每轮抽查 20 行全部配置列。新版额外的小表测试做完整回读，见下文。

## Python 3 前后对照

| 配置 | 三轮成功行/秒 | 中位数 行/秒 | 客户端 CPU 中位数 | 请求 P95 中位数 ms |
| --- | --- | ---: | ---: | ---: |
| 旧版，1 进程 × 16 producer，async | 3,050 / 3,409 / 2,668 | 3,050 | 106.5% | 128.255 |
| 新版，4 进程 × 1 producer，async | 12,438 / 13,147 / 12,700 | 12,700 | 363.1% | 44.066 |
| 旧版，1 进程 × 1 producer，async | 6,029 / 6,267 / 6,340 | 6,267 | 100.4% | 136.757 |
| 新版，1 进程 × 1 producer，async | 5,577 / 5,763 / 6,181 | 5,763 | 100.4% | 126.085 |
| 旧版，1 进程 × 8 producer，batch=8 | 7,745 / 9,090 / 9,424 | 9,090 | 106.1% | 15.873 |
| 新版，4 进程 × 1 producer，batch=8 | 19,337 / 20,477 / 28,646 | 20,477 | 344.7% | 37.565 |

新版 4 进程单行写相对旧版 16 producer 为 **4.16 倍**；即便把旧版调为 1 producer，新版 4 进程仍为其 **2.03 倍**。同分区 batch 前后为 **2.25 倍**。新版单进程并没有同等提升，因此主要收益应归于多进程结构与配置，不能声称每个单进程都变快。

CPU 是压测写入进程 CPU 总和，100% 约一个逻辑核。不是 Cassandra CPU，也不包含父进程。吞吐和 CPU 表列均取三轮中位数；原始逐轮数据附在报告后面。

P95 是客户端请求级延迟，不是服务端独立处理延迟。旧版和新版的回收统计实现不同，且多 producer 会改变实际排队与提交节奏；不能仅据旧版某个较小 P95 判定数据库更快，也不能用 batch 行数放大请求吞吐。主要对照是成功行吞吐、错误与 CPU 利用情况。

## 不升级依赖：Python 2 + cqlsh bundle

此组直接复用 Cassandra 3.11.19 `lib` 下的 `cassandra-driver-internal-only-3.11.0-bb96859b.zip` 以及附带 futures/six；实际 driver 版本为 3.11.0.post0，Asyncore reactor，Python 2.7.18。未为该组安装新依赖。

这组比较的是**新版内部 1 进程与 4 进程**，不是旧代码与新代码。参数仍为 batch=8、全局 concurrency=80、16 列，每组 20 秒 × 3 轮。

| 配置 | 三轮成功行/秒 | 中位数 行/秒 | 客户端 CPU 中位数 | 请求 P95 中位数 ms |
| --- | --- | ---: | ---: | ---: |
| 新版，cqlsh bundle，1 进程，batch=8 | 3,474 / 3,633 / 3,604 | 3,604 | 110.3% | 4.416 |
| 新版，cqlsh bundle，4 进程，batch=8 | 11,833 / 11,718 / 11,860 | 11,833 | 408.9% | 14.034 |

该旧依赖组合中，4 进程相对 1 进程为 **3.28 倍**；六轮全部零写入错误、回读通过。

不同 Python/driver/reactor 的结果应分开看，不能把 Python 3 + C 扩展的数字直接承诺给旧 cqlsh 环境。

## 功能回归

以下在交付脚本上验证；故障注入只作用于本地临时实例和其压测子进程。

| 检查 | 结果 |
| --- | --- |
| Python 2.7 / Python 3：14 项回归测试 | 均通过：乱序回收、退避不阻塞回收、低速限流回收、callback 计数、批体大小/跨分区保护、分片不重叠、窗口滚动、unique payload、全列校验、加权分位数、timestamp/date 等 |
| Python 3：4 进程，async / batch 各写 10,003 行 | 精确达到目标，失败 0，回读缺失/错值 0；batch 共 1,252 请求，包含各进程尾批 |
| 全局限速 100 行/秒、总量 101 行 | 约 100.7 行/秒，不是每进程 100 行/秒 |
| duration=3 秒 | 到期停止提交并完成排空；不是强杀进程 |
| Python 2 / 3 × async / batch，7 辆车、37 行 | 每种组合 SELECT 全表确认正好 37 行、7 辆车，并对 37 行全部 18 列回读；含 timestamp/date，错值 0 |
| Python 2 + cqlsh bundle，2 进程 batch，1,003 行 | 含 timestamp/date，回读通过；兼容旧 ResultSet 无 one() 的情况 |
| 发布副本，Python 2 -S + cqlsh bundle，4 进程 batch | 禁用 site-packages 后，1,003 行真实写入与回读通过；不依赖另装的 driver 或其他第三方包 |
| 发布副本，Python 2 / 3 -S 内置自测 | 均通过，不连接 Cassandra |
| sync 兼容路径，2 进程、全局 concurrency=80 | 1,003 行写入及回读通过；sync 线程数由 concurrency 决定 |
| custom CK、rolling/uniform 窗口、unique payload | 普通 Cassandra 预建表，10,003 行写入/回读通过；不代表验证了 CHS 行为 |
| 实际分区键与配置不符 | 发压前失败，退出码 2 |
| 父进程 SIGTERM | INTERRUPTED / 130，排空后 pending 和 completion queue 均为 0 |
| 强杀一个确认属于本轮的 writer | FAIL / 非零，不产生虚假的 PASS |

日期回读测试发现并修复了两个实际兼容问题：CQL timestamp 截断微秒；Python 2 的 datetime.date 与 driver Date 比较需要显式转换。它们不影响默认 bigint 时间列的性能，却会误报自定义列的回读失败。

## 如何复现

1. 准备普通 Cassandra 测试节点，在 example-config.json 中删除 table_cql 末尾的 CHS 属性；单节点测试 keyspace 使用 RF=1。填写实际 contact_points/local_dc。不要修改已有生产 keyspace。
2. 保持 16 列、payload、车辆数、CL、时间模式相同，保存旧版脚本与新版脚本的 SHA。每轮使用独立测试表，禁止用反复覆盖相同主键代替新增数据。
3. 每次只运行一个压测命令，至少 3 轮并交错顺序。保存 stdout 日志与 summary JSON。先做短跑；内部性能结论需要更长的稳态测试。

```bash
# 旧版 16 producer，单行异步。旧版没有 --processes 参数。
python -u old/cassandra_vehicle_timeseries_load.py --config old/bench.json \
  --write-mode async --producer-threads 16 --concurrency 640 \
  --rows 0 --duration 20s --summary-json old-async.json

# 新版 4 进程，单行异步；global concurrency 不乘进程数。
python -u cassandra_vehicle_timeseries_load.py --config bench.json \
  --write-mode async --processes 4 --producer-threads 1 --concurrency 640 \
  --run-id '' --rows 0 --duration 20s --summary-json new-async.json

# 新版同分区 batch；与单行组保持相近的最大未完成行数。
python -u cassandra_vehicle_timeseries_load.py --config bench.json \
  --write-mode unlogged_batch --batch-size 8 \
  --processes 4 --producer-threads 1 --concurrency 80 \
  --run-id '' --rows 0 --duration 20s --summary-json new-batch.json

# 旧 cqlsh bundle：只切换已有 Python/Cassandra 路径，不要求 pip install。
python2.7 -u cassandra_vehicle_timeseries_load.py --config bench.json \
  --cassandra-home /path/to/apache-cassandra-3.11.19 \
  --write-mode unlogged_batch --batch-size 8 \
  --processes 4 --producer-threads 1 --concurrency 80 \
  --run-id '' --rows 0 --duration 20s --summary-json bundle-batch.json
```

bench.json 是修改后的普通 Cassandra 配置。用于旧版的配置不要加入 processes、run_id、max_batch_bytes 等 v2 专用字段；旧版对未知字段严格报错。运行前务必更换每轮的 schema.table。调整进程数做 1/2/4/8 扩展曲线时，先保持全局并发不变，再单独增加并发。

## 结论边界

本报告证明本机客户端能利用多个 CPU，并在真实 Cassandra 上提高实际成功写入吞吐。它不是对内部 3 × 16U64G 实例的容量预测，更没有测试 CHS 迁移、三副本写入、跨机网络、长时间 compaction、TTL 清理或故障恢复。

本次是 20 秒重复对照，服务器已经过此前功能测试和预热，表之间共享 JVM、磁盘和后台任务；不是冷启动隔离实验，也不是长压稳态结果。pooled payload 有重复，磁盘增长量不能等同于应用写入字节数。

部署仍只需要一个脚本和一个配置。完整 nohup、停止、字段语义及调参说明见 [README](README.md)。

## 附：交付版本逐轮原始计数

仅列最终脚本冻结后的对照；不混入修复过程中的试跑。main 表全部 18 轮写入错误/失败行数均为 0。

| case | 轮次 | 成功行 | 请求数 | 写入秒数 | 行/秒 | 请求 P95 ms | 客户端 CPU % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| final_old16_async | 1 | 61026 | 61026 | 20.009495 | 3049.852 | 83.735 | 106.200 |
| final_old16_async | 2 | 68337 | 68337 | 20.045170 | 3409.150 | 141.395 | 106.460 |
| final_old16_async | 3 | 53495 | 53495 | 20.047335 | 2668.434 | 128.255 | 107.795 |
| final_new4_async | 1 | 248972 | 248972 | 20.016655 | 12438.242 | 45.014 | 363.098 |
| final_new4_async | 2 | 263089 | 263089 | 20.011134 | 13147.131 | 43.263 | 363.098 |
| final_new4_async | 3 | 254089 | 254089 | 20.007192 | 12699.883 | 44.066 | 356.072 |
| final_old1_async | 1 | 120623 | 120623 | 20.008187 | 6028.682 | 138.382 | 100.359 |
| final_old1_async | 2 | 125401 | 125401 | 20.009891 | 6266.951 | 136.757 | 100.400 |
| final_old1_async | 3 | 126920 | 126920 | 20.018160 | 6340.243 | 134.032 | 100.459 |
| final_new1_async | 1 | 111602 | 111602 | 20.010002 | 5577.311 | 127.679 | 100.450 |
| final_new1_async | 2 | 115407 | 115407 | 20.024926 | 5763.167 | 126.085 | 100.325 |
| final_new1_async | 3 | 123819 | 123819 | 20.033565 | 6180.577 | 122.709 | 100.531 |
| final_old8_batch | 1 | 154968 | 19371 | 20.009297 | 7744.800 | 15.873 | 106.151 |
| final_old8_batch | 2 | 181880 | 22735 | 20.008716 | 9090.038 | 8.462 | 106.054 |
| final_old8_batch | 3 | 188576 | 23572 | 20.009395 | 9424.373 | 21.147 | 105.800 |
| final_new4_batch | 1 | 386984 | 48373 | 20.012805 | 19336.820 | 37.565 | 344.729 |
| final_new4_batch | 2 | 409880 | 51235 | 20.016131 | 20477.484 | 43.036 | 343.873 |
| final_new4_batch | 3 | 573088 | 71636 | 20.006128 | 28645.624 | 24.346 | 353.392 |
| bundle1_batch | 1 | 69576 | 8697 | 20.027627 | 3474.001 | 4.768 | 110.248 |
| bundle1_batch | 2 | 72760 | 9095 | 20.025882 | 3633.298 | 4.327 | 110.257 |
| bundle1_batch | 3 | 72176 | 9022 | 20.025691 | 3604.170 | 4.416 | 110.358 |
| bundle4_batch | 1 | 237024 | 29628 | 20.031290 | 11832.688 | 13.749 | 410.707 |
| bundle4_batch | 2 | 234856 | 29357 | 20.042283 | 11718.026 | 14.523 | 408.886 |
| bundle4_batch | 3 | 237728 | 29716 | 20.044810 | 11859.828 | 14.034 | 408.834 |
服务端 JVM 的命令区间 CPU 辅助观察：async 旧版 16 producer / 新版 4 进程中位数分别为 77.3% / 184.9%，batch 旧版 / 新版分别为 88.7% / 232.2%。此数来自 JVM 进程 CPU 时间差 / 整条压测命令墙钟时间，包含客户端启动及回读区间，与上表仅写入阶段的客户端 CPU 口径不同，不作为严格 CPU 成本比较。
