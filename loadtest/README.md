# 压测与故障演练

三个问题、四个脚本：

| 脚本 | 模型 | 回答什么问题 | 关键断言 |
| --- | --- | --- | --- |
| `seckill-burst.js` | 开放（到达速率） | 洪峰打进来会不会超卖 / 丢单 | 客户端成功数 == DB 订单数 == 预扣数，DB 库存不为负，无死信 |
| `seckill-e2e.js` | 闭环（固定 VU） | 用户从下单到看到最终结果要等多久 | 异步落定 p95 < 1s，且无请求卡在 PENDING |
| `like-burst.js` | 开放（到达速率） | 乱序/重投的增量事件会不会算错 | Redis 计数 == DB 计数 == 净点赞人数 |
| `idempotency-check.js` | 闭环（同一请求号狂点） | 重复提交会不会落两单 | 同一 requestId 全局只成功 1 次 |

另外两个脚本是运维向的：

| 脚本 | 作用 |
| --- | --- |
| `run-k6.sh` | 用容器跑 k6，避免本机安装；透传压测参数 |
| `fault-drill.sh` | 压测中途拔掉 MQ broker，验证缓冲兜底 + 自愈 + 不丢单 |

## 怎么跑

```bash
# 需要应用已在 8080 启动，docker compose 里的 MySQL/Redis/RocketMQ 已就绪
./loadtest/run-k6.sh seckill-burst.js
RATE=3000 DURATION=60s ./loadtest/run-k6.sh seckill-burst.js
./loadtest/run-k6.sh like-burst.js
./loadtest/run-k6.sh idempotency-check.js
./loadtest/run-k6.sh seckill-e2e.js

./loadtest/fault-drill.sh
```

`run-k6.sh` 用 `grafana/k6` 镜像，脚本目录挂到容器内 `/scripts`，`BASE_URL` 默认
`http://host.docker.internal:8080`（k6 在容器里，应用在宿主机上）。本机装了 k6 的话直接
`k6 run loadtest/seckill-burst.js` 也行，但要自己把 `BASE_URL` 指到 `http://127.0.0.1:8080`。

退出码就是结论：`0` 通过；`99` 阈值没过；抛异常（teardown 断言失败）也会非 0。
所以可以直接挂到 CI 或发布卡点上。

## 压测方案怎么选

### 1. 工具选型

| 工具 | 适合 | 代价 | 本项目为什么没用它 |
| --- | --- | --- | --- |
| **k6**（选它） | HTTP/WS 接口压测、脚本即代码、阈值可做断言、CI 友好 | JS 生态、UI 报告要接 Grafana | —— |
| JMeter | 图形化、协议全、老团队熟悉 | GUI 编脚本，改一次点一堆；断言能力弱 | 脚本要进 Git，JMeter 的 XML 不好 review |
| wrk / wrk2 | 单接口极限吞吐，压力机资源效率最高 | 脚本能力弱（Lua），做不了「下单→轮询→断言」的链路 | 我们还要验一致性不变量，wrk 干不了 |
| Gatling | Scala DSL，报告漂亮 | JVM 环境 + 学习曲线 | 为了一个压测再引一套 Scala 工程，不划算 |
| Locust | Python，脚本灵活、易扩展 | 单进程并发能力弱，压高 RPS 要吃 CPU | 高 RPS 场景需要多进程编排 |

结论：接口契约、断言、CI 可跑，这三点决定了 k6。工具本身不是重点，**能自动判定「不变量是否成立」才是重点**——
本项目的压测结论不是「QPS 多少」，而是「客户端成功数、Redis 预扣数、DB 订单数三者相等且库存不为负」。

### 2. 发压模型：开放 vs 闭环

这是秒杀压测里最容易搞错的一件事。

- **闭环（固定 VU / closed model）**：VU 发完一个请求、等响应、再发下一个。后端一慢，VU 自动少发，压力跟着回落。
  用它压秒杀会掩盖最需要暴露的场景：**洪峰到达时不因为后端变慢而减少**。
- **开放（到达速率 / open model，`ramping-arrival-rate`）**：不管后端多慢，都按既定 RPS 发。
  后端慢了就排队、超时、或者被业务拒绝（1004 排队过多），这才是线上的真实形态。

所以 `seckill-burst.js` 与 `like-burst.js` 用开放模型，`seckill-e2e.js` / `idempotency-check.js` 用闭环模型——
后两者的目的是量时延和验幂等，需要的是「每单都有确定结果」，不是压满。

### 3. 压力机放哪

| 方案 | 适用 | 注意 |
| --- | --- | --- |
| 本机 k6 容器（本仓库默认） | 开发自测、< 3k RPS | k6 和被压服务抢同一台机器，测出来的时延偏悲观；Windows 上 Docker 端口映射还有额外开销 |
| 独立压测机 | 常规验证 | 千兆网卡跑到 ~10w RPS 就受带宽限制，注意客户端端口耗尽（见下） |
| 分布式 k6（k6 operator on k8s / 多容器） | 需要跑满多核或 RPS > 5w | 分片按 `__VU`/`__ITER` 切分，userId 生成要避免跨实例碰撞（本仓库用 `UID_BASE` 错开） |
| 云压测（阿里云 PTS 等） | 要百万并发的报告 | 花钱，且压测流量出口 IP 固定，容易被网关限流误判 |

## 参数速查

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `RATE` | 秒杀 2000 / 点赞 3000 | 峰值到达速率（每秒请求数） |
| `RAMP` / `DURATION` | 10s / 30s | 爬到峰值的时间 / 峰值持续时长 |
| `DRAIN_TIMEOUT` | 90000 ms | teardown 等缓冲排干的最长时间 |
| `ACTIVITY_ID` | 1 | 秒杀活动 ID |
| `TARGET_ID` | 101 | 点赞对象 ID |
| `UNLIKE_RATIO` | 2 | 每 N 个用户有 1 个「点了又取消」 |
| `UID_BASE` | 各脚本不同 | userId 起始偏移，分布式压测时用来错开 |
| `VUS` / `ITERATIONS` | 50 / 10 | 仅端到端脚本：并发数 / 每 VU 下单次数 |

脚本在 `setup()` 里会调 `/api/ops/seckill/{id}/reset` 与 `/api/ops/like/{id}/reset` 复位数据，
**会把该活动的订单和 Redis 状态清空**，别对着线上环境跑。

## 故障演练在验什么

`fault-drill.sh` 的剧本（默认库存 1000、RATE=120）：

```
t=0    复位 + 预热，k6 后台开始下单
t=5    停掉 broker  →  生产者发送失败，消息 XACK 不掉，堆在 Redis Stream 里（在途台账同步上涨）
t=5..17 故障期：/api/ops/seckill/1/stats 能看到在途与 Stream 长度一路涨到几百，DB 订单数不动
t=17   拉起 broker  →  生产者恢复；超过 5s 未确认的消息被 XCLAIM 认领后重投
t≈30   k6 收尾，teardown 等排干并断言
```

关键点：**受理链路全程没有受影响**（库存判断在 Redis 上），MQ 挂了用户照样能下单成功，
只是「最终落库」被推迟到 MQ 恢复之后。这正是「Redis 缓冲 + MQ 异步」要买的那份韧性。

一次真实运行的观测值：

```
正常期  在途/Stream长度/未确认 = 0 / 0 / 0
故障期  在途/Stream长度/未确认 = 768 / 768 / 768      ← broker 停机 12s 期间堆下来的
恢复后  最终校验：drained=true pass=true 预扣=1000 DB订单=1000 DB库存=0 死信=0
```

> 调参提示：库存只有 1000，所以要让「售罄」发生在故障窗口之后，否则断线期间没消息可堆，演练看不出效果。
> 想堆更多消息就把 `RATE` 调小或把 `OUTAGE` 调大（例如 `RATE=60 OUTAGE=20`）。

## 瓶颈排查清单

压不上去、或时延突然抬头时，按链路顺序看：

| 环节 | 现象 | 观测点 |
| --- | --- | --- |
| k6 压力机 | `dropped_iterations` 增长、响应时间整体抬升 | k6 的 `vus_max` 是否撞到 `MAX_VUS`；容器 CPU |
| 客户端端口耗尽 | 出现 `address already in use` / 连接超时 | Windows `netsh int ipv4 show dynamicport tcp`；Linux `TIME_WAIT` 数量 |
| Tomcat 线程 | 请求排队、`http_req_duration` p99 跳变 | `server.tomcat.threads.max`（当前 500）、`accept-count`（2000）、actuator 指标 |
| Redis | 受理接口 p99 变差、Lua 执行排队 | `SLOWLOG`、`INFO commandstats`；单实例是单线程，Lua 里别放重活 |
| 在途限流 | 大量 1004「排队人数过多」 | `demo.seckill.max-in-flight`（当前 20000）与在途 ZSet 基数 |
| MQ | 在途/Stream 长度持续上涨不回落 | broker 是否健康、发送线程是否够、消费组是否积压 |
| 消费端 | 落库慢，Stream 排不干 | `demo.mq.consumer-threads`（32）、消费批量、单批事务时长 |
| DB | 落库阶段变慢，`streamPending` 高 | Hikari 池（64）、`innodb_flush_log_at_trx_commit`、唯一索引冲突率 |

## 已知边界

- **幂等窗口 = 请求状态 TTL**（`demo.seckill.request-ttl-seconds`，默认 7200s）。TTL 过期后同一 requestId
  会重新被受理，但 DB 的 `uk_order_request` 唯一索引仍会拦下落库，对账侧会出现「预扣 > 订单」。生产上要么把
  幂等记录放 DB/长 TTL，要么让请求号带上活动期。
- **点赞的用户集合是全量加载**（`SADD` 所有已点赞用户）。对象有大 V 量级时必须换结构（分片集合 / Bitmap / Roaring）。
- **Redis 单分片**：`XADD` 写在 Lua 里，要求同一个 Lua 里的 key 落在同一个 slot。秒杀按活动分片
  （`sk:{aid}:*`）、点赞按对象分片（`lk:{targetId}:*`），但 `lk:inflight`、`sk:stream`/`lk:stream`
  与排行榜 `lk:rank` 是全局 key，上 Redis Cluster 时这几个 key 必须和分片 key 一起重新设计
  （例如把 Stream 也做成按活动/对象分片，或改用非 Cluster 的哨兵部署）。
- **`reset` 接口必须在空闲时调用**：它会删掉共享的 Stream/在途集合/排行榜，有流量时调用会丢在途消息。
