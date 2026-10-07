# 高并发写 + 最终一致 参考实现（秒杀 / 点赞）

用 Redis 做缓冲与预扣、RocketMQ 做异步削峰、MySQL 做最终落库，加定时对账补偿。
两个场景共用一套骨架：

```
客户端 ──► 应用（受理：一次 Lua 原子完成校验+预扣+写缓冲）
                │
                ├─ Redis：库存/计数、去重集合、在途台账、Stream 缓冲   ← 承接流量
                │
                └─ StreamDispatcher ──► RocketMQ ──► 消费者 ──► MySQL  ← 最终落库
                                                          │
                                        定时对账 / 超时重投 / 回滚 ◄─┘
```

核心不变式：**受理成功数 == Redis 预扣数 == DB 落库数，库存永不为负。**
压测脚本不是只看 QPS，而是直接断言这条不变式（见 `loadtest/README.md`）。

## 为什么这么分层

| 层 | 承担 | 不承担 |
| --- | --- | --- |
| Redis | 准入判断、预扣、去重、计数、缓冲、在途台账 | 最终真相。它是有 TTL、可能丢的 |
| MQ | 异步削峰、解耦、可重投 | 事务。它是 at-least-once，不保证不重复 |
| MySQL | 唯一真相、唯一索引兜底、对账基准 | 承接洪峰。它扛不住瞬时写 |
| 定时对账 | 补漏：超时重投、重投上限回滚、计数修复 | 实时性。它是「最后一层安全网」 |

## 两个场景的差异

| | 秒杀 | 点赞 |
| --- | --- | --- |
| 事件语义 | 一次性扣减（不可交换） | 增量 delta（可交换） |
| 顺序要求 | 同一用户必须串行，靠唯一索引 + 条件更新保证不超卖 | 无顺序要求，乱序/重投都对 |
| 去重键 | `requestId` + `(activityId, userId)` | `streamId` + `(targetId, userId)` |
| 落库写法 | `UPDATE ... WHERE stock_available > 0` 条件更新 | `like_count = like_count + delta` 增量累加 |
| 状态收敛 | DB 为唯一真相，Redis 预扣与 DB 订单对齐 | Redis 计数为真相，DB 是它的投影，不一致时**从 Redis 修 DB** |
| 冲突处理 | 抛异常 → 重投 → 到上限回滚 + 死信 | LWW 按 `seq` 覆盖最终状态，计数永不回退 |

点赞的「可交换」是它能容忍乱序的根本原因：`+1/-1` 加法与顺序无关，所以 MQ 里怎么乱、重投几次，
只要最终每个事件都被消费一次（幂等靠 `idempotent_record`），计数就是对的。换成「最终状态覆盖」的写法，
「点赞→取消→点赞」在乱序下就会丢更新。

## 关键实现点

**1. 受理是一次 Lua，不是多条命令**

`lua/seckill_accept.lua` 在一次原子执行里完成：库存存在性检查 → requestId 幂等检查 → 用户限购去重 →
售罄判断 → 在途限流 → `DECR` 库存 → 写 Stream → 记在途 ZSet → 写请求状态。任何一步非预期，整个脚本不产生副作用。

`lua/like_toggle.lua` 同理：状态去重 → `INCRBY` 计数 → 写 Stream → 记在途 → 更新排行榜。

> 踩过的坑：**Redis 的 Lua 出错不会回滚已执行的写命令**。我们给排行榜加「热度归零就摘除」时写了
> `ZINCRBY(...) <= 0`，而 `ZINCRBY` 返回的是 bulk string，Lua 直接报 `attempt to compare string with number`——
> 计数、Stream、在途都已经写进去了，只有排行榜那步失败，客户端拿到 500 但业务其实生效了。
> 脚本里凡是要算术比较的返回值，先 `tonumber`；另外别把「可能报错」的操作放在写操作后面。

**2. 先发 MQ 再 XACK，顺序不能反**

`infra/StreamDispatcher` 从 Stream 读一批 → 投递 MQ → **成功后才** `XACK + XDEL`。
投递抛异常就不 XACK，消息留在 PEL 里，等 `XCLAIM` 重投。反过来先 XACK 再发 MQ，进程一挂消息就真丢了。

**3. 三层幂等，逐层兜底**

| 层 | 手段 | 挡住什么 |
| --- | --- | --- |
| Redis Lua | `EXISTS req` / `SISMEMBER buyers` | 用户狂点、网关重试 |
| MySQL | `uk_order_request`、`uk_order_activity_user` 唯一索引 | Lua 之后的重复（含幂等窗口过期后） |
| 消费端 | `idempotent_record` 唯一键 + 条件更新 | MQ at-least-once 重投、重投批次重叠 |

**4. 在途台账让「排干」可判定**

`sk:{aid}:pending`（ZSet，member=requestId，score=受理时间）和 `lk:inflight`（Set，member=streamId）。
消费者无论成功、失败、重投，最后都要摘掉自己那条（`ZREM`/`SREM` 幂等）。
于是「排干」有了严格定义：`在途 == 0 且 Stream 长度 == 0 且 PEL == 0`。
**只有排干之后**，Redis 与 DB 的比对才是有意义的——否则永远在跟「正在飞的消息」打架。

**5. 对账不只是比对，还要动手修**

- 在途超时（默认 20s）→ 重投 MQ（覆盖「MQ 丢消息」这条缓冲兜不住的路径）；
- 重投到上限（默认 5 次）仍失败 → `rollback` 归还预扣库存 + 写死信 + 记 `reconcile_diff`；
- 已落库但确认丢了的 → 补一次 `confirm`，不重投；
- 秒杀：排干后比对 `预扣数 vs DB 订单数`、`Redis 库存 vs DB 库存`；
- 点赞：排干后比对每个对象的 `Redis 计数 vs DB 计数`，不一致就**从 Redis 修 DB** 并记 `reconcile_diff`。

**6. 自动预热必须「只补不覆盖」**

请求路径上的首访预热用 `setIfAbsent`（`ensureWarm`），绝不清理已购集合/已点赞用户。
否则并发首访会互相把去重状态清掉，同一个用户被重复受理——这个坑在压测里真的踩到了
（同一 requestId 出现 3 次成功，见 `idempotency-check.js` 的注释）。
覆盖式的 `warmup` 只留给运维接口和无流量场景。

## 目录

```
src/main/java/com/example/demo/
  common/     ApiResponse、ResultCode、BizException、死信 DAO
  config/     AppProps（demo.* 配置）、Redis/MQ/消费者/调度器装配
  infra/      MsgCodec（URL 编码的扁平报文）、RedisBuffer（Stream 封装）、StreamDispatcher、MqSender
  seckill/    秒杀：Service(受理/确认/回滚/预热)、Writer(幂等落库)、Consumer、ReconcileJob、Controller
  like/       点赞：Service、Writer(增量累加+LWW)、Consumer、ReconcileJob、Controller
  ops/        运维接口：统计、一致性校验、预热、复位、死信
src/main/resources/lua/   5 个 Lua 脚本（受理/确认/回滚/点赞/点赞确认）
sql/schema.sql            建表 + 种子数据
docker-compose.yml        MySQL 8.4 + Redis 7.4 + RocketMQ 5.3.2（含 broker 配置与 topic 预建）
docker/rocketmq/          broker.conf
loadtest/                 k6 压测脚本 + 故障演练 + 方案选型说明
```

## 启动

```bash
# 1. 中间件（首次会自动建表建库、预建 topic，broker 要 30s 左右才 healthy）
docker compose up -d
docker compose ps

# 2. 应用（宿主机的 8080）
./mvnw spring-boot:run
```

几个和默认不一样、需要注意的点：

- **Redis 映射到宿主机 6380**（不是 6379）。宿主 6379 可能已被别的 Redis 占用，避免冲突。
  `application.yml` 里对应 `spring.data.redis.port: 6380`。
- **broker 的 `brokerIP1 = 127.0.0.1`**：应用跑在宿主机上，broker 在容器里，客户端拿到的是
  `127.0.0.1:10911` 并通过端口映射连回来。同时 `rocketmq-topic-init` 用容器名 `rocketmq-broker:10911`
  建 topic，两者互不影响。
- **具名卷要先 chown**：broker 以 uid=3000 运行，而新建的具名卷属于 root，会让 store 初始化失败
  （容器退出码 255、日志里只有一句被掩盖的 NPE）。compose 里的 `rocketmq-volume-init` 就是干这个的。
- topic 已预建（8 队列）：不然消费者首次订阅拿不到路由，要等生产者第一次发送触发自动建 topic 才开动，
  首次调用会有几十秒的「只受理不落库」窗口。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/seckill/{aid}/orders` | 下单受理，body `{userId, requestId}`；同步返回受理结果与剩余库存 |
| GET | `/api/seckill/{aid}/requests/{requestId}` | 查询请求最终状态（PENDING / CONFIRMED / FAILED） |
| POST | `/api/like/{tid}/like?userId=` | 点赞 |
| POST | `/api/like/{tid}/unlike?userId=` | 取消点赞 |
| GET | `/api/like/{tid}?userId=` | 查计数与「我是否点过」 |
| GET | `/api/like/rank?top=` | 热度排行榜 |

业务码：`0` 成功、`1001` 未预热、`1002` 售罄、`1003` 重复参与、`1004` 排队过多。

运维接口（压测脚本与演练靠它做断言）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/ops/verify?activityId=&targetId=` | 一次算完所有不变量，`pass=true` 才代表一致 |
| GET | `/api/ops/seckill/{aid}/stats` | 秒杀一致性快照（库存、预扣、订单、在途、Stream） |
| GET | `/api/ops/like/stats` | 点赞计数差异 |
| POST | `/api/ops/seckill/{aid}/warmup`、`/reset` | 预热（覆盖式）/ 复位 |
| POST | `/api/ops/like/{tid}/warmup`、`/reset`、`/like/rank/rebuild` | 同上 |
| GET | `/api/ops/dead-letters?bizType=&limit=` | 死信查询 |

> `reset` 会清空该活动的订单、Redis 状态与共享 Stream，**只能在系统空闲时调用**。

## 压测结果（本机 Docker Desktop，单实例，含 k6 自身开销）

| 场景 | 脚本与参数 | 结果 |
| --- | --- | --- |
| 秒杀洪峰 | `seckill-burst.js`，RATE=2000、爬坡 10s + 峰值 20s | 55,499 次请求：受理 avg 4.2ms / p95 6.5ms / p99 10.2ms，HTTP 失败 0；1000 单全部落库，预扣==订单，DB 库存 0，死信 0 |
| 端到端时延 | `seckill-e2e.js`，30 VU × 8 单 | 受理 p95 16ms；从下单到状态落定 p50 75ms / p95 189ms / p99 353ms，无 PENDING 残留 |
| 点赞洪峰 | `like-burst.js`，RATE=1500、含「点了又取消」 | 28,687 次点赞 + 14,392 次取消：p99 28ms，异常 0；Redis 计数 == DB 计数 == 14,295 |
| 幂等 | `idempotency-check.js`，20 VU 并发同一 requestId ×40 | 成功 1 次、重复 39 次；Redis 预扣 1、DB 订单 1、库存只扣 1 |
| MQ 中断演练 | `fault-drill.sh`，broker 停机 12s | 停机期间 768 条消息堆在 Redis（受理不受影响）；恢复后全部落库，1000/1000，死信 0 |

> 数字是「能被压到多少」而不是「极限在哪」：压测机和应用在同一台笔记本上，
> k6 容器本身也吃 CPU。真要报吞吐量，把 k6 挪到独立压测机（见 `loadtest/README.md`）。

## 已知边界

- **不适合强一致资金交易**。这套结构买到的是吞吐和韧性，代价是「确认前的不确定窗口」；
  转账、扣款这类必须同步一致的场景应该走 DB 事务 + 行锁，或 TCC/SAGA。
- **幂等窗口 = 请求状态 TTL**（默认 7200s）。TTL 过期后重复提交会被 DB 唯一索引拦下，但对账会看到
  「预扣 > 订单」。生产上把幂等记录放 DB 或用带活动期的请求号。
- **Redis Cluster 不直接可用**：Lua 里的多个 key 必须在同一 slot。秒杀按活动分片、点赞按对象分片，
  但 Stream 缓冲与排行榜是全局 key，需要重新设计（分片 Stream / 非 Cluster 部署）。
- **点赞的用户集合全量加载**，大 V 量级要换 Bitmap / Roaring / 分片集合。
- **排行榜始终以 Redis 为准**，只用于展示，不参与对账（对账只比计数）；归零的对象会从榜单摘除，
  但榜单本身不保证与 DB 逐条一致。
