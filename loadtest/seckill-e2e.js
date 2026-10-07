/**
 * 秒杀端到端时延：受理（Redis 预扣）→ 缓冲 → MQ → 消费者落库 → 状态回写。
 *
 * 它回答的问题和洪峰脚本不同：
 *   洪峰脚本测「扛不扛得住、会不会超卖」；
 *   这个脚本测「异步链路多久能让用户看到最终结果」，也就是前端轮询要等多久。
 *
 * 闭环模型（固定 VU）+ 有界迭代：每个 VU 只做 N 单，避免把库存打光后全部变成 1002，
 * 也避免请求量太小导致 p95 抖动失真。
 *
 * 运行：./loadtest/run-k6.sh seckill-e2e.js
 */
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import {
    CODE_DUPLICATE, CODE_OK, CODE_SOLD_OUT,
    fail, get, intEnv, parse, post, report, resetSeckill,
    uniqueRequestId, uniqueUserId, waitDrained,
} from './lib/common.js';

const ACTIVITY_ID = intEnv('ACTIVITY_ID', 1);
const VUS = intEnv('VUS', 50);
const ITERATIONS = intEnv('ITERATIONS', 10);   // 每个 VU 的下单次数
const POLL_MAX_MS = intEnv('POLL_MAX_MS', 5000);
const POLL_INTERVAL_MS = intEnv('POLL_INTERVAL_MS', 20);
const DRAIN_TIMEOUT = intEnv('DRAIN_TIMEOUT', 60000);

const accepted = new Counter('e2e_accepted');
const confirmed = new Counter('e2e_confirmed');
const rejected = new Counter('e2e_rejected');
const timedOut = new Counter('e2e_timeout');
const acceptMs = new Trend('e2e_accept_ms', true);
const settleMs = new Trend('e2e_settle_ms', true);   // 从下单到状态落定的墙钟时间

export const options = {
    scenarios: {
        e2e: {
            executor: 'per-vu-iterations',
            vus: VUS,
            iterations: ITERATIONS,
            maxDuration: '5m',
        },
    },
    thresholds: {
        'e2e_timeout': ['count==0'],
        'e2e_settle_ms': ['p(95)<1000', 'p(99)<3000'],
        'e2e_accept_ms': ['p(95)<200'],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
};

// 自定义计时器：只统计超时次数
export function setup() {
    const res = resetSeckill(ACTIVITY_ID);
    check(res, { '重置秒杀活动成功': (r) => parse(r).code === CODE_OK });
    const stats = parse(get(`/api/ops/seckill/${ACTIVITY_ID}/stats`));
    if (stats.code !== CODE_OK || !stats.data) {
        fail('拿不到活动信息');
    }
    const needed = VUS * ITERATIONS;
    if (stats.data.stockTotal < needed) {
        console.log(`警告：库存 ${stats.data.stockTotal} 小于计划下单数 ${needed}，部分请求会返回售罄`);
    }
    return { stockTotal: stats.data.stockTotal, planned: needed };
}

export default function () {
    const userId = uniqueUserId(intEnv('UID_BASE', 200000000));
    const requestId = uniqueRequestId('e2e');
    const startedAt = Date.now();

    const res = post(`/api/seckill/${ACTIVITY_ID}/orders`,
        { userId: userId, requestId: requestId }, { name: 'seckill_accept' });
    acceptMs.add(res.timings.duration);
    const body = parse(res);

    if (body.code === CODE_SOLD_OUT || body.code === CODE_DUPLICATE) {
        rejected.add(1);
        return;
    }
    if (body.code !== CODE_OK) {
        fail(`受理失败 code=${body.code} msg=${body.message}`);
    }
    accepted.add(1);

    // 轮询最终状态：真实前端也是这么做的
    const deadline = startedAt + POLL_MAX_MS;
    let state = 'PENDING';
    while (Date.now() < deadline) {
        const view = parse(get(`/api/seckill/${ACTIVITY_ID}/requests/${requestId}`, { name: 'seckill_result' }));
        if (view.code === CODE_OK && view.data && view.data.state !== 'PENDING') {
            state = view.data.state;
            break;
        }
        sleep(POLL_INTERVAL_MS / 1000);
    }

    if (state === 'PENDING') {
        // 超过轮询上限还没落定：算超时，阈值里要求它为 0
        timedOut.add(1);
        return;
    }
    settleMs.add(Date.now() - startedAt);
    if (state === 'CONFIRMED') {
        confirmed.add(1);
    } else {
        rejected.add(1);
    }
}

export function teardown(data) {
    const final = waitDrained(ACTIVITY_ID, undefined, DRAIN_TIMEOUT);
    console.log(report('秒杀端到端最终校验', final));
    if (!final) {
        fail('校验接口无返回');
    }
    if (!final.seckill.drained) {
        fail(`仍有未落定的请求：在途=${final.seckill.inFlight} Stream长度=${final.seckill.streamLength}`);
    }
    if (!final.pass) {
        fail('一致性校验不通过：' + JSON.stringify(final.seckill.violations));
    }
    if (final.seckill.dbOrders !== final.seckill.redisPreDeducted) {
        fail(`丢单或重单：DB订单=${final.seckill.dbOrders} 预扣=${final.seckill.redisPreDeducted}`);
    }
    console.log(`端到端验证通过：受理成功 ${final.seckill.dbOrders} 单，批次内无超时；` +
        `确认前的轮询延迟看 e2e_settle_ms 指标（计划下单 ${data.planned} 次）`);
}
