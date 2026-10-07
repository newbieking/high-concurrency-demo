/**
 * 秒杀瞬时洪峰：开放模型（按到达速率发压），验证「削峰 + 不超卖 + 不丢单」。
 *
 * 为什么用 ramping-arrival-rate 而不是固定 VU：
 *   固定 VU 是闭环模型，一旦单请求变慢，VU 自己就少发请求了，压力会被动回落，
 *   恰好把「尖峰打进来」这个最需要验证的场景抹平。到达速率模型不管后端多慢都按
 *   既定 RPS 发压，溢出只能体现为排队、超时或业务拒绝（1004 BUSY），这才是秒杀的真实形态。
 *
 * 断言链路：客户端成功数 == DB 订单数 == 预扣数，且 DB 库存不为负。
 *   - 客户端成功数多 → 丢单（缓冲/MQ/消费者丢消息）
 *   - DB 订单数多 → 超卖或重复落库
 *
 * 运行：./loadtest/run-k6.sh seckill-burst.js
 *      RATE=3000 DURATION=60s ./loadtest/run-k6.sh seckill-burst.js
 */
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
    CODE_BUSY, CODE_DUPLICATE, CODE_OK, CODE_SOLD_OUT,
    fail, get, intEnv, parse, post, report, resetSeckill, strEnv,
    uniqueRequestId, uniqueUserId, waitDrained,
} from './lib/common.js';

const ACTIVITY_ID = intEnv('ACTIVITY_ID', 1);
const RATE = intEnv('RATE', 2000);          // 峰值 RPS
const RAMP = strEnv('RAMP', '10s');         // 爬到峰值的时间
const HOLD = strEnv('DURATION', '30s');     // 峰值持续时间
const DRAIN_TIMEOUT = intEnv('DRAIN_TIMEOUT', 90000);

const accepted = new Counter('seckill_accepted');
const soldOut = new Counter('seckill_sold_out');
const duplicate = new Counter('seckill_duplicate');
const busy = new Counter('seckill_busy');
const unexpected = new Counter('seckill_unexpected');
const acceptOk = new Rate('seckill_accept_ok');
const acceptMs = new Trend('seckill_accept_ms', true);

export const options = {
    scenarios: {
        burst: {
            executor: 'ramping-arrival-rate',
            startRate: Math.max(1, Math.floor(RATE / 20)),
            timeUnit: '1s',
            preAllocatedVUs: intEnv('PRE_VUS', 200),
            maxVUs: intEnv('MAX_VUS', 3000),
            stages: [
                { target: RATE, duration: RAMP },
                { target: RATE, duration: HOLD },
                { target: 0, duration: '5s' },
            ],
        },
    },
    // 阈值只兜「服务还在正常应答」这一层，业务正确性由 teardown 的不变量断言负责
    thresholds: {
        'seckill_accept_ok': ['rate>0.99'],
        'seckill_unexpected': ['count==0'],
        'http_req_failed': ['rate<0.01'],
        'seckill_accept_ms': ['p(95)<1000', 'p(99)<3000'],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
};

function durationSec(text) {
    const m = /^(\d+)(ms|s|m)$/.exec(String(text).trim());
    if (!m) {
        fail(`时间格式只支持 500ms / 30s / 2m，收到: ${text}`);
    }
    const n = parseInt(m[1], 10);
    return m[2] === 'ms' ? n / 1000 : m[2] === 's' ? n : n * 60;
}

export function setup() {
    const res = resetSeckill(ACTIVITY_ID);
    check(res, { '重置秒杀活动成功': (r) => parse(r).code === CODE_OK });
    const stats = parse(get(`/api/ops/seckill/${ACTIVITY_ID}/stats`));
    if (stats.code !== CODE_OK || !stats.data) {
        fail('拿不到活动信息，确认应用与 MySQL/Redis 已启动');
    }
    const stockTotal = stats.data.stockTotal;
    // 到达速率线性爬坡，总请求数 ≈ 各阶段平均速率 × 时长
    const planned = Math.round((RATE / 20 + RATE) / 2 * durationSec(RAMP)
        + RATE * durationSec(HOLD) + RATE / 2 * 5);
    const fullySoldOut = planned > stockTotal * 1.5;
    console.log(`活动 ${ACTIVITY_ID} 库存 ${stockTotal}，压力 ${RATE}/s 持续 ${HOLD}（含 ${RAMP} 爬坡），` +
        `计划请求数约 ${planned}${fullySoldOut ? '，预期全部售罄' : '，请求数不足以售罄'}`);
    return { stockTotal: stockTotal, planned: planned, fullySoldOut: fullySoldOut };
}

export default function () {
    const userId = uniqueUserId(intEnv('UID_BASE', 100000000));
    const requestId = uniqueRequestId('burst');
    const res = post(`/api/seckill/${ACTIVITY_ID}/orders`,
        { userId: userId, requestId: requestId }, { name: 'seckill_accept' });
    acceptMs.add(res.timings.duration);

    const body = parse(res);
    switch (body.code) {
        case CODE_OK:
            accepted.add(1);
            acceptOk.add(true);
            break;
        case CODE_SOLD_OUT:
            soldOut.add(1);
            acceptOk.add(res.status === 200);
            break;
        case CODE_DUPLICATE:
            duplicate.add(1);
            acceptOk.add(res.status === 200);
            break;
        case CODE_BUSY:
            busy.add(1);
            acceptOk.add(res.status === 200);
            break;
        default:
            unexpected.add(1);
            acceptOk.add(false);
            console.log(`意外响应 status=${res.status} code=${body.code} msg=${body.message}`);
    }
}

export function teardown(data) {
    const final = waitDrained(ACTIVITY_ID, undefined, DRAIN_TIMEOUT);
    console.log(report('秒杀洪峰最终校验', final));
    if (!final) {
        fail('校验接口无返回');
    }
    if (!final.seckill.drained) {
        fail(`缓冲没排干：在途=${final.seckill.inFlight} Stream长度=${final.seckill.streamLength} ` +
            `未确认=${final.seckill.streamPending}，检查 MQ broker / 消费者日志`);
    }
    if (!final.pass) {
        fail('一致性校验不通过：' + JSON.stringify(final.seckill.violations));
    }
    if (final.seckill.dbStock < 0) {
        fail('DB 库存为负：超卖');
    }
    if (final.seckill.dbOrders !== final.seckill.redisPreDeducted) {
        fail(`丢单或重单：DB订单=${final.seckill.dbOrders} 预扣=${final.seckill.redisPreDeducted}`);
    }
    if (final.seckill.dbOrders > data.stockTotal) {
        fail(`DB 订单 ${final.seckill.dbOrders} 超过总库存 ${data.stockTotal}：超卖`);
    }
    if (data.fullySoldOut && final.seckill.dbOrders !== data.stockTotal) {
        fail(`请求数 ${data.planned} 远超库存，但只落库 ${final.seckill.dbOrders}/${data.stockTotal} 单：有请求被丢了`);
    }
    if (final.seckillDeadLetters > 0) {
        fail(`出现 ${final.seckillDeadLetters} 条死信，说明有请求重投到上限仍未落库`);
    }
    console.log('秒杀洪峰验证通过：无超卖、无丢单、无死信');
}
