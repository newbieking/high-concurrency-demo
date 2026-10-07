/**
 * 幂等验证：同一个 requestId 被重复提交（用户狂点、网关重试、MQ 重投）时，
 * 只能产生一单，且每次查询都必须拿到同一个结果。
 *
 * 三层防线一起验：
 *   1) Redis Lua 里的 SADD 请求去重（同一 requestId 只有第一次能进）；
 *   2) DB 唯一索引 uk_order_request / uk_order_activity_user（兜住 Lua 之后的重复）；
 *   3) 消费端 idempotent_record + 条件更新（兜住 MQ at-least-once 重投）。
 *
 * 这里验的是入口层（1、2）。第 3 层由 fault-drill.sh 停 broker 触发重投来验。
 *
 * 运行：./loadtest/run-k6.sh idempotency-check.js
 */
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import {
    CODE_DUPLICATE, CODE_OK, fail, get, intEnv, parse, post, report, resetSeckill, waitDrained,
} from './lib/common.js';

const ACTIVITY_ID = intEnv('ACTIVITY_ID', 1);
const REPLAYS = intEnv('REPLAYS', 40);       // 同一 requestId 并发+串行提交的总次数
const DRAIN_TIMEOUT = intEnv('DRAIN_TIMEOUT', 60000);

const okResponses = new Counter('idem_ok');
const dupResponses = new Counter('idem_duplicate');
const otherResponses = new Counter('idem_other');

const REQUEST_ID = 'idem-fixed-request';
const USER_ID = 424242;
const VUS = 20;
const ITERATIONS_PER_VU = Math.max(1, Math.ceil(REPLAYS / VUS));

export const options = {
    scenarios: {
        replay: {
            executor: 'per-vu-iterations',
            vus: VUS,
            iterations: ITERATIONS_PER_VU,
            maxDuration: '1m',
        },
    },
    thresholds: {
        // 同一个 requestId 全局只允许成功一次
        'idem_ok': ['count==1'],
        'idem_other': ['count==0'],
    },
};

export function setup() {
    const res = resetSeckill(ACTIVITY_ID);
    check(res, { '重置秒杀活动成功': (r) => parse(r).code === CODE_OK });
    console.log(`同一个 requestId=${REQUEST_ID} 将被提交 ${VUS * ITERATIONS_PER_VU} 次（${VUS} 个 VU 并发）`);
    return {};
}

export default function () {
    const res = post(`/api/seckill/${ACTIVITY_ID}/orders`,
        { userId: USER_ID, requestId: REQUEST_ID }, { name: 'seckill_accept' });
    const body = parse(res);
    if (body.code === CODE_OK) {
        okResponses.add(1);
    } else if (body.code === CODE_DUPLICATE) {
        dupResponses.add(1);
    } else {
        otherResponses.add(1);
        console.log(`意外响应 code=${body.code} msg=${body.message}`);
    }
}

export function teardown() {
    const final = waitDrained(ACTIVITY_ID, undefined, DRAIN_TIMEOUT);
    console.log(report('幂等最终校验', final));
    if (!final) {
        fail('校验接口无返回');
    }
    if (!final.seckill.drained) {
        fail('缓冲没排干，无法判定幂等结果');
    }
    // 成功一次 → 预扣 1、DB 订单 1；多次成功或 0 次成功都是错的
    if (final.seckill.redisPreDeducted !== 1) {
        fail(`Redis 侧接受了 ${final.seckill.redisPreDeducted} 次，期望恰好 1 次`);
    }
    if (final.dbSuccessOrders !== 1) {
        fail(`DB 里落了 ${final.dbSuccessOrders} 单，期望恰好 1 单（唯一索引没兜住）`);
    }
    if (final.seckill.dbStock !== final.seckill.stockTotal - 1) {
        fail(`库存扣减异常：DB 库存=${final.seckill.dbStock}，期望 ${final.seckill.stockTotal - 1}`);
    }

    // 重复查询必须稳定返回同一结果
    let stable = true;
    for (let i = 0; i < 5; i++) {
        const view = parse(get(`/api/seckill/${ACTIVITY_ID}/requests/${REQUEST_ID}`, { name: 'seckill_result' }));
        const ok = view.code === CODE_OK && view.data && view.data.state === 'CONFIRMED' && view.data.userId === USER_ID;
        if (!ok) {
            stable = false;
            console.log(`第 ${i + 1} 次查询结果异常：${JSON.stringify(view)}`);
        }
    }
    if (!stable) {
        fail('重复查询状态不稳定');
    }
    console.log('幂等验证通过：同一 requestId 只落一单，状态查询稳定');
}
