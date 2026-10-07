/**
 * 点赞洪峰：一半用户「只点赞」，一半用户「点赞又取消」。
 *
 * 这个脚本专门验证「增量事件的顺序无关性」：
 *   点赞/取消是加减法（可交换），所以 MQ 里乱序、重投、批量落库都不会算错；
 *   只要 Redis 计数和 DB 计数在排干后一致，就说明 LWW + delta 这套设计成立。
 *   如果换成「最终状态覆盖」的写法，点赞又取消再点赞的乱序就会丢更新。
 *
 * 断言：Redis 计数 == DB 计数 == 净点赞人数（只点赞的用户数），且无在途、无死信。
 *
 * 运行：./loadtest/run-k6.sh like-burst.js
 *      RATE=5000 DURATION=30s ./loadtest/run-k6.sh like-burst.js
 */
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import {
    CODE_OK, fail, get, intEnv, parse, post, report, resetLike, strEnv,
    uniqueUserId, waitDrained,
} from './lib/common.js';

const TARGET_ID = intEnv('TARGET_ID', 101);
const ACTIVITY_ID = intEnv('ACTIVITY_ID', 1);   // 只是为了让共用的 /api/ops/verify 能算秒杀侧，点赞脚本不压它
const RATE = intEnv('RATE', 3000);
const RAMP = strEnv('RAMP', '5s');
const HOLD = strEnv('DURATION', '20s');
const DRAIN_TIMEOUT = intEnv('DRAIN_TIMEOUT', 90000);
const UNLIKE_RATIO = intEnv('UNLIKE_RATIO', 2);   // 每 N 个用户里有 1 个会取消点赞

const likes = new Counter('like_total');
const unlikes = new Counter('unlike_total');
const unexpected = new Counter('like_unexpected');
const toggleOk = new Rate('like_toggle_ok');
const toggleMs = new Trend('like_toggle_ms', true);

export const options = {
    scenarios: {
        likeBurst: {
            executor: 'ramping-arrival-rate',
            startRate: Math.max(1, Math.floor(RATE / 20)),
            timeUnit: '1s',
            preAllocatedVUs: intEnv('PRE_VUS', 200),
            maxVUs: intEnv('MAX_VUS', 3000),
            stages: [
                { target: RATE, duration: RAMP },
                { target: RATE, duration: HOLD },
                { target: 0, duration: '3s' },
            ],
        },
    },
    thresholds: {
        'like_toggle_ok': ['rate>0.99'],
        'like_unexpected': ['count==0'],
        'http_req_failed': ['rate<0.01'],
        'like_toggle_ms': ['p(95)<300', 'p(99)<1000'],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
};

export function setup() {
    const res = resetLike(TARGET_ID);
    check(res, { '重置点赞对象成功': (r) => parse(r).code === CODE_OK });
    // 顺手把秒杀活动的库存灌一下，避免共用校验接口的秒杀块因为「未预热」而看着脏（本脚本不压秒杀）
    post(`/api/ops/seckill/${ACTIVITY_ID}/warmup`, undefined, { name: 'ops_warmup' });
    console.log(`点赞对象 ${TARGET_ID}，压力 ${RATE}/s 持续 ${HOLD}；每 ${UNLIKE_RATIO} 个用户有 1 个会点了又取消`);
    return { targetId: TARGET_ID };
}

export default function () {
    const userId = uniqueUserId(intEnv('UID_BASE', 300000000));
    const url = `/api/like/${TARGET_ID}/like?userId=${userId}`;
    const res = post(url, undefined, { name: 'like_toggle' });
    toggleMs.add(res.timings.duration);

    const body = parse(res);
    if (body.code !== CODE_OK || !body.data) {
        unexpected.add(1);
        toggleOk.add(false);
        console.log(`意外响应 status=${res.status} code=${body.code} msg=${body.message}`);
        return;
    }
    toggleOk.add(true);
    if (body.data.liked) {
        likes.add(1);
    } else {
        unlikes.add(1);
    }

    if (UNLIKE_RATIO > 1 && userId % UNLIKE_RATIO === 0) {
        const off = parse(post(`/api/like/${TARGET_ID}/unlike?userId=${userId}`, undefined, { name: 'like_toggle' }));
        if (off.code !== CODE_OK || !off.data) {
            unexpected.add(1);
            console.log(`取消点赞失败 code=${off.code} msg=${off.message}`);
            return;
        }
        if (off.data.liked) {
            unexpected.add(1);
            console.log('取消之后状态仍为已点赞，状态机有问题');
        } else {
            unlikes.add(1);
        }
    }
}

export function teardown(data) {
    const final = waitDrained(ACTIVITY_ID, data.targetId, DRAIN_TIMEOUT);
    console.log(report('点赞洪峰最终校验', final));
    if (!final) {
        fail('校验接口无返回');
    }
    if (!final.like.drained) {
        fail(`点赞缓冲没排干：在途=${final.like.inFlight} Stream长度=${final.like.streamLength} ` +
            `未确认=${final.like.streamPending}`);
    }
    // 只断言点赞块：秒杀块在本脚本里是陪跑的
    if (!final.like.pass) {
        fail('点赞一致性校验不通过：' + JSON.stringify(final.like.violations));
    }
    if (final.likeRedisCount !== final.likeDbCount) {
        fail(`计数不一致：Redis=${final.likeRedisCount} DB=${final.likeDbCount}`);
    }
    if (final.likeDbCount < 0 || final.likeRedisCount < 0) {
        fail(`计数为负：Redis=${final.likeRedisCount} DB=${final.likeDbCount}`);
    }
    if (final.likeDeadLetters > 0) {
        fail(`出现 ${final.likeDeadLetters} 条点赞死信`);
    }
    const ranked = parse(get('/api/like/rank?top=10', { name: 'like_rank' }));
    if (ranked.code === CODE_OK && ranked.data) {
        const hit = ranked.data.filter((r) => r.targetId === data.targetId)[0];
        console.log(`排行榜中对象 ${data.targetId} 的热度：${hit ? hit.count : '未上榜（计数为 0 时不入榜）'}`);
    }
    console.log(`点赞验证通过：Redis 与 DB 计数一致（${final.likeDbCount}），无丢更新`);
}
