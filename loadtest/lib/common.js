import http from 'k6/http';
import { sleep } from 'k6';

// k6 跑在容器里，访问宿主机上的应用要用 host.docker.internal（run-k6.sh 会注入）
export const BASE = (__ENV.BASE_URL || 'http://host.docker.internal:8080').replace(/\/+$/, '');

export const JSON_HEADERS = { 'Content-Type': 'application/json' };

// 与后端 ResultCode 对齐
export const CODE_OK = 0;
export const CODE_SOLD_OUT = 1002;
export const CODE_DUPLICATE = 1003;
export const CODE_BUSY = 1004;

export function parse(res) {
    try {
        return JSON.parse(res.body);
    } catch (e) {
        return { code: -1, message: '非 JSON 响应: ' + String(res.body).slice(0, 120) };
    }
}

export function post(url, payload, tags) {
    return http.post(BASE + url, payload === undefined ? null : JSON.stringify(payload),
        { headers: JSON_HEADERS, tags: tags });
}

export function get(url, tags) {
    return http.get(BASE + url, { tags: tags });
}

export function resetSeckill(activityId) {
    return post(`/api/ops/seckill/${activityId}/reset`, undefined, { name: 'ops_reset' });
}

export function resetLike(targetId) {
    return post(`/api/ops/like/${targetId}/reset`, undefined, { name: 'ops_reset' });
}

export function verify(activityId, targetId) {
    let url = `/api/ops/verify?activityId=${activityId}`;
    if (targetId) {
        url += `&targetId=${targetId}`;
    }
    return parse(get(url, { name: 'ops_verify' }));
}

/**
 * 等到 Redis 缓冲和在途台账都排干。只有排干之后，Redis 与 DB 的比对才有意义。
 * 空转超过 timeoutMs 仍没排干就返回最后一次结果，由调用方判定失败。
 */
export function waitDrained(activityId, targetId, timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 60000);
    let last = null;
    while (Date.now() < deadline) {
        const res = verify(activityId, targetId);
        last = res;
        if (res.code === CODE_OK) {
            const d = res.data;
            const seckillDrained = d.seckill && d.seckill.drained;
            const likeDrained = !d.like || d.like.drained;
            if (seckillDrained && likeDrained) {
                return d;
            }
        }
        sleep(1);
    }
    return last ? last.data : null;
}

/** 把最终校验结果打成人能读的多行文本，失败信息尽量可定位 */
export function report(title, data) {
    const lines = [`\n===== ${title} =====`];
    if (!data) {
        lines.push('校验接口没有返回数据');
        return lines.join('\n');
    }
    const s = data.seckill || {};
    lines.push(`秒杀: 排干=${s.drained} 通过=${s.pass} 总库存=${s.stockTotal} Redis库存=${s.redisStock} ` +
        `预扣=${s.redisPreDeducted} DB库存=${s.dbStock} DB订单=${s.dbOrders} ` +
        `在途=${s.inFlight} Stream长度=${s.streamLength} Stream未确认=${s.streamPending}`);
    if (s.violations && s.violations.length) {
        lines.push('  秒杀违约: ' + JSON.stringify(s.violations));
    }
    const l = data.like || {};
    lines.push(`点赞: 排干=${l.drained} 通过=${l.pass} 在途=${l.inFlight} Stream长度=${l.streamLength} ` +
        `Stream未确认=${l.streamPending}`);
    if (l.violations && l.violations.length) {
        lines.push('  点赞违约: ' + JSON.stringify(l.violations));
    }
    lines.push(`DB成功订单=${data.dbSuccessOrders} 秒杀死信=${data.seckillDeadLetters} 点赞死信=${data.likeDeadLetters}`);
    if (data.likeTargetId !== undefined) {
        lines.push(`点赞对象 ${data.likeTargetId}: Redis计数=${data.likeRedisCount} DB计数=${data.likeDbCount}`);
    }
    lines.push(`总判定: ${data.pass ? '通过' : '不通过'}`);
    return lines.join('\n');
}

/** 失败就抛异常，让 k6 以非 0 退出，方便接到 CI 上 */
export function fail(message) {
    throw new Error('压测断言失败: ' + message);
}

export function intEnv(name, fallback) {
    const v = __ENV[name];
    return v === undefined || v === '' ? fallback : parseInt(v, 10);
}

export function strEnv(name, fallback) {
    const v = __ENV[name];
    return v === undefined || v === '' ? fallback : v;
}

/** 每个 (VU, 迭代) 组合唯一；配额限制 + 去重决定了同一个 userId 只能成功一次 */
export function uniqueUserId(base) {
    return base + __VU * 1000000 + __ITER;
}

export function uniqueRequestId(prefix) {
    return `${prefix}-${__VU}-${__ITER}`;
}
