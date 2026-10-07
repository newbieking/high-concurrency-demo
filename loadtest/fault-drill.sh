#!/usr/bin/env bash
# 故障演练：压测过程中把 MQ broker 拔掉，验证「缓冲兜住 + 对账自愈 + 不丢单不超卖」。
#
# 剧本：
#   1. 复位 + 预热
#   2. k6 按固定速率持续下单（后台跑）
#   3. 压到一半时停掉 broker —— 生产者发送失败 → Stream 消息不 XACK，堆在 Redis 里
#   4. 观察 Stream 长度 / 在途数上涨（DB 订单数不长）
#   5. 把 broker 拉起来 —— 生产者恢复，消息重新投递并被消费
#   6. 等排干，用 /api/ops/verify 验证不变量：预扣==DB订单、库存不为负、无死信
#
# 用法：./loadtest/fault-drill.sh
#      RATE=300 DURATION=40s OUTAGE=15 BURST_BEFORE=4 ./loadtest/fault-drill.sh
set -uo pipefail

# 用量说明：库存 1000，所以「压测总时长 × RATE」要略大于 1000，
# 但要保证「库存被打光」发生在故障窗口之后，否则断线期间没有消息可缓冲，演练就看不出效果。
# 默认 RATE=120 → 约 8 秒售罄；broker 在第 5 秒停、第 17 秒起 → 断线期间约 400 单堆在 Redis 里。
RATE="${RATE:-120}"
DURATION="${DURATION:-30s}"
OUTAGE="${OUTAGE:-12}"            # broker 停机秒数
BURST_BEFORE="${BURST_BEFORE:-5}" # 压多少秒后开始拔线
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"                     # 宿主机上 curl 用
K6_BASE_URL="${K6_BASE_URL:-http://host.docker.internal:8080}"    # k6 容器里访问宿主机用
ACTIVITY_ID="${ACTIVITY_ID:-1}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

post() { curl -s -X POST "$BASE_URL$1" > /dev/null; }

stats() {
    curl -s "$BASE_URL/api/ops/seckill/$ACTIVITY_ID/stats"
}

watch() {
    local label="$1" seconds="$2"
    local i=0
    while [ "$i" -lt "$seconds" ]; do
        local s
        s=$(stats)
        printf '%s  在途/Stream长度/未确认 = %s\n' "$label" \
            "$(echo "$s" | sed -E 's/.*"inFlight":([0-9]+).*"streamLength":([0-9]+).*"streamPending":([0-9]+).*/\1 \/ \2 \/ \3/')"
        sleep 5
        i=$((i + 5))
    done
}

log "复位并预热活动 $ACTIVITY_ID"
post "/api/ops/seckill/$ACTIVITY_ID/reset"
sleep 1

K6_LOG="$(mktemp -t k6-fault-drill.XXXXXX.log)"
log "后台启动 k6：RATE=$RATE DURATION=$DURATION"
(
    cd "$HERE"
    RATE="$RATE" DURATION="$DURATION" DRAIN_TIMEOUT=240000 BASE_URL="$K6_BASE_URL" \
        ./run-k6.sh seckill-burst.js > "$K6_LOG" 2>&1
    echo $? > "$K6_LOG.exit"
) &

log "让压测先跑 $BURST_BEFORE 秒，把链路喂起来"
watch "  正常期" "$BURST_BEFORE"

log "停掉 broker（模拟 MQ 不可用），停机 ${OUTAGE}s"
docker compose -f "$ROOT/docker-compose.yml" stop rocketmq-broker
watch "  故障期" "$OUTAGE"

log "拉起 broker"
docker compose -f "$ROOT/docker-compose.yml" start rocketmq-broker
for _ in $(seq 1 24); do
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' demo1-broker 2>/dev/null)
    [ "$status" = "healthy" ] && break
    sleep 5
done
log "broker 状态：$status"

log "等 k6 收尾（它在 teardown 里会等到排干并断言）"
for _ in $(seq 1 90); do
    [ -f "$K6_LOG.exit" ] && break
    sleep 5
done

K6_EXIT="$(cat "$K6_LOG.exit" 2>/dev/null || echo '?')"
log "k6 退出码 = $K6_EXIT"
tail -40 "$K6_LOG"

log "最终一致性快照"
curl -s "$BASE_URL/api/ops/verify?activityId=$ACTIVITY_ID&targetId=101"
echo
log "死信（bizType=seckill）"
curl -s "$BASE_URL/api/ops/dead-letters?bizType=seckill&limit=5"
echo

if [ "$K6_EXIT" = "0" ]; then
    echo "==> 故障演练通过：MQ 中断期间的消息全部由 Redis 缓冲兜住，恢复后不丢单、不超卖"
else
    echo "==> 故障演练失败：k6 退出码 $K6_EXIT，看上面 teardown 的断言输出" >&2
    exit 1
fi
