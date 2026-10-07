#!/usr/bin/env bash
# k6 跑在容器里，避免本机装 k6（Windows 上尤其省事）。
# 用法：./loadtest/run-k6.sh seckill-burst.js [额外的 k6 参数]
#      RATE=3000 DURATION=60s ./loadtest/run-k6.sh seckill-burst.js
set -euo pipefail

SCRIPT="${1:-seckill-burst.js}"
shift || true

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOUNT="$HERE"
# Git Bash 下把 /d/... 转成 D:/...，否则 Docker Desktop 认不出这个路径
if command -v cygpath >/dev/null 2>&1; then
    MOUNT="$(cygpath -w "$HERE")"
fi

if [ ! -f "$HERE/$SCRIPT" ]; then
    echo "找不到脚本 $SCRIPT，可选："
    ls -1 "$HERE"/*.js | xargs -n1 basename
    exit 1
fi

# 宿主机上的应用地址；k6 容器里访问宿主机要走 host.docker.internal
export BASE_URL="${BASE_URL:-http://host.docker.internal:8080}"
K6_IMAGE="${K6_IMAGE:-grafana/k6}"

# 压测参数通过环境变量透传进容器，未设置的就不传，让脚本用自己的默认值
PASSTHROUGH=(RATE RAMP DURATION DRAIN_TIMEOUT TARGET_ID VUS ITERATIONS UNLIKE_RATIO \
    UID_BASE PRE_VUS MAX_VUS REPLAYS POLL_MAX_MS POLL_INTERVAL_MS)
ENV_ARGS=()
for name in "${PASSTHROUGH[@]}"; do
    if [ -n "${!name:-}" ]; then
        ENV_ARGS+=(-e "$name=${!name}")
    fi
done

echo "==> k6 ${SCRIPT}  BASE_URL=${BASE_URL} ${ENV_ARGS[*]:-}"
set -x
# MSYS_NO_PATHCONV=1：Git Bash 会把 /scripts/xxx.js 这类容器内路径改写成 Windows 路径，必须关掉
MSYS_NO_PATHCONV=1 docker run --rm -i \
    --add-host=host.docker.internal:host-gateway \
    -e BASE_URL="$BASE_URL" \
    -e ACTIVITY_ID="${ACTIVITY_ID:-1}" \
    "${ENV_ARGS[@]}" \
    -v "$MOUNT:/scripts:ro" \
    "$K6_IMAGE" run "$@" "/scripts/$SCRIPT"
