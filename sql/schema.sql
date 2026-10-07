CREATE DATABASE IF NOT EXISTS demo1 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE demo1;

-- ---------- 秒杀 ----------

CREATE TABLE IF NOT EXISTS seckill_activity (
    id              BIGINT       NOT NULL COMMENT '活动ID',
    name            VARCHAR(64)  NOT NULL,
    stock_total     INT          NOT NULL COMMENT '总库存',
    stock_available INT          NOT NULL COMMENT 'DB侧可用库存（兜底真值）',
    start_time      DATETIME     NOT NULL,
    end_time        DATETIME     NOT NULL,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT '秒杀活动';

CREATE TABLE IF NOT EXISTS seckill_order (
    id          BIGINT        NOT NULL AUTO_INCREMENT,
    request_id  VARCHAR(64)   NOT NULL COMMENT '客户端请求号，幂等键',
    activity_id BIGINT        NOT NULL,
    user_id     BIGINT        NOT NULL,
    status      VARCHAR(16)   NOT NULL COMMENT 'SUCCESS / REJECTED',
    reject_note VARCHAR(64)   NULL     COMMENT '被拒原因，如 OVERSELL_GUARD',
    created_at  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    -- 防重复落库：同一 requestId 只会成功一次
    UNIQUE KEY uk_order_request (request_id),
    -- 防一人多单（限购 1）
    UNIQUE KEY uk_order_activity_user (activity_id, user_id),
    KEY idx_order_activity (activity_id)
) ENGINE = InnoDB COMMENT '秒杀订单';

-- ---------- 点赞 ----------

CREATE TABLE IF NOT EXISTS like_record (
    id         BIGINT      NOT NULL AUTO_INCREMENT,
    target_id  BIGINT      NOT NULL,
    user_id    BIGINT      NOT NULL,
    seq        BIGINT      NOT NULL DEFAULT 0 COMMENT 'LWW 版本号，取 Redis Stream ID 换算，单调递增',
    liked      TINYINT     NOT NULL DEFAULT 1,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_like_target_user (target_id, user_id)
) ENGINE = InnoDB COMMENT '点赞明细，seq 大者胜出';

CREATE TABLE IF NOT EXISTS like_counter (
    target_id  BIGINT      NOT NULL,
    like_count BIGINT      NOT NULL DEFAULT 0,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (target_id)
) ENGINE = InnoDB COMMENT '点赞计数，最终真值';

-- ---------- 公共 ----------

CREATE TABLE IF NOT EXISTS idempotent_record (
    id         BIGINT      NOT NULL AUTO_INCREMENT,
    biz_type   VARCHAR(32) NOT NULL,
    biz_id     VARCHAR(96) NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_idem (biz_type, biz_id)
) ENGINE = InnoDB COMMENT '消费者幂等表，唯一索引是防重复的最后一道闸';

CREATE TABLE IF NOT EXISTS dead_letter (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    biz_type    VARCHAR(32)  NOT NULL,
    biz_id      VARCHAR(96)  NULL,
    topic       VARCHAR(128) NULL,
    msg_id      VARCHAR(96)  NULL,
    reason      VARCHAR(255) NULL,
    retry_count INT          NOT NULL DEFAULT 0,
    body        TEXT         NULL,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_dl_biz (biz_type, biz_id)
) ENGINE = InnoDB COMMENT '死信，人工介入入口';

CREATE TABLE IF NOT EXISTS reconcile_diff (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    biz_type    VARCHAR(32)  NOT NULL,
    biz_key     VARCHAR(64)  NOT NULL,
    redis_value BIGINT       NOT NULL,
    db_value    BIGINT       NOT NULL,
    diff        BIGINT       NOT NULL,
    action      VARCHAR(32)  NOT NULL COMMENT 'FIXED_FROM_REDIS / ALERT_ONLY / REDELIVER / ROLLBACK',
    detail      VARCHAR(255) NULL,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_diff_created (created_at)
) ENGINE = InnoDB COMMENT '对账差异流水';

INSERT INTO seckill_activity (id, name, stock_total, stock_available, start_time, end_time)
VALUES (1, 'iPhone 秒杀', 1000, 1000, NOW(), DATE_ADD(NOW(), INTERVAL 10 YEAR))
ON DUPLICATE KEY UPDATE name = VALUES(name);

INSERT INTO like_counter (target_id, like_count)
VALUES (101, 0), (102, 0), (103, 0)
ON DUPLICATE KEY UPDATE target_id = like_counter.target_id;
