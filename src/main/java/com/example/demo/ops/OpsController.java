package com.example.demo.ops;

import com.example.demo.common.ApiResponse;
import com.example.demo.common.DeadLetterDao;
import com.example.demo.like.LikeConsistency;
import com.example.demo.like.LikeReconcileJob;
import com.example.demo.like.LikeService;
import com.example.demo.seckill.SeckillConsistency;
import com.example.demo.seckill.SeckillReconcileJob;
import com.example.demo.seckill.SeckillService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 运维/对账接口。压测脚本靠它做断言，故障演练靠它看恢复情况。
 */
@RestController
@RequestMapping("/api/ops")
public class OpsController {

    private final SeckillService seckillService;
    private final SeckillReconcileJob seckillReconcileJob;
    private final LikeService likeService;
    private final LikeReconcileJob likeReconcileJob;
    private final DeadLetterDao deadLetterDao;
    private final JdbcTemplate jdbc;

    public OpsController(SeckillService seckillService, SeckillReconcileJob seckillReconcileJob,
                         LikeService likeService, LikeReconcileJob likeReconcileJob,
                         DeadLetterDao deadLetterDao, JdbcTemplate jdbc) {
        this.seckillService = seckillService;
        this.seckillReconcileJob = seckillReconcileJob;
        this.likeService = likeService;
        this.likeReconcileJob = likeReconcileJob;
        this.deadLetterDao = deadLetterDao;
        this.jdbc = jdbc;
    }

    @GetMapping("/seckill/{activityId}/stats")
    public ApiResponse<SeckillConsistency> seckillStats(@PathVariable long activityId) {
        return ApiResponse.ok(seckillReconcileJob.checkConsistency(activityId));
    }

    @PostMapping("/seckill/{activityId}/warmup")
    public ApiResponse<Void> seckillWarmup(@PathVariable long activityId) {
        seckillService.warmup(activityId);
        return ApiResponse.ok();
    }

    @PostMapping("/seckill/{activityId}/reset")
    public ApiResponse<Void> seckillReset(@PathVariable long activityId) {
        seckillService.reset(activityId);
        return ApiResponse.ok();
    }

    @GetMapping("/like/stats")
    public ApiResponse<LikeConsistency> likeStats() {
        return ApiResponse.ok(likeReconcileJob.checkConsistency());
    }

    @PostMapping("/like/{targetId}/warmup")
    public ApiResponse<Void> likeWarmup(@PathVariable long targetId) {
        likeService.warmup(targetId);
        return ApiResponse.ok();
    }

    @PostMapping("/like/{targetId}/reset")
    public ApiResponse<Void> likeReset(@PathVariable long targetId) {
        likeService.reset(targetId);
        return ApiResponse.ok();
    }

    @PostMapping("/like/rank/rebuild")
    public ApiResponse<Integer> rebuildRank() {
        return ApiResponse.ok(likeService.rebuildRank());
    }

    @GetMapping("/dead-letters")
    public ApiResponse<List<Map<String, Object>>> deadLetters(@RequestParam(defaultValue = "seckill") String bizType,
                                                              @RequestParam(defaultValue = "20") int limit) {
        return ApiResponse.ok(deadLetterDao.recent(bizType, limit));
    }

    /**
     * 压测脚本用的总校验：把「不超卖、不丢单、不重单、计数一致」几个不变量一次算完。
     * pass=true 只在全部排干且所有不变量成立时出现。
     */
    @GetMapping("/verify")
    public ApiResponse<Map<String, Object>> verify(@RequestParam long activityId,
                                                   @RequestParam(required = false) Long targetId) {
        SeckillConsistency seckill = seckillReconcileJob.checkConsistency(activityId);
        LikeConsistency like = likeReconcileJob.checkConsistency();

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("seckill", seckill);
        data.put("like", like);
        data.put("seckillDeadLetters", deadLetterDao.count("seckill"));
        data.put("likeDeadLetters", deadLetterDao.count("like"));
        data.put("dbSuccessOrders", jdbc.queryForObject(
                "SELECT COUNT(*) FROM seckill_order WHERE activity_id = ? AND status = 'SUCCESS'",
                Long.class, activityId));
        if (targetId != null) {
            Long dbCount = jdbc.queryForObject(
                    "SELECT like_count FROM like_counter WHERE target_id = ?", Long.class, targetId);
            data.put("likeTargetId", targetId);
            data.put("likeRedisCount", likeService.redisCount(targetId));
            data.put("likeDbCount", dbCount == null ? 0 : dbCount);
        }
        data.put("pass", seckill.pass() && like.pass());
        return ApiResponse.ok(data);
    }
}
