package com.example.demo.like;

import com.example.demo.common.ApiResponse;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/like")
public class LikeController {

    private final LikeService likeService;

    public LikeController(LikeService likeService) {
        this.likeService = likeService;
    }

    @PostMapping("/{targetId}/like")
    public ApiResponse<LikeToggleView> like(@PathVariable long targetId, @RequestParam long userId) {
        return ApiResponse.ok(likeService.toggle(targetId, userId, true));
    }

    @PostMapping("/{targetId}/unlike")
    public ApiResponse<LikeToggleView> unlike(@PathVariable long targetId, @RequestParam long userId) {
        return ApiResponse.ok(likeService.toggle(targetId, userId, false));
    }

    @GetMapping("/{targetId}")
    public ApiResponse<Map<String, Object>> stat(@PathVariable long targetId,
                                                 @RequestParam(required = false) Long userId) {
        Map<String, Object> data = new LinkedHashMap<>();
        Long count = likeService.redisCount(targetId);
        data.put("targetId", targetId);
        data.put("count", count == null ? 0 : count);
        if (userId != null) {
            data.put("liked", likeService.isLiked(targetId, userId));
        }
        return ApiResponse.ok(data);
    }

    @GetMapping("/rank")
    public ApiResponse<List<Map<String, Object>>> rank(@RequestParam(defaultValue = "10") int top) {
        List<Map<String, Object>> data = new ArrayList<>();
        int position = 1;
        for (ZSetOperations.TypedTuple<String> tuple : likeService.topRanked(top)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("rank", position++);
            row.put("targetId", Long.parseLong(tuple.getValue()));
            row.put("count", tuple.getScore() == null ? 0 : tuple.getScore().longValue());
            data.add(row);
        }
        return ApiResponse.ok(data);
    }
}
