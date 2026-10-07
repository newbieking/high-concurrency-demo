package com.example.demo.seckill;

import com.example.demo.common.ApiResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/seckill")
public class SeckillController {

    private final SeckillService seckillService;

    public SeckillController(SeckillService seckillService) {
        this.seckillService = seckillService;
    }

    /**
     * 秒杀下单。同步只做「Redis 原子预扣 + 写缓冲」，不等 DB 落库就返回 PENDING，
     * 真实下单结果通过下面那个查询接口确认。
     */
    @PostMapping("/{activityId}/orders")
    public ApiResponse<SeckillAcceptView> order(@PathVariable long activityId,
                                                @Valid @RequestBody SeckillAcceptRequest request) {
        return ApiResponse.ok(seckillService.accept(activityId, request.userId(), request.requestId()));
    }

    @GetMapping("/{activityId}/requests/{requestId}")
    public ApiResponse<SeckillResultView> result(@PathVariable long activityId,
                                                 @PathVariable String requestId) {
        return ApiResponse.ok(seckillService.getResult(activityId, requestId));
    }
}
