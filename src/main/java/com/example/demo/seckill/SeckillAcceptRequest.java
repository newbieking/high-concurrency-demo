package com.example.demo.seckill;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record SeckillAcceptRequest(
        @NotNull @Positive Long userId,
        @NotBlank String requestId) {
}
