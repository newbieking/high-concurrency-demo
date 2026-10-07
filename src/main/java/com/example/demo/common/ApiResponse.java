package com.example.demo.common;

public record ApiResponse<T>(int code, String message, T data) {

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(0, "ok", data);
    }

    public static <T> ApiResponse<T> ok() {
        return new ApiResponse<>(0, "ok", null);
    }

    public static <T> ApiResponse<T> fail(ResultCode code) {
        return new ApiResponse<>(code.getCode(), code.getMessage(), null);
    }

    public static <T> ApiResponse<T> fail(ResultCode code, T data) {
        return new ApiResponse<>(code.getCode(), code.getMessage(), data);
    }
}
