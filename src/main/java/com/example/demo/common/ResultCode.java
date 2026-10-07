package com.example.demo.common;

public enum ResultCode {

    OK(0, "ok"),
    BAD_REQUEST(400, "参数错误"),

    SECKILL_NOT_READY(1001, "活动未开始或未预热"),
    SECKILL_SOLD_OUT(1002, "已售罄"),
    SECKILL_DUPLICATE(1003, "您已参与过，请勿重复下单"),
    SECKILL_BUSY(1004, "当前排队人数过多，请稍后重试"),
    SECKILL_NOT_FOUND(1005, "没有找到对应的请求记录"),

    INTERNAL_ERROR(5000, "服务内部错误");

    private final int code;
    private final String message;

    ResultCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
