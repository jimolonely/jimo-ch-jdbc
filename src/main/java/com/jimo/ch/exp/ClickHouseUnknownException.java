package com.jimo.ch.exp;

public class ClickHouseUnknownException extends ClickHouseException {
    public ClickHouseUnknownException(int code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

    public ClickHouseUnknownException(String msg, Throwable e, String host, int port) {
        super(0, msg, e, host, port);
    }
}
