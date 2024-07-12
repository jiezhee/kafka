package com.eris.kafka.util;

public class LogUtil {

    public static void printWithPrefix(String text) {
        System.out.println(String.format("【%s】thread: %s, %s", DateUtil.getCurrentDateAndTime(), Thread.currentThread().getName(), text));
    }
}
