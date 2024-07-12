package com.eris.kafka.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateUtil {

    public static String getCurrentDateAndTime() {
        return toStringAsDateAndTime(System.currentTimeMillis());
    }

    public static String toStringAsDateAndTime(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
//        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
        return localDateTime.format(formatter);
    }

    public static void main(String[] args) {
        System.out.println(toStringAsDateAndTime(System.currentTimeMillis()));
    }

}

