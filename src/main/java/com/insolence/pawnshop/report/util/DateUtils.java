package com.insolence.pawnshop.report.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class DateUtils {
    public static long getFirstMomentOfYear(int year) {
        return LocalDate.ofYearDay(year, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static long getLastMomentOfYear(int year) {
        return LocalDate.ofYearDay(year + 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli() - 1;
    }

    public static long getCurrentYearStartTimestamp() {
        return LocalDate.now().withDayOfYear(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static long startYearTimestampFrom(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDate.ofInstant(instant, ZoneId.of("UTC")).withDayOfYear(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}
