package com.insolence.pawnshop.report.util;

import java.math.BigDecimal;

public class BigDecimalUtils {
    public static BigDecimal noNull(BigDecimal bd) {
        return bd == null ? BigDecimal.ZERO : bd;
    }
}
