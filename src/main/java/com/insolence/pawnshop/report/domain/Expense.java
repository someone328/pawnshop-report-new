package com.insolence.pawnshop.report.domain;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Expense {
    private long id;
    private String name;
    private BigDecimal sum;
}
