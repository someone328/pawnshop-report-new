package com.insolence.pawnshop.report.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
public class ReportCalculations {
    private BigDecimal loanersAsset = new BigDecimal(0.00);
    private BigDecimal volume = new BigDecimal(0.00);
    private BigDecimal goldBalance = new BigDecimal(0.00);
    private BigDecimal silverBalance = new BigDecimal(0.00);
    private BigDecimal diamondBalance = new BigDecimal(0.00);
    private BigDecimal goodsBalance = new BigDecimal(0.00);
    private BigDecimal cashboxEvening = new BigDecimal(0.00);
}
