package com.insolence.pawnshop.report.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Data
public class StatisticsReportForBranchRow {
    private int monthNum;
    private BigDecimal monthAverageBasket = BigDecimal.ZERO;
    private BigDecimal monthTradeBalance = BigDecimal.ZERO;
    private BigDecimal monthTradeSum = BigDecimal.ZERO;
    private BigDecimal tradeIncome = BigDecimal.ZERO;
    private BigDecimal cashboxStartMorning = BigDecimal.ZERO;
    private BigDecimal cashboxEndMorning = BigDecimal.ZERO;
    private BigDecimal monthLoanRub = BigDecimal.ZERO;
    private BigDecimal monthRepayRub = BigDecimal.ZERO;
    private BigDecimal startBasket = BigDecimal.ZERO;
    private BigDecimal endBasket = BigDecimal.ZERO;
    private BigDecimal monthExpenses = BigDecimal.ZERO;
    //
    public Map<String, String> errors = new HashMap<>();
    @JsonIgnore
    private BigDecimal monthGoldTradeSum = BigDecimal.ZERO;
    @JsonIgnore
    private BigDecimal monthGoldTradeWeight = BigDecimal.ZERO;
    @JsonIgnore
    private BigDecimal monthSilverTradeSum = BigDecimal.ZERO;
    @JsonIgnore
    private BigDecimal monthSilverTradeWeight = BigDecimal.ZERO;
    @JsonIgnore
    private BigDecimal monthlyGoodsTradeSum = BigDecimal.ZERO;
    @JsonIgnore
    private BigDecimal monthlyVolumeSum = BigDecimal.ZERO;
    @JsonIgnore
    private Report lastReport;
}
