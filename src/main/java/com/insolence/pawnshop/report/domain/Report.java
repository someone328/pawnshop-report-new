package com.insolence.pawnshop.report.domain;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Report {

    private String _id;
    private String branch;
    private String user;
    private Long date;
    private BigDecimal loanersPawned;
    private BigDecimal loanersBought;
    private BigDecimal loanersAsset;
    private BigDecimal loanedRub;
    private BigDecimal repayedRub;
    private String percentRecieved;
    private BigDecimal volume;
    private BigDecimal goldBought;
    private BigDecimal goldSold;
    private BigDecimal goldBalance;
    private BigDecimal silverBought;
    private BigDecimal silverSold;
    private BigDecimal silverBalance;
    private BigDecimal diamondBought;
    private BigDecimal diamondSold;
    private BigDecimal diamondBalance;
    private BigDecimal goodsBought;
    private BigDecimal goodsSold;
    private BigDecimal goodsBalance;
    private BigDecimal cashboxEvening;
    private BigDecimal cashboxMorning;
    private BigDecimal tradesActive;
    private BigDecimal goldTradeSum;
    private BigDecimal goldTradeWeight;
    private BigDecimal silverTradeSum;
    private BigDecimal silverTradeWeight;
    private BigDecimal diamondsTradeWeight;
    private BigDecimal goodsTradeSum;
}
