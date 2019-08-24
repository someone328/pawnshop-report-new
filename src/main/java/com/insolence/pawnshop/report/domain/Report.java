package com.insolence.pawnshop.report.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Report {

    private String _id;
    private String branch;
    private String user;
    private Long date;
    private BigDecimal loanersPawned = new BigDecimal(0.00);
    private BigDecimal loanersBought = new BigDecimal(0.00);
    private BigDecimal loanersAsset = new BigDecimal(0.00);
    private BigDecimal loanedRub = new BigDecimal(0.00);
    private BigDecimal repayedRub = new BigDecimal(0.00);
    private String percentRecieved;
    private BigDecimal volume = new BigDecimal(0.00);
    private BigDecimal goldBought = new BigDecimal(0.00);
    private BigDecimal goldSold = new BigDecimal(0.00);
    private BigDecimal goldBalance = new BigDecimal(0.00);
    private BigDecimal silverBought = new BigDecimal(0.00);
    private BigDecimal silverSold = new BigDecimal(0.00);
    private BigDecimal silverBalance = new BigDecimal(0.00);
    private BigDecimal diamondBought = new BigDecimal(0.00);
    private BigDecimal diamondSold = new BigDecimal(0.00);
    private BigDecimal diamondBalance = new BigDecimal(0.00);
    private BigDecimal goodsBought = new BigDecimal(0.00);
    private BigDecimal goodsSold = new BigDecimal(0.00);
    private BigDecimal goodsBalance = new BigDecimal(0.00);
    private BigDecimal cashboxEvening = new BigDecimal(0.00);
    private BigDecimal cashboxMorning = new BigDecimal(0.00);
    private BigDecimal tradesActive = new BigDecimal(0.00);
    private BigDecimal goldTradeSum = new BigDecimal(0.00);
    private BigDecimal goldTradeWeight = new BigDecimal(0.00);
    private BigDecimal silverTradeSum = new BigDecimal(0.00);
    private BigDecimal silverTradeWeight = new BigDecimal(0.00);
    private BigDecimal diamondsTradeWeight = new BigDecimal(0.00);
    private BigDecimal goodsTradeSum = new BigDecimal(0.00);
    private List<Expense> expenses = new ArrayList<>();

    public BigDecimal getExpensesSum() {
        return expenses.stream().map(e -> noNull(e.getSum())).reduce(BigDecimal.ZERO, (x, z) -> x.add(z));
    }

    public BigDecimal getVolume() {
        return noNull(loanedRub).subtract(noNull(repayedRub));
    }

    public BigDecimal getPawnersRate(){
        return getLoanersPawned().subtract(getLoanersBought());
    }
}
