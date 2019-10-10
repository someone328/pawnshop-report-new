package com.insolence.pawnshop.report.domain;

import com.fasterxml.jackson.annotation.JsonGetter;
import lombok.*;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;

@Data
public class DailyReport {
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, DailyReportPerBranch> perBranch = new LinkedHashMap<>();
    // total
    private BigDecimal totalLoanedToday = new BigDecimal(0.00);
    private BigDecimal totalRepayedToday = new BigDecimal(0.00);
    private BigDecimal totalBalance = new BigDecimal(0.00);
    private BigDecimal totalCashBox = new BigDecimal(0.00);

    public void addDailyReportPerBranch(Report report) {
        DailyReportPerBranch tmp = perBranch.getOrDefault(report.getBranch(), new DailyReportPerBranch(report.getBranch()));

        tmp.setLoanedToday(tmp.getLoanedToday().add(noNull(report.getLoanedRub())));
        tmp.setRepayedToday(tmp.getRepayedToday().add(noNull(report.getRepayedRub())));
        tmp.setBalance(tmp.getLoanedToday().subtract(tmp.getRepayedToday()));
        tmp.setCashbox(tmp.getCashbox().add(noNull(report.getCashboxEvening())));

        perBranch.putIfAbsent(report.getBranch(), tmp);
    }

    @JsonGetter("perBranch")
    public List<DailyReportPerBranch> perBranch() {
        return perBranch.values().stream().collect(Collectors.toList());
    }

    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class DailyReportPerBranch {
        private final String branchName;
        private BigDecimal loanedToday = new BigDecimal(0.00);
        private BigDecimal repayedToday = new BigDecimal(0.00);
        private BigDecimal balance = new BigDecimal(0.00);
        private BigDecimal cashbox = new BigDecimal(0.00);


    }
}
