package com.insolence.pawnshop.report.domain;

import lombok.Data;

import java.util.LinkedList;

@Data
public class StatisticReportForBranch {
    private String branchName;
    private LinkedList<StatisticsReportForBranchRow> monthlyReports = new LinkedList<>();
}
