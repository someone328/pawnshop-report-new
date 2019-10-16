package com.insolence.pawnshop.report.domain;

import lombok.Data;

import java.util.LinkedList;

@Data
public class StatisticReportForBranch {
    private BranchInfo branchInfo;
    private LinkedList<StatisticsReportForBranchRow> monthlyReports = new LinkedList<>();
}
