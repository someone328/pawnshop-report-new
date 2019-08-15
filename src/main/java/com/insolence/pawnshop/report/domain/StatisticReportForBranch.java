package com.insolence.pawnshop.report.domain;

import lombok.Data;
import lombok.Setter;

import java.util.ArrayList;

@Data
public class StatisticReportForBranch {
    private String branchName;
    private ArrayList monthlyReports = new ArrayList<StatisticsReportForBranchRow>();
}
