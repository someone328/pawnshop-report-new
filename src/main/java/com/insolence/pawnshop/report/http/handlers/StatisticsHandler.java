package com.insolence.pawnshop.report.http.handlers;


import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.domain.StatisticReportForBranch;
import com.insolence.pawnshop.report.domain.StatisticsReportForBranchRow;
import com.insolence.pawnshop.report.util.Pair;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;
import static com.insolence.pawnshop.report.util.DateUtils.getCurrentYearStartTimestamp;

@Slf4j
public class StatisticsHandler implements Handler<RoutingContext> {
    private static final String statisticsRequest = "[\n" +
            "    {\n" +
            "        \"$match\": {\n" +
            "            \"branch\": {\"$ne\": null},\n" +
            "            \"date\": {\"$gte\": %s}\n" +
            "        }\n" +
            "    },\n" +
            "    {   \"$sort\": {\"branch\": 1, \"date\": 1}},\n" +
            "    {   \"$lookup\": {\"from\": \"report\", \"localField\": \"_id\", \"foreignField\": \"_id\", \"as\": \"report\"}},\n" +
            "    {\n" +
            "        \"$group\": {\n" +
            "            \"_id\": {\"branch\": \"$branch\", \"month\": {\"$month\": {\"$toDate\": \"$date\"}}},\n" +
            "            \"documentCount\": {\"$sum\": 1},\n" +
            "            \"reports\": {\"$push\": {\"$arrayElemAt\" :[\"$report\", 0]}}\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"$group\": {\n" +
            "            \"_id\": {\"branch\": \"$_id.branch\"},\n" +
            "            \"reportStatIndex\": {\"$push\": {\"month\": \"$_id.month\", \"reports\": \"$reports\"}}\n" +
            "        }\n" +
            "    },\n" +
            "    {\"$unwind\": \"$reportStatIndex\"},\n" +
            "    {\"$sort\": {\"reportStatIndex.month\": 1}},\n" +
            "    {\"$group\": {\"_id\": \"$_id\", \"reportStatIndex\": {\"$push\": \"$reportStatIndex\"}}},\n" +
            "    {\"$lookup\": {\"from\": \"branch\", \"localField\": \"_id.branch\", \"foreignField\": \"_id\", \"as\": \"branchInfo\"}},\n" +
            "    {\"$unwind\" : \"$branchInfo\" }, \n" +
            "    {\"$sort\": {\"branchInfo.name\": 1}}\n" +
            "]";
    private static final BigDecimal goldBySilverContentDivision = new BigDecimal(999.9).divide(new BigDecimal(585.0), 4, RoundingMode.HALF_UP);

    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }

        long currentYearStartTimestamp = getCurrentYearStartTimestamp();
        Instant instant = Instant.ofEpochMilli(currentYearStartTimestamp);
        JsonArray pipeline = new JsonArray(String.format(statisticsRequest, currentYearStartTimestamp));
        client.aggregate(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline)
                .toObservable()
                .map(json -> {
                    var branchReport = new StatisticReportForBranch();
                    branchReport.setBranchName(json.getJsonObject("branchInfo").getString("name"));
                    return Pair.of(branchReport, json.getJsonArray("reportStatIndex"));
                })
                .concatMapSingle(pair ->
                        Observable.fromIterable(pair.getRight())
                                .map(e -> (JsonObject) e)
                                .concatMapSingle(month ->
                                        Observable.fromIterable(month.getJsonArray("reports"))
                                                .map(x -> ((JsonObject) x).mapTo(Report.class))
                                                .reduce(new StatisticsReportForBranchRow(), (row, report) -> {
                                                    JsonObject firstReportInMonth = (JsonObject) Observable.fromIterable(month.getJsonArray("reports")).blockingFirst();


                                                    System.out.println(month.getInteger("month") + " " + pair.getLeft().getBranchName());
                                                    row.setMonthNum(month.getInteger("month"));
                                                    row.setMonthlyVolumeSum(row.getMonthlyVolumeSum().add(noNull(report.getVolume())));
                                                    row.setMonthTradeSum(row.getMonthTradeSum().add(noNull(report.getAuctionAmount())));
                                                    row.setTradeIncome(row.getMonthTradeSum().subtract(row.getMonthTradeBalance()));
                                                    row.setCashboxStartMorning(firstReportInMonth.mapTo(Report.class).getCashboxMorning());
                                                    row.setCashboxEndMorning(report.getCashboxEvening());
                                                    row.setMonthLoanRub(row.getMonthLoanRub().add(noNull(report.getLoanedRub())));
                                                    row.setMonthRepayRub(row.getMonthRepayRub().add(noNull(report.getRepayedRub())));
                                                    row.setEndBasket(noNull(report.getVolume()));
                                                    row.setMonthExpenses(row.getMonthExpenses().add(noNull(report.getExpensesSum())));
                                                    //
                                                    row.setMonthGoldTradeSum(row.getMonthGoldTradeSum().add(noNull(report.getGoldTradeSum())));
                                                    row.setMonthGoldTradeWeight(row.getMonthGoldTradeWeight().add(noNull(report.getGoldTradeWeight())));
                                                    row.setMonthSilverTradeSum(row.getMonthSilverTradeSum().add(noNull(report.getSilverTradeSum())));
                                                    row.setMonthSilverTradeWeight(row.getMonthSilverTradeWeight().add(noNull(report.getSilverTradeWeight())));
                                                    row.setMonthlyGoodsTradeSum(row.getMonthlyGoodsTradeSum().add(noNull(report.getGoodsTradeSum())));
                                                    row.setLastReport(report);

                                                    return row;
                                                }))
                                .map(this::calculateMonthTradeBalance)
                                .map(this::calculateMonthAverageBasket)
                                .map(row -> calculateStartBasket(row, pair.getLeft()))
                                .reduceWith(pair::getLeft, (yearReport, monthReport) -> {
                                    yearReport.getMonthlyReports().add(monthReport);
                                    return yearReport;
                                })
                )
                .reduce(new JsonArray(), (arr, br) -> arr.add(JsonObject.mapFrom(br)))
                .subscribe(
                        success -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8").end(success.encodePrettily()),
                        error -> log.error("", error)
                );
    }

    private StatisticsReportForBranchRow calculateStartBasket(StatisticsReportForBranchRow row, StatisticReportForBranch report) {
        long previousMonthLastDay = LocalDate.now().withMonth(row.getMonthNum()).withDayOfMonth(1).minusDays(1).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long currentMonthFirstDay = LocalDate.now().withMonth(row.getMonthNum()).withDayOfMonth(1).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

        if (report.getMonthlyReports().size() == 0) {
            client.findWithOptions("report",
                    new JsonObject()
                            .put("branch", report.getBranchName())
                            .put("date", new JsonObject()
                                    .put("$gte", previousMonthLastDay))
                            .put("date", new JsonObject()
                                    .put("$lt", currentMonthFirstDay)),
                    new FindOptions()
                            .setSort(new JsonObject()
                                    .put("date", -1))
                            .setLimit(1), handler -> {
                        Report report1 = handler.result().stream().map(json -> json.mapTo(Report.class)).findFirst().orElseGet(() -> new Report());
                        row.setStartBasket(report1.getVolume());
                    });
        } else {
            BigDecimal bigDecimal = report.getMonthlyReports().stream().map(r -> r.getLastReport().getVolume()).reduce((f, s) -> s).orElseGet(() -> BigDecimal.ZERO);
            row.setStartBasket(bigDecimal);
        }
        return row;
    }

    private StatisticsReportForBranchRow calculateMonthAverageBasket(StatisticsReportForBranchRow row) {
        row.setMonthAverageBasket(row.getMonthlyVolumeSum().divide(new BigDecimal(30), 2, RoundingMode.HALF_UP));
        return row;
    }

    private StatisticsReportForBranchRow calculateMonthTradeBalance(StatisticsReportForBranchRow row) {
        try {
            row.setMonthTradeBalance(
                    row.getMonthTradeBalance().add(row.getMonthGoldTradeSum().add(row.getMonthSilverTradeSum()).add(row.getMonthlyGoodsTradeSum()))
            );
            return row;
        } catch (Exception e) {
            row.errors.put("monthGoldTradeSum", "деление на ноль");
        }
        return row;
    }
}
