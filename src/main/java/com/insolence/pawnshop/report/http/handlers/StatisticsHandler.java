package com.insolence.pawnshop.report.http.handlers;


import com.insolence.pawnshop.report.domain.*;
import com.insolence.pawnshop.report.util.Pair;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;
import static com.insolence.pawnshop.report.util.DateUtils.*;
import static com.insolence.pawnshop.report.verticles.CalculateDynamicsVerticle.DYNAMICS_CALCULATIONS;

@Slf4j
public class StatisticsHandler implements Handler<RoutingContext> {
    private static final String statisticsRequest = "[\n" +
            "{\"$match\": {\"branch\": {\"$ne\": null},\"date\": {\"$gte\": %s}}},\n" +
            "{\"$match\": {\"branch\": {\"$ne\": null},\"date\": {\"$lte\": %s}}},\n" +
            "{\"$lookup\":{\n" +
            "        \"from\":\"report\",\n" +
            "        \"let\":{\"report_branch\":\"$branch\", \"report_date\":\"$date\"},\n" +
            "        \"pipeline\":[\n" +
            "            {\"$match\":{\n" +
            "                \"$expr\":{\n" +
            "                    \"$and\":[\n" +
            "                    {\"$eq\":[\"$branch\",\"$$report_branch\"]},\n" +
            "                    {\"$lt\":[\"$date\",\"$$report_date\"]}\n" +
            "                    ]}}},\n" +
            "            {\"$sort\":{\"date\":-1}},\n" +
            "            {\"$limit\":1},\n" +
            "            {\"$project\":{\"_id\":0, \"cashboxEvening\":1}}\n" +
            "        ],\n" +
            "        \"as\":\"cashboxMorning\"\n" +
            "        }\n" +
            "    },\n" +
            "    {\"$unwind\":\"$cashboxMorning\"},\n" +
            "     {\"$project\":{\"_id\":1, \"branch\": 1, \"user\": 1, \"date\": 1, \"loanersPawned\":1, \"loanersBought\": 1, \"loanedRub\": 1,     \"repayedRub\": 1,\"percentRecieved\": 1,\"goldBought\": 1,\"goldSold\": 1,\"silverBought\": 1,\n" +
            "    \"silverSold\": 1,\"diamondBought\": 1,\"diamondSold\": 1,\"goodsBought\": 1,\"goodsSold\": 1,\"cashboxEvening\": 1,\n" +
            "    \"cashboxMorning\":\"$cashboxMorning.cashboxEvening\",\"tradesActive\": 1,\"goldTradeSum\": 1,\"goldTradeWeight\": 1,\n" +
            "    \"silverTradeSum\": 1,\"silverTradeWeight\": 1,\"diamondsTradeWeight\": 1,\"goodsTradeSum\": 1,\"expenses\": 1,\n" +
            "    \"auctionAmount\": 1,\"version\":1}},\n" +
            "    {\"$sort\": {\"branch\": 1, \"date\": 1}},\n" +
            "    {\"$lookup\":{\"from\":\"report\",\"localField\":\"_id\",\"foreignField\":\"_id\",\"as\":\"report\"}},        \n" +
            "    {\"$addFields\":{\"report.cashboxMorning\":\"$cashboxMorning\"}},\n" +
            "    {\"$group\": {\n" +
            "        \"_id\": {\"branch\": \"$branch\", \"month\": {\"$month\": {\"$toDate\": \"$date\"}}},\n" +
            "        \"documentCount\": {\"$sum\": 1},\n" +
            "        \"reports\": {\"$push\": {\"$arrayElemAt\" :[\"$report\", 0]}}\n" +
            "    }},\n" +
            "    {\"$group\": {\n" +
            "        \"_id\": {\"branch\": \"$_id.branch\"},\n" +
            "        \"reportStatIndex\": {\"$push\": {\"month\": \"$_id.month\", \"reports\": \"$reports\"}}\n" +
            "    }},\n" +
            "    {\"$unwind\": \"$reportStatIndex\"},\n" +
            "    {\"$sort\": {\"reportStatIndex.month\": 1}},\n" +
            "    {\"$group\": {\"_id\": \"$_id\", \"reportStatIndex\": {\"$push\": \"$reportStatIndex\"}}},\n" +
            "    {\"$lookup\": {\"from\": \"branch\", \"localField\": \"_id.branch\", \"foreignField\": \"_id\", \"as\": \"branchInfo\"}},\n" +
            "    {\"$unwind\" : \"$branchInfo\" }, \n" +
            "    {\"$sort\": {\"branchInfo.name\": 1}}\n" +
            "]";
    private static final BigDecimal goldBySilverContentDivision = new BigDecimal(999.9).divide(new BigDecimal(585.0), 4, RoundingMode.HALF_UP);
    private EventBus bus;

    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }
        int year = Integer.valueOf(rc.request().getParam("year"));
        System.out.println(year);
        bus = rc.vertx().eventBus();

        long startOfYear = getFirstMomentOfYear(year);
        long endOfYear = getLastMomentOfYear(year);
        System.out.println(startOfYear);
        System.out.println(endOfYear);
        JsonArray pipeline = new JsonArray(String.format(statisticsRequest, startOfYear, endOfYear));
        client.aggregate(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline)
                .toObservable()
                .map(json -> {
                    var branchReport = new StatisticReportForBranch();
                    branchReport.setBranchInfo(json.getJsonObject("branchInfo").mapTo(BranchInfo.class));
                    return Pair.of(branchReport, json.getJsonArray("reportStatIndex"));
                })
                .concatMapSingle(pair ->
                        Observable.fromIterable(pair.getRight())
                                .map(e -> (JsonObject) e)
                                .concatMapSingle(month ->
                                        Observable.fromIterable(month.getJsonArray("reports"))
                                                .map(x -> ((JsonObject) x).mapTo(Report.class))
                                                .reduceWith(StatisticsReportForBranchRow::new, (row, report) -> {
                                                    JsonObject firstReportInMonth = (JsonObject) Observable.fromIterable(month.getJsonArray("reports")).blockingFirst();
                                                    row.setMonthNum(month.getInteger("month"));
                                                    row.setMonthlyVolumeSum(row.getMonthlyVolumeSum().add(noNull(report.getVolume())));
                                                    row.setMonthTradeSum(row.getMonthTradeSum().add(noNull(report.getAuctionAmount())));
                                                    row.setCashboxStartMorning(firstReportInMonth.mapTo(Report.class).getCashboxMorning());
                                                    row.setCashboxEndMorning(report.getCashboxEvening());
                                                    row.setMonthLoanRub(row.getMonthLoanRub().add(noNull(report.getLoanedRub())));
                                                    row.setMonthRepayRub(row.getMonthRepayRub().add(noNull(report.getRepayedRub())));
                                                    row.setMonthExpenses(row.getMonthExpenses().add(noNull(report.getExpensesSum())));
                                                    //
                                                    row.setMonthGoldTradeSum(row.getMonthGoldTradeSum().add(noNull(report.getGoldTradeSum())));
                                                    row.setMonthGoldTradeWeight(row.getMonthGoldTradeWeight().add(noNull(report.getGoldTradeWeight())));
                                                    row.setMonthSilverTradeSum(row.getMonthSilverTradeSum().add(noNull(report.getSilverTradeSum())));
                                                    row.setMonthSilverTradeWeight(row.getMonthSilverTradeWeight().add(noNull(report.getSilverTradeWeight())));
                                                    row.setMonthlyGoodsTradeSum(row.getMonthlyGoodsTradeSum().add(noNull(report.getGoodsTradeSum())));
                                                    row.setLastReport(report);
                                                    return row;
                                                })
                                )
                                //don`t move this rows upper. They must be executed after all calculations
                                .map(this::calculateMonthTradeBalance)
                                .map(this::calculateTradeIncome)
                                .concatMapSingle(row -> calculateStartBasket(row, pair.getLeft()))
                                .concatMapSingle(row -> calculateEndBasket(row, pair.getLeft()))
                                .concatMapSingle(row -> calculateMonthAverageBasket(row, pair))
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

    private SingleSource<? extends StatisticsReportForBranchRow> calculateMonthAverageBasket(StatisticsReportForBranchRow row, Pair<StatisticReportForBranch, JsonArray> pair) {
        Observable<BigDecimal> volumes = Observable.fromIterable(pair.getRight())
                .filter(month -> ((JsonObject) month).getInteger("month") == row.getMonthNum())
                .concatMap(month -> Observable.fromIterable(((JsonObject) month).getJsonArray("reports")))
                .map(x -> ((JsonObject) x).mapTo(Report.class))
                .filter(report -> report.getBranch().equals(pair.getLeft().getBranchInfo().get_id()))
                .map(Report::getBalancedVolume);
        return volumes.startWith(row.getStartBasket())
                .scan(BigDecimal::add)
                .reduceWith(() -> BigDecimal.ZERO, BigDecimal::add)
                .map(summ -> {
                    row.setMonthAverageBasket(summ.subtract(row.getStartBasket())
                            .divide(new BigDecimal(30), 2, RoundingMode.HALF_UP));
                    return row;
                });
    }

    private StatisticsReportForBranchRow calculateTradeIncome(StatisticsReportForBranchRow row) {
        row.setTradeIncome(row.getMonthTradeBalance().subtract(row.getMonthTradeSum()));
        return row;
    }

    private Single<StatisticsReportForBranchRow> calculateStartBasket(StatisticsReportForBranchRow row, StatisticReportForBranch report) {
        long currentMonthFirstDay = LocalDate.now().withMonth(row.getMonthNum()).withDayOfMonth(1).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        return requestCalculationsFor(report, currentMonthFirstDay)
                .map(calcs -> {
                    row.setStartBasket(calcs.getVolume());
                    return row;
                });
    }

    private Single<StatisticsReportForBranchRow> calculateEndBasket(StatisticsReportForBranchRow row, StatisticReportForBranch report) {
        long nextMonthFirstDay = LocalDate.now().withMonth(row.getMonthNum()).plusMonths(1).withDayOfMonth(1).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        return requestCalculationsFor(report, nextMonthFirstDay)
                .map(calcs -> {
                    row.setEndBasket(calcs.getVolume());
                    return row;
                });
    }

    private Single<ReportCalculations> requestCalculationsFor(StatisticReportForBranch report, long nextMonthFirstDay) {
        return bus.<String>rxRequest(DYNAMICS_CALCULATIONS, new JsonObject().put("branchId", report.getBranchInfo().get_id()).put("reportDate", nextMonthFirstDay + ""))
                .map(resp -> new JsonObject(resp.body()).mapTo(ReportCalculations.class));
    }

    /*private StatisticsReportForBranchRow calculateMonthAverageBasket(StatisticsReportForBranchRow row) {
        row.setMonthAverageBasket(row.getMonthlyVolumeSum()
                .divide(new BigDecimal(30), 2, RoundingMode.HALF_UP)
        );
        return row;
    }*/

    private StatisticsReportForBranchRow calculateMonthTradeBalance(StatisticsReportForBranchRow row) {
        row.setMonthTradeBalance(
                row.getMonthGoldTradeSum()
                        .add(row.getMonthSilverTradeSum())
                        .add(row.getMonthlyGoodsTradeSum()));
        return row;
    }
}
