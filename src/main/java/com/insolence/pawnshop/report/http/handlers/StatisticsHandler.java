package com.insolence.pawnshop.report.http.handlers;


import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.domain.StatisticReportForBranch;
import com.insolence.pawnshop.report.domain.StatisticsReportForBranchRow;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZonedDateTime;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;

@Slf4j
public class StatisticsHandler implements Handler<RoutingContext> {
    private static final String statisticsRequest = "[\n" +
            "    {\n" +
            "        \"$match\": {\n" +
            "            \"branch\": {\"$ne\": null},\n" +
            "            \"date\": {\"$gte\": %s}\n" +
            "        }\n" +
            "    },\n" +
            // "    {   \"$sort\": {\"branch\": 1, \"date\": 1}},\n" +
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
            "    {\"$lookup\": {\"from\": \"branch\", \"localField\": \"_id.branch\", \"foreignField\": \"_id\", \"as\": \"branchInfo\"}},\n" +
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
        JsonArray pipeline = new JsonArray(String.format(statisticsRequest, currentYearStartTimestamp));
        client.aggregate(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline)
                .toObservable()
                .map(json -> {
                    var branchReport = new StatisticReportForBranch();
                    branchReport.setBranchName(json.getJsonArray("branchInfo").getJsonObject(0).getString("name"));
                    Observable.fromIterable(json.getJsonArray("reportStatIndex"))
                            .map(e -> (JsonObject) e)
                            //.doOnEach(System.out::println)
                            .flatMapSingle(month ->
                                    Observable.fromIterable(month.getJsonArray("reports"))
                                            .map(x -> ((JsonObject) x).mapTo(Report.class))
                                            .reduce(new StatisticsReportForBranchRow(), (row, report) -> {
                                                JsonObject first = (JsonObject) Observable.fromIterable(month.getJsonArray("reports")).blockingFirst();

                                                row.setMonthNum(month.getInteger("month"));
                                                //monthAverageBasket
                                                row.setMonthTradeSum(
                                                        row.getMonthTradeSum()
                                                                .add(noNull(report.getGoldTradeSum()))
                                                                .add(noNull(report.getSilverTradeSum()))
                                                                .add(noNull(report.getGoodsTradeSum())));
                                                row.setTradeIncome(row.getMonthTradeSum().subtract(row.getMonthTradeBalance()));
                                                row.setCashboxStartMorning(first.mapTo(Report.class).getCashboxMorning());
                                                row.setCashboxEndMorning(report.getCashboxMorning());
                                                row.setMonthLoanRub(row.getMonthLoanRub().add(noNull(report.getLoanedRub())));
                                                row.setMonthRepayRub(row.getMonthRepayRub().add(noNull(report.getRepayedRub())));
                                                //startBasket
                                                //endBasket
                                                row.setMonthExpenses(row.getMonthExpenses().add(noNull(report.getExpensesSum())));
                                                //
                                                row.setMonthGoldTradeSum(row.getMonthGoldTradeSum().add(noNull(report.getGoldTradeSum())));
                                                row.setMonthGoldTradeWeight(row.getMonthGoldTradeWeight().add(noNull(report.getGoldTradeWeight())));
                                                row.setMonthSilverTradeSum(row.getMonthSilverTradeSum().add(noNull(report.getSilverTradeSum())));
                                                row.setMonthSilverTradeWeight(row.getMonthSilverTradeWeight().add(noNull(report.getSilverTradeWeight())));

                                                return row;
                                            }))
                                            .map(this::calculateMonthTradeBalance)
                            .subscribe(s -> branchReport.getMonthlyReports().add(s));
                    return branchReport;
                })
                .reduce(new JsonArray(), (arr, br) -> arr.add(JsonObject.mapFrom(br)))
                .subscribe(
                        success -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8").end(success.encodePrettily()),
                        error -> log.error("", error)
                );
    }

    private StatisticsReportForBranchRow calculateMonthTradeBalance(StatisticsReportForBranchRow row) {
        try {
            var preciousMetalsSum = row.getMonthGoldTradeSum()
                    .add(row.getMonthSilverTradeSum());
            var preciousMetalsWeight = row.getMonthGoldTradeWeight().add(row.getMonthSilverTradeWeight());
            row.setMonthTradeBalance(
                preciousMetalsSum
                        .divide(preciousMetalsWeight, 4, RoundingMode.HALF_UP)
                        .multiply(goldBySilverContentDivision)
                        .setScale(0, RoundingMode.HALF_UP)
            );
            return row;
        } catch (Exception e) {
            row.errors.put("monthGoldTradeSum", "деление на ноль");
        }
        return row;
    }

    private long getCurrentYearStartTimestamp()
    {
        return Instant.from(ZonedDateTime.now().withDayOfMonth(1).withDayOfYear(1).withHour(0).withMinute(0).withSecond(0)).toEpochMilli();
    }
}
