package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.domain.DailyReport;
import com.insolence.pawnshop.report.domain.Report;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Date;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.BRANCH;
import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.REPORT;
import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;

@Slf4j
public class DailyReportHandler implements Handler<RoutingContext> {
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }

        LocalDate requestedDate = null;
        try {
            Long timestamp = Long.valueOf(rc.request().getParam("timestamp"));
            requestedDate = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC")).toLocalDate();
            System.out.println(requestedDate);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        long currentDateStartMillis = requestedDate.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long nextDayStartMillis = requestedDate.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();

        client.rxFind(
                REPORT.name().toLowerCase(),
                new JsonObject()
                        .put("date", new JsonObject().put("$gte", currentDateStartMillis).put("$lt", nextDayStartMillis)))
                .flatMapObservable(list -> Observable.fromIterable(list))
                .map(json -> json.mapTo(Report.class))
                .flatMapMaybe(report -> client.rxFindOne(BRANCH.name().toLowerCase(), new JsonObject().put("_id", report.getBranch()), new JsonObject()).map(json -> {
                    report.setBranch(json.getString("name"));
                    return report;
                }))
                .sorted(Comparator.comparing(Report::getBranch))
                .collect(DailyReport::new, (dr, report) -> {
                    System.out.println(new Date(report.getDate()));
                    dr.addDailyReportPerBranch(report);
                    dr.setTotalLoanedToday(dr.getTotalLoanedToday().add(noNull(report.getLoanedRub())));
                    dr.setTotalRepayedToday(dr.getTotalRepayedToday()
                            .add(noNull(report.getRepayedRub())));
                    //.add(report.getPawnersRate().multiply(new BigDecimal(100)).divide(report.getLoanersAsset(), 2, RoundingMode.HALF_UP)));
                    dr.setTotalCashBox(dr.getTotalCashBox().add(noNull(report.getCashboxEvening())));

                })
                .map(dr -> {
                    dr.setTotalBalance(dr.getTotalLoanedToday().subtract(dr.getTotalRepayedToday()));
                    return dr;
                })
                .map(JsonObject::mapFrom)
                .subscribe(
                        report -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8").end(report.encodePrettily()),
                        error -> log.error("Daily report error", error)
                );

    }
}
