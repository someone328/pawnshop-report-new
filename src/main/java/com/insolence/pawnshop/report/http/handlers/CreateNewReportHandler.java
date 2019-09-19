package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.domain.ReportCalculations;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.reactivex.ext.auth.User;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;

import java.math.BigDecimal;
import java.util.List;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;

public class CreateNewReportHandler implements Handler<RoutingContext> {
    private static final JsonObject EMPTY_JSON = new JsonObject();
    private static final BigDecimal DEFAULT_BIGDECIMAL_VALUE = new BigDecimal(0.00);
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        var bodyAsJson = rc.getBodyAsJson();
        String branchId = bodyAsJson.getString("branchId");
        Long reportDate = Long.valueOf(bodyAsJson.getString("reportDate"));
        User user = rc.user();
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }

        Single<List<JsonObject>> reportsHistory = client.rxFindWithOptions(
                "report",
                new JsonObject().put("branch", branchId).put("date", new JsonObject().put("$lt", reportDate)),
                new FindOptions()
                        .setSort(new JsonObject().put("date", 1))
                        .setLimit(500));

        reportsHistory
                .flatMapObservable(list -> Observable.fromIterable(list))
                .map(jo -> jo.mapTo(Report.class))
                .reduceWith(ReportCalculations::new, this::calculateReportInfo)
                //.map(lastReport -> this.createNewReport(null, lastReport))
                .subscribe(
                        x -> rc.response().end(JsonObject.mapFrom(x).encodePrettily()),
                        error -> error.printStackTrace()
                );
    }

    private ReportCalculations calculateReportInfo(ReportCalculations calculations, Report lastReport) {
        BigDecimal loanersPawned = noNull(lastReport.getLoanersPawned());
        BigDecimal loanersBought = noNull(lastReport.getLoanersBought());
        BigDecimal tradesActive = noNull(lastReport.getTradesActive());
        calculations.setLoanersAsset(
                calculations.getLoanersAsset()
                        .add(loanersPawned)
                        .subtract(loanersBought)
                        .subtract(tradesActive));

        calculations.setVolume(
                calculations.getVolume()
                        .add(lastReport.getVolume())
                        .subtract(lastReport.getGoldTradeSum())
                        .subtract(lastReport.getSilverTradeSum())
                        .subtract(lastReport.getGoodsTradeSum()));

        calculations.setGoldBalance(
                calculations.getGoldBalance()
                        .subtract(noNull(lastReport.getGoldSold()))
                        .add(noNull(lastReport.getGoldBought()))
                        .subtract(noNull(lastReport.getGoldTradeWeight())));

        calculations.setSilverBalance(
                calculations.getSilverBalance()
                        .subtract(noNull(lastReport.getSilverSold()))
                        .add(noNull(lastReport.getSilverBought()))
                        .subtract(noNull(lastReport.getSilverTradeWeight())));

        calculations.setDiamondBalance(
                calculations.getDiamondBalance()
                        .subtract(noNull(lastReport.getDiamondSold()))
                        .add(noNull(lastReport.getDiamondBought()))
                        .subtract(noNull(lastReport.getDiamondsTradeWeight())));

        calculations.setGoodsBalance(
                calculations.getGoodsBalance()
                        .subtract(noNull(lastReport.getGoodsSold()))
                        .add(noNull(lastReport.getGoodsBought()))
                        .subtract(noNull(lastReport.getGoodsTradeSum())));

        calculations.setCashboxEvening(noNull(lastReport.getCashboxEvening()));

        return calculations;
    }
}
