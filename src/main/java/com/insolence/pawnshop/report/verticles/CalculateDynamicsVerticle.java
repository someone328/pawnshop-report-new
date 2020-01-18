package com.insolence.pawnshop.report.verticles;

import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.domain.ReportCalculations;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

import static com.insolence.pawnshop.report.util.BigDecimalUtils.noNull;

@Slf4j
public class CalculateDynamicsVerticle extends AbstractVerticle {
    public static final String DYNAMICS_CALCULATIONS = "dynamics-calculations";
    private MongoClient client;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        client = MongoClient.createShared(vertx, new JsonObject(), "pawnshop-report");

        vertx
                .eventBus()
                .<JsonObject>consumer(DYNAMICS_CALCULATIONS)
                .toFlowable()
                .flatMapCompletable(
                        message -> {
                            var bodyAsJson = message.body();
                            String branchId = bodyAsJson.getString("branchId");
                            Long reportDate = Long.valueOf(bodyAsJson.getString("reportDate"));
                            return client.rxFindWithOptions(
                                    "report",
                                    new JsonObject().put("branch", branchId).put("date", new JsonObject().put("$lt", reportDate)),
                                    new FindOptions()
                                            .setSort(new JsonObject().put("date", 1))
                                            .setLimit(500))

                                    .flatMapObservable(Observable::fromIterable)
                                    .map(jo -> jo.mapTo(Report.class))
                                    .reduceWith(ReportCalculations::new, this::calculateReportInfo)
                                    .flatMapCompletable(calculations -> {
                                        message.reply(JsonObject.mapFrom(calculations).encodePrettily());
                                        return Completable.complete();
                                    })
                                    .doOnError(error -> {
                                        log.error("Calculate dynamics error: " + branchId + " " + reportDate, error);
                                        message.fail(500, error.getMessage());
                                    })
                                    ;
                            //.map(lastReport -> this.createNewReport(null, lastReport))
                /*.subscribe(
                        x -> rc.response().end(JsonObject.mapFrom(x).encodePrettily()),
                        error -> error.printStackTrace()
                );*/
                        })
                .doOnError(error -> log.error("Calculate dynamics error.", error))
                .retry()
                .subscribe();
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
                        .add(lastReport.getBalancedVolume()));

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
