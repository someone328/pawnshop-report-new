package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.verticles.TotalPercentReceivedCalculationsVerticle;
import io.vertx.core.Handler;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TotalPercentHandler implements Handler<RoutingContext> {
    @Override
    public void handle(RoutingContext rc) {
        String reportId = rc.request().getParam("reportId").substring(0, 24);
        rc.vertx()
                .eventBus()
                .rxRequest(TotalPercentReceivedCalculationsVerticle.INCOME_ADDRESS, reportId)
                .map(response -> response.body().toString())
                .subscribe(success -> rc.response()
                                .putHeader("Encoding", "UTF-8")
                                .setStatusCode(200)
                                .end(success),
                        error -> {
                            log.error("TotalPercentHandler error", error);
                            rc.response().setStatusCode(500).end(error.getMessage());
                        });
    }
}
