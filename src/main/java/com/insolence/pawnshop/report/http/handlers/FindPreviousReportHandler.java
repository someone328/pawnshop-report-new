package com.insolence.pawnshop.report.http.handlers;

import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.reactivex.ext.auth.User;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FindPreviousReportHandler implements Handler<RoutingContext> {
    private static final JsonObject EMPTY_JSON = new JsonObject();
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }
        User rcUser = rc.user();
        var bodyAsJson = rc.getBodyAsJson();
        String currentReportId = bodyAsJson.getString("reportId");

        Maybe<JsonObject> userMaybe = client.rxFindOne("user", new JsonObject().put("username", rcUser.principal().getValue("sub")), EMPTY_JSON);
        Maybe<JsonObject> currentReportMaybe = client.rxFindOne("report", new JsonObject().put("_id", currentReportId), EMPTY_JSON);
        Maybe.zip(userMaybe, currentReportMaybe, (user, currentReport) ->
                client.rxFindWithOptions("report",
                        new JsonObject()
                                .put("user", user.getString("_id"))
                                .put("date", new JsonObject().put("$lt", currentReport.getValue("date"))),
                        new FindOptions().setSort(new JsonObject().put("date", -1)).setLimit(1)
                )
        )
                .doOnComplete(() -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8").end(new JsonObject().toString()))
                .flatMapSingle(e -> e)
                //.defaultIfEmpty(List.of(JsonObject.mapFrom(new Report())))
                .flatMap(list -> Single.just(list.iterator().next()))
                .subscribe(
                        success -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8").end(success.encodePrettily()),
                        error -> {
                            log.error("Error get previous report for id: " + currentReportId, error);
                            rc.response().setStatusCode(500).end("Error get previous report for id: " + currentReportId);
                        }
                );
    }
}
