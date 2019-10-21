package com.insolence.pawnshop.report.http.handlers;


import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;

import io.vertx.core.http.HttpHeaders;

public class AvailableYearsHandler implements Handler<RoutingContext> {
    private static final String availableYearRequest = "[{\"$project\":{\"_id\":\"0\", \"year\":{\"$year\":{\"$toDate\":\"$date\"}}}},\n" +
            "{\"$group\":{\"_id\":\"$year\",\"count\":{\"$sum\":1}}},\n" +
            "{\"$sort\":{\"_id\":1}},\n" +
            "{\"$project\":{\"_id\":0,\"year\":\"$_id\",\"count\":1}}]";
    private MongoClient client;
    private EventBus bus;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }

        bus = rc.vertx().eventBus();
        JsonArray pipeline = new JsonArray(availableYearRequest);
        client.aggregate(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline)
                .toObservable()
                .reduce(new JsonArray(), (arr, ya) -> arr.add(ya))
                .subscribe(
                        data -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                .setChunked(true).end(data.toString()));
    }
}
