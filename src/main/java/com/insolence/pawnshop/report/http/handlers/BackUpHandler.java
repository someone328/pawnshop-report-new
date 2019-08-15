package com.insolence.pawnshop.report.http.handlers;

import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;

import java.util.stream.Collector;

public class BackUpHandler implements Handler<RoutingContext> {
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }

        client.rxGetCollections()
                .flatMapObservable(c -> Observable.fromIterable(c))
                .flatMapSingle(c -> client.rxFind(c, new JsonObject()))
                .map(list -> list.stream().collect(Collector.of(JsonArray::new, (arr, o) -> arr.add(o), (arr1, arr2) -> arr1.addAll(arr2))))
                .subscribe(
                        x -> rc.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                .setChunked(true)
                                .write(x.encodePrettily()),
                        e -> e.printStackTrace(),
                        () -> rc.response().end());
    }
}
