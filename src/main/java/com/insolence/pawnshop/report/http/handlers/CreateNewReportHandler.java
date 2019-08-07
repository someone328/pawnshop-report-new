package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.reactivex.ext.auth.User;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class CreateNewReportHandler implements Handler<RoutingContext> {
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        User user = rc.user();
        client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");

        client.rxFindOne("user", new JsonObject().put("username", user.principal().getValue("sub")), new JsonObject())
                .flatMapSingle(userJson -> client.rxFindWithOptions("report", new JsonObject().put("user", userJson.getValue("_id")), new FindOptions().setSort(new JsonObject().put("date", 1)).setLimit(1)))
                .map(l -> l.iterator().next())
                .doOnSuccess(x -> System.out.println(LocalDateTime.ofEpochSecond(x.getLong("date") / 1000, 0, ZoneOffset.UTC)))
                .subscribe(
                        x -> rc.response().end(x.encodePrettily()),
                        error -> error.printStackTrace()
                );

        /*DeliveryOptions deliveryOptionsReport = new DeliveryOptions().addHeader("objectType", CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase());
        DeliveryOptions deliveryOptionsUser = new DeliveryOptions().addHeader("objectType", CrudHandler.SupportedObjectTypes.USER.name().toLowerCase());

        System.out.println(user.principal().getValue("sub"));

        rc.vertx()
                .eventBus()
                .rxSend("crud.get", new JsonObject().put("username", user.principal().getValue("sub")), deliveryOptionsUser)
                .map(m -> (JsonArray) m.body())
                .map(arr -> arr.getJsonObject(0))
                .flatMap(userJson -> rc.vertx()
                        .eventBus()
                        .rxSend("crud.get", new JsonObject().put("user", userJson.getValue("_id")).put("$max", "date"), deliveryOptionsReport))
                .map(m -> (JsonArray) m.body())
                .flatMapObservable(arr -> Observable.fromArray(arr))
                .subscribe(
                        report -> rc.response().end(report.encodePrettily()),
                        error -> rc.response().setStatusCode(500).end(error.getMessage())
                );*/
    }
}
