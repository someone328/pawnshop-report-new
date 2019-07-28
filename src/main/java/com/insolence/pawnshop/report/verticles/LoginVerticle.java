package com.insolence.pawnshop.report.verticles;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.mongo.MongoClient;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.USER;

public class LoginVerticle extends AbstractVerticle {
    private MongoClient client;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        client = MongoClient.createShared(vertx, new JsonObject(), "pawnshop-report");
        client.rxGetCollections().subscribe(System.out::println);

        vertx
                .eventBus()
                .consumer("login")
                .toFlowable()
                .flatMapSingle(
                        message ->
                                client
                                        .rxFindOne(
                                                USER.name().toLowerCase(),
                                                (JsonObject) message.body(),
                                                new JsonObject().put("password", false))
                                        .doOnComplete(() -> message.fail(401, "User does not exists"))
                                        .flatMapSingle(user -> message.rxReply(user)))
                .retry()
                .subscribe();
    }
}
