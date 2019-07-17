package com.insolence.pawnshop.report.verticles;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.USER;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.mongo.MongoClient;

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
        .flatMap(
            message ->
                client
                    .rxCount(USER.name().toLowerCase(), (JsonObject) message.body())
                    .flatMapPublisher(res -> message.rxReply(res).toFlowable()))
        .retry()
        .subscribe();
  }
}
