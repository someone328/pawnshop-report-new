package com.insolence.pawnshop.report.verticles;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CrudVerticle extends AbstractVerticle {
  private MongoClient client;

  @Override
  public void start() throws Exception {
    client = MongoClient.createShared(vertx, new JsonObject(), "pawnshop-report");

    vertx
        .eventBus()
        .consumer("crud.delete")
        .toFlowable()
        .flatMap(
            message ->
                client
                    .rxRemoveDocument(
                        message.headers().get("objectType"),
                        new JsonObject().put("_id", (String) message.body()))
                    .doOnError(error -> message.fail(500, error.getMessage()))
                    .flatMap(result -> message.rxReply(result.toJson()))
                    .toFlowable())
        .retry()
        .subscribe(r -> {}, error -> log.error("crud error", error));

    vertx
        .eventBus()
        .consumer("crud.put")
        .toFlowable()
        .flatMap(
            message ->
                client
                    .rxSave(message.headers().get("objectType"), (JsonObject) message.body())
                    .defaultIfEmpty(((JsonObject) message.body()).getString("_id"))
                    .doOnError(error -> message.fail(500, error.getMessage()))
                    .flatMapPublisher(res -> message.rxReply(res).toFlowable()))
        .retry()
        .subscribe(r -> {}, error -> log.error("crud error", error));

    vertx
        .eventBus()
        .consumer("crud.get")
        .toFlowable()
        .flatMap(
            message ->
                client
                    .findBatch(message.headers().get("objectType"), (JsonObject) message.body())
                    .toFlowable()
                    .map(
                        object -> {
                          object.remove("password");
                          return object;
                        })
                    .collectInto(new JsonArray(), (array, object) -> array.add(object))
                    .doOnError(error -> message.fail(500, error.getMessage()))
                    .flatMapPublisher(array -> message.rxReply(array).toFlowable()))
        .retry()
        .subscribe(r -> {}, error -> log.error("crud error", error));

    log.info("CrudVerticle Ready!");
  }
}
