package com.insolence.pawnshop.report.verticles;

import com.mongodb.MongoException;
import io.reactivex.Maybe;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.UpdateOptions;
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
                .flatMapSingle(
                        message ->
                                client
                                        .rxRemoveDocument(
                                                message.headers().get("objectType"),
                                                new JsonObject().put("_id", (String) message.body()))
                                        .doOnError(error -> message.fail(500, error.getMessage()))
                                        .flatMapSingle(result -> message.rxReply(result.toJson())))
                .retry()
                .subscribe(r -> {
                        },
                        error -> log.error("crud error", error));

        vertx
                .eventBus()
                .consumer("crud.put")
                .toFlowable()
                .flatMapSingle(
                        message -> {
                            JsonObject body = (JsonObject) message.body();
                            body.put("version", (body.getLong("version", 0L) + 1L));
                            Maybe<String> upsert;

                            if (body.containsKey("_id")) {
                                upsert = client.rxFindOneAndReplaceWithOptions(
                                        message.headers().get("objectType"),
                                        new JsonObject().put("_id", body.getValue("_id")),
                                                /*.put("$or", new JsonArray().add(new JsonObject().put("version", (body.getLong("version") - 1)))
                                                        .add(new JsonObject().put("version", new JsonObject().put("$exists", false)))),*/
                                        body,
                                        new FindOptions(),
                                        new UpdateOptions().setUpsert(true).setReturningNewDocument(true))
                                        .map(doc -> doc.getString("_id"))
                                        .doOnError(e -> message.fail(((MongoException) e).getCode(), e.getMessage()));
                            } else {
                                upsert = client.rxSave(message.headers().get("objectType"), body)
                                        .doOnError(e -> message.fail(((MongoException) e).getCode(), e.getMessage()));
                            }
                            return upsert.flatMapSingle(res -> message.rxReply(res));
                        })
                .retry()
                .subscribe(r -> {
                        },
                        error -> log.error("crud error", error));

        vertx
                .eventBus()
                .consumer("crud.get")
                .toFlowable()
                .flatMap(
                        message ->
                                client
                                        .findBatch(message.headers().get("objectType"), (JsonObject) message.body())
                                        .toFlowable()
                                        /*.map(
                                                object -> {
                                                    object.remove("password");
                                                    return object;
                                                })*/
                                        .map(this::GetInterceptor)
                                        .collectInto(new JsonArray(), (array, object) -> array.add(object))
                                        .doOnError(
                                                error -> {
                                                    log.error("crud GET error", error);
                                                    message.fail(500, error.getMessage());
                                                })
                                        .flatMapPublisher(array -> message.rxReply(array).toFlowable()))
                .retry()
                .subscribe(r -> {
                }, error -> log.error("crud error", error));

        log.info("CrudVerticle Ready!");
    }

    private JsonObject GetInterceptor(JsonObject entry) {


        return entry;
    }
}
