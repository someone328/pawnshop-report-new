package com.insolence.pawnshop.report.verticles;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.REPORT;
import static com.insolence.pawnshop.report.util.DateUtils.startYearTimestampFrom;

@Slf4j
public class TotalPercentReceivedCalculationsVerticle extends AbstractVerticle {
    public static final String INCOME_ADDRESS = "calculateTotalReceivedPercent";
    private static final String request = "" +
            "[\n" +
            "    { \"$match\": { \"date\": { \"$gte\": %s, \"$lte\": %s }, \"branch\": \"%s\" }},\n" +
            "    {\n" +
            "        \"$group\": {\n" +
            "            \"_id\": { \"year\": { \"$year\": { \"$toDate\": { \"$toLong\": \"$date\" } } } },\n" +
            "            \"totalAmount\": { \"$sum\": \"$percentRecieved\" },\n" +
            "            \"count\": { \"$sum\": 1 }\n" +
            "        }\n" +
            "    }\n" +
            "]";
    private MongoClient client;

    @Override
    public void start() throws Exception {
        client = MongoClient.createShared(vertx, new JsonObject(), "pawnshop-report");

        vertx
                .eventBus()
                .consumer(INCOME_ADDRESS)
                .toFlowable()
                .flatMapSingle(message -> {
                    String reportId = (String) message.body();
                    return client.rxFindOne(REPORT.name().toLowerCase(), new JsonObject().put("_id", reportId), new JsonObject())
                            .doOnComplete(() -> message.reply(0.0d))
                            //.map(report -> report.getLong("date"))
                            .doOnEvent((s, e) -> System.out.println(s))
                            .flatMap(report -> {
                                long startYear = startYearTimestampFrom(report.getLong("date"));
                                JsonObject command = new JsonObject()
                                        .put("aggregate", REPORT.name().toLowerCase())
                                        .put("pipeline", new JsonArray(String.format(request, startYear, report.getLong("date"), report.getString("branch"))))
                                        .put("cursor", new JsonObject());
                                return client.rxRunCommand("aggregate", command);
                            })
                            .doOnEvent((s, e) -> System.out.println(s))
                            .map(cursor -> cursor.getJsonObject("cursor").getJsonArray("firstBatch").stream().findFirst().orElseGet(() -> new JsonObject().put("totalAmount", 0)))
                            .map(json -> ((JsonObject) json).getDouble("totalAmount"))
                            .flatMapSingle(message::rxReplyAndRequest)
                            .doOnError(e -> message.fail(500, e.getMessage()));
                })
                .retry()
                .subscribe(success -> {
                        },
                        error -> log.error("crud error", error));
    }
}
