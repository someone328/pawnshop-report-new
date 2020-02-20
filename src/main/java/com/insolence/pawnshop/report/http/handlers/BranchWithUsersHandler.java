package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;

public class BranchWithUsersHandler implements Handler<RoutingContext> {
    private static final String branchWithUsersQuery = "[\n" +
            "  {\n" +
            "    \"$lookup\": {\n" +
            "      \"from\": \"user\",\n" +
            "      \"localField\": \"_id\",\n" +
            "      \"foreignField\": \"branches\",\n" +
            "      \"as\": \"users\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$sort\": {\n" +
            "      \"name\": 1,\n" +
            "      \"users.name\": 1\n" +
            "    }\n" +
            "  }\n" +
            "]";
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }
        JsonArray pipeline = new JsonArray(branchWithUsersQuery);
        client.aggregate(CrudHandler.SupportedObjectTypes.BRANCH.name().toLowerCase(), pipeline)
                .toObservable()
                .reduce(new JsonArray(), (arr, ya) -> arr.add(ya))
                .subscribe(
                        data -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                .setChunked(true).end(data.toString()));
    }
}
