package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.domain.ValidationError;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class CreateNewReportHandler implements Handler<RoutingContext> {
    private static final JsonObject EMPTY_JSON = new JsonObject();
    private static final BigDecimal DEFAULT_BIGDECIMAL_VALUE = new BigDecimal(0.00);
    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        List<ValidationError> errorList = validate(rc);
        if (errorList.size() > 0) {
            rc.fail(400, new Exception(errorList.toString()));
            return;
        }
        rc.vertx()
                .eventBus()
                .<String>rxRequest("dynamics-calculations", rc.getBodyAsJson())
                .subscribe(success -> {
                            rc.response()
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                    .end(success.body());

                        },
                        error -> {
                            rc.fail(500, error);
                        });
    }

    private List<ValidationError> validate(RoutingContext rc) {
        JsonObject bodyAsJson = rc.getBodyAsJson();
        List<ValidationError> result = new ArrayList<>();
        if (!bodyAsJson.containsKey("branchId")) {
            result.add(new ValidationError("branchId - обязательный параметр"));
        }
        if (!bodyAsJson.containsKey("reportDate")) {
            result.add(new ValidationError("reportDate - обязательный параметр"));
        }

        return result;
    }
}
