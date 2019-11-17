package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.util.Pair;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.Operarions.*;
import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.REPORT;

@Slf4j
public class CrudHandler implements Handler<RoutingContext> {

    private Map<SupportedObjectTypes, List<BiFunction<JsonObject, RoutingContext, Maybe<Pair<Integer, String>>>>> putValidations = new HashMap<>();

    public CrudHandler() {
        initValidations();
    }

    @Override
    public void handle(RoutingContext rc) {
        String objectType = rc.request().getParam("objectType");
        String operationType = rc.request().getParam("operationType");
        Operarions operationTypeEnum;
        SupportedObjectTypes objectTypeEnum;

        try {
            objectTypeEnum = SupportedObjectTypes.valueOf(objectType.toUpperCase());
        } catch (Exception e) {
            rc.response().setStatusCode(400).end("Unsupported object type " + objectType);
            return;
        }

        try {
            operationTypeEnum = valueOf(operationType.toUpperCase());
        } catch (Exception e) {
            rc.response().setStatusCode(400).end("Unsupported operationType type " + operationType);
            return;
        }

        Observable.fromArray(objectTypeEnum.getPermissions().get(operationTypeEnum))
                .flatMapSingle(role -> rc.user()
                        .rxIsAuthorized(role)
                        .map(b -> Pair.of(role, b)))
                .reduce(Boolean.FALSE, (one, two) -> Boolean.logicalOr(one, two.getRight()))
                .subscribe(
                        success -> {
                            if (!success) {
                                rc.response().setStatusCode(403).end("User not permitted for requested operation");
                            } else {
                                DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("objectType", objectType);
                                JsonObject jsonBody = rc.getBodyAsJson();
                                switch (operationTypeEnum) {
                                    case PUT -> processPutOperation(rc, deliveryOptions, jsonBody);
                                    case GET -> processGetOperation(rc, deliveryOptions, jsonBody);
                                    case DELETE -> processDeleteOperation(rc, deliveryOptions, jsonBody);
                                    default -> rc.response().setStatusCode(400).end("Unsupported operationType type " + operationType);
                                }
                            }
                        },
                        error -> rc.response().setStatusCode(500).end(error.getMessage()));
    }

    private void processDeleteOperation(RoutingContext rc, DeliveryOptions deliveryOptions, JsonObject jsonBody) {
        String idToDelete = jsonBody.getString("_id");
        log.info(idToDelete);
        rc.vertx()
                .eventBus()
                .rxSend("crud.delete", idToDelete, deliveryOptions)
                .map(response -> response.body().toString())
                .subscribe(success -> rc.response()
                                .putHeader("Encoding", "UTF-8")
                                .setStatusCode(200)
                                .end(success),
                        error -> {
                            log.error("Process DELETE error", error);
                            rc.response().setStatusCode(500).end();
                        });
    }

    private void processGetOperation(RoutingContext rc, DeliveryOptions deliveryOptions, JsonObject jsonBody) {
        String objectType = deliveryOptions.getHeaders().get("objectType");
        if (objectType.equals("userp")) {
            deliveryOptions.getHeaders().set("objectType", "user");
        }
        Single<String> data = rc.vertx()
                .eventBus()
                .rxSend("crud.get", jsonBody, deliveryOptions)
                .map(response -> response.body().toString())
                .doOnError(e -> log.error("Crud get error. ", e));
        if (objectType.equals("user")) {
            data.subscribe(
                    crudResult -> {
                        JsonArray resJson = new JsonArray(crudResult);
                        resJson.stream().forEach(c -> ((JsonObject) c).remove("password"));
                        rc.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                .end(resJson.toString());
                    },
                    error -> {
                        log.error("Crud get error. ", error);
                        rc.fail(500);
                    });
        } else {
            data.subscribe(
                    crudResult -> rc.response()
                            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                            .end(crudResult),
                    error -> {
                        log.error("Crud get error. ", error);
                        rc.fail(500);
                    });
        }
    }

    //TODO доделать прокидывание сообщений валидации
    private void processPutOperation(RoutingContext rc, DeliveryOptions deliveryOptions, JsonObject jsonBody) {
        SupportedObjectTypes objectType = SupportedObjectTypes.valueOf(deliveryOptions.getHeaders().get("objectType").toUpperCase());
        List<BiFunction<JsonObject, RoutingContext, Maybe<Pair<Integer, String>>>> validations = putValidations.computeIfAbsent(objectType, k -> new ArrayList<>());
        Observable.fromIterable(validations)
                .subscribeOn(Schedulers.io())
                .flatMapMaybe(f -> f.apply(jsonBody, rc))
                .toList()
                .flatMapMaybe(list -> {
                    if (list.size() == 0) {
                        return Maybe.empty();
                    }
                    rc.fail(400, new Exception(list.toString()));
                    return Maybe.just(list.toString());
                })
                .switchIfEmpty(rc.vertx()
                        .eventBus()
                        .rxSend("crud.put", jsonBody, deliveryOptions)
                        .map(message -> (String) message.body()))
                .subscribe(result -> {
                            if (rc.failed()) {
                                return;
                            }
                            rc.response()
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                    .end("Success");
                        },
                        error -> {
                            log.error("Crud error", error);
                            rc.fail(500, error);
                        }
                );
    }

    public boolean isSupportableType(String type) {
        return SupportedObjectTypes.valueOf(type.toUpperCase()).ordinal() != 9999;
    }

    //TODO причесать, вынести в отдельный классы?
    private void initValidations() {
        BiFunction<JsonObject, RoutingContext, Maybe<Pair<Integer, String>>> isDuplicateCheck = (reportJson, rc) -> {
            Report report = reportJson.mapTo(Report.class);
            if (report.get_id() != null) {
                return Maybe.empty();
            }
            return rc.vertx().eventBus()
                    .rxSend("crud.get",
                            new JsonObject().put("branch", report.getBranch()).put("date", report.getDate()),
                            new DeliveryOptions().addHeader("objectType", REPORT.name().toLowerCase()).setSendTimeout(2000))
                    .map(response -> (JsonArray) response.body())
                    .doOnEvent((s, e) -> System.out.println(Thread.currentThread().getName()))
                    .map(JsonArray::size)
                    .map(size -> size == 0)
                    .flatMapMaybe(b -> b ?
                            Maybe.empty() :
                            Maybe.just(
                                    Pair.of(10000,
                                            "Дубликат отчета. " + Instant.ofEpochMilli(report.getDate()).atZone(ZoneId.of("UTC")).toLocalDate() + ":" + report.getBranch())))
                    .doOnError(e -> log.error("isDuplicateCheck", e));
        };
        List<BiFunction<JsonObject, RoutingContext, Maybe<Pair<Integer, String>>>> reportValidations = putValidations.computeIfAbsent(REPORT, k -> new ArrayList<>());
        reportValidations.add(isDuplicateCheck);
    }

    @AllArgsConstructor
    @Getter
    public enum SupportedObjectTypes {
        BRANCH(Map.of(
                PUT, new String[]{"admin"},
                GET, new String[]{"admin", "reviewer", "user"},
                DELETE, new String[]{"admin"})),
        USER(Map.of(
                PUT, new String[]{"admin"},
                GET, new String[]{"admin", "reviewer", "user"},
                DELETE, new String[]{"admin"})),
        USERP(Map.of(
                PUT, new String[]{"nobody"},
                GET, new String[]{"admin", "reviewer", "user"},
                DELETE, new String[]{"nobody"})),
        REPORT(Map.of(
                PUT, new String[]{"admin", "reviewer", "user"},
                GET, new String[]{"admin", "reviewer", "user"},
                DELETE, new String[]{"admin"})),
        ROLE(Map.of(
                PUT, new String[]{"admin"},
                GET, new String[]{"admin"},
                DELETE, new String[]{"admin"}));

        private Map<Operarions, String[]> permissions;
    }

    public enum Operarions {
        PUT,
        GET,
        DELETE
    }
}
