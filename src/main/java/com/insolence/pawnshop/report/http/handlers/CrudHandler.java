package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.util.Pair;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.Operarions.*;
import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.REPORT;

@Slf4j
public class CrudHandler implements Handler<RoutingContext> {

    private Map<SupportedObjectTypes, List<BiFunction<JsonObject, RoutingContext, Single<Boolean>>>> putValidations = new HashMap<>();

    public CrudHandler(){
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
                .filter(Pair::getRight)
                .take(1)
                .switchIfEmpty(Observable.error(new RuntimeException()))
                .subscribe(
                        success -> {
                            DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("objectType", objectType);
                            JsonObject jsonBody = rc.getBodyAsJson();
                            switch (operationTypeEnum) {
                                case PUT -> processPutOperation(rc, deliveryOptions, jsonBody);
                                case GET -> processGetOperation(rc, deliveryOptions, jsonBody);
                                case DELETE -> processDeleteOperation(rc, deliveryOptions, jsonBody);
                                default -> rc.response().setStatusCode(400).end("Unsupported operationType type " + operationType);
                            }
                        },
                        error -> rc.response().setStatusCode(403).end("User not permitted for requested operation"));
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
        rc.vertx()
                .eventBus()
                .rxSend("crud.get", jsonBody, deliveryOptions)
                .map(response -> response.body().toString())
                .subscribe(
                        crudResult ->
                                rc.response()
                                        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                        .end(crudResult),
                        error -> {
                            log.error("Crud handler error. ", error);
                            rc.fail(500);
                        });
    }

    //TODO доделать прокидывание сообщений валидации
    private void processPutOperation(RoutingContext rc, DeliveryOptions deliveryOptions, JsonObject jsonBody) {
        SupportedObjectTypes objectType = SupportedObjectTypes.valueOf(deliveryOptions.getHeaders().get("objectType").toUpperCase());
        List<BiFunction<JsonObject, RoutingContext, Single<Boolean>>> validations = putValidations.get(objectType);
        Observable.fromIterable(validations)
                .subscribeOn(Schedulers.io())
                .flatMapSingle(f -> f.apply(jsonBody, rc))
                .reduce(Boolean.TRUE, (one, two) -> one && two)
                .flatMap(isValid -> {
                    if(!isValid){
                        throw new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 100000, "Дубликат");
                    }
                    return rc.vertx()
                            .eventBus()
                            .rxSend("crud.put", jsonBody, deliveryOptions);
                })
                .map(response -> response.body().toString())
                .subscribe(
                        crudResult ->
                                rc.response()
                                        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                        .end(crudResult),
                        error -> {
                            log.error("Crud handler error. ", error);
                            if (error instanceof ReplyException) {
                                rc.fail(((ReplyException) error).failureCode(), error);
                            } else {
                                rc.fail(500, error);
                            }
                        });
    }

    public boolean isSupportableType(String type) {
        return SupportedObjectTypes.valueOf(type.toUpperCase()).ordinal() != 9999;
    }

    //TODO причесать, вынести в отдельный классы?
    private void initValidations() {
        List<BiFunction<JsonObject, RoutingContext, Single<Boolean>>> reportValidations = putValidations.computeIfAbsent(REPORT, k -> new ArrayList<>());
        reportValidations.add((reportJson, rc) -> {
            Report report = reportJson.mapTo(Report.class);
            return rc.vertx().eventBus()
                    .rxSend("crud.get",
                            new JsonObject().put("branch", report.getBranch()).put("date", report.getDate()),
                            new DeliveryOptions().addHeader("objectType", REPORT.name().toLowerCase()))
                    .map(response -> (JsonArray)response.body())
                    .map(JsonArray::size)
                    .map(size -> size == 0);
        });
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
