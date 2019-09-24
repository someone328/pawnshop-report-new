package com.insolence.pawnshop.report.http.handlers;

import com.insolence.pawnshop.report.util.Pair;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.Operarions.*;

@Slf4j
public class CrudHandler implements Handler<RoutingContext> {

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

    private void processPutOperation(RoutingContext rc, DeliveryOptions deliveryOptions, JsonObject jsonBody) {
        rc.vertx()
                .eventBus()
                .rxSend("crud.put", jsonBody, deliveryOptions)
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

    public boolean isSupportableType(String type) {
        return SupportedObjectTypes.valueOf(type.toUpperCase()).ordinal() != 9999;
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
