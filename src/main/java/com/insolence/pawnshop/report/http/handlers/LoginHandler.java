package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.reactivex.ext.auth.jwt.JWTAuth;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class LoginHandler implements Handler<RoutingContext> {

    private JWTAuth authProvider;

    @Override
    public void handle(RoutingContext rc) {
        var bodyAsJson = rc.getBodyAsJson();
        var userRequest =
                new JsonObject()
                        .put("username", bodyAsJson.getString("username"))
                        .put("password", bodyAsJson.getString("password"));
        rc.vertx()
                .eventBus()
                .rxSend("login", userRequest)
                .map(m -> (JsonObject) m.body())
                .subscribe(
                        success -> {
                            List<String> roles = success.getJsonArray("roles").stream().map(e -> (String) e).collect(Collectors.toList());
                            JsonObject claims = new JsonObject().put("sub", bodyAsJson.getString("username"));
                            JWTOptions options = new JWTOptions().setExpiresInMinutes(30).setPermissions(roles);
                            rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                         .end(success.put("token", authProvider.generateToken(claims, options)).encodePrettily());

                        },
                        error -> rc.response().setStatusCode(401).end(error.getMessage()));
    }
}
