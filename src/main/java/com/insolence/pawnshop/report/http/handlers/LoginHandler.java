package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.reactivex.ext.auth.jwt.JWTAuth;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

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
                .map(user -> {
                    var claims = new JsonObject().put("sub", user.getString("username"));
                    var options = new JWTOptions().setExpiresInMinutes(30).setPermissions(user.getJsonArray("roles", new JsonArray()).getList());
                    return user.put("token", authProvider.generateToken(claims, options));
                })
                .subscribe(
                        success -> rc.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                .end(success.encodePrettily()),
                        error -> rc.response().setStatusCode(401).end(error.getMessage()));
    }
}
