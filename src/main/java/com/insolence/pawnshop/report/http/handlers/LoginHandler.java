package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
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
        .subscribe(
            success ->
                rc.response()
                    .end(
                        success
                            .put(
                                "token",
                                authProvider.generateToken(
                                    new JsonObject().put("sub", bodyAsJson.getString("username")),
                                    new JWTOptions().setExpiresInMinutes(30)))
                            .encodePrettily()),
            error -> rc.response().setStatusCode(401).end(error.getMessage()));
  }
}
