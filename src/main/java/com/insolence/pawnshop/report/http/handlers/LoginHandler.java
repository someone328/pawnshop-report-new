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
        .map(
            x -> {
              long loginCount = (long) x.body();
              if (loginCount == 1) {
                rc.response()
                    .end(
                        authProvider.generateToken(
                            new JsonObject().put("sub", bodyAsJson.getString("username")),
                            new JWTOptions().setExpiresInMinutes(30)));
              }
              return loginCount;
            })
        .filter(count -> count != 1)
        .subscribe(success -> rc.response().setStatusCode(401).end());
  }
}
