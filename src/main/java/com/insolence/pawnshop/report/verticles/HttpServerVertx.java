package com.insolence.pawnshop.report.verticles;

import com.insolence.pawnshop.report.http.handlers.CrudHandler;
import com.insolence.pawnshop.report.http.handlers.LoginHandler;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.KeyStoreOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.auth.jwt.JWTOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.auth.jwt.JWTAuth;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import io.vertx.reactivex.ext.web.handler.CookieHandler;
import io.vertx.reactivex.ext.web.handler.CorsHandler;
import io.vertx.reactivex.ext.web.handler.JWTAuthHandler;
import io.vertx.reactivex.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.reactivex.ext.web.handler.SessionHandler;
import io.vertx.reactivex.ext.web.sstore.LocalSessionStore;
import io.vertx.reactivex.ext.web.sstore.SessionStore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpServerVertx extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    HttpServer httpServer = vertx.createHttpServer();
    Router router = Router.router(vertx);
    router.post().handler(BodyHandler.create());
    /*router.route().handler(CookieHandler.create());
    router
        .route()
        .handler(
            SessionHandler.create(
                SessionStore.newInstance(LocalSessionStore.create(vertx).getDelegate())));*/
    router
        .route()
        .handler(
            CorsHandler.create("*")
                .allowedHeader("Origin")
                .allowedHeader("X-Requested-With")
                .allowedHeader("Content-Type")
                .allowedHeader("Accept")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.POST));
    router.route().handler(ResponseContentTypeHandler.create());

    JWTAuth authProvider =
        JWTAuth.create(
            vertx,
            new JWTAuthOptions()
                .addPubSecKey(
                    new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setPublicKey("dRRVnUmUHXOTt9nk")
                        .setSymmetric(true)));
    router.route("/protected/*").handler(JWTAuthHandler.create(authProvider));

    router.post("/login").handler(new LoginHandler(authProvider));

    /** crud */
    router.post("/protected/v1/crud/:objectType/:operationType").handler(new CrudHandler());

    /** hystrix mectrix */
    /*
    router.getDelegate()
          .get(EventMetricsStreamHandler.DEFAULT_HYSTRIX_PREFIX)
          .handler(EventMetricsStreamHandler.createHandler(vertx.getDelegate()));*/

    httpServer.requestHandler(router).listen(8181);
    log.info("HTTP restEndpoint started");
    log.info("Config:" + config());
  }
}
