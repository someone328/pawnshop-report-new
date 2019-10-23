package com.insolence.pawnshop.report.verticles;

import com.insolence.pawnshop.report.http.handlers.*;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.auth.jwt.JWTAuth;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import io.vertx.reactivex.ext.web.handler.CorsHandler;
import io.vertx.reactivex.ext.web.handler.JWTAuthHandler;
import io.vertx.reactivex.ext.web.handler.ResponseContentTypeHandler;
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
                                .allowedHeader("Authorization")
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

        router.post("/login").handler(new LoginHandler(authProvider));
        router.route("/protected/*").handler(JWTAuthHandler.create(authProvider));

        /** crud */
        router.post("/protected/v1/crud/:objectType/:operationType").handler(new CrudHandler());
        router.post("/protected/v1/crud/report/get/previous").handler(new FindPreviousReportHandler());
        /** new report */
        router.post("/protected/v1/calculateDynamics").handler(new CreateNewReportHandler());
        router.get("/protected/v1/statistics/:year").handler(new StatisticsHandler());
        router.get("/protected/v1/availableYears").handler(new AvailableYearsHandler());
        router.get("/protected/v1/dailyReport/:timestamp").handler(new DailyReportHandler());
        router.get("/protected/v1/totalPercent/:reportId").handler(new TotalPercentHandler());
        router.get("/protected/v1/backup").handler(new BackUpHandler());
        router.post("/protected/v1/report/export/excel").handler(new ExcelExportHandler());
        //TODO не забыть перенести в защищенную область
        router.get("/test/v1/report/export/excel").handler(new ExcelExportHandler());

        router.errorHandler(400, rc -> {
            rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "text/html; charset=utf-8").setStatusCode(400).end(rc.failure().getMessage());
        });
        router.errorHandler(11000, rc -> {
            rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "text/html; charset=utf-8").setStatusCode(500).end("Версия объекта не актуальна.");
        });
        router.errorHandler(100000, rc -> {
            rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "text/html; charset=utf-8").setStatusCode(500).end("Такой обьект уже существует.");
        });

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
