package com.insolence.pawnshop.report.starter;

import com.insolence.pawnshop.report.verticles.SelfDeployerVerticle;
import io.vertx.reactivex.core.Vertx;

public class Application {
    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
        Vertx.vertx()
                .deployVerticle(SelfDeployerVerticle.class.getName());
    }
} 
