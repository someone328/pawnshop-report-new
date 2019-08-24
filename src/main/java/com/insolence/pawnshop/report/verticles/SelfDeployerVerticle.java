package com.insolence.pawnshop.report.verticles;

import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

import static com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes.USER;

@Slf4j
public class SelfDeployerVerticle extends AbstractVerticle {
    private static final List<String> verticles2Deploy =
            Arrays.asList(
                    HttpServerVertx.class.getName(),
                    CrudVerticle.class.getName(),
                    LoginVerticle.class.getName(),
                    TotalPercentReceivedCalculationsVerticle.class.getName());

    @Override
    public void start() throws Exception {
        String envName = System.getProperty("env", "prod");
        ConfigStoreOptions store =
                new ConfigStoreOptions()
                        .setType("file")
                        .setFormat("yaml")
                        .setConfig(new JsonObject().put("path", "config/config-" + envName + ".yml"));

        ConfigRetriever retriever =
                ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store));

        retriever
                .rxGetConfig()
                .subscribe(
                        config -> deployVerticles(new DeploymentOptions().setConfig(config)),
                        error -> log.error("config load", error));
    }

    private void deployVerticles(DeploymentOptions options) {

        initMongoClient(options);

        verticles2Deploy.stream().forEach(clazz -> vertx.deployVerticle(clazz, options));
        log.info("Verticles deployed");
    }

    private void initMongoClient(DeploymentOptions options) {
        JsonObject config = options.getConfig();
        String mongoHost = config.getJsonObject("database").getJsonObject("mongo").getString("host");
        final var adminUser = new JsonObject().put("username", "admin").put("password", "cocacola#1");

        MongoClient client =
                MongoClient.createShared(
                        vertx,
                        new JsonObject().put("db_name", "pawnshop-report").put("host", mongoHost),
                        "pawnshop-report");
        client
                .rxFindOne(USER.name().toLowerCase(), adminUser, new JsonObject())
                .subscribe(
                        admin -> {
                        },
                        error -> log.error("init mongo client", error),
                        () ->
                                client.save(
                                        USER.name().toLowerCase(),
                                        adminUser,
                                        h -> {
                                            client.close();
                                        }));
    }

    private void undeployVerticles() {
        vertx
                .deploymentIDs()
                .stream()
                .filter(id -> !id.equals(deploymentID()))
                .forEach(vertx::undeploy);
    }

    private void redeployVerticles(DeploymentOptions options) {
        undeployVerticles();
        deployVerticles(options);
    }
}
