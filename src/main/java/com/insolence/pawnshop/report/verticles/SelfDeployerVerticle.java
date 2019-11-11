package com.insolence.pawnshop.report.verticles;

import com.insolence.pawnshop.report.http.handlers.CrudHandler.SupportedObjectTypes;
import com.mongodb.MongoCommandException;
import io.reactivex.Observable;
import io.reactivex.plugins.RxJavaPlugins;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
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
                    TotalPercentReceivedCalculationsVerticle.class.getName(),
                    CalculateDynamicsVerticle.class.getName());

    @Override
    public void start() throws Exception {
        RxJavaPlugins.setErrorHandler(e -> log.error("Main error handler", e));
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
        final var adminUser = new JsonObject()
                .put("username", "admin")
                .put("password", "cocacola#1")
                .put("roles", new JsonArray()
                        .add("admin")
                        .add("reviewer")
                        .add("user"));

        MongoClient client =
                MongoClient.createShared(
                        vertx,
                        new JsonObject().put("db_name", "pawnshop-report").put("host", mongoHost),
                        "pawnshop-report");

        createTablesAndAdmin(adminUser, client);
        createIndexes(client);
    }

    private void createIndexes(MongoClient client) {
        client.rxCreateIndex("report", new JsonObject()
                .put("branch", "hashed"))
                .subscribe(
                        () -> System.out.println("Branch index created or was there"),
                        onError -> System.out.println("Cannot create Branch index:" + onError));
        client.rxCreateIndex("report", new JsonObject().put("date", 1))
                .subscribe(
                        () -> System.out.println("Date index created or was there"),
                        onError -> System.out.println("Cannot create Date index:" + onError));
    }

    private void createTablesAndAdmin(JsonObject adminUser, MongoClient client) {
        Observable.fromArray(SupportedObjectTypes.values())
                .map(Enum::name)
                .map(String::toLowerCase)
                .flatMapCompletable(v -> client.rxCreateCollection(v))
                .onErrorComplete(error -> error instanceof MongoCommandException && "NamespaceExists".equals(((MongoCommandException) error).getErrorCodeName()))
                .andThen(client.rxFindOne(USER.name().toLowerCase(),
                        new JsonObject()
                                .put("username", "admin")
                                .put("password", "cocacola#1"),
                        new JsonObject()))
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
