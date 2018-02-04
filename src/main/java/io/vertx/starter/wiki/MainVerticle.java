package io.vertx.starter.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.starter.wiki.database.WikiDatabaseVerticle;

public class MainVerticle extends AbstractVerticle {
  @Override
  public void start(Future<Void> startFuture) {
    Future<String> deploymentFuture = Future.future();
    vertx.deployVerticle(new WikiDatabaseVerticle(), deploymentFuture.completer());

    deploymentFuture.compose(s -> {
      Future<String> httpVerticleDeployment = Future.future();
      vertx.deployVerticle("io.vertx.starter.wiki.http.HttpServerVerticle",
        new DeploymentOptions().setInstances(2),
        httpVerticleDeployment.completer());
      return httpVerticleDeployment;
    }).setHandler(ar -> {
      if (ar.succeeded()) {
        startFuture.complete();
      } else {
        startFuture.fail(ar.cause());
      }
    });
  }
}
