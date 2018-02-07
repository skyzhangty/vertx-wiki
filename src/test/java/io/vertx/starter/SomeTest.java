package io.vertx.starter;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.starter.wiki.database.WikiDatabaseService;
import io.vertx.starter.wiki.database.WikiDatabaseVerticle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SomeTest {

  private Vertx vertx;

  private WikiDatabaseService service;

  @Before
  public void setUp(TestContext testContext) {
    vertx = Vertx.vertx();

    JsonObject conf = new JsonObject()
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
      .put(WikiDatabaseVerticle.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4);

    vertx.deployVerticle(new WikiDatabaseVerticle(), new DeploymentOptions().setConfig(conf),
      testContext.asyncAssertSuccess(id -> service = WikiDatabaseService.createProxy(vertx, WikiDatabaseVerticle.CONFIG_WIKIDB_QUEUE))
    );
  }

  @After
  public void tearDown(TestContext testContext) {
    vertx.close(testContext.asyncAssertSuccess());
  }

  @Test
  public void async_behavior(TestContext testContext) {
    Vertx vertx = Vertx.vertx();
    testContext.assertEquals("foo", "foo");
    Async async1 = testContext.async();
    Async async2 = testContext.async(3);

    vertx.setTimer(100, n -> async1.complete());
    vertx.setPeriodic(100, n -> async2.countDown());
  }

  @Test
  public void testCRUD(TestContext testContext) {
    Async async = testContext.async();

    service.createPage("TestTitle", "TestMarkdown", testContext.asyncAssertSuccess(v1 -> {
      service.fetchPage("TestTitle", testContext.asyncAssertSuccess(json1 -> {
        testContext.assertTrue(json1.getBoolean("found"));
        testContext.assertTrue(json1.containsKey("id"));
        testContext.assertEquals("TestMarkdown", json1.getString("rawContent"));

        service.savePage(json1.getInteger("id"), "Yo!", testContext.asyncAssertSuccess(v2 -> {
          service.fetchAllPages(testContext.asyncAssertSuccess(array1 -> {
            testContext.assertEquals(1, array1.size());

            service.fetchPage("TestTitle", testContext.asyncAssertSuccess(json2 -> {
              testContext.assertEquals("Yo!", json2.getString("rawContent"));
              service.deletePage(json2.getInteger("id"), testContext.asyncAssertSuccess(v3 -> {
                service.fetchAllPages(testContext.asyncAssertSuccess(array2 -> {
                  testContext.assertEquals(0, array2.size());
                  async.complete();
                }));
              }));
            }));
          }));
        }));
      }));
    }));
  }

  @Test
  public void testWebClient(TestContext testContext) {
    Async async = testContext.async();
    vertx.createHttpServer().requestHandler(
      req -> req.response().putHeader("Content-Type", "text/plain").end("OK")
    ).listen(8080, testContext.asyncAssertSuccess(server -> {
      WebClient webClient = WebClient.create(vertx);
      webClient.get(8080, "localhost", "/").send(asyncResult -> {
        if (asyncResult.succeeded()) {
          HttpResponse<Buffer> httpResponse = asyncResult.result();
          testContext.assertTrue(httpResponse.headers().contains("Content-Type"));
          testContext.assertEquals("text/plain", httpResponse.getHeader("Content-Type"));
          testContext.assertEquals("OK", httpResponse.body().toString());
          webClient.close();
          async.complete();
        } else {
          async.resolve(Future.failedFuture(asyncResult.cause()));
        }
      });
    }));
  }
}
