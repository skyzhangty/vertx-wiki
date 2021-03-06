package io.vertx.starter.wiki.http;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import io.vertx.starter.wiki.database.WikiDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class HttpServerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  private static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  private static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private String wikiDbQueue = "wikidb.queue";

  private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

  private WikiDatabaseService dbService;

  private WebClient webClient;

  @Override
  public void start(Future<Void> startFuture) {
    wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, wikiDbQueue);

    dbService = WikiDatabaseService.createProxy(vertx, wikiDbQueue);

    webClient = WebClient.create(vertx);

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);

    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeleteHandler);
    router.get("/backup").handler(this::backupHandler);

    int port = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server.requestHandler(router::accept)
      .listen(port, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("Http server running on port 8080");
          startFuture.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          startFuture.fail(ar.cause());
        }
      });

  }

  private void backupHandler(RoutingContext context) {
    this.dbService.fetchAllPagesData(reply -> {
      if (reply.succeeded()) {
        JsonObject filesObject = new JsonObject();
        JsonObject gistPayload = new JsonObject().put("files", filesObject).put("discription", "a wiki backup").put("public", true);
        reply.result().forEach(page -> {
          JsonObject fileObject = new JsonObject();
          fileObject.put(page.getString("NAME"), fileObject);
          fileObject.put("content", page.getString("CONTENT"));
        });

        this.webClient.post(443, "api.github.com", "/gists")
          .putHeader("Accept", "application/vnd.github.v3+json")
          .as(BodyCodec.jsonObject())
          .sendJsonObject(gistPayload, ar -> {
            if (ar.succeeded()) {
              HttpResponse<JsonObject> response = ar.result();
              if (response.statusCode() == 201) {
                context.put("backup_gist_url", response.body().getString("html_url"));
                indexHandler(context);
              } else {
                StringBuffer message = new StringBuffer();
                message.append("Could not backup the wiki:");
                message.append(response.statusMessage());
                JsonObject body = response.body();
                if (body != null) {
                  message.append(System.getProperty("line.separator")).append(body.encodePrettily());
                }
                LOGGER.error(message.toString());
                context.fail(502);
              }
            } else {
              Throwable err = ar.cause();
              LOGGER.error("HTTP Client error", err);
              context.fail(err);
            }
          });
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void indexHandler(RoutingContext context) {
    this.dbService.fetchAllPages(reply -> {
      if (reply.succeeded()) {
        context.put("title", "Wiki home");
        context.put("pages", reply.result().getList());
        templateEngine.render(context, "templates", "/index.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageDeleteHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    this.dbService.deletePage(Integer.valueOf(id), reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/");
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;
    if (pageName == null || pageName.isEmpty()) {
      location = "/";
    }

    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageUpdateHandler(RoutingContext context) {
    String title = context.request().getParam("title");

    Handler<AsyncResult<Void>> handler = reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/wiki/" + title);
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    };

    String markdown = context.request().getParam("markdown");
    if (context.request().getParam("newPage").equals("yes")) {
      dbService.createPage(title, markdown, handler);
    } else {
      String id = context.request().getParam("id");
      dbService.savePage(Integer.valueOf(id), markdown, handler);
    }
  }

  private static final String EMPTY_PAGE_MARKDOWN =
    "# A new page\n" +
      "\n" +
      "Feel-free to write in Markdown!\n";

  private void pageRenderingHandler(RoutingContext context) {
    String requestedPage = context.request().getParam("page");
    this.dbService.fetchPage(requestedPage, reply -> {
      if (reply.succeeded()) {
        JsonObject payload = reply.result();

        boolean found = payload.getBoolean("found");
        String rawContent = payload.getString("rawContent", EMPTY_PAGE_MARKDOWN);
        context.put("title", requestedPage);
        context.put("id", payload.getInteger("id", -1));
        context.put("newPage", found ? "no" : "yes");
        context.put("rawContent", rawContent);
        context.put("content", Processor.process(rawContent));
        context.put("timestamp", new Date().toString());

        templateEngine.render(context, "templates", "/page.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });

      } else {
        context.fail(reply.cause());
      }
    });
  }
}
