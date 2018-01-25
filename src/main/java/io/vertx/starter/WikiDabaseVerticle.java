package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class WikiDabaseVerticle extends AbstractVerticle {

  public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
  public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
  public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
  public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";

  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDabaseVerticle.class);

  private enum SqlQuery {
    CREATE_PAGES_TABLE,
    ALL_PAGES,
    GET_PAGE,
    CREATE_PAGE,
    SAVE_PAGE,
    DELETE_PAGE
  }

  private Map<SqlQuery, String> sqlQueries = new HashMap<>();

  private JDBCClient jdbcClient;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    loadSqlQueries();

    JsonObject jdbcConfig = new JsonObject();
    jdbcConfig.put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
      .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30));

    jdbcClient = JDBCClient.createShared(vertx, jdbcConfig);

    jdbcClient.getConnection(car -> {
      if (car.failed()) {
        LOGGER.error("Could not open a database connection", car.cause());
        startFuture.fail(car.cause());
      } else {
        SQLConnection sqlConnection = car.result();
        sqlConnection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
          sqlConnection.close();
          if (create.failed()) {
            LOGGER.error("Database preparation failed", create.cause());
            startFuture.fail(create.cause());
          } else {
            vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"), this::onMessage);
            startFuture.complete();
          }
        });
      }
    });
  }

  private enum ErrorCodes {
    NO_ACTION_SPECIFIED, BAD_ACTION, DB_ERROR
  }

  private void onMessage(Message<JsonObject> message) {
    if (!message.headers().contains("action")) {
      LOGGER.error("No action header specified for message with headers {} and body {}", message.headers(), message.body().encodePrettily());
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
      return;
    }

    String action = message.headers().get("action");
    switch (action) {
      case "all-pages":
        this.fetchAllPages(message);
        break;
      case "get-page":
        this.fetchPage(message);
        break;
      case "create-page":
        this.createPage(message);
        break;
      case "save-page":
        this.savePage(message);
        break;
      case "delete-page":
        this.deletePage(message);
        break;
      default:
        message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action);
    }
  }

  private void deletePage(Message<JsonObject> message) {
    JsonArray data = new JsonArray().add(message.body().getString("id"));

    jdbcClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
      if (res.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void savePage(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray params = new JsonArray().add(request.getString("markdown")).add(request.getString("id"));

    jdbcClient.queryWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), params, ar -> {
      if (ar.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, ar.cause());
      }
    });
  }

  private void fetchAllPages(Message<JsonObject> message) {
    jdbcClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
      if (res.succeeded()) {
        List<String> pages
          = res.result().getResults()
          .stream()
          .map(json -> json.getString(0))
          .sorted()
          .collect(Collectors.toList());
        message.reply(new JsonObject().put("pages", new JsonArray(pages)));
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void fetchPage(Message<JsonObject> message) {
    String requestedPage = message.body().getString("page");
    JsonArray params = new JsonArray().add(requestedPage);
    jdbcClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), params, res -> {
      if (res.succeeded()) {
        JsonObject response = new JsonObject();
        ResultSet resultSet = res.result();
        if (resultSet.getNumRows() == 0) {
          response.put("found", false);
        } else {
          response.put("found", true);
          JsonArray row = resultSet.getResults().get(0);
          response.put("id", row.getInteger(0));
          response.put("rawContent", row.getString(1));
        }
        message.reply(response);
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void createPage(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray params = new JsonArray().add(request.getString("title")).add(request.getString("markdown"));

    jdbcClient.queryWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), params, ar -> {
      if (ar.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, ar.cause());
      }
    });
  }

  private void reportQueryError(Message<JsonObject> message, Throwable cause) {
    LOGGER.error("Database query error", cause);
    message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }

  private void loadSqlQueries() throws IOException {
    String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
    InputStream queriesInputStream;

    if (queriesFile != null) {
      queriesInputStream = new FileInputStream(queriesFile);
    } else {
      queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesProps = new Properties();
    queriesProps.load(queriesInputStream);
    queriesInputStream.close();

    sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"));
    sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"));
    sqlQueries.put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"));
    sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"));
    sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"));
    sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"));

  }
}