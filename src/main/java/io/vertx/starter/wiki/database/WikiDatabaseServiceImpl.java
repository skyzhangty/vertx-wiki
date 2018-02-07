package io.vertx.starter.wiki.database;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class WikiDatabaseServiceImpl implements WikiDatabaseService {
  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);
  private final JDBCClient jdbcClient;
  private final HashMap<SqlQuery, String> sqlQueries;

  public WikiDatabaseServiceImpl(JDBCClient jdbcClient, HashMap<SqlQuery, String> sqlQueries, Handler<AsyncResult<WikiDatabaseService>> resultHandler) {
    this.jdbcClient = jdbcClient;
    this.sqlQueries = sqlQueries;

    this.jdbcClient.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause());
        resultHandler.handle(Future.failedFuture(ar.cause()));
      } else {
        SQLConnection sqlConnection = ar.result();
        sqlConnection.execute(this.sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
          if (create.failed()) {
            LOGGER.error("Database preparation error", create.cause());
            resultHandler.handle(Future.failedFuture(create.cause()));
          } else {
            resultHandler.handle(Future.succeededFuture(this));
          }
        });
      }
    });
  }

  @Override
  public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {
    this.jdbcClient.query(this.sqlQueries.get(SqlQuery.ALL_PAGES), fetch -> {
      if (fetch.succeeded()) {
        List<JsonArray> results = fetch.result().getResults();
        JsonArray pages = new JsonArray(results.stream().map(json -> json.getString(0)).collect(Collectors.toList()));
        resultHandler.handle(Future.succeededFuture(pages));
      } else {
        LOGGER.error("Faile to fetch all pages", fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {
    this.jdbcClient.queryWithParams(this.sqlQueries.get(SqlQuery.GET_PAGE), new JsonArray().add(name), fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();
        ResultSet resultSet = fetch.result();

        if (resultSet.getNumRows() == 0) {
          response.put("found", false);
        } else {
          response.put("found", true);
          JsonArray row = resultSet.getResults().get(0);
          response.put("id", row.getInteger(0));
          response.put("rawContent", row.getString(1));
        }
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("fetch page failed", fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray params = new JsonArray().add(title).add(markdown);
    this.jdbcClient.updateWithParams(this.sqlQueries.get(SqlQuery.CREATE_PAGE), params, create -> {
      if (create.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("create page failed", create.cause());
        resultHandler.handle(Future.failedFuture(create.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray params = new JsonArray().add(markdown).add(id);
    this.jdbcClient.updateWithParams(this.sqlQueries.get(SqlQuery.SAVE_PAGE), params, save -> {
      if (save.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("save page failed", save.cause());
        resultHandler.handle(Future.failedFuture(save.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray params = new JsonArray().add(id);
    this.jdbcClient.updateWithParams(this.sqlQueries.get(SqlQuery.DELETE_PAGE), params, delete -> {
      if (delete.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("delete page failed", delete.cause());
        resultHandler.handle(Future.failedFuture(delete.cause()));
      }
    });
    return this;
  }

  @Override
  public WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    this.jdbcClient.query(this.sqlQueries.get(SqlQuery.ALL_PAGES_DATA), fetch -> {
      if (fetch.succeeded()) {
        List<JsonObject> rows = fetch.result().getRows();
        resultHandler.handle(Future.succeededFuture(rows));
      } else {
        LOGGER.error("fetch all pages data failed", fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }
}
