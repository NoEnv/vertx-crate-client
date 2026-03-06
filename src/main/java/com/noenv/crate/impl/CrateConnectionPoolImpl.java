package com.noenv.crate.impl;

import com.noenv.crate.CrateConnection;
import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import io.vertx.core.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.util.concurrent.TimeUnit;

/**
 * Pool-backed implementation of {@link CrateConnection}.
 * Each call to {@link #streamQuery} acquires a connection from the pool
 * and releases it when the stream ends.
 */
public class CrateConnectionPoolImpl implements CrateConnection {

  private final CrateConnectionFactory factory;
  private final ContextInternal context;

  public CrateConnectionPoolImpl(CrateConnectionFactory factory, ContextInternal context) {
    this.factory = factory;
    this.context = context;
  }

  @Override
  public RowStream<JsonObject> streamQuery(CrateQuery query) {
    return new RowStream<>() {
      private Handler<JsonObject> handler;
      private Handler<Throwable> exceptionHandler;
      private Handler<Void> endHandler;

      {
        factory.acquireConnection()
          .onSuccess(conn ->
            conn.sendQuery(context, query)
              .handler(row -> { if (handler != null) handler.handle(row); })
              .exceptionHandler(err -> {
                conn.close();
                if (exceptionHandler != null) exceptionHandler.handle(err);
              })
              .endHandler(v -> {
                conn.close();
                if (endHandler != null) endHandler.handle(null);
              })
          )
          .onFailure(err -> {
            if (exceptionHandler != null) exceptionHandler.handle(err);
          });
      }

      @Override
      public RowStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
      }

      @Override
      public RowStream<JsonObject> handler(Handler<JsonObject> handler) {
        this.handler = handler;
        return this;
      }

      @Override
      public RowStream<JsonObject> pause() { return this; }

      @Override
      public RowStream<JsonObject> resume() { return this; }

      @Override
      public RowStream<JsonObject> fetch(long amount) { return this; }

      @Override
      public RowStream<JsonObject> endHandler(Handler<Void> handler) {
        this.endHandler = handler;
        return this;
      }

      @Override
      public Future<Void> close() {
        return Future.succeededFuture();
      }
    };
  }

  @Override
  public Future<CrateMessage> query(CrateQuery query) {
    return factory.acquireConnection()
      .compose(conn -> conn.sendRequest(context, query)
        .onComplete(ar -> conn.close()));
  }

  @Override
  public Future<Void> cancelRequest() {
    return Future.succeededFuture();
  }

  @Override
  public CrateConnection exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public CrateConnection closeHandler(Handler<Void> handler) {
    return this;
  }

  @Override
  public Future<Transaction> begin() {
    throw new UnsupportedOperationException("Transactions are not supported by CrateDB");
  }

  @Override
  public Transaction transaction() {
    throw new UnsupportedOperationException("Transactions are not supported by CrateDB");
  }

  @Override
  public boolean isSSL() {
    return factory.options.getSslMode() != null;
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    return null;
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    return null;
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return null;
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
    return null;
  }

  @Override
  public Future<PreparedStatement> prepare(String s) {
    return null;
  }

  @Override
  public Future<PreparedStatement> prepare(String s, PrepareOptions prepareOptions) {
    return null;
  }

  @Override
  public Future<Void> close() {
    return factory.shutdown(10L, TimeUnit.SECONDS);
  }
}
