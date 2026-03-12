/*
 * Copyright (C) 2026 Lukas Prettenthaler
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateConnection;
import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import io.vertx.core.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.PromiseInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.util.concurrent.TimeUnit;

/**
 * The implementation of the {@link CrateConnection}.
 *
 * @author Lukas Prettenthaler
 */
public class CrateConnectionImpl implements CrateConnection, Closeable {
  private volatile Handler<Throwable> exceptionHandler;
  private volatile Handler<Void> closeHandler;
  private volatile boolean closeAgentAfterUsage;
  protected final ContextInternal context;
  protected final CrateConnectionFactory factory;
  /** Current connection; replaced on failover to another endpoint. */
  protected volatile CrateHttpConnection conn;

  public static Future<CrateConnection> connect(ContextInternal context, CrateConnectOptions options) {
    var client = new CrateConnectionFactory(context, options);
    return client
      .connect()
      .map(conn -> new CrateConnectionImpl(client, context, conn));
  }

  public CrateConnectionImpl(CrateConnectionFactory factory, ContextInternal context, CrateHttpConnection conn) {
    this.factory = factory;
    this.context = context;
    this.conn = conn;
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
  public Future<Void> cancelRequest() {
    return Future.succeededFuture();
  }

  @Override
  public CrateConnection exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public CrateConnection closeHandler(Handler<Void> handler) {
    closeHandler = handler;
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
    return conn.isSSL();
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    return conn.dbMetaData;
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    return new CrateQueryExecution(this, s);
  }

  @Override
  public RowStream<JsonObject> streamQuery(CrateQuery query) {
    CrateHttpConnection c = conn;
    return c.sendQuery(context, query, err -> {
      if (CrateFailoverPredicate.isFailoverError(err)) {
        factory.markEndpointUnhealthy(c.remoteAddress());
      }
    });
  }

  @Override
  public Future<CrateMessage> query(CrateQuery query) {
    return sendRequest(conn, query, factory.options.getFailoverMaxRetries());
  }

  Future<CrateMessage> sendRequest(CrateHttpConnection currentConn, CrateQuery query, int remaining) {
    return currentConn.sendRequest(context, query)
      .recover(err -> {
        if (remaining <= 1 || !CrateFailoverPredicate.isFailoverError(err)) {
          return context.failedFuture(err);
        }
        factory.markEndpointUnhealthy(currentConn.remoteAddress());
        return factory.connect()
          .compose(newConn -> {
            conn = newConn;
            return sendRequest(newConn, query, remaining - 1);
          });
      });
  }

  // TODO: implement
  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return null;
  }

  // TODO: implement
  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
    return null;
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = promise();
    close(promise);
    return promise.future();
  }

  @Override
  public void close(Completable<Void> completion) {
    if (closeAgentAfterUsage) {
      Completable<Void> next = completion;
      completion = (res, err) -> {
        try {
          next.complete(res, err);
        } finally {
          factory.shutdown(10L, TimeUnit.SECONDS);
        }
      };
    }
    doClose(completion);
    if (closeAgentAfterUsage) {
      context.removeCloseHook(this);
    }
  }

  private void doClose(Completable<Void> promise) {
    context.execute(promise, p ->
      conn.close().onComplete(p)
    );
  }

  protected static Future<CrateConnection> prepareForClose(ContextInternal ctx, Future<CrateConnection> future) {
    return future.andThen(ar -> {
      if (ar.succeeded()) {
        prepareForClose(ctx, (CrateConnectionImpl) ar.result());
      }
    });
  }

  private static void prepareForClose(ContextInternal ctx, CrateConnectionImpl base) {
    base.closeAgentAfterUsage = true;
    ctx.addCloseHook(base);
  }

  protected ContextInternal context() {
    return this.context;
  }

  protected <T> PromiseInternal<T> promise() {
    return this.context.promise();
  }

  public void handleException(Throwable failure) {
    Handler<Throwable> handler = this.exceptionHandler;
    if (handler != null) {
      this.context.emit(failure, handler);
    } else {
      failure.printStackTrace();
    }
  }

  public void handleClosed() {
    Handler<Void> handler = this.closeHandler;
    if (handler != null) {
      this.context.emit(handler);
    }
  }
}
