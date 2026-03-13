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
package com.noenv.crate;

import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.impl.connection.CrateConnectionImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;

/**
 * A connection to CrateDB.
 *
 * @author Lukas Prettenthaler
 */
@VertxGen
public interface CrateConnection extends SqlConnection {

  /**
   * Connects to the database and returns the connection if that succeeds.
   * <p/>
   * The connection interracts directly with the database is not a proxy, so closing the
   * connection will close the underlying connection to the database.
   *
   * @param vertx the vertx instance
   * @param options the connect options
   * @return a future notified with the connection or the failure
   */
  static Future<CrateConnection> connect(Vertx vertx, CrateConnectOptions options) {
    return CrateConnectionImpl.connect((ContextInternal) vertx.getOrCreateContext(), options);
  }

  /**
   * Streams the result of the given query as a row stream.
   *
   * @param query the query to execute
   * @return a stream of result rows as {@link JsonObject}
   */
  RowStream<JsonObject> streamQuery(CrateQuery query);

  /**
   * Executes the given query and returns the full response as a {@link CrateMessage}.
   *
   * @param query the query to execute
   * @return a future notified with the CrateDB response message, or the failure
   */
  Future<CrateMessage> query(CrateQuery query);

  /**
   * Send a request cancellation message to tell the server to cancel processing request in this connection.
   * <br>Note: Use this with caution because the cancellation signal may or may not have any effect.
   *
   * @return a future notified if cancelling request is sent
   */
  Future<Void> cancelRequest();

  /**
   * {@inheritDoc}
   */
  @Fluent
  @Override
  CrateConnection exceptionHandler(Handler<Throwable> handler);

  /**
   * {@inheritDoc}
   */
  @Fluent
  @Override
  CrateConnection closeHandler(Handler<Void> handler);
}
