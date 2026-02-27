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
import com.noenv.crate.SslMode;
import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.metrics.ClientMetrics;

import java.util.function.Predicate;

public class CrateHttpConnection {

  public CrateDatabaseMetadata dbMetaData;
  private CrateConnectOptions options;
  private final HttpClientConnection httpClientConnection;

  public CrateHttpConnection(HttpClientConnection httpClientConnection,
                             ClientMetrics metrics,
                             CrateConnectOptions options,
                             boolean cachePreparedStatements,
                             int preparedStatementCacheSize,
                             Predicate<String> preparedStatementCacheSqlFilter,
                             int pipeliningLimit,
                             ContextInternal context) {
    this.httpClientConnection = httpClientConnection;
    this.options = options;
  }

  protected CrateConnectOptions options() {
    return options;
  }

  public Future<CrateMessage> sendRequest(ContextInternal context, CrateQuery query) {
    // "/_sql?types", "/_sql?error_trace=true"
    return httpClientConnection.request(HttpMethod.POST, "/_sql?types")
      .compose(r -> r
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .send(query.toJson().toBuffer())
      )
      .compose(res -> {
      if (res.statusCode() != 200) {
        res.body()
          .map(Buffer::toJsonObject)
          .onSuccess(j -> System.out.println("Error response body: " + j.encodePrettily()));
        return context.failedFuture(new RuntimeException("Unexpected response status code: " + res.statusCode()));
      }
      return res.body()
        .map(Buffer::toJsonObject)
        .map(CrateMessage::new);
    });
  }

  public Future<CrateDatabaseMetadata> getMetadata(ContextInternal context) {
    return httpClientConnection.request(HttpMethod.GET, "/")
      .compose(r -> r
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .send()
      )
      .compose(res -> {
        if (res.statusCode() != 200) {
          return context.failedFuture(new RuntimeException("Unexpected response status code: " + res.statusCode()));
        }
        return res.body()
          .map(Buffer::toJsonObject)
          .map(json -> new CrateDatabaseMetadata(json.getJsonObject("version", new JsonObject()).getString("number", "0.0.0")));
      });
  }

  public boolean isSSL() {
    return options.getSslMode() != null && options.getSslMode() != SslMode.DISABLE;
  }

  public Future<Void> close() {
    return httpClientConnection.close();
  }
}
