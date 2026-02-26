package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.SslMode;
import com.noenv.crate.codec.CrateMessage;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
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

  public Future<CrateMessage> sendRequest(ContextInternal context, String request) {
    return httpClientConnection.request(HttpMethod.GET, "/")
      .compose(r -> r
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .send(Buffer.buffer(request))
      )
      .compose(res -> {
      if (res.statusCode() != 200) {
        return context.failedFuture(new RuntimeException("Unexpected response status code: " + res.statusCode()));
      }
      return res.body().map(body -> new CrateMessage(body));
    });
  }

  public boolean isSSL() {
    return options.getSslMode() != null && options.getSslMode() != SslMode.DISABLE;
  }

  public Future<Void> close() {
    return httpClientConnection.close();
  }
}
