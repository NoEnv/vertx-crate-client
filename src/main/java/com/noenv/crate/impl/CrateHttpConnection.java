package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateException;
import com.noenv.crate.CrateSessionOptions;
import com.noenv.crate.SslMode;
import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientConnection;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.sqlclient.RowStream;

import java.util.function.Predicate;

public class CrateHttpConnection {

  private final ClientMetrics<?, ?, ?> metrics; // TODO: report requests to metrics
  private final ContextInternal context;
  private Predicate<String> preparedStatementCacheSqlFilter = s -> false; // TODO: implement prepared statement caching
  public CrateDatabaseMetadata dbMetaData;
  private final CrateConnectOptions options;
  private final HttpClientConnection httpClientConnection;

  // TODO: implement prepared statement caching
  public CrateHttpConnection(HttpClientConnection httpClientConnection,
                             ClientMetrics<?,?,?> metrics,
                             CrateConnectOptions options,
                             ContextInternal context,
                             Predicate<String> preparedStatementCacheSqlFilter) {
    this.httpClientConnection = httpClientConnection;
    this.options = options;
    this.metrics = metrics;
    this.preparedStatementCacheSqlFilter = preparedStatementCacheSqlFilter;
    this.context = context;
  }

  public CrateHttpConnection(HttpClientConnection httpClientConnection,
                             ClientMetrics<?,?,?> metrics,
                             CrateConnectOptions options,
                             ContextInternal context) {
    this.httpClientConnection = httpClientConnection;
    this.options = options;
    this.metrics = metrics;
    this.context = context;
  }

  protected CrateConnectOptions options() {
    return options;
  }

  public RowStream<JsonObject> sendQuery(ContextInternal context, CrateQuery query) {
    return sendQuery(context, query, null);
  }

  public RowStream<JsonObject> sendQuery(ContextInternal context, CrateQuery query, Handler<Throwable> onFailoverError) {
    return new RowStreamImpl(httpClientConnection, options, context, query, onFailoverError);
  }

  public Future<CrateMessage> sendRequest(ContextInternal context, CrateQuery query) {
    return httpClientConnection.request(new RequestOptions().setMethod(HttpMethod.POST).setURI("/_sql").setHeaders(options.getDefaultHeaders()))
      .compose(r -> r.send(query.toJson().toBuffer()))
      .compose(res -> {
        if (res.statusCode() != 200) {
          return res.body()
            .compose(buf -> {
              JsonObject body = buf.toJsonObject();
              JsonObject error = body.getJsonObject("error", new JsonObject());
              CrateException e = new CrateException(
                res.statusCode(),
                error.getInteger("code", -1),
                error.getString("message", "HTTP " + res.statusCode()));
              return context.<CrateMessage>failedFuture(e);
            })
            .recover(t -> context.failedFuture(new RuntimeException("HTTP " + res.statusCode(), t)));
        }
        return res.body()
          .map(Buffer::toJsonObject)
          .map(CrateMessage::new);
      });
  }

  public Future<Void> initSession(ContextInternal context) {
    return initSession(context, new CrateSessionOptions());
  }

  public Future<Void> initSession(ContextInternal context, CrateSessionOptions sessionOptions) {
    return sendRequest(context, new CrateQuery(String.format("SET statement_timeout = %d", sessionOptions.getStatementTimeout())))
      .map(msg -> null);
  }

  public Future<CrateDatabaseMetadata> getMetadata(ContextInternal context) {
    return httpClientConnection.request(new RequestOptions().setMethod(HttpMethod.GET).setURI( "/").setHeaders(options.getDefaultHeaders()))
      .compose(HttpClientRequest::send)
      .compose(res -> {
        if (res.statusCode() != 200) {
          return context.failedFuture(new RuntimeException("Unexpected response status code: " + res.statusCode()));
        }
        return res.body()
          .map(Buffer::toJsonObject)
          .map(json -> new CrateDatabaseMetadata(
            json.getJsonObject("version", new JsonObject()).getString("number", "0.0.0")));
      });
  }

  public SocketAddress remoteAddress() {
    return httpClientConnection.remoteAddress();
  }

  public boolean isSSL() {
    return options.getSslMode() != null && options.getSslMode() != SslMode.DISABLE;
  }

  public Future<Void> close() {
    return httpClientConnection.close();
  }
}
