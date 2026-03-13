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
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.sqlclient.RowStream;

public class CrateHttpConnection {

  private static final Logger logger = LoggerFactory.getLogger(CrateHttpConnection.class);

  private final ClientMetrics<?, ?, ?> metrics; // TODO: report requests to metrics
  public CrateDatabaseMetadata dbMetaData;
  private final CrateConnectOptions options;
  private final HttpClientConnection httpClientConnection;
  private final CrateEndpoint endpoint;

  public CrateHttpConnection(HttpClientConnection httpClientConnection,
                             ClientMetrics<?, ?, ?> metrics,
                             CrateConnectOptions options,
                             CrateEndpoint endpoint) {
    this.httpClientConnection = httpClientConnection;
    this.options = options;
    this.metrics = metrics;
    this.endpoint = endpoint;
  }


  public CrateEndpoint getEndpoint() {
    return endpoint;
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
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Sending request to %s:%d URI=%s", endpoint.getHost(), endpoint.getPort(), options.getSqlRequestUri(query)));
    }
    return httpClientConnection.request(new RequestOptions()
        .setMethod(HttpMethod.POST)
        .setURI(options.getSqlRequestUri(query))
        .setHeaders(options.getRequestHeaders(query))
      )
      .compose(r -> r.send(query.toRequestBodyJson().toBuffer()))
      .compose(res -> {
        if (res.statusCode() != 200) {
          return res.body()
            .compose(buf -> {
              JsonObject body = buf.toJsonObject();
              JsonObject error = body.getJsonObject("error", new JsonObject());
              String errorTrace = body.getString("error_trace");
              String msg = error.getString("message", "HTTP " + res.statusCode());
              logger.error(String.format("CrateDB error from %s:%d status=%d code=%d message=%s",
                endpoint.getHost(), endpoint.getPort(), res.statusCode(),
                error.getInteger("code", -1), msg));
              CrateException e = new CrateException(
                res.statusCode(),
                error.getInteger("code", -1),
                msg,
                errorTrace
              );
              return context.<CrateMessage>failedFuture(e);
            })
            .recover(t -> {
              logger.error(String.format("Failed to read error body from %s:%d status=%d", endpoint.getHost(), endpoint.getPort(), res.statusCode()), t);
              return context.failedFuture(new RuntimeException("HTTP " + res.statusCode(), t));
            });
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
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Initializing session on %s:%d statement_timeout=%dms", endpoint.getHost(), endpoint.getPort(), sessionOptions.getStatementTimeout()));
    }
    return sendRequest(context, new CrateQuery(String.format("SET statement_timeout = %d", sessionOptions.getStatementTimeout())))
      .<Void>map(msg -> null)
      .onFailure(err -> logger.warn(String.format("Session init failed on %s:%d", endpoint.getHost(), endpoint.getPort()), err));
  }

  public Future<CrateDatabaseMetadata> getMetadata(ContextInternal context) {
    return httpClientConnection.request(new RequestOptions()
        .setMethod(HttpMethod.GET).setURI("/")
        .setHeaders(options.getDefaultHeaders())
      )
      .compose(HttpClientRequest::send)
      .compose(res -> {
        if (res.statusCode() != 200) {
          logger.error(String.format("Failed to get metadata from %s:%d status=%d", endpoint.getHost(), endpoint.getPort(), res.statusCode()));
          return context.failedFuture(new RuntimeException("Unexpected response status code: " + res.statusCode()));
        }
        return res.body()
          .map(Buffer::toJsonObject)
          .map(json -> {
            String version = json.getJsonObject("version", new JsonObject()).getString("number", "0.0.0");
            if (logger.isDebugEnabled()) {
              logger.debug(String.format("Metadata from %s:%d version=%s", endpoint.getHost(), endpoint.getPort(), version));
            }
            return new CrateDatabaseMetadata(version);
          });
      });
  }

  public boolean isSSL() {
    return options.getSslMode() != null && options.getSslMode() != SslMode.DISABLE;
  }

  public Future<Void> close() {
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Closing connection to %s:%d", endpoint.getHost(), endpoint.getPort()));
    }
    return httpClientConnection.close();
  }
}
