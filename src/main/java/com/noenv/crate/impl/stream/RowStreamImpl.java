package com.noenv.crate.impl.stream;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateException;
import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.impl.connection.CrateEndpoint;
import com.noenv.crate.impl.connection.CrateFailoverPredicate;
import com.noenv.crate.impl.tracing.CrateQueryReporter;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.sqlclient.RowStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Streams CrateDB SQL response rows by parsing the HTTP JSON response
 * and emitting one {@link JsonObject} per row (column names as keys).
 */
public class RowStreamImpl implements RowStream<JsonObject> {

  private static final Logger logger = LoggerFactory.getLogger(RowStreamImpl.class);

  private final ContextInternal context;
  private final Handler<Throwable> onFailoverError;
  private Handler<JsonObject> handler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private boolean paused = false;
  private boolean closed = false;
  private final Promise<Void> closePromise = Promise.promise();

  private HttpClientResponse response;

  public RowStreamImpl(HttpClientConnection httpClientConnection, CrateConnectOptions options, ContextInternal context, CrateQuery query) {
    this(httpClientConnection, options, context, query, null);
  }

  public RowStreamImpl(HttpClientConnection httpClientConnection, CrateConnectOptions options, ContextInternal context, CrateQuery query, Handler<Throwable> onFailoverError) {
    this(httpClientConnection, options, context, query, onFailoverError, null, options.getSqlRequestUri(query));
  }

  public RowStreamImpl(HttpClientConnection httpClientConnection, CrateConnectOptions options, ContextInternal context, CrateQuery query, Handler<Throwable> onFailoverError,
                       io.vertx.core.spi.metrics.ClientMetrics<?, HttpClientRequest, HttpClientResponse> metrics, String requestUri) {
    this(httpClientConnection, options, context, query, onFailoverError, metrics, requestUri, null);
  }

  public RowStreamImpl(HttpClientConnection httpClientConnection, CrateConnectOptions options, ContextInternal context, CrateQuery query, Handler<Throwable> onFailoverError,
                       io.vertx.core.spi.metrics.ClientMetrics<?, HttpClientRequest, HttpClientResponse> metrics, String requestUri, CrateEndpoint endpoint) {
    this.context = context;
    this.onFailoverError = onFailoverError;
    long requestStart = System.currentTimeMillis();
    String uri = requestUri != null ? requestUri : options.getSqlRequestUri(query);
    io.vertx.core.spi.tracing.VertxTracer<?, ?> tracer = context.owner().tracer();
    boolean useReporter = (tracer != null || metrics != null) && endpoint != null;
    String address = endpoint != null ? String.format("%s:%d", endpoint.getHost(), endpoint.getPort()) : "";
    Buffer bodyBuf = query.toRequestBodyJson().toBuffer();
    httpClientConnection
      .request(new RequestOptions()
        .setMethod(HttpMethod.POST)
        .setURI(uri)
        .setHeaders(options.getRequestHeaders(query))
      )
      .compose(req -> {
        CrateQueryReporter reporter = useReporter
            ? new CrateQueryReporter(tracer, metrics, context, TracingPolicy.PROPAGATE, address, options.getUser(), "crate", null)
            : null;
        if (reporter != null) {
          reporter.before(uri, query.getStmt(), req);
        }
        return req
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .send(bodyBuf)
          .map(res -> new ResponseWithReporter(res, reporter))
          .onFailure(t -> {
            if (reporter != null) {
              reporter.after(null, t);
            }
          });
      })
      .onSuccess(responseWithReporter -> {
        HttpClientResponse res = responseWithReporter.response();
        CrateQueryReporter reporter = responseWithReporter.reporter();
        if (logger.isDebugEnabled()) {
          logger.debug("HTTP round trip: " + (System.currentTimeMillis() - requestStart) + "ms");
        }

        if (res.statusCode() != 200) {
          res.body()
            .onSuccess(buf -> {
              JsonObject body = buf.toJsonObject();
              JsonObject error = body.getJsonObject("error", new JsonObject());
              String errorTrace = body.getString("error_trace");
              String msg = error.getString("message", "HTTP " + res.statusCode());
              logger.error(String.format("Stream query HTTP error status=%d code=%d message=%s", res.statusCode(), error.getInteger("code", -1), msg));
              CrateException e = new CrateException(res.statusCode(),
                error.getInteger("code", -1),
                msg,
                errorTrace
              );
              handleException(e);
              if (reporter != null) {
                reporter.after(null, e);
              }
            })
            .onFailure(t -> {
              logger.warn("Failed to read error body from stream response", t);
              handleException(t);
              if (reporter != null) {
                reporter.after(null, t);
              }
            });
          return;
        }

        if (reporter != null) {
          reporter.setResponse(res, -1L);
          reporter.after(null, null);
        }
        this.response = res;
        res.pause();

        JsonParser parser = JsonParser.newParser();
        List<String> columns = new ArrayList<>();
        AtomicBoolean parserDone = new AtomicBoolean(false);
        long parseStart = System.currentTimeMillis();

        parser.endHandler(v -> {
          if (logger.isDebugEnabled()) {
            logger.debug("Parse time: " + (System.currentTimeMillis() - parseStart) + "ms");
          }
          parserDone.set(true);
          handleEnd();
        });

        parser.exceptionHandler(this::handleException);

        parser.handler(event -> {
          try {
            if (event.type() == JsonEventType.START_ARRAY) {
              if ("cols".equals(event.fieldName())) {
                parser.objectValueMode();
              } else if ("rows".equals(event.fieldName())) {
                parser.arrayValueMode();
              }
            } else if (event.type() == JsonEventType.VALUE) {
              Object val = event.value();
              if (val instanceof String) {
                columns.add((String) val);
              } else if (val instanceof JsonArray) {
                var row = (JsonArray) val;
                JsonObject obj = new JsonObject();
                int size = Math.min(row.size(), columns.size());
                for (int i = 0; i < size; i++) {
                  obj.put(columns.get(i), row.getValue(i));
                }
                handleRow(obj);
              }
            }
          } catch (Exception e) {
            handleException(e);
          }
        });

        res.handler(parser);
        res.exceptionHandler(this::handleException);
        res.endHandler(v -> {
          if (parserDone.compareAndSet(false, true)) {
            parser.end();
          }
        });
        if (!paused) {
          res.resume();
        }
      })
      .onFailure(this::handleException);
  }

  private static final class ResponseWithReporter {
    private final HttpClientResponse response;
    private final CrateQueryReporter reporter;

    ResponseWithReporter(HttpClientResponse response, CrateQueryReporter reporter) {
      this.response = response;
      this.reporter = reporter;
    }

    HttpClientResponse response() {
      return response;
    }

    CrateQueryReporter reporter() {
      return reporter;
    }
  }

  private void handleRow(JsonObject row) {
    if (!closed && handler != null) {
      handler.handle(row);
    }
  }

  private void handleException(Throwable t) {
    if (!closed) {
      if (CrateFailoverPredicate.isFailoverError(t)) {
        logger.warn("Stream error (failover eligible): " + t.getMessage());
        if (onFailoverError != null) {
          onFailoverError.handle(t);
        }
      } else {
        logger.error("Stream error", t);
      }
      if (exceptionHandler != null) {
        exceptionHandler.handle(t);
      }
    }
    context.failedFuture(t.getMessage());
    closePromise.tryFail(t);
  }

  private void handleEnd() {
    if (!closed && endHandler != null) {
      endHandler.handle(null);
    }
    closePromise.tryComplete();
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
  public RowStream<JsonObject> pause() {
    paused = true;
    if (response != null) {
      response.pause();
    }
    return this;
  }

  @Override
  public RowStream<JsonObject> resume() {
    paused = false;
    if (response != null) {
      response.resume();
    }
    return this;
  }

  @Override
  public RowStream<JsonObject> endHandler(Handler<Void> handler) {
    this.endHandler = handler;
    return this;
  }

  @Override
  public RowStream<JsonObject> fetch(long l) {
    return this;
  }

  @Override
  public Future<Void> close() {
    if (logger.isDebugEnabled() && !closed) {
      logger.debug("Row stream closed");
    }
    closed = true;
    return closePromise.future();
  }
}
