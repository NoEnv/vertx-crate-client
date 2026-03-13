package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateException;
import com.noenv.crate.codec.CrateQuery;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.sqlclient.RowStream;

import java.util.ArrayList;
import java.util.List;

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

  /** Response reference so we can pause/resume the underlying HTTP stream. */
  private HttpClientResponse response;

  /**
   * Creates a row stream that executes the given query on the connection
   * and parses the CrateDB JSON response, emitting one {@link JsonObject} per row.
   *
   * @param httpClientConnection the HTTP connection to CrateDB
   * @param query                 the query to execute
   */
  public RowStreamImpl(HttpClientConnection httpClientConnection, CrateConnectOptions options, ContextInternal context, CrateQuery query) {
    this(httpClientConnection, options, context, query, null);
  }

  /**
   * Same as above with an optional handler invoked when a failover error occurs (endpoint is not marked unhealthy here; caller may do so).
   */
  public RowStreamImpl(HttpClientConnection httpClientConnection, CrateConnectOptions options, ContextInternal context, CrateQuery query, Handler<Throwable> onFailoverError) {
    this.context = context;
    this.onFailoverError = onFailoverError;
    long requestStart = System.currentTimeMillis();
    httpClientConnection
      .request(new RequestOptions()
        .setMethod(HttpMethod.POST)
        .setURI(options.getSqlRequestUri(query))
        .setHeaders(options.getRequestHeaders(query))
      )
      .compose(req -> req
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .send(query.toRequestBodyJson().toBuffer())
      )
      .onSuccess(res -> {
        if (logger.isDebugEnabled()) {
          logger.debug("HTTP round trip: " + (System.currentTimeMillis() - requestStart) + "ms");
        }

        if (res.statusCode() != 200) {
          res.body()
            .onSuccess(buf -> {
              JsonObject body = buf.toJsonObject();
              JsonObject error = body.getJsonObject("error", new JsonObject());
              String errorTrace = body.getString("error_trace");
              handleException(new CrateException(res.statusCode(),
                error.getInteger("code", -1),
                error.getString("message", "HTTP " + res.statusCode()),
                errorTrace
              ));
            })
            .onFailure(this::handleException);
          return;
        }

        this.response = res;
        res.pause();

        JsonParser parser = JsonParser.newParser();
        List<String> columns = new ArrayList<>();
        boolean[] parserDone = {false};
        long parseStart = System.currentTimeMillis();

        parser.endHandler(v -> {
          if (logger.isDebugEnabled()) {
            logger.debug("Parse time: " + (System.currentTimeMillis() - parseStart) + "ms");
          }
          parserDone[0] = true;
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
          if (!parserDone[0]) {
            parserDone[0] = true;
            parser.end();
          }
        });
        if (!paused) {
          res.resume();
        }
      })
      .onFailure(this::handleException);
  }

  private void handleRow(JsonObject row) {
    if (!closed && handler != null) {
      handler.handle(row);
    }
  }

  private void handleException(Throwable t) {
    if (!closed && CrateFailoverPredicate.isFailoverError(t) && onFailoverError != null) {
      onFailoverError.handle(t);
    }
    if (!closed && exceptionHandler != null) {
      exceptionHandler.handle(t);
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
    closed = true;
    return closePromise.future();
  }
}
