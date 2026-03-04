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
import com.noenv.crate.CrateException;
import com.noenv.crate.SslMode;
import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.sqlclient.RowStream;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class CrateHttpConnection {

  public CrateDatabaseMetadata dbMetaData;
  private final CrateConnectOptions options;
  private final HttpClientConnection httpClientConnection;

  private static final Logger logger = LoggerFactory.getLogger(CrateHttpConnection.class);

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

  public RowStream<JsonObject> sendQuery(ContextInternal context, CrateQuery query) {
    return new RowStream<>() {
      private Handler<JsonObject> handler;
      private Handler<Throwable> exceptionHandler;
      private Handler<Void> endHandler;
      private boolean paused = false;
      private boolean closed = false;
      private final Promise<Void> closePromise = Promise.promise();

      {
        long requestStart = System.currentTimeMillis();
        httpClientConnection.request(HttpMethod.POST, "/_sql")
          .compose(r -> r
            .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            .send(query.toJson().toBuffer())
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
                  handleException(new CrateException(res.statusCode(),
                    error.getInteger("code", -1),
                    error.getString("message", "HTTP " + res.statusCode())));
                })
                .onFailure(this::handleException);
              return;
            }

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
                  if (val instanceof String s) {
                    columns.add(s);
                  } else if (val instanceof JsonArray row) {
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
            res.endHandler(_ -> {
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
        if (!closed && exceptionHandler != null) {
          exceptionHandler.handle(t);
        }
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
        return this;
      }

      @Override
      public RowStream<JsonObject> resume() {
        paused = false;
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
    };
  }

  public Future<CrateMessage> sendRequest(ContextInternal context, CrateQuery query) {
    return httpClientConnection.request(HttpMethod.POST, "/_sql")
      .compose(r -> r
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .send(query.toJson().toBuffer())
      )
      .compose(res -> {
        if (res.statusCode() != 200) {
          return context.failedFuture(new RuntimeException("Unexpected response status code: " + res.statusCode()));
        }
        return res.body()
          .map(Buffer::toJsonObject)
          .map(CrateMessage::new);
      });
  }

  public Future<Void> initSession(ContextInternal context) {
    return sendRequest(context, new CrateQuery("SET statement_timeout = 10000"))
      .map(_ -> null);
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
          .map(json -> new CrateDatabaseMetadata(
            json.getJsonObject("version", new JsonObject()).getString("number", "0.0.0")));
      });
  }

  public boolean isSSL() {
    return options.getSslMode() != null && options.getSslMode() != SslMode.DISABLE;
  }

  public Future<Void> close() {
    return httpClientConnection.close();
  }
}
