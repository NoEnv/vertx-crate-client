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
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.rxjava3.core.parsetools.JsonParser;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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

  public Observable<JsonObject> sendQuery(ContextInternal context, CrateQuery query) {
    return Observable.create(emitter -> {
      long requestStart = System.currentTimeMillis();
      httpClientConnection.request(HttpMethod.POST, "/_sql")
        .compose(r -> r
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .send(query.toJson().toBuffer())
        )
        .onSuccess(res -> {
          logger.debug("HTTP round trip: " + (System.currentTimeMillis() - requestStart) + "ms");

          if (res.statusCode() != 200) {
            res.body()
              .onSuccess(buf -> {
                JsonObject error = buf.toJsonObject();
                emitter.onError(new CrateException(res.statusCode(),
                  error.getInteger("error_code", -1),
                  error.getString("error", "HTTP " + res.statusCode())));
              })
              .onFailure(emitter::onError);
            return;
          }

          JsonParser parser = JsonParser.newParser();
          List<String> columns = new ArrayList<>();
          AtomicBoolean parserDone = new AtomicBoolean(false);
          long parseStart = System.currentTimeMillis();

          emitter.setCancellable(() -> {
            if (parserDone.compareAndSet(false, true)) {
              try { parser.end(); } catch (IllegalStateException ignored) {}
            }
          });

          parser.endHandler(v -> {
            logger.debug("Parse time: " + (System.currentTimeMillis() - parseStart) + "ms");
            parserDone.set(true);
            emitter.onComplete();
          });

          parser.exceptionHandler(emitter::onError);

          parser.handler(event -> {
            try {
              if (event.type() == JsonEventType.START_ARRAY) {
                if ("cols".equals(event.fieldName())) {
                  parser.objectValueMode();
                } else if ("rows".equals(event.fieldName())) {
                  parser.arrayValueMode();
                }
              }
              if (event.type() == JsonEventType.VALUE) {
                Object val = event.value();
                if (val instanceof String s) {
                  columns.add(s);
                } else if (val instanceof JsonArray row) {
                  JsonObject obj = new JsonObject();
                  int size = Math.min(row.size(), columns.size());
                  for (int i = 0; i < size; i++) {
                    obj.put(columns.get(i), row.getValue(i));
                  }
                  emitter.onNext(obj);
                }
              }
            } catch (Exception e) {
              emitter.onError(e);
            }
          });

          res.handler(parser::handle);
          res.exceptionHandler(emitter::onError);
          res.endHandler(v -> {
            if (parserDone.compareAndSet(false, true)) {
              parser.end();
            }
          });
          res.resume();
        })
        .onFailure(emitter::onError);
    });
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
    return sendQuery(context, new CrateQuery("SET statement_timeout = 10000"))
      .ignoreElements()
      .to(c -> Future.future(p -> c.subscribe(p::complete, p::fail)))
      .mapEmpty();
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
