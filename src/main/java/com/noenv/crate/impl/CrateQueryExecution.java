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

import com.noenv.crate.CrateException;
import com.noenv.crate.codec.CrateMessage;
import com.noenv.crate.codec.CrateQuery;
import io.vertx.core.Future;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Query;

import java.util.stream.Collector;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Query}{@code <}{@link RowSet}{@code <}{@link Row}{@code >}{@code >} for the CrateDB SQL client.
 */
public class CrateQueryExecution implements Query<RowSet<Row>> {

  private final CrateConnectionImpl connection;
  private final String sql;

  public CrateQueryExecution(CrateConnectionImpl connection, String sql) {
    this.connection = connection;
    this.sql = sql;
  }

  @Override
  public Future<RowSet<Row>> execute() {
    return connection.sendRequest(connection.conn, new CrateQuery(sql), connection.factory.options.getFailoverMaxRetries())
      .compose(this::messageToRowSet);
  }

  @Override
  public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
    return new Query<>() {
      @Override
      public Future<RowSet<U>> execute() {
        return CrateQueryExecution.this.execute()
          .map(rowSet -> new CrateMappedRowSet<>(rowSet.columnDescriptors(),
            rowSet.stream().map(mapper).collect(Collectors.toList())));
      }

      @Override
      public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
        return CrateQueryExecution.this.collecting(collector);
      }

      @Override
      public <V> Query<RowSet<V>> mapping(Function<Row, V> m) {
        return CrateQueryExecution.this.mapping(m);
      }
    };
  }

  @Override
  public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
    return new Query<>() {
      @Override
      public Future<SqlResult<R>> execute() {
        return CrateQueryExecution.this.execute()
          .map(rowSet -> {
            R value = rowSet.stream().collect(collector);
            return new CrateSqlResult<>(value);
          });
      }

      @Override
      public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
        return CrateQueryExecution.this.mapping(mapper);
      }

      @Override
      public <S> Query<SqlResult<S>> collecting(Collector<Row, ?, S> coll) {
        return CrateQueryExecution.this.collecting(coll);
      }
    };
  }

  private Future<RowSet<Row>> messageToRowSet(CrateMessage msg) {
    ContextInternal ctx = connection.context;
    JsonObject error = msg.getError();
    if (error != null) {
      CrateException e = new CrateException(
        200,
        error.getInteger("code", -1),
        error.getString("message", "CrateDB error"));
      return ctx.failedFuture(e);
    }
    return ctx.succeededFuture(CrateRowSet.fromMessage(msg));
  }
}
