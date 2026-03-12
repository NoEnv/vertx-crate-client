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
import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.RowStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RowStreamImpl} using the world table from create-crate.sql
 * (id INTEGER, randomnumber INTEGER, 10000 rows).
 */
@ExtendWith(VertxExtension.class)
class RowStreamImplTest extends CrateContainerTest {

  private CrateHttpConnection connection;

  @BeforeEach
  void setUp(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())));

    CrateConnectionImpl.connect(context, options)
      .onSuccess(conn -> {
        connection = ((CrateConnectionImpl) conn).conn;
        ctx.completeNow();
      })
      .onFailure(ctx::failNow);
  }

  @Test
  void stream_worldTable_emitsRowsWithIdAndRandomnumber(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    AtomicInteger expectedId = new AtomicInteger(1);
    AtomicInteger rowCount = new AtomicInteger(0);

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 10"));
    stream.handler(row -> ctx.verify(() -> {
      int id = expectedId.getAndIncrement();
      assertEquals(id, row.getInteger("id"), "Row should have id " + id);
      assertTrue(row.containsKey("randomnumber"));
      assertNotNull(row.getValue("randomnumber"));
      rowCount.incrementAndGet();
    }));
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertEquals(10, rowCount.get(), "Should have received exactly 10 rows");
      ctx.completeNow();
    }));
  }

  @Test
  void stream_worldTableLimit1000_receivesAllRowsInOrder(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    AtomicInteger expectedId = new AtomicInteger(1);
    AtomicInteger rowCount = new AtomicInteger(0);
    int limit = 1000;

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT " + limit));
    stream.handler(row -> ctx.verify(() -> {
      int id = expectedId.getAndIncrement();
      assertEquals(id, row.getInteger("id"), "Row should have id " + id);
      assertTrue(row.containsKey("randomnumber"));
      assertNotNull(row.getValue("randomnumber"));
      rowCount.incrementAndGet();
    }));
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertEquals(limit, rowCount.get(), "Should have received exactly " + limit + " rows");
      ctx.completeNow();
    }));
  }

  @Test
  void stream_worldTableFull_emitsAll10000Rows(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    AtomicInteger expectedId = new AtomicInteger(1);
    AtomicInteger rowCount = new AtomicInteger(0);

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id"));
    stream.handler(row -> ctx.verify(() -> {
      int id = expectedId.getAndIncrement();
      assertEquals(id, row.getInteger("id"), "Row should have id " + id);
      assertTrue(row.containsKey("randomnumber"));
      assertNotNull(row.getValue("randomnumber"));
      rowCount.incrementAndGet();
    }));
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertEquals(10_000, rowCount.get(), "Should have received exactly 10000 rows");
      ctx.completeNow();
    }));
  }

  @Test
  void stream_emptyResult_endHandlerCalledWithoutRows(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world WHERE id = -1"));
    stream.handler(rows::add);
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertTrue(rows.isEmpty());
      ctx.completeNow();
    }));
  }

  @Test
  void stream_singleRow_emitsOneRow(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world WHERE id = 42"));
    stream.handler(rows::add);
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertEquals(1, rows.size());
      assertEquals(42, rows.get(0).getInteger("id"));
      assertTrue(rows.get(0).getInteger("randomnumber") >= 1);
      assertTrue(rows.get(0).getInteger("randomnumber") <= 10000);
      ctx.completeNow();
    }));
  }

  @Test
  void stream_handlerInvokedPerRow_assertsPreciseIds(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    AtomicInteger expectedId = new AtomicInteger(1);
    AtomicInteger rowCount = new AtomicInteger(0);

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 100"));
    stream.handler(row -> ctx.verify(() -> {
      int id = expectedId.getAndIncrement();
      assertEquals(id, row.getInteger("id"), "Row should have id " + id);
      rowCount.incrementAndGet();
    }));
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertEquals(100, rowCount.get(), "Should have received exactly 100 rows");
      ctx.completeNow();
    }));
  }

  @Test
  void stream_close_returnsFutureThatCompletes(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 5"));
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> stream.close().onComplete(ar -> ctx.verify(() -> {
      assertTrue(ar.succeeded());
      ctx.completeNow();
    })));
  }

  @Test
  void stream_selectStar_worldTable_hasIdAndRandomnumber(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    RowStream<JsonObject> stream = connection.sendQuery(context,
      new CrateQuery("SELECT * FROM world ORDER BY id LIMIT 1"));
    stream.handler(rows::add);
    stream.exceptionHandler(ctx::failNow);
    stream.endHandler(v -> ctx.verify(() -> {
      assertEquals(1, rows.size());
      JsonObject row = rows.get(0);
      assertEquals(1, row.getInteger("id"));
      assertTrue(row.containsKey("randomnumber"));
      assertEquals(2, row.size(), "world table has only id and randomnumber");
      ctx.completeNow();
    }));
  }
}
