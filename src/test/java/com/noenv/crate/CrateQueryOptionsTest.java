/*
 * Copyright (C) 2026 Christoph Spörk
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
package com.noenv.crate;

import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.codec.CrateQueryOptions;
import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link CrateQueryOptions} (default schema, column types, error trace)
 * against the CrateDB test container.
 */
@ExtendWith(VertxExtension.class)
class CrateQueryOptionsTest extends CrateContainerTest {

  @Test
  void includeColumnTypes_responseContainsColTypes(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())));

    CrateQuery query = new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 2",
      new CrateQueryOptions().setIncludeColumnTypes(true));

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(query))
      .onSuccess(msg -> ctx.verify(() -> {
        assertNotNull(msg.getCols());
        assertNotNull(msg.getColTypes(), "response should contain col_types when includeColumnTypes is true");
        assertEquals(msg.getCols().size(), msg.getColTypes().size(),
          "col_types length should match cols length");
        assertNotNull(msg.getRows());
        assertEquals(2, msg.getRows().size());
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  /**
   * Unit test: per-query default schema must be reflected in request headers.
   */
  @Test
  void defaultSchema_perQueryOptionsSetHeader() {
    CrateConnectOptions opts = new CrateConnectOptions();
    CrateQuery q = new CrateQuery("SELECT 1", new CrateQueryOptions().setDefaultSchema("test_schema"));
    MultiMap headers = opts.getRequestHeaders(q);
    assertEquals("test_schema", headers.get("Default-Schema"),
      "Default-Schema header must be set from query's CrateQueryOptions");
  }

  /** Fall-through: connection defaultSchema is used when query has no CrateQueryOptions. */
  @Test
  void defaultSchema_connectionOptionUsedWhenQueryHasNoOptions() {
    CrateConnectOptions opts = new CrateConnectOptions().setDefaultSchema("test_schema");
    CrateQuery q = new CrateQuery("SELECT 1");
    MultiMap headers = opts.getRequestHeaders(q);
    assertEquals("test_schema", headers.get("Default-Schema"));
  }

  /** Fall-through: connection includeColumnTypes is used when query has no CrateQueryOptions. */
  @Test
  void includeColumnTypes_connectionOptionUsedWhenQueryHasNoOptions(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setIncludeColumnTypes(true);
    CrateQuery query = new CrateQuery("SELECT id FROM world LIMIT 1");
    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(query))
      .onSuccess(msg -> ctx.verify(() -> {
        assertNotNull(msg.getColTypes(), "col_types when set on connection and query has no options");
        assertEquals(msg.getCols().size(), msg.getColTypes().size());
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  /** Default-Schema header (connection-level here) so unqualified "opt_test" resolves to test_schema.opt_test. */
  @Test
  void defaultSchema_queryUsesSchema(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions connectOptions = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setDefaultSchema("test_schema");

    CrateQuery q = new CrateQuery("SELECT id FROM opt_test LIMIT 1");

    CrateConnection.connect(vertx, connectOptions)
      .compose(conn -> conn.query(q))
      .onSuccess(msg -> ctx.verify(() -> {
        assertNull(msg.getError(), "query should succeed when Default-Schema header is applied");
        assertNotNull(msg.getRows());
        assertEquals(1, msg.getRows().size());
        assertEquals(42, ((io.vertx.core.json.JsonArray) msg.getRows().getValue(0)).getInteger(0));
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  /** Fall-through: connection defaultSchema is used when query has no options; integration test. */
  @Test
  void defaultSchema_connectionOptionUsedWhenQueryHasNoOptions_integration(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions connectOptions = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setDefaultSchema("test_schema");
    CrateQuery q = new CrateQuery("SELECT id FROM opt_test LIMIT 1");
    CrateConnection.connect(vertx, connectOptions)
      .compose(conn -> conn.query(q))
      .onSuccess(msg -> ctx.verify(() -> {
        assertNull(msg.getError());
        assertEquals(1, msg.getRows().size());
        assertEquals(42, ((io.vertx.core.json.JsonArray) msg.getRows().getValue(0)).getInteger(0));
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  /** Client sends includeErrorTrace (connection option); error response exposes error_trace on CrateException. */
  @Test
  void errorTrace_clientSendsOptionAndExceptionContainsTrace(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setIncludeErrorTrace(true);
    // Use invalid SQL so CrateDB returns 4xx with a body (relation-unknown can be 404 with or without body)
    CrateQuery query = new CrateQuery("SELECT FROM broken_syntax");
    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(query))
      .onSuccess(msg -> ctx.failNow(new AssertionError("expected query to fail")))
      .onFailure(t -> ctx.verify(() -> {
        CrateException e = t instanceof CrateException ? (CrateException) t
          : (t.getCause() instanceof CrateException ? (CrateException) t.getCause() : null);
        assertNotNull(e, "expected CrateException, got " + t);
        assertNotNull(e.getErrorTrace(),
          "CrateException should carry error_trace when includeErrorTrace was set on connection");
        assertFalse(e.getErrorTrace().isEmpty());
        ctx.completeNow();
      }));
  }

}
