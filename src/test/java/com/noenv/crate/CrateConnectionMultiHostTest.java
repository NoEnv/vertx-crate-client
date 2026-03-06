package com.noenv.crate;

import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class CrateConnectionMultiHostTest extends CrateContainerTest {

  private CrateConnection connection;

  @AfterEach
  void tearDown() {
    if (connection != null) {
      connection.close();
    }
  }

  private CrateConnection buildPool(Vertx vertx, CrateConnectOptions options) {
    return CrateBuilder.pool()
      .connectingTo(options)
      .using(vertx)
      .build();
  }

  private Future<List<JsonObject>> collectStream(CrateConnection conn, CrateQuery query) {
    List<JsonObject> rows = new ArrayList<>();
    return Future.future(promise ->
      conn.streamQuery(query)
        .handler(rows::add)
        .exceptionHandler(promise::fail)
        .endHandler(v -> promise.complete(rows))
    );
  }

  @Test
  void multiHost_roundRobin_distributesRequestsAcrossBothEntries(Vertx vertx, VertxTestContext ctx) {
    String host = cratedb.getHost();
    int port = cratedb.getMappedPort(4200);

    connection = buildPool(vertx, new CrateConnectOptions()
      .setHost(host + "," + host)
      .setPort(port));

    List<Future<List<JsonObject>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(collectStream(connection, new CrateQuery("SELECT id FROM world ORDER BY id LIMIT 1")));
    }

    Future.all(futures).onComplete(ar -> ctx.verify(() -> {
      assertTrue(ar.succeeded(), "All queries should succeed across multi-host round-robin");
      for (Future<List<JsonObject>> f : futures) {
        List<JsonObject> rows = f.result();
        assertFalse(rows.isEmpty());
        assertEquals(1, rows.get(0).getInteger("id"));
      }
      ctx.completeNow();
    }));
  }

  @Test
  void multiHost_singleEntry_behavesIdenticallyToNonMultiHost(Vertx vertx, VertxTestContext ctx) {
    String host = cratedb.getHost();
    int port = cratedb.getMappedPort(4200);

    connection = buildPool(vertx, new CrateConnectOptions()
      .setHost(host)
      .setPort(port));

    collectStream(connection, new CrateQuery("SELECT id FROM world ORDER BY id LIMIT 3"))
      .onSuccess(rows -> ctx.verify(() -> {
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).getInteger("id"));
        assertEquals(2, rows.get(1).getInteger("id"));
        assertEquals(3, rows.get(2).getInteger("id"));
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  @Test
  void multiHost_withExplicitPortSyntax_parsedCorrectly(Vertx vertx, VertxTestContext ctx) {
    String host = cratedb.getHost();
    int port = cratedb.getMappedPort(4200);

    connection = buildPool(vertx, new CrateConnectOptions()
      .setHost(host + ":" + port + "," + host + ":" + port)
      .setPort(port));

    collectStream(connection, new CrateQuery("SELECT id FROM world ORDER BY id LIMIT 1"))
      .onSuccess(rows -> ctx.verify(() -> {
        assertFalse(rows.isEmpty());
        assertEquals(1, rows.get(0).getInteger("id"));
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }
}
