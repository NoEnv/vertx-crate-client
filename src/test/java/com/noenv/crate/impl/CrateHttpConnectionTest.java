package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateException;
import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class CrateHttpConnectionTest extends CrateContainerTest {

  private CrateHttpConnection connection;

  @BeforeEach
  void setUp(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions()
      .setHost(cratedb.getHost())
      .setPort(cratedb.getMappedPort(4200));

    CrateConnectionImpl.connect(context, options)
      .onSuccess(conn -> {
        connection = ((CrateConnectionImpl) conn).conn;
        ctx.completeNow();
      })
      .onFailure(ctx::failNow);
  }

  @Test
  void query_emitsRowsAsJsonObjects(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    connection.sendQuery(context, new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 2"))
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertEquals(2, rows.size());
          assertEquals(1, rows.get(0).getInteger("id"));
          assertNotNull(rows.get(0).getValue("randomnumber"));
          assertEquals(2, rows.get(1).getInteger("id"));
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_emptyResultSet_completesWithNoItems(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    connection.sendQuery(context, new CrateQuery("SELECT id FROM world WHERE id = -1"))
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertTrue(rows.isEmpty());
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_nullValuesInRow_handledGracefully(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELECT id, test_int_2, test_int_4 FROM basicdatatype WHERE id = 3"))
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          assertEquals(3, row.getInteger("id"));
          assertNull(row.getValue("test_int_2"));
          assertNull(row.getValue("test_int_4"));
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }

  @Test
  void query_withArgs_filtersCorrectly(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    CrateQuery query = new CrateQuery("SELECT id, message FROM fortune WHERE id = ?")
      .setArgs(new JsonArray().add(1));

    connection.sendQuery(context, query)
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          assertEquals(1, row.getInteger("id"));
          assertEquals("fortune: No such file or directory", row.getString("message"));
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }

  @Test
  void query_multipleArgs_filtersWithAny(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    CrateQuery query = new CrateQuery("SELECT id, message FROM fortune WHERE id = ANY(?) ORDER BY id")
      .setArgs(new JsonArray().add(new JsonArray().add(1).add(2).add(3)));

    List<JsonObject> rows = new ArrayList<>();
    connection.sendQuery(context, query)
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertEquals(3, rows.size());
          assertEquals(1, rows.get(0).getInteger("id"));
          assertEquals(2, rows.get(1).getInteger("id"));
          assertEquals(3, rows.get(2).getInteger("id"));
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_invalidSql_propagatesAsCrateException(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELEC broken sql"))
      .subscribe(
        row -> ctx.failNow(new AssertionError("Expected no rows but got: " + row)),
        err -> ctx.verify(() -> {
          assertInstanceOf(CrateException.class, err);
          assertEquals(400, ((CrateException) err).getHttpStatus());
          ctx.completeNow();
        }),
        () -> ctx.failNow(new AssertionError("Expected error but stream completed normally"))
      );
  }

  @Test
  void query_unknownTable_propagatesAsCrateException(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELECT * FROM nonexistent_table"))
      .subscribe(
        row -> ctx.failNow(new AssertionError("Expected no rows but got: " + row)),
        err -> ctx.verify(() -> {
          assertInstanceOf(CrateException.class, err);
          assertEquals(404, ((CrateException) err).getHttpStatus());
          ctx.completeNow();
        }),
        () -> ctx.failNow(new AssertionError("Expected error but stream completed normally"))
      );
  }

  @Test
  void query_manyRows_assemblesAllCorrectly(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    connection.sendQuery(context, new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 100"))
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertEquals(100, rows.size());
          for (int i = 0; i < 100; i++) {
            assertEquals(i + 1, rows.get(i).getInteger("id"));
          }
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_numericTypes_parsedCorrectly(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery(
        "SELECT \"Short\", \"Integer\", \"Long\", \"Float\", \"Double\", \"Boolean\" FROM numericdatatype WHERE id = 1"))
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          assertNotNull(row.getValue("Short"));
          assertNotNull(row.getValue("Integer"));
          assertNotNull(row.getValue("Long"));
          assertNotNull(row.getValue("Float"));
          assertNotNull(row.getValue("Double"));
          assertTrue(row.getBoolean("Boolean"));
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }

  @Test
  void query_objectType_parsedAsJsonObject(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery(
        "SELECT \"ObjectDynamic\" FROM specialdatatype WHERE id = 1"))
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          assertNotNull(row.getJsonObject("ObjectDynamic"));
          assertEquals("Bob", row.getJsonObject("ObjectDynamic").getString("name"));
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }

  @Test
  void query_arrayType_parsedAsJsonArray(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery(
        "SELECT \"Tags\" FROM specialdatatype WHERE id = 1"))
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          JsonArray tags = row.getJsonArray("Tags");
          assertNotNull(tags);
          assertEquals("foo", tags.getString(0));
          assertEquals("bar", tags.getString(1));
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }
}
