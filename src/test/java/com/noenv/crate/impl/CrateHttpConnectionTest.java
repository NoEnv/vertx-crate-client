package com.noenv.crate.impl;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateException;
import com.noenv.crate.codec.CrateQuery;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class CrateHttpConnectionTest {

  @RegisterExtension
  static WireMockExtension wm = WireMockExtension.newInstance()
    .options(wireMockConfig().dynamicPort())
    .build();

  private static final String EMPTY_RESPONSE = """
    {"cols":[],"rows":[],"rowcount":0,"duration":0.1}
    """;

  private CrateHttpConnection connection;

  @BeforeEach
  void setUp(Vertx vertx, VertxTestContext ctx) {
    // initSession fires a SET statement on connect — stub it
    wm.stubFor(post(urlEqualTo("/_sql")).willReturn(okJson(EMPTY_RESPONSE)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions()
      .setHost("localhost")
      .setPort(wm.getPort());

    CrateConnectionImpl.connect(context, options)
      .onSuccess(conn -> {
        connection = ((CrateConnectionImpl) conn).conn;
        ctx.completeNow();
      })
      .onFailure(ctx::failNow);
  }

  // ---------------------------------------------------------------------------
  // Happy path
  // ---------------------------------------------------------------------------

  @Test
  void query_emitsRowsAsJsonObjects(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(okJson("""
        {
          "cols": ["id", "name", "score"],
          "rows": [
            [1, "alice", 9.5],
            [2, "bob",   7.0]
          ],
          "rowcount": 2,
          "duration": 1.23
        }
        """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    connection.sendQuery(context, new CrateQuery("SELECT id, name, score FROM users"))
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertThat(rows).hasSize(2);
          assertThat(rows.get(0).getInteger("id")).isEqualTo(1);
          assertThat(rows.get(0).getString("name")).isEqualTo("alice");
          assertThat(rows.get(0).getDouble("score")).isEqualTo(9.5);
          assertThat(rows.get(1).getInteger("id")).isEqualTo(2);
          assertThat(rows.get(1).getString("name")).isEqualTo("bob");
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_emptyResultSet_completesWithNoItems(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(okJson("""
        {"cols":["id"],"rows":[],"rowcount":0,"duration":0.5}
        """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    connection.sendQuery(context, new CrateQuery("SELECT id FROM users WHERE 1=0"))
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertThat(rows).isEmpty();
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_nullValuesInRow_handledGracefully(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(okJson("""
        {
          "cols": ["id", "name"],
          "rows": [[1, null]],
          "rowcount": 1,
          "duration": 0.5
        }
        """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELECT id, name FROM users"))
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          assertThat(row.getInteger("id")).isEqualTo(1);
          assertThat(row.getValue("name")).isNull();
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }

  @Test
  void query_sendsCorrectRequestBody(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql")).willReturn(okJson(EMPTY_RESPONSE)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELECT 1"))
      .subscribe(
        row -> {},
        ctx::failNow,
        () -> ctx.verify(() -> {
          wm.verify(postRequestedFor(urlEqualTo("/_sql"))
            .withHeader("Content-Type", containing("application/json"))
            .withRequestBody(matchingJsonPath("$.stmt", equalTo("SELECT 1"))));
          ctx.completeNow();
        })
      );
  }

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  @Test
  void query_400Response_propagatesAsCrateException(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(badRequest().withHeader("Content-Type", "application/json").withBody("""
        {
          "error": "SQLParseException: no viable alternative at input 'SELEC'",
          "error_code": 4000
        }
        """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELEC broken"))
      .subscribe(
        row -> ctx.failNow(new AssertionError("Expected no rows but got: " + row)),
        err -> ctx.verify(() -> {
          assertThat(err).isInstanceOf(CrateException.class);
          assertThat(((CrateException) err).getHttpStatus()).isEqualTo(400);
          assertThat(((CrateException) err).getErrorCode()).isEqualTo(4000);
          ctx.completeNow();
        }),
        () -> ctx.failNow(new AssertionError("Expected error but stream completed normally"))
      );
  }

  @Test
  void query_500Response_propagatesAsCrateException(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(serverError().withHeader("Content-Type", "application/json").withBody("""
        {"error": "UnhandledException", "error_code": 5000}
        """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    connection.sendQuery(context, new CrateQuery("SELECT 1"))
      .subscribe(
        row -> ctx.failNow(new AssertionError("Expected no rows but got: " + row)),
        err -> ctx.verify(() -> {
          assertThat(err).isInstanceOf(CrateException.class);
          assertThat(((CrateException) err).getHttpStatus()).isEqualTo(500);
          ctx.completeNow();
        }),
        () -> ctx.failNow(new AssertionError("Expected error but stream completed normally"))
      );
  }

  // ---------------------------------------------------------------------------
  // Large responses
  // ---------------------------------------------------------------------------

  @Test
  void query_manyRows_assemblesAllCorrectly(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(okJson("""
        {
          "cols": ["n"],
          "rows": [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]],
          "rowcount": 10,
          "duration": 2.0
        }
        """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    List<JsonObject> rows = new ArrayList<>();

    connection.sendQuery(context, new CrateQuery("SELECT unnest(ARRAY[1,2,3,4,5,6,7,8,9,10]) AS n"))
      .subscribe(
        rows::add,
        ctx::failNow,
        () -> ctx.verify(() -> {
          assertThat(rows).hasSize(10);
          for (int i = 0; i < 10; i++) {
            assertThat(rows.get(i).getInteger("n")).isEqualTo(i + 1);
          }
          ctx.completeNow();
        })
      );
  }

  @Test
  void query_withArgs_sendsArgsInRequestBody(Vertx vertx, VertxTestContext ctx) {
    wm.stubFor(post(urlEqualTo("/_sql"))
      .willReturn(okJson("""
      {
        "cols": ["id", "name"],
        "rows": [[1, "alice"]],
        "rowcount": 1,
        "duration": 0.5
      }
      """)));

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    CrateQuery query = new CrateQuery("SELECT id, name FROM users WHERE id = ?")
      .setArgs(new io.vertx.core.json.JsonArray().add(1));

    connection.sendQuery(context, query)
      .firstOrError()
      .subscribe(
        row -> ctx.verify(() -> {
          wm.verify(postRequestedFor(urlEqualTo("/_sql"))
            .withRequestBody(matchingJsonPath("$.stmt", equalTo("SELECT id, name FROM users WHERE id = ?")))
            .withRequestBody(matchingJsonPath("$.args[0]", equalTo("1"))));
          assertThat(row.getInteger("id")).isEqualTo(1);
          assertThat(row.getString("name")).isEqualTo("alice");
          ctx.completeNow();
        }),
        ctx::failNow
      );
  }
}
