package com.noenv.crate;

import com.noenv.crate.codec.CrateQuery;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class CrateIntegrationTest {

  // Just to test auth connection, etc against local DB
  @Disabled
  @Test
  void connectsToLocalCrateDB(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setHost("localhost")
      .setPort(4200);

    CrateConnection.connect(vertx, options)
      .onSuccess(conn -> {
        conn.queryObservable(new CrateQuery("SELECT name FROM sys.cluster"))
          .subscribe(
            row -> System.out.println("Connected to: " + row.encodePrettily()),
            ctx::failNow,
            () -> {
              conn.close();
              ctx.completeNow();
            }
          );
      })
      .onFailure(ctx::failNow);
  }
}
