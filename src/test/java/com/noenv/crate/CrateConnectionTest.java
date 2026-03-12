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
package com.noenv.crate;

import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class CrateConnectionTest extends CrateContainerTest {
  @Test
  public void connectTest(Vertx vertx, VertxTestContext testContext) {
    CrateConnection.connect(vertx, new CrateConnectOptions()
        .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      )
      .compose(c -> c.query(new CrateQuery("SELECT * FROM world LIMIT 10")))
      .onSuccess(m -> System.out.println(m.toJson().encodePrettily()))
      .onFailure(Throwable::printStackTrace)
      .onComplete(ar -> testContext.completeNow());
  }

  @Test
  public void querySqlContract_returnsRowSet(Vertx vertx, VertxTestContext testContext) {
    CrateConnection.connect(vertx, new CrateConnectOptions()
        .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      )
      .compose(conn -> conn.query("SELECT 1 AS one").execute())
      .onSuccess(rowSet -> {
        assertNotNull(rowSet);
        assertTrue(rowSet.size() >= 1);
        Row row = rowSet.iterator().next();
        assertEquals(1, row.getInteger(0));
        assertEquals(1, row.getInteger("one"));
        testContext.completeNow();
      })
      .onFailure(testContext::failNow);
  }
}
