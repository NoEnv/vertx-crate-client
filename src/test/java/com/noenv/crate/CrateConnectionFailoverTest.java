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
 */
package com.noenv.crate;

import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

@ExtendWith(VertxExtension.class)
class CrateConnectionFailoverTest extends CrateContainerTest {

  @Test
  void connect_singleEndpoint_succeeds(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())));

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(m -> ctx.completeNow())
      .onFailure(ctx::failNow);
  }

  @Test
  void connect_twoEndpointsDifferentNodes_firstFails_failoverSucceeds(Vertx vertx, VertxTestContext ctx) {
    // First endpoint is non-existent (connection refused); client fails over to the second.
    String workingHost = cratedb.getHost();
    int workingPort = cratedb.getMappedPort(4200);
    int nonExistentPort = 19; // port that nothing listens on → connection refused
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(
        SocketAddress.inetSocketAddress(nonExistentPort, workingHost),
        SocketAddress.inetSocketAddress(workingPort, workingHost)))
      .setFailoverMaxRetries(3);

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(m -> ctx.completeNow())
      .onFailure(ctx::failNow);
  }

  @Test
  void connect_failoverDisabled_usesFirstEndpoint(Vertx vertx, VertxTestContext ctx) {
    String host = cratedb.getHost();
    int port = cratedb.getMappedPort(4200);
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(port, host), SocketAddress.inetSocketAddress(port, host)));

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(m -> ctx.completeNow())
      .onFailure(ctx::failNow);
  }
}
