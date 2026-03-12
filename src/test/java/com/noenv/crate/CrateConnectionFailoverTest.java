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
import com.noenv.crate.resolver.CrateEndpoint;
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
      .setEndpoints(List.of(new CrateEndpoint(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost()))))
      .setFailoverEnabled(true);

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(m -> ctx.completeNow())
      .onFailure(ctx::failNow);
  }

  @Test
  void connect_twoEndpointsSameNode_failoverEnabled_succeeds(Vertx vertx, VertxTestContext ctx) {
    SocketAddress addr = SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost());
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(new CrateEndpoint(addr), new CrateEndpoint(addr)))
      .setFailoverEnabled(true)
      .setFailoverMaxRetries(3);

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(m -> ctx.completeNow())
      .onFailure(ctx::failNow);
  }

  @Test
  void connect_failoverDisabled_usesFirstEndpoint(Vertx vertx, VertxTestContext ctx) {
    SocketAddress addr = SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost());
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(new CrateEndpoint(addr), new CrateEndpoint(addr)))
      .setFailoverEnabled(false);

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(m -> ctx.completeNow())
      .onFailure(ctx::failNow);
  }
}
