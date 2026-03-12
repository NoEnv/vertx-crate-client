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
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.noenv.crate.resolver;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.impl.CrateEndpoint;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
class CrateEndpointTest {

  @Test
  void isHealthy_byDefault(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    CrateEndpoint e = CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "localhost"), context, options);
    assertTrue(e.isHealthy());
    ctx.completeNow();
  }

  @Test
  void isHealthy_whenConfiguredUnhealthy_returnsFalse(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    CrateEndpoint e = CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "localhost"), context, options);
    e.setHealthy(false);
    assertFalse(e.isHealthy());
    ctx.completeNow();
  }

  @Test
  void markUnhealthy_makesEndpointUnhealthyForBackoffPeriod(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    CrateEndpoint e = CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "localhost"), context, options);
    assertTrue(e.isHealthy());
    e.markUnhealthy(500); // 500 ms backoff
    assertFalse(e.isHealthy());
    ctx.completeNow();
  }

  @Test
  void markUnhealthy_zeroBackoff_doesNotMarkUnhealthy(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    CrateEndpoint e = CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "localhost"), context, options);
    e.markUnhealthy(0);
    assertTrue(e.isHealthy());
    ctx.completeNow();
  }

  @Test
  void markUnhealthy_afterBackoffExpires_endpointBecomesHealthyAgain(Vertx vertx, VertxTestContext ctx) throws InterruptedException {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    CrateEndpoint e = CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "localhost"), context, options);
    e.markUnhealthy(50); // 50 ms
    assertFalse(e.isHealthy());
    Thread.sleep(60);
    assertTrue(e.isHealthy());
    ctx.completeNow();
  }
}
