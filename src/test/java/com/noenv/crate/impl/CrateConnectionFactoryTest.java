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
 */
package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.resolver.CrateEndpoint;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
class CrateConnectionFactoryTest {

  @Test
  void markEndpointUnhealthy_marksMatchingEndpointUnhealthy(Vertx vertx) {
    CrateEndpoint e1 = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host1"));
    CrateEndpoint e2 = new CrateEndpoint(SocketAddress.inetSocketAddress(4201, "host2"));
    List<CrateEndpoint> endpoints = List.of(e1, e2);
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(endpoints)
      .setFailoverBackoffMs(60_000);

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectionFactory factory = new CrateConnectionFactory(context, options);

    assertTrue(e1.isHealthy());
    assertTrue(e2.isHealthy());

    factory.markEndpointUnhealthy(e1.getAddress());

    assertFalse(e1.isHealthy());
    assertTrue(e2.isHealthy());
  }

  @Test
  void markEndpointUnhealthy_unknownAddress_doesNotThrow(Vertx vertx) {
    CrateEndpoint e1 = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host1"));
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(e1))
      .setFailoverBackoffMs(60_000);

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectionFactory factory = new CrateConnectionFactory(context, options);

    factory.markEndpointUnhealthy(SocketAddress.inetSocketAddress(9999, "unknown"));

    assertTrue(e1.isHealthy());
  }
}
