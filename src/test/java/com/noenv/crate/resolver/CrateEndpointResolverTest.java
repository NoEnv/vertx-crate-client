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
package com.noenv.crate.resolver;

import io.vertx.core.Future;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.endpoint.EndpointBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrateEndpointResolverTest {

  private static EndpointBuilder<CrateEndpoint, SocketAddress> mockBuilder() {
    return new EndpointBuilder<CrateEndpoint, SocketAddress>() {
      @Override
      public EndpointBuilder<CrateEndpoint, SocketAddress> addServer(SocketAddress server, String metric) {
        return this;
      }

      @Override
      public CrateEndpoint build() {
        return new CrateEndpoint(SocketAddress.inetSocketAddress(0, "dummy"));
      }
    };
  }

  @Test
  void isValid_whenAllHealthy_returnsTrue() {
    List<CrateEndpoint> endpoints = List.of(
      new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host1")),
      new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host2"))
    );
    CrateEndpointResolver resolver = new CrateEndpointResolver(endpoints);
    CrateLookup lookup = new CrateLookup(endpoints.get(0).getAddress(), mockBuilder());
    assertTrue(resolver.isValid(lookup));
  }

  @Test
  void isValid_whenAllUnhealthy_returnsFalse() {
    List<CrateEndpoint> endpoints = List.of(
      new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host1")),
      new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host2"))
    );
    endpoints.get(0).markUnhealthy(60_000);
    endpoints.get(1).markUnhealthy(60_000);
    CrateEndpointResolver resolver = new CrateEndpointResolver(endpoints);
    CrateLookup lookup = new CrateLookup(endpoints.get(0).getAddress(), mockBuilder());
    assertFalse(resolver.isValid(lookup));
  }

  @Test
  void isValid_whenOneHealthy_returnsTrue() {
    List<CrateEndpoint> endpoints = List.of(
      new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host1")),
      new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host2"))
    );
    endpoints.get(0).markUnhealthy(60_000);
    CrateEndpointResolver resolver = new CrateEndpointResolver(endpoints);
    CrateLookup lookup = new CrateLookup(endpoints.get(0).getAddress(), mockBuilder());
    assertTrue(resolver.isValid(lookup));
  }

  @Test
  void endpoint_buildsOnlyFromHealthyEndpoints() throws Exception {
    CrateEndpoint healthy = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "host1"));
    CrateEndpoint unhealthy = new CrateEndpoint(SocketAddress.inetSocketAddress(4201, "host2"));
    unhealthy.markUnhealthy(60_000);
    List<CrateEndpoint> endpoints = List.of(healthy, unhealthy);
    CrateEndpointResolver resolver = new CrateEndpointResolver(endpoints);

    CrateLookup state = resolver.resolve(healthy.getAddress(), mockBuilder())
      .toCompletionStage().toCompletableFuture().get();
    resolver.endpoint(state);
    assertEquals(1, state.endpoints.size());
    assertEquals(healthy.getAddress(), state.endpoints.get(0).getAddress());
  }

  @Test
  void resolve_returnsSucceededFuture() {
    List<CrateEndpoint> endpoints = List.of(new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "localhost")));
    CrateEndpointResolver resolver = new CrateEndpointResolver(endpoints);
    SocketAddress addr = endpoints.get(0).getAddress();
    Future<CrateLookup> f = resolver.resolve(addr, mockBuilder());
    assertTrue(f.succeeded());
    assertEquals(addr, f.result().address);
  }
}
