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

import io.vertx.core.net.SocketAddress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrateEndpointTest {

  @Test
  void isHealthy_byDefault() {
    CrateEndpoint e = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "localhost"));
    assertTrue(e.isHealthy());
  }

  @Test
  void isHealthy_whenConfiguredUnhealthy_returnsFalse() {
    CrateEndpoint e = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "localhost"));
    e.setHealthy(false);
    assertFalse(e.isHealthy());
  }

  @Test
  void markUnhealthy_makesEndpointUnhealthyForBackoffPeriod() {
    CrateEndpoint e = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "localhost"));
    assertTrue(e.isHealthy());

    e.markUnhealthy(500); // 500 ms backoff
    assertFalse(e.isHealthy());
  }

  @Test
  void markUnhealthy_zeroBackoff_doesNotMarkUnhealthy() {
    CrateEndpoint e = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "localhost"));
    e.markUnhealthy(0);
    assertTrue(e.isHealthy());
  }

  @Test
  void markUnhealthy_afterBackoffExpires_endpointBecomesHealthyAgain() throws InterruptedException {
    CrateEndpoint e = new CrateEndpoint(SocketAddress.inetSocketAddress(4200, "localhost"));
    e.markUnhealthy(50); // 50 ms
    assertFalse(e.isHealthy());
    Thread.sleep(60);
    assertTrue(e.isHealthy());
  }
}
