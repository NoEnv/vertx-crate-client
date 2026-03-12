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
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;

@ExtendWith(VertxExtension.class)
class CrateConnectionFactoryTest {

  @Test
  void getOptions_returnsSameOptions(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(4200, "host1")))
      .setFailoverBackoffMs(60_000);

    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectionFactory factory = new CrateConnectionFactory(context, options);

    assertSame(options, factory.getOptions());
    ctx.completeNow();
  }
}
