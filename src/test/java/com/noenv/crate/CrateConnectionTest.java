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

import com.noenv.crate.junit.CrateContainerTest;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class CrateConnectionTest extends CrateContainerTest {
  @Test
  public void connectTest(Vertx vertx, VertxTestContext testContext) {
    CrateConnection.connect(vertx, new CrateConnectOptions()
      .setHost(cratedb.getHost())
      .setPort(cratedb.getMappedPort(4200)))
      .map(c -> c.query("test"))
      .onComplete(ar -> {
        testContext.completeNow();
      });
  }
}
