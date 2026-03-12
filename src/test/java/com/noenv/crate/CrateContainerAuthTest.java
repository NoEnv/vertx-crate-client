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
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests Crate DB testcontainer with user:password authentication enabled.
 * Uses a dedicated container with HBA: trust for crate from local (for setup),
 * password for all other connections.
 */
@ExtendWith(VertxExtension.class)
class CrateContainerAuthTest {

  private static final String AUTH_USER = "testuser";
  private static final String AUTH_PASSWORD = "testpass";

  static GenericContainer<?> cratedb = new GenericContainer<>("crate:6.2.1");

  @BeforeAll
  static void startContainer() throws IOException, InterruptedException {
    cratedb
      .withCommand(
        "crate -C discovery.type=single-node" +
          " -C auth.host_based.enabled=true" +
          " -C auth.host_based.config.0.method=trust" +
          " -C auth.host_based.config.0.address=_local_" +
          " -C auth.host_based.config.0.user=crate" +
          " -C auth.host_based.config.99.method=password"
      )
      .waitingFor(Wait
        .forHttp("/")
        .forPort(4200)
        .forStatusCode(401)  // 401 = server up and requiring auth
        .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS))
      )
      .withClasspathResourceMapping("create-crate.sql", "/tmp/create-crate.sql", BindMode.READ_ONLY)
      .withClasspathResourceMapping("create-crate-auth-user.sql", "/tmp/create-crate-auth-user.sql", BindMode.READ_ONLY)
      .withExposedPorts(4200);
    cratedb.start();

    // Create user with password and grant privileges (crash runs inside container → local → trust as crate)
    var execResult = cratedb.execInContainer("/bin/sh", "-c", "cat /tmp/create-crate-auth-user.sql | crash --hosts localhost:4200 -U crate");
    if (execResult.getExitCode() != 0) {
      throw new IllegalStateException("User creation failed: " + execResult.getStderr());
    }
  }

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    cratedb.execInContainer("/bin/sh", "-c", "cat /tmp/create-crate.sql | crash --hosts localhost:4200 -U crate");
  }

  @AfterAll
  static void stopContainer() {
    cratedb.stop();
  }

  @Test
  void connectWithUserAndPassword_succeeds(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setUser(AUTH_USER)
      .setPassword(AUTH_PASSWORD);

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT name FROM sys.cluster")))
      .onSuccess(msg -> ctx.verify(() -> {
        assertNull(msg.getError());
        assertNotNull(msg.getRows());
        assertFalse(msg.getRows().isEmpty());
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  @Test
  void connectWithUserAndPassword_queryWorldTable_returnsRows(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setUser(AUTH_USER)
      .setPassword(AUTH_PASSWORD);

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT id, randomnumber FROM world ORDER BY id LIMIT 3")))
      .onSuccess(msg -> ctx.verify(() -> {
        assertNull(msg.getError());
        assertNotNull(msg.getRows());
        assertEquals(3, msg.getRows().size());
        JsonArray firstRow = msg.getRows().getJsonArray(0);
        assertEquals(1, firstRow.getInteger(0));
        assertNotNull(firstRow.getValue(1));
        ctx.completeNow();
      }))
      .onFailure(ctx::failNow);
  }

  @Test
  void connectWithWrongPassword_fails(Vertx vertx, VertxTestContext ctx) {
    CrateConnectOptions options = new CrateConnectOptions()
      .setEndpoints(List.of(SocketAddress.inetSocketAddress(cratedb.getMappedPort(4200), cratedb.getHost())))
      .setUser(AUTH_USER)
      .setPassword("wrongpassword");

    CrateConnection.connect(vertx, options)
      .compose(conn -> conn.query(new CrateQuery("SELECT 1")))
      .onSuccess(msg -> ctx.failNow(new AssertionError("Expected query to fail with wrong password")))
      .onFailure(err -> ctx.completeNow());
  }
}
