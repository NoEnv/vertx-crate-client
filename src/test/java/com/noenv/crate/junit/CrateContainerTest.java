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
package com.noenv.crate.junit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public abstract class CrateContainerTest {
  public static GenericContainer<?> cratedb = new GenericContainer<>("crate:6.2.1");

  @BeforeAll
  static void startContainer() throws IOException, InterruptedException {
    cratedb
      .waitingFor(Wait
        .forHttp("/")
        .forPort(4200)
        .forStatusCode(200)
        .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS))
      )
      .withCommand("crate -C discovery.type=single-node")
      .withClasspathResourceMapping("create-crate.sql", "/tmp/create-crate.sql", BindMode.READ_ONLY)
      .withExposedPorts(4200);
    cratedb.start();
  }

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    cratedb.execInContainer("/bin/sh", "-c", "cat /tmp/create-crate.sql | crash");
  }

  @AfterAll
  static void stopContainer() {
    cratedb.stop();
  }
}
