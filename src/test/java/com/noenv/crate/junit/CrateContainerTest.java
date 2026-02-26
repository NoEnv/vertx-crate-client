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
import org.testcontainers.cratedb.CrateDBContainer;

import java.io.IOException;

public abstract class CrateContainerTest {
  public static CrateDBContainer cratedb = new CrateDBContainer("crate:6.2.1");

  @BeforeAll
  static void startContainer() {
    cratedb.withClasspathResourceMapping("create-crate.sql", "/data/create-crate.sql", BindMode.READ_ONLY);
    cratedb.start();
  }

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    var result = cratedb.execInContainer("/bin/sh", "-c", "cat /data/create-crate.sql | crash");
    if (result.getExitCode() != 0) {
      System.err.println("Failed to initialize crate database: " + result.getStderr());
    }
  }

  @AfterAll
  static void stopContainer() {
    cratedb.stop();
  }
}
