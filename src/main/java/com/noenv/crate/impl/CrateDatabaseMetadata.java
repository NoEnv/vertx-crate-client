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
package com.noenv.crate.impl;

import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CrateDatabaseMetadata implements DatabaseMetadata {

  private static final Pattern VERSION_PATTERN = Pattern.compile(
    "^(\\d+)(?:\\.(\\d+))?");

  private final String fullVersion;
  private int majorVersion;
  private int minorVersion;

  public CrateDatabaseMetadata(String serverVersion) {
    fullVersion = serverVersion;
    Matcher matcher = VERSION_PATTERN.matcher(serverVersion);
    if (matcher.find()) {
      majorVersion = Integer.parseInt(matcher.group(1));
      String minorPart = matcher.group(2);
      if (minorPart != null) {
        minorVersion = Integer.parseInt(minorPart);
      }
    }
  }

  @Override
  public String productName() {
    return "CrateDB";
  }

  @Override
  public String fullVersion() {
    return fullVersion;
  }

  @Override
  public int majorVersion() {
    return majorVersion;
  }

  @Override
  public int minorVersion() {
    return minorVersion;
  }
}
