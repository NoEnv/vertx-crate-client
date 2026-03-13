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
package com.noenv.crate.results;

import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.sql.JDBCType;

/**
 * Column descriptor for CrateDB result set columns.
 */
public class CrateColumnDescriptor implements ColumnDescriptor {

  private final String name;
  private final String typeName;
  private final JDBCType jdbcType;
  private final boolean array;

  public CrateColumnDescriptor(String name) {
    this(name, null, JDBCType.OTHER, false);
  }

  public CrateColumnDescriptor(String name, String typeName, JDBCType jdbcType, boolean array) {
    this.name = name;
    this.typeName = typeName;
    this.jdbcType = jdbcType != null ? jdbcType : JDBCType.OTHER;
    this.array = array;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean isArray() {
    return array;
  }

  @Override
  public String typeName() {
    return typeName;
  }

  @Override
  public JDBCType jdbcType() {
    return jdbcType;
  }
}
