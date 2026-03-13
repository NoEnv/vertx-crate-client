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
 *
 */
package com.noenv.crate.results;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.util.Collections;
import java.util.List;

/**
 * Simple {@link SqlResult} implementation holding a collected value.
 */
public class CrateSqlResult<R> implements SqlResult<R> {

  private final R value;

  public CrateSqlResult(R value) {
    this.value = value;
  }

  @Override
  public R value() {
    return value;
  }

  @Override
  public int rowCount() {
    return 0;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public List<String> columnsNames() {
    return Collections.emptyList();
  }

  @Override
  public List<ColumnDescriptor> columnDescriptors() {
    return Collections.emptyList();
  }

  @Override
  public SqlResult<R> next() {
    return null;
  }

  @Override
  public <V> V property(PropertyKind<V> propertyKind) {
    return null;
  }
}
