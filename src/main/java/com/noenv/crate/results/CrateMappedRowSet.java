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

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link RowSet} of mapped elements (e.g. from {@link Query#mapping}).
 */
public class CrateMappedRowSet<U> implements RowSet<U> {

  private final List<ColumnDescriptor> columnDescriptors;
  private final List<U> rows;

  public CrateMappedRowSet(List<ColumnDescriptor> columnDescriptors, List<U> rows) {
    this.columnDescriptors = columnDescriptors != null ? List.copyOf(columnDescriptors) : List.of();
    this.rows = rows != null ? List.copyOf(rows) : List.of();
  }

  @Override
  public RowIterator<U> iterator() {
    return new RowIterator<>() {
      private final java.util.Iterator<U> it = rows.iterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public U next() {
        return it.next();
      }
    };
  }

  @Override
  public RowSet<U> next() {
    return null;
  }

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public int rowCount() {
    return rows.size();
  }

  @Override
  public List<String> columnsNames() {
    return columnDescriptors.stream()
      .map(ColumnDescriptor::name)
      .collect(Collectors.toList());
  }

  @Override
  public List<ColumnDescriptor> columnDescriptors() {
    return columnDescriptors;
  }

  @Override
  public RowSet<U> value() {
    return this;
  }

  @Override
  public <V> V property(PropertyKind<V> propertyKind) {
    return null;
  }

  @Override
  public Stream<U> stream() {
    return rows.stream();
  }
}
