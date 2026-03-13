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
package com.noenv.crate.result;

import com.noenv.crate.codec.CrateMessage;
import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Result of a CrateDB query execution, implementing the Vert.x SQL client {@link RowSet} interface.
 */
public class CrateRowSet implements RowSet<Row> {

  private final List<ColumnDescriptor> columnDescriptors;
  private final List<Row> rows;

  public CrateRowSet(List<ColumnDescriptor> columnDescriptors, List<Row> rows) {
    this.columnDescriptors = columnDescriptors != null ? List.copyOf(columnDescriptors) : List.of();
    this.rows = rows != null ? List.copyOf(rows) : List.of();
  }

  @Override
  public RowIterator<Row> iterator() {
    return new RowIterator<>() {
      private final Iterator<Row> it = rows.iterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Row next() {
        return it.next();
      }
    };
  }

  @Override
  public RowSet<Row> next() {
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
  public RowSet<Row> value() {
    return this;
  }

  @Override
  public <V> V property(PropertyKind<V> propertyKind) {
    return null;
  }

  @Override
  public Stream<Row> stream() {
    return rows.stream();
  }

  /**
   * Builds a {@link CrateRowSet} from a CrateDB SQL response message.
   * The message must represent a successful result (no error set).
   *
   * @param message the CrateMessage from the /_sql response
   * @return the row set
   */
  public static CrateRowSet fromMessage(CrateMessage message) {
    List<String> columnNames = new ArrayList<>();
    JsonArray cols = message.getCols();
    if (cols != null) {
      for (int i = 0; i < cols.size(); i++) {
        columnNames.add(cols.getString(i));
      }
    }

    List<ColumnDescriptor> descriptors = columnNames.stream()
      .map(CrateColumnDescriptor::new)
      .collect(Collectors.toList());

    List<Row> rowList = new ArrayList<>();
    JsonArray rows = message.getRows();
    if (rows != null) {
      for (int i = 0; i < rows.size(); i++) {
        Object el = rows.getValue(i);
        if (el instanceof JsonArray) {
          rowList.add(new CrateRow(columnNames, (JsonArray) el));
        }
      }
    }

    return new CrateRowSet(descriptors, rowList);
  }
}
