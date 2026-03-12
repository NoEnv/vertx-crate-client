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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;

import java.util.Collections;
import java.util.List;

/**
 * A single row of a CrateDB query result, implementing the Vert.x SQL client {@link Row} interface.
 */
public class CrateRow implements Row {

  private final List<String> columnNames;
  private final JsonArray values;

  public CrateRow(List<String> columnNames, JsonArray values) {
    this.columnNames = List.copyOf(columnNames);
    this.values = values != null ? values : new JsonArray();
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public Object getValue(int pos) {
    if (pos < 0 || pos >= values.size()) {
      return null;
    }
    return values.getValue(pos);
  }

  @Override
  public String getColumnName(int pos) {
    if (pos < 0 || pos >= columnNames.size()) {
      return null;
    }
    return columnNames.get(pos);
  }

  @Override
  public int getColumnIndex(String column) {
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnNames.get(i).equals(column)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public Row addValue(Object value) {
    throw new UnsupportedOperationException("Row is read-only");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Row is read-only");
  }

  @Override
  public List<Class<?>> types() {
    return Collections.nCopies(size(), Object.class);
  }

  @Override
  public JsonObject toJson() {
    JsonObject obj = new JsonObject();
    for (int i = 0; i < Math.min(columnNames.size(), values.size()); i++) {
      obj.put(columnNames.get(i), values.getValue(i));
    }
    return obj;
  }
}
