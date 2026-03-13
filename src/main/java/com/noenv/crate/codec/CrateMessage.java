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
package com.noenv.crate.codec;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.format.SnakeCase;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@DataObject
@JsonGen(jsonPropertyNameFormatter = SnakeCase.class)
public class CrateMessage {
  private JsonArray cols;
  private JsonArray colTypes;
  private JsonArray rows;
  private Long rowCount;
  private JsonArray results;
  private JsonObject error;
  private String errorTrace;
  private Double duration;

  public CrateMessage(JsonObject json) {
    CrateMessageConverter.fromJson(json, this);
  }

  public JsonArray getCols() {
    return cols;
  }

  public CrateMessage setCols(JsonArray cols) {
    this.cols = cols;
    return this;
  }

  public JsonArray getColTypes() {
    return colTypes;
  }

  public CrateMessage setColTypes(JsonArray colTypes) {
    this.colTypes = colTypes;
    return this;
  }

  public JsonArray getRows() {
    return rows;
  }

  public CrateMessage setRows(JsonArray rows) {
    this.rows = rows;
    return this;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public CrateMessage setRowCount(Long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  public JsonArray getResults() {
    return results;
  }

  public CrateMessage setResults(JsonArray results) {
    this.results = results;
    return this;
  }

  public JsonObject getError() {
    return error;
  }

  public CrateMessage setError(JsonObject error) {
    this.error = error;
    return this;
  }

  public String getErrorTrace() {
    return errorTrace;
  }

  public CrateMessage setErrorTrace(String errorTrace) {
    this.errorTrace = errorTrace;
    return this;
  }

  public Double getDuration() {
    return duration;
  }

  public CrateMessage setDuration(Double duration) {
    this.duration = duration;
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    CrateMessageConverter.toJson(this, json);
    return json;
  }
}
