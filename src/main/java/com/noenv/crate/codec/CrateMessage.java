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
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@DataObject
@JsonGen
public class CrateMessage {
  private JsonArray cols;
  private JsonArray col_types;
  private JsonArray rows;
  private Long rowcount;
  private JsonArray results;
  private JsonObject error;
  private String error_trace;
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

  public JsonArray getCol_types() {
    return col_types;
  }

  public CrateMessage setCol_types(JsonArray col_types) {
    this.col_types = col_types;
    return this;
  }

  public JsonArray getRows() {
    return rows;
  }

  public CrateMessage setRows(JsonArray rows) {
    this.rows = rows;
    return this;
  }

  public Long getRowcount() {
    return rowcount;
  }

  public CrateMessage setRowcount(Long rowcount) {
    this.rowcount = rowcount;
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

  public String getError_trace() {
    return error_trace;
  }

  public CrateMessage setError_trace(String error_trace) {
    this.error_trace = error_trace;
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
