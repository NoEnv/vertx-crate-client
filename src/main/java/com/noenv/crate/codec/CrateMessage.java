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

/**
 * Raw response payload from a CrateDB SQL request.
 * <p>
 * Maps the JSON structure returned by the CrateDB {@code /_sql} HTTP API: column names
 * ({@code cols}), optional column type IDs ({@code col_types}), result rows ({@code rows}),
 * row count, duration, and error details when the request failed. Used by the client to
 * decode query results and errors.
 * </p>
 *
 * @see com.noenv.crate.CrateConnection#query(CrateQuery)
 */
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

  /**
   * Creates a message from the given CrateDB response JSON.
   *
   * @param json the JSON from the {@code /_sql} response body
   */
  public CrateMessage(JsonObject json) {
    CrateMessageConverter.fromJson(json, this);
  }

  /**
   * Returns the column names ({@code cols} in the CrateDB response).
   *
   * @return the column names array, or null
   */
  public JsonArray getCols() {
    return cols;
  }

  /**
   * Sets the column names.
   *
   * @param cols the column names array
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setCols(JsonArray cols) {
    this.cols = cols;
    return this;
  }

  /**
   * Returns the column type IDs ({@code col_types} in the CrateDB response), when requested.
   *
   * @return the column type IDs array, or null
   */
  public JsonArray getColTypes() {
    return colTypes;
  }

  /**
   * Sets the column type IDs.
   *
   * @param colTypes the column type IDs array
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setColTypes(JsonArray colTypes) {
    this.colTypes = colTypes;
    return this;
  }

  /**
   * Returns the result rows ({@code rows} in the CrateDB response).
   *
   * @return the rows array, or null
   */
  public JsonArray getRows() {
    return rows;
  }

  /**
   * Sets the result rows.
   *
   * @param rows the rows array
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setRows(JsonArray rows) {
    this.rows = rows;
    return this;
  }

  /**
   * Returns the row count from the response, when present.
   *
   * @return the row count, or null
   */
  public Long getRowCount() {
    return rowCount;
  }

  /**
   * Sets the row count.
   *
   * @param rowCount the row count
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setRowCount(Long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  /**
   * Returns the results array from the response, when present.
   *
   * @return the results array, or null
   */
  public JsonArray getResults() {
    return results;
  }

  /**
   * Sets the results array.
   *
   * @param results the results array
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setResults(JsonArray results) {
    this.results = results;
    return this;
  }

  /**
   * Returns the error object from the response when the request failed.
   *
   * @return the error details, or null if the request succeeded
   */
  public JsonObject getError() {
    return error;
  }

  /**
   * Sets the error object.
   *
   * @param error the error details
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setError(JsonObject error) {
    this.error = error;
    return this;
  }

  /**
   * Returns the error stack trace from CrateDB when {@code error_trace=true} was requested.
   *
   * @return the error trace string, or null
   */
  public String getErrorTrace() {
    return errorTrace;
  }

  /**
   * Sets the error trace string.
   *
   * @param errorTrace the error trace
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setErrorTrace(String errorTrace) {
    this.errorTrace = errorTrace;
    return this;
  }

  /**
   * Returns the request duration in seconds, when present.
   *
   * @return the duration in seconds, or null
   */
  public Double getDuration() {
    return duration;
  }

  /**
   * Sets the request duration.
   *
   * @param duration the duration in seconds
   * @return a reference to this, so the API can be used fluently
   */
  public CrateMessage setDuration(Double duration) {
    this.duration = duration;
    return this;
  }

  /**
   * Converts this object to JSON.
   *
   * @return the JSON representation
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    CrateMessageConverter.toJson(this, json);
    return json;
  }
}
