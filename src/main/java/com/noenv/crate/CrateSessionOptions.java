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
package com.noenv.crate;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

/**
 * Session-level options for a CrateDB connection.
 * <p>
 * Holds settings such as statement timeout that apply for the lifetime of the session.
 * Can be merged with additional JSON via {@link #merge(JsonObject)}.
 * </p>
 */
@DataObject
@JsonGen(publicConverter = false)
public class CrateSessionOptions {

  public static final int DEFAULT_STATEMENT_TIMEOUT = 0;

  private int statementTimeout = DEFAULT_STATEMENT_TIMEOUT;

  /** Creates session options with default values. */
  public CrateSessionOptions() {
  }

  /**
   * Creates session options from the given JSON.
   *
   * @param json the JSON to copy from
   */
  public CrateSessionOptions(JsonObject json) {
    CrateSessionOptionsConverter.fromJson(json, this);
  }

  /**
   * Copies the given session options.
   *
   * @param other the options to copy from
   */
  public CrateSessionOptions(CrateSessionOptions other) {
    this.statementTimeout = other.statementTimeout;
  }

  /**
   * Converts this object to JSON.
   *
   * @return the JSON representation
   */
  public JsonObject toJson() {
    JsonObject json = JsonObject.of();
    CrateSessionOptionsConverter.toJson(this, json);
    return json;
  }

  /**
   * Returns the statement timeout in milliseconds (0 means no timeout).
   *
   * @return the statement timeout in ms, or 0 for no timeout
   */
  public int getStatementTimeout() {
    return statementTimeout;
  }

  /**
   * Sets the statement timeout in milliseconds (0 for no timeout).
   *
   * @param statementTimeout the timeout in ms
   */
  public void setStatementTimeout(int statementTimeout) {
    this.statementTimeout = statementTimeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CrateSessionOptions)) return false;
    if (!super.equals(o)) return false;

    var that = (CrateSessionOptions) o;
    return statementTimeout == that.statementTimeout;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + statementTimeout;
    return result;
  }

  /**
   * Merges the given JSON into this options and returns a new instance.
   * Existing properties are overwritten by keys present in {@code other}.
   *
   * @param other the JSON to merge in
   * @return a new CrateSessionOptions with merged values
   */
  public CrateSessionOptions merge(JsonObject other) {
    JsonObject json = toJson();
    json.mergeIn(other);
    return new CrateSessionOptions(json);
  }
}
