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
package com.noenv.crate.codec;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

/**
 * Options for a single SQL request (default schema, column types, error trace).
 * Pass to {@link CrateQuery#CrateQuery(String, CrateQueryOptions)} or {@link CrateQuery#setQueryOptions(CrateQueryOptions)}.
 * When not set on a query, connection defaults apply.
 *
 * @see <a href="https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#default-schema">Default schema (CrateDB)</a>
 * @see <a href="https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#column-types">Column types (CrateDB)</a>
 * @see <a href="https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#error-handling">Error handling (CrateDB)</a>
 */
@DataObject
@JsonGen(publicConverter = false)
public class CrateQueryOptions {

  private String defaultSchema;
  private Boolean includeColumnTypes;
  private Boolean includeErrorTrace;

  public CrateQueryOptions() {
  }

  public CrateQueryOptions(JsonObject json) {
    CrateQueryOptionsConverter.fromJson(json, this);
  }

  public CrateQueryOptions(CrateQueryOptions other) {
    this.defaultSchema = other.defaultSchema;
    this.includeColumnTypes = other.includeColumnTypes;
    this.includeErrorTrace = other.includeErrorTrace;
  }

  public JsonObject toJson() {
    JsonObject json = JsonObject.of();
    CrateQueryOptionsConverter.toJson(this, json);
    return json;
  }

  public String getDefaultSchema() {
    return defaultSchema;
  }

  /**
   * Default schema for this request (CrateDB {@code Default-Schema} HTTP header).
   *
   * @param defaultSchema the schema name, or null to use connection default
   * @return a reference to this, so the API can be used fluently
   */
  public CrateQueryOptions setDefaultSchema(String defaultSchema) {
    this.defaultSchema = defaultSchema;
    return this;
  }

  /**
   * Request column type IDs in the response (CrateDB {@code types} query param).
   *
   * @return true/false to override, or null to use connection default
   */
  public Boolean getIncludeColumnTypes() {
    return includeColumnTypes;
  }

  public CrateQueryOptions setIncludeColumnTypes(Boolean includeColumnTypes) {
    this.includeColumnTypes = includeColumnTypes;
    return this;
  }

  /**
   * Request error stack trace in error responses (CrateDB {@code error_trace} query param).
   *
   * @return true/false to override, or null to use connection default
   */
  public Boolean getIncludeErrorTrace() {
    return includeErrorTrace;
  }

  public CrateQueryOptions setIncludeErrorTrace(Boolean includeErrorTrace) {
    this.includeErrorTrace = includeErrorTrace;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CrateQueryOptions)) return false;
    CrateQueryOptions that = (CrateQueryOptions) o;
    return java.util.Objects.equals(defaultSchema, that.defaultSchema) &&
      java.util.Objects.equals(includeColumnTypes, that.includeColumnTypes) &&
      java.util.Objects.equals(includeErrorTrace, that.includeErrorTrace);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(defaultSchema, includeColumnTypes, includeErrorTrace);
  }
}
