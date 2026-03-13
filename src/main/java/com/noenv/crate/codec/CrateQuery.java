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
public class CrateQuery {
  private String stmt;
  private JsonArray args;
  private JsonArray bulk_args;
  private CrateQueryOptions queryOptions;

  /**
   * Create a query with the given SQL
   *
   * @param stmt    the SQL statement
   */
  public CrateQuery(String stmt) {
    this.stmt = stmt;
  }

  /**
   * Create a query with the given SQL and request options (default schema, column types, error trace).
   *
   * @param stmt    the SQL statement
   * @param options per-request options, or null to use connection defaults
   */
  public CrateQuery(String stmt, CrateQueryOptions options) {
    this.stmt = stmt;
    this.queryOptions = options;
  }

  public CrateQuery(JsonObject json) {
    CrateQueryConverter.fromJson(json, this);
  }

  public String getStmt() {
    return stmt;
  }

  public CrateQuery setStmt(String stmt) {
    this.stmt = stmt;
    return this;
  }

  public JsonArray getArgs() {
    return args;
  }

  public CrateQuery setArgs(JsonArray args) {
    this.args = args;
    return this;
  }

  public JsonArray getBulk_args() {
    return bulk_args;
  }

  public CrateQuery setBulk_args(JsonArray bulk_args) {
    this.bulk_args = bulk_args;
    return this;
  }

  /**
   * Per-request options (default schema, column types, error trace). Null = use connection defaults.
   */
  public CrateQueryOptions getQueryOptions() {
    return queryOptions;
  }

  /**
   * Set per-request options. Overrides connection defaults when set.
   *
   * @param queryOptions the options, or null to use connection defaults
   * @return a reference to this, so the API can be used fluently
   */
  public CrateQuery setQueryOptions(CrateQueryOptions queryOptions) {
    this.queryOptions = queryOptions;
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    CrateQueryConverter.toJson(this, json);
    return json;
  }

  /**
   * JSON for the CrateDB request body only (stmt, args, bulk_args).
   * Query options are not sent in the body.
   */
  public JsonObject toRequestBodyJson() {
    JsonObject json = new JsonObject();
    if (stmt != null) {
      json.put("stmt", stmt);
    }
    if (args != null) {
      json.put("args", args);
    }
    if (bulk_args != null) {
      json.put("bulk_args", bulk_args);
    }
    return json;
  }
}
