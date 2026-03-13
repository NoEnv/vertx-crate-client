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

/**
 * A SQL statement and parameters to execute against CrateDB.
 * <p>
 * Holds the statement text, optional positional arguments ({@code args}) or bulk arguments
 * ({@code bulk_args}), and optional per-request options (default schema, column types,
 * error trace). Serializes to the JSON body format expected by the CrateDB {@code /_sql}
 * endpoint.
 * </p>
 *
 * @see CrateQueryOptions
 * @see com.noenv.crate.CrateConnection#query(CrateQuery)
 * @see com.noenv.crate.CrateConnection#streamQuery(CrateQuery)
 */
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

  /**
   * Creates a query from the given JSON (e.g. from serialization).
   *
   * @param json the JSON to copy from
   */
  public CrateQuery(JsonObject json) {
    CrateQueryConverter.fromJson(json, this);
  }

  /**
   * Returns the SQL statement.
   *
   * @return the statement text
   */
  public String getStmt() {
    return stmt;
  }

  /**
   * Sets the SQL statement.
   *
   * @param stmt the statement text
   * @return a reference to this, so the API can be used fluently
   */
  public CrateQuery setStmt(String stmt) {
    this.stmt = stmt;
    return this;
  }

  /**
   * Returns the positional query arguments ({@code args} in the CrateDB request).
   *
   * @return the args array, or null
   */
  public JsonArray getArgs() {
    return args;
  }

  /**
   * Sets the positional query arguments.
   *
   * @param args the args array
   * @return a reference to this, so the API can be used fluently
   */
  public CrateQuery setArgs(JsonArray args) {
    this.args = args;
    return this;
  }

  /**
   * Returns the bulk arguments ({@code bulk_args} in the CrateDB request) for bulk execution.
   *
   * @return the bulk_args array, or null
   */
  public JsonArray getBulk_args() {
    return bulk_args;
  }

  /**
   * Sets the bulk arguments for bulk execution.
   *
   * @param bulk_args the bulk_args array
   * @return a reference to this, so the API can be used fluently
   */
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

  /**
   * Converts this object to JSON (full representation including query options).
   *
   * @return the JSON representation
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    CrateQueryConverter.toJson(this, json);
    return json;
  }

  /**
   * Returns JSON for the CrateDB request body only (stmt, args, bulk_args).
   * Query options are not sent in the body.
   *
   * @return the request body JSON for the {@code /_sql} endpoint
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
