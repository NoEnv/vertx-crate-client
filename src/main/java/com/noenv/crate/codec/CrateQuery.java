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

  public CrateQuery(String stmt) {
    this.stmt = stmt;
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

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    CrateQueryConverter.toJson(this, json);
    return json;
  }
}
