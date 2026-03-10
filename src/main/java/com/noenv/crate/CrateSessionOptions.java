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

@DataObject
@JsonGen(publicConverter = false)
public class CrateSessionOptions {

  public static final int DEFAULT_STATEMENT_TIMEOUT = 0;

  private int statementTimeout = DEFAULT_STATEMENT_TIMEOUT;

  public CrateSessionOptions() {
  }

  public CrateSessionOptions(JsonObject json) {
    CrateSessionOptionsConverter.fromJson(json, this);
  }

  public CrateSessionOptions(CrateSessionOptions other) {
    this.statementTimeout = other.statementTimeout;
  }

  public JsonObject toJson() {
    JsonObject json = JsonObject.of();
    CrateSessionOptionsConverter.toJson(this, json);
    return json;
  }


  public int getStatementTimeout() {
    return statementTimeout;
  }

  public void setStatementTimeout(int statementTimeout) {
    this.statementTimeout = statementTimeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CrateSessionOptions that)) return false;
    if (!super.equals(o)) return false;

    return statementTimeout == that.statementTimeout;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + statementTimeout;
    return result;
  }

  public CrateSessionOptions merge(JsonObject other) {
    JsonObject json = toJson();
    json.mergeIn(other);
    return new CrateSessionOptions(json);
  }
}
