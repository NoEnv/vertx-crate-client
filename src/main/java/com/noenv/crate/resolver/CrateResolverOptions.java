package com.noenv.crate.resolver;


import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

@DataObject
@JsonGen(publicConverter = false)
public class CrateResolverOptions {



  public CrateResolverOptions(JsonObject json) {
    CrateResolverOptionsConverter.fromJson(json, this);
  }

  public JsonObject toJson() {
    JsonObject json = JsonObject.of();
    CrateResolverOptionsConverter.toJson(this, json);
    return json;
  }
}
