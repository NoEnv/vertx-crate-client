package com.noenv.crate.resolver;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;

@DataObject
@JsonGen(publicConverter = false)
public class CrateEndpoint {
  SocketAddress address;
  JsonObject properties;
  boolean healthy;

  public CrateEndpoint(SocketAddress address) {
    this.address = address;
    this.properties = JsonObject.of();
    this.healthy = true;
  }

  /**
   * Creates an endpoint from JSON. Expects {@code host} and {@code port}; optionally
   * {@code properties} (JsonObject) and {@code healthy} (boolean).
   * When {@code host} and {@code port} are present, they are used to build the address;
   * otherwise the generated converter is used (e.g. for {@code address} key).
   */
  public CrateEndpoint(JsonObject json) {
    if (json.containsKey("host") && json.containsKey("port")) {
      String host = json.getString("host");
      Integer port = json.getInteger("port");
      if (host == null || port == null) {
        throw new IllegalArgumentException("CrateEndpoint JSON must contain 'host' and 'port'");
      }
      this.address = SocketAddress.inetSocketAddress(port, host);
      this.properties = json.getJsonObject("properties", JsonObject.of());
      this.healthy = json.getBoolean("healthy", true);
    } else {
      CrateEndpointConverter.fromJson(json, this);
    }
  }

  public JsonObject toJson() {
    JsonObject json = JsonObject.of();
    CrateEndpointConverter.toJson(this, json);
    return json;
  }

  public JsonObject getProperties() {
    return properties;
  }

  public CrateEndpoint setProperties(JsonObject properties) {
    this.properties = properties;
    return this;
  }

  public boolean isHealthy() {
    return healthy;
  }

  public CrateEndpoint setHealthy(boolean healthy) {
    this.healthy = healthy;
    return this;
  }

  public SocketAddress getAddress() {
    return address;
  }

  public CrateEndpoint setAddress(SocketAddress address) {
    this.address = address;
    return this;
  }
}
