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
  /** If positive, endpoint is considered unhealthy until this time (ms). 0 or negative = available. */
  private volatile long unhealthyUntilMillis;

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

  /**
   * True if this endpoint is configured healthy and not in backoff (or backoff has expired).
   */
  public boolean isHealthy() {
    if (!healthy) {
      return false;
    }
    long until = unhealthyUntilMillis;
    return until <= 0 || System.currentTimeMillis() >= until;
  }

  public CrateEndpoint setHealthy(boolean healthy) {
    this.healthy = healthy;
    return this;
  }

  /**
   * Mark this endpoint as unhealthy for the given backoff period (ms).
   * It will be considered healthy again after {@code backoffMs} have passed.
   */
  public void markUnhealthy(long backoffMs) {
    if (backoffMs > 0) {
      this.unhealthyUntilMillis = System.currentTimeMillis() + backoffMs;
    }
  }

  public SocketAddress getAddress() {
    return address;
  }

  public CrateEndpoint setAddress(SocketAddress address) {
    this.address = address;
    return this;
  }
}
