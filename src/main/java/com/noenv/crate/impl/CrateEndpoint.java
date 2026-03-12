package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;

public class CrateEndpoint {
  private final SocketAddress address;
  private JsonObject properties;
  private boolean healthy;
  /** If positive, endpoint is considered unhealthy until this time (ms). 0 or negative = available. */
  private volatile long unhealthyUntilMillis;
  private final HttpClientAgent agent;

  public static CrateEndpoint create(SocketAddress address, ContextInternal context, CrateConnectOptions options) {
    return new CrateEndpoint(address, context.owner().httpClientBuilder()
      .with(options.getHttpClientOptions())
      .with(options.getHttpPoolOptions())
      .withLoadBalancer(options.getLoadBalancer())
      .build()
    );
  }

  private CrateEndpoint(SocketAddress address, HttpClientAgent agent) {
    this.agent = agent;
    this.address = address;
    this.properties = JsonObject.of();
    this.healthy = true;
  }

  public HttpClientAgent getAgent() {
    return agent;
  }

  public SocketAddress getAddress() {
    return address;
  }

  public String getHost() {
    return address.hostName();
  }

  public int getPort() {
    return address.port();
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

  public long getUnhealthyUntilMillis() {
    return unhealthyUntilMillis;
  }

  public CrateEndpoint setUnhealthyUntilMillis(long unhealthyUntilMillis) {
    this.unhealthyUntilMillis = unhealthyUntilMillis;
    return this;
  }
}
