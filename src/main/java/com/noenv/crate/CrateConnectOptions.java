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

import com.noenv.crate.impl.CrateConnectionUriParser;
import com.noenv.crate.resolver.CrateEndpoint;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.PoolOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.endpoint.LoadBalancer;

import java.util.List;
import java.util.Map;

import static io.vertx.core.http.HttpClientOptions.DEFAULT_KEEP_ALIVE_TIMEOUT;

@DataObject
@JsonGen(publicConverter = false)
public class CrateConnectOptions {


  /**
   * Provide a {@link CrateConnectOptions} configured from a connection URI.
   *
   * @param connectionUri the connection URI to configure from
   * @return a {@link CrateConnectOptions} parsed from the connection URI
   * @throws IllegalArgumentException when the {@code connectionUri} is in an invalid format
   */
  public static CrateConnectOptions fromUri(String connectionUri) throws IllegalArgumentException {
    return new CrateConnectOptions(CrateConnectionUriParser.parse(connectionUri));
  }

  public static final String DEFAULT_HOST = "localhost";
  public static int DEFAULT_PORT = 4200;
  public static final String DEFAULT_USER = "crate";
  public static final String DEFAULT_PASSWORD = "crate";
  public static final int DEFAULT_PIPELINING_LIMIT = 10_000;
  public static final HttpVersion DEFAULT_HTTP_VERSION = HttpVersion.HTTP_2;
  public static final String DEFAULT_METRICS_NAME = "vertx-crate-client";
  public static final LoadBalancer DEFAULT_LOAD_BALANCER = LoadBalancer.ROUND_ROBIN;
  public static final SslMode DEFAULT_SSLMODE = SslMode.DISABLE;
  public static final Map<String, String> DEFAULT_PROPERTIES;
  public static final boolean DEFAULT_CACHE_PREPARED_STATEMENTS = false;
  public static final int DEFAULT_PREPARED_STATEMENTS_CACHE_SIZE = 1000;
  /** Default: failover enabled when multiple endpoints are configured. */
  public static final boolean DEFAULT_FAILOVER_ENABLED = true;
  /** Default backoff time in ms before a failed endpoint is retried (fixed). */
  public static final long DEFAULT_FAILOVER_BACKOFF_MS = 30_000L;
  /** Default max number of failover attempts per request (including first try). */
  public static final int DEFAULT_FAILOVER_MAX_RETRIES = 3;

  static {
    DEFAULT_PROPERTIES = Map.of(
      "application_name", "vertx-crate-client",
      "client_encoding", "utf8",
      "DateStyle", "ISO",
      "extra_float_digits", "2"
    );
  }


  private String user = DEFAULT_USER;
  private String password = DEFAULT_PASSWORD;
  private List<CrateEndpoint> endpoints = List.of(new CrateEndpoint(SocketAddress.inetSocketAddress(DEFAULT_PORT, DEFAULT_HOST)));
  private String metricsName = DEFAULT_METRICS_NAME;
  private int pipeliningLimit = DEFAULT_PIPELINING_LIMIT;
  private SslMode sslMode = DEFAULT_SSLMODE;
  private int keepAliveTimeout = DEFAULT_KEEP_ALIVE_TIMEOUT;
  private HttpVersion httpVersion = DEFAULT_HTTP_VERSION;
  private PoolOptions httpPoolOptions = new PoolOptions();
  private LoadBalancer loadBalancer = DEFAULT_LOAD_BALANCER;
  private boolean cachePreparedStatements = DEFAULT_CACHE_PREPARED_STATEMENTS;
  private int preparedStatementCacheSize = DEFAULT_PREPARED_STATEMENTS_CACHE_SIZE;
  private boolean failoverEnabled = DEFAULT_FAILOVER_ENABLED;
  private long failoverBackoffMs = DEFAULT_FAILOVER_BACKOFF_MS;
  private int failoverMaxRetries = DEFAULT_FAILOVER_MAX_RETRIES;

  public CrateConnectOptions() {
  }

  public CrateConnectOptions(JsonObject json) {
    CrateConnectOptionsConverter.fromJson(json, this);
  }

  public CrateConnectOptions(CrateConnectOptions other) {
    endpoints = other.endpoints;
    pipeliningLimit = other.pipeliningLimit;
    sslMode = other.sslMode;
    httpVersion = other.httpVersion;
    httpPoolOptions = other.httpPoolOptions;
    loadBalancer = other.loadBalancer;
    failoverEnabled = other.failoverEnabled;
    failoverBackoffMs = other.failoverBackoffMs;
    failoverMaxRetries = other.failoverMaxRetries;
  }

  public CrateConnectOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public CrateConnectOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  public int getPipeliningLimit() {
    return pipeliningLimit;
  }

  public CrateConnectOptions setPipeliningLimit(int pipeliningLimit) {
    if (pipeliningLimit < 1) {
      throw new IllegalArgumentException();
    }
    this.pipeliningLimit = pipeliningLimit;
    return this;
  }

  public CrateConnectOptions setHttpVersion(HttpVersion httpVersion) {
    this.httpVersion = httpVersion;
    return this;
  }

  public CrateConnectOptions setKeepAliveTimeout(int keepAliveTimeout) {
    this.keepAliveTimeout = keepAliveTimeout;
    return this;
  }

  public int getKeepAliveTimeout() {
    return keepAliveTimeout;
  }

  public HttpVersion getHttpVersion() {
    return httpVersion;
  }

  public CrateConnectOptions setHttpPoolOptions(PoolOptions httpPoolOptions) {
    this.httpPoolOptions = httpPoolOptions;
    return this;
  }

  public PoolOptions getHttpPoolOptions() {
    return httpPoolOptions;
  }

  public LoadBalancer getLoadBalancer() {
    return loadBalancer;
  }

  public void setLoadBalancer(LoadBalancer loadBalancer) {
    this.loadBalancer = loadBalancer;
  }

  /**
   * @return the value of current sslmode
   */
  public SslMode getSslMode() {
    return sslMode;
  }

  /**
   * Set {@link SslMode} for the client, this option can be used to provide different levels of secure protection.
   *
   * @param sslmode the value of sslmode
   * @return a reference to this, so the API can be used fluently
   */
  public CrateConnectOptions setSslMode(SslMode sslmode) {
    this.sslMode = sslmode;
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = JsonObject.of();
    CrateConnectOptionsConverter.toJson(this, json);
    return json;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CrateConnectOptions)) return false;
    if (!super.equals(o)) return false;

    var that = (CrateConnectOptions) o;
    return pipeliningLimit == that.pipeliningLimit &&
      sslMode == that.sslMode &&
      httpVersion == that.httpVersion &&
      httpPoolOptions.equals(that.httpPoolOptions) &&
      loadBalancer == that.loadBalancer &&
      failoverEnabled == that.failoverEnabled &&
      failoverBackoffMs == that.failoverBackoffMs &&
      failoverMaxRetries == that.failoverMaxRetries;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + pipeliningLimit;
    result = 31 * result + sslMode.hashCode();
    result = 31 * result + httpVersion.hashCode();
    result = 31 * result + httpPoolOptions.hashCode();
    result = 31 * result + loadBalancer.hashCode();
    result = 31 * result + Boolean.hashCode(failoverEnabled);
    result = 31 * result + Long.hashCode(failoverBackoffMs);
    result = 31 * result + failoverMaxRetries;
    return result;
  }

  public String getMetricsName() {
    return metricsName;
  }

  public CrateConnectOptions setMetricsName(String metricsName) {
    this.metricsName = metricsName;
    return this;
  }

  public int getPreparedStatementCacheSize() {
    return preparedStatementCacheSize;
  }

  public CrateConnectOptions setPreparedStatementCacheSize(int preparedStatementCacheSize) {
    this.preparedStatementCacheSize = preparedStatementCacheSize;
    return this;
  }

  public boolean isCachePreparedStatements() {
    return cachePreparedStatements;
  }

  public List<CrateEndpoint> getEndpoints() {
    return endpoints;
  }

  public CrateConnectOptions setEndpoints(List<CrateEndpoint> endpoints) {
    this.endpoints = endpoints;
    return this;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public boolean isFailoverEnabled() {
    return failoverEnabled;
  }

  public CrateConnectOptions setFailoverEnabled(boolean failoverEnabled) {
    this.failoverEnabled = failoverEnabled;
    return this;
  }

  public long getFailoverBackoffMs() {
    return failoverBackoffMs;
  }

  public CrateConnectOptions setFailoverBackoffMs(long failoverBackoffMs) {
    if (failoverBackoffMs < 0) {
      throw new IllegalArgumentException("failoverBackoffMs must be >= 0");
    }
    this.failoverBackoffMs = failoverBackoffMs;
    return this;
  }

  public int getFailoverMaxRetries() {
    return failoverMaxRetries;
  }

  public CrateConnectOptions setFailoverMaxRetries(int failoverMaxRetries) {
    if (failoverMaxRetries < 1) {
      throw new IllegalArgumentException("failoverMaxRetries must be >= 1");
    }
    this.failoverMaxRetries = failoverMaxRetries;
    return this;
  }

  public MultiMap getDefaultHeaders() {
    var headers = MultiMap.caseInsensitiveMultiMap()
      .add(HttpHeaders.USER_AGENT, "vertx-crate-client")
      .add(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    if (user != null && password != null) {
      headers.add(HttpHeaders.AUTHORIZATION, "Basic " + java.util.Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
    }
    return headers;
  }
}
