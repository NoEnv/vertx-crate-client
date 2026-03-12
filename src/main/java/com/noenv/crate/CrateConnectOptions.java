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
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientOptions;
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
  private List<SocketAddress> endpoints = List.of(SocketAddress.inetSocketAddress(DEFAULT_PORT, DEFAULT_HOST));
  private String metricsName = DEFAULT_METRICS_NAME;
  private SslMode sslMode = DEFAULT_SSLMODE;
  private HttpClientOptions httpClientOptions = defaultHttpClientOptions();
  private PoolOptions httpPoolOptions = new PoolOptions();
  private LoadBalancer loadBalancer = DEFAULT_LOAD_BALANCER;
  private boolean cachePreparedStatements = DEFAULT_CACHE_PREPARED_STATEMENTS;
  private int preparedStatementCacheSize = DEFAULT_PREPARED_STATEMENTS_CACHE_SIZE;
  private long failoverBackoffMs = DEFAULT_FAILOVER_BACKOFF_MS;
  private int failoverMaxRetries = DEFAULT_FAILOVER_MAX_RETRIES;

  public CrateConnectOptions() {
  }

  public CrateConnectOptions(JsonObject json) {
    CrateConnectOptionsConverter.fromJson(json, this);
  }

  public CrateConnectOptions(CrateConnectOptions other) {
    endpoints = other.endpoints;
    sslMode = other.sslMode;
    httpPoolOptions = other.httpPoolOptions;
    httpClientOptions = other.httpClientOptions;
    loadBalancer = other.loadBalancer;
    failoverBackoffMs = other.failoverBackoffMs;
    failoverMaxRetries = other.failoverMaxRetries;
    preparedStatementCacheSize = other.preparedStatementCacheSize;
    cachePreparedStatements = other.cachePreparedStatements;
    user = other.user;
    password = other.password;
    metricsName = other.metricsName;
  }


  private static boolean isSsl(SslMode sslMode) {
    return sslMode != SslMode.DISABLE;
  }

  private static boolean isTrustAll(SslMode sslMode) {
    return sslMode == SslMode.TRUST_ALL;
  }

  private static boolean isVerifyHost(SslMode sslMode) {
    return sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_FULL;
  }

  public static HttpClientOptions defaultHttpClientOptions() {
    return new HttpClientOptions()
      .setSsl(isSsl(DEFAULT_SSLMODE))
      .setTrustAll(isTrustAll(DEFAULT_SSLMODE))
      .setVerifyHost(isVerifyHost(DEFAULT_SSLMODE))
      .setUseAlpn(true)
      .setKeepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT)
      .setProtocolVersion(DEFAULT_HTTP_VERSION)
      .setPipelining(true)
      .setPipeliningLimit(DEFAULT_PIPELINING_LIMIT);
  }

  public HttpClientOptions getHttpClientOptions() {
    return httpClientOptions;
  }

  public CrateConnectOptions setHttpClientOptions(HttpClientOptions httpClientOptions) {
    this.httpClientOptions = httpClientOptions;
    return this;
  }

  public CrateConnectOptions setCachePreparedStatements(boolean cachePreparedStatements) {
    this.cachePreparedStatements = cachePreparedStatements;
    return this;
  }

  public CrateConnectOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public CrateConnectOptions setPassword(String password) {
    this.password = password;
    return this;
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
    return sslMode == that.sslMode &&
      httpClientOptions.equals(that.httpClientOptions) &&
      httpPoolOptions.equals(that.httpPoolOptions) &&
      loadBalancer == that.loadBalancer &&
      failoverBackoffMs == that.failoverBackoffMs &&
      failoverMaxRetries == that.failoverMaxRetries;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + sslMode.hashCode();
    result = 31 * result + httpClientOptions.hashCode();
    result = 31 * result + httpPoolOptions.hashCode();
    result = 31 * result + loadBalancer.hashCode();
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

  public List<SocketAddress> getEndpoints() {
    return endpoints;
  }

  public CrateConnectOptions setEndpoints(List<SocketAddress> endpoints) {
    this.endpoints = endpoints;
    return this;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
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
