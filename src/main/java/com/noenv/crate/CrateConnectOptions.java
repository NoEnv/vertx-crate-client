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

import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.impl.CrateConnectionUriParser;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.QueryStringEncoder;
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
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

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
  public static final String DEFAULT_USER = null;
  public static final String DEFAULT_PASSWORD = null;
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
  /** Default min ms for initial backoff (first failed endpoint in a round); random between min and max for quicker recovery. */
  public static final long DEFAULT_FAILOVER_INITIAL_BACKOFF_MIN_MS = 1_000L;
  /** Default max ms for initial backoff (first failed endpoint in a round). */
  public static final long DEFAULT_FAILOVER_INITIAL_BACKOFF_MAX_MS = 10_000L;
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
  private long failoverInitialBackoffMinMs = DEFAULT_FAILOVER_INITIAL_BACKOFF_MIN_MS;
  private long failoverInitialBackoffMaxMs = DEFAULT_FAILOVER_INITIAL_BACKOFF_MAX_MS;
  private int failoverMaxRetries = DEFAULT_FAILOVER_MAX_RETRIES;
  /** Default schema for SQL requests (CrateDB {@code Default-Schema} header). If null, CrateDB uses {@code doc}. */
  private String defaultSchema = null;
  /** Request column type IDs in response (CrateDB {@code types} query param). */
  private boolean includeColumnTypes = false;
  /** Request error stack trace in error responses (CrateDB {@code error_trace} query param). */
  private boolean includeErrorTrace = false;

  /** HTTP header name for default schema (CrateDB). */
  public static final String HEADER_DEFAULT_SCHEMA = "Default-Schema";

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
    failoverInitialBackoffMinMs = other.failoverInitialBackoffMinMs;
    failoverInitialBackoffMaxMs = other.failoverInitialBackoffMaxMs;
    failoverMaxRetries = other.failoverMaxRetries;
    preparedStatementCacheSize = other.preparedStatementCacheSize;
    cachePreparedStatements = other.cachePreparedStatements;
    user = other.user;
    password = other.password;
    metricsName = other.metricsName;
    defaultSchema = other.defaultSchema;
    includeColumnTypes = other.includeColumnTypes;
    includeErrorTrace = other.includeErrorTrace;
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
      Objects.equals(httpClientOptions, that.httpClientOptions) &&
      Objects.equals(httpPoolOptions, that.httpPoolOptions) &&
      loadBalancer == that.loadBalancer &&
      failoverBackoffMs == that.failoverBackoffMs &&
      failoverInitialBackoffMinMs == that.failoverInitialBackoffMinMs &&
      failoverInitialBackoffMaxMs == that.failoverInitialBackoffMaxMs &&
      failoverMaxRetries == that.failoverMaxRetries &&
      Objects.equals(defaultSchema, that.defaultSchema) &&
      includeColumnTypes == that.includeColumnTypes &&
      includeErrorTrace == that.includeErrorTrace;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + sslMode.hashCode();
    result = 31 * result + (httpClientOptions != null ? httpClientOptions.hashCode() : 0);
    result = 31 * result + (httpPoolOptions != null ? httpPoolOptions.hashCode() : 0);
    result = 31 * result + loadBalancer.hashCode();
    result = 31 * result + Long.hashCode(failoverBackoffMs);
    result = 31 * result + Long.hashCode(failoverInitialBackoffMinMs);
    result = 31 * result + Long.hashCode(failoverInitialBackoffMaxMs);
    result = 31 * result + failoverMaxRetries;
    result = 31 * result + (defaultSchema != null ? defaultSchema.hashCode() : 0);
    result = 31 * result + Boolean.hashCode(includeColumnTypes);
    result = 31 * result + Boolean.hashCode(includeErrorTrace);
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

  public long getFailoverInitialBackoffMinMs() {
    return failoverInitialBackoffMinMs;
  }

  /**
   * Set the minimum backoff time in ms when the last healthy endpoint fails (all endpoints unavailable).
   * When set with {@link #setFailoverInitialBackoffMaxMs}, that endpoint gets a random backoff in [min, max]
   * so we can retry sooner (e.g. 1–10 seconds instead of waiting the full {@link #getFailoverBackoffMs}).
   * Other failed endpoints keep the fixed {@link #getFailoverBackoffMs}.
   *
   * @param failoverInitialBackoffMinMs min ms (must be &gt;= 0 and &lt;= failoverInitialBackoffMaxMs)
   * @return a reference to this, so the API can be used fluently
   */
  public CrateConnectOptions setFailoverInitialBackoffMinMs(long failoverInitialBackoffMinMs) {
    if (failoverInitialBackoffMinMs < 0) {
      throw new IllegalArgumentException("failoverInitialBackoffMinMs must be >= 0");
    }
    if (failoverInitialBackoffMinMs > this.failoverInitialBackoffMaxMs) {
      throw new IllegalArgumentException("failoverInitialBackoffMinMs must be <= failoverInitialBackoffMaxMs");
    }
    this.failoverInitialBackoffMinMs = failoverInitialBackoffMinMs;
    return this;
  }

  public long getFailoverInitialBackoffMaxMs() {
    return failoverInitialBackoffMaxMs;
  }

  /**
   * Set the maximum backoff time in ms for the first endpoint that fails in a failover round.
   *
   * @param failoverInitialBackoffMaxMs max ms (must be &gt;= failoverInitialBackoffMinMs)
   * @return a reference to this, so the API can be used fluently
   */
  public CrateConnectOptions setFailoverInitialBackoffMaxMs(long failoverInitialBackoffMaxMs) {
    if (failoverInitialBackoffMaxMs < this.failoverInitialBackoffMinMs) {
      throw new IllegalArgumentException("failoverInitialBackoffMaxMs must be >= failoverInitialBackoffMinMs");
    }
    this.failoverInitialBackoffMaxMs = failoverInitialBackoffMaxMs;
    return this;
  }

  /**
   * Returns the backoff time in ms to use when marking an endpoint unhealthy.
   * When this is the last healthy endpoint (so all will be unavailable), returns a random value between
   * {@link #getFailoverInitialBackoffMinMs} and {@link #getFailoverInitialBackoffMaxMs} so we can retry sooner;
   * otherwise returns {@link #getFailoverBackoffMs}.
   *
   * @param isLastHealthyEndpoint true if this was the only healthy endpoint (all endpoints now unavailable)
   * @return backoff time in milliseconds
   */
  public long computeFailoverBackoffMs(boolean isLastHealthyEndpoint) {
    if (isLastHealthyEndpoint && failoverInitialBackoffMinMs >= 0 && failoverInitialBackoffMaxMs >= failoverInitialBackoffMinMs) {
      long min = failoverInitialBackoffMinMs;
      long max = failoverInitialBackoffMaxMs;
      if (min == max) {
        return min;
      }
      return ThreadLocalRandom.current().nextLong(min, max + 1);
    }
    return failoverBackoffMs;
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

  /**
   * Returns the default HTTP headers for requests (user-agent, content-type, auth).
   */
  public MultiMap getDefaultHeaders() {
    var headers = MultiMap.caseInsensitiveMultiMap()
      .add(HttpHeaders.USER_AGENT, "vertx-crate-client")
      .add(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    if (user != null && password != null) {
      headers.add(HttpHeaders.AUTHORIZATION, "Basic " + java.util.Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
    }
    return headers;
  }

  /**
   * Headers for a single request. Default schema: query override then connection default.
   *
   * @param query the query for this request, or null
   * @return headers to use for the request
   */
  public MultiMap getRequestHeaders(CrateQuery query) {
    MultiMap headers = getDefaultHeaders();
    String schema = effectiveDefaultSchema(query);
    if (schema != null && !schema.isEmpty()) {
      headers = MultiMap.caseInsensitiveMultiMap().addAll(headers).set(HEADER_DEFAULT_SCHEMA, schema);
    }
    return headers;
  }

  private String effectiveDefaultSchema(CrateQuery query) {
    if (query != null && query.getQueryOptions() != null) {
      String s = query.getQueryOptions().getDefaultSchema();
      if (s != null && !s.isEmpty()) {
        return s;
      }
    }
    return defaultSchema;
  }

  /**
   * URI for the SQL endpoint, including optional query params {@code types} and {@code error_trace}
   * from query overrides then connection defaults.
   *
   * @param query the query for this request, or null
   * @return path and query string, e.g. {@code /_sql} or {@code /_sql?types} or {@code /_sql?error_trace=true}
   */
  public String getSqlRequestUri(CrateQuery query) {
    boolean types = effectiveIncludeColumnTypes(query);
    boolean errorTrace = effectiveIncludeErrorTrace(query);

    QueryStringEncoder encoder = new QueryStringEncoder("/_sql");
    if (types) {
      encoder.addParam("types", null);
    }
    if (errorTrace) {
      encoder.addParam("error_trace", "true");
    }
    return encoder.toString();
  }

  private boolean effectiveIncludeColumnTypes(CrateQuery query) {
    if (query != null && query.getQueryOptions() != null && query.getQueryOptions().getIncludeColumnTypes() != null) {
      return query.getQueryOptions().getIncludeColumnTypes();
    }
    return includeColumnTypes;
  }

  private boolean effectiveIncludeErrorTrace(CrateQuery query) {
    if (query != null && query.getQueryOptions() != null && query.getQueryOptions().getIncludeErrorTrace() != null) {
      return query.getQueryOptions().getIncludeErrorTrace();
    }
    return includeErrorTrace;
  }

  public String getDefaultSchema() {
    return defaultSchema;
  }

  /**
   * Set the default schema for SQL requests (CrateDB {@code Default-Schema} HTTP header).
   * When not set, CrateDB uses the {@code doc} schema.
   *
   * @param defaultSchema the schema name, or null to use CrateDB default
   * @return a reference to this, so the API can be used fluently
   * @see <a href="https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#default-schema">Default schema (CrateDB)</a>
   */
  public CrateConnectOptions setDefaultSchema(String defaultSchema) {
    this.defaultSchema = defaultSchema;
    return this;
  }

  /**
   * Whether to request column type IDs in the response (CrateDB {@code types} query param).
   * When true, the response includes a {@code col_types} array with data type IDs per column.
   *
   * @return true to request column types (default false)
   * @see <a href="https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#column-types">Column types (CrateDB)</a>
   */
  public boolean isIncludeColumnTypes() {
    return includeColumnTypes;
  }

  public CrateConnectOptions setIncludeColumnTypes(boolean includeColumnTypes) {
    this.includeColumnTypes = includeColumnTypes;
    return this;
  }

  /**
   * Whether to request error stack trace in error responses (CrateDB {@code error_trace} query param).
   * Intended for debugging; client libraries typically should not enable this by default.
   *
   * @return true to request error_trace in errors (default false)
   * @see <a href="https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#error-handling">Error handling (CrateDB)</a>
   */
  public boolean isIncludeErrorTrace() {
    return includeErrorTrace;
  }

  public CrateConnectOptions setIncludeErrorTrace(boolean includeErrorTrace) {
    this.includeErrorTrace = includeErrorTrace;
    return this;
  }
}
