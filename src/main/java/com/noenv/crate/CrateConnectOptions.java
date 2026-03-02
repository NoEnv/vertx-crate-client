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
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.Unstable;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ClientSSLOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.sqlclient.SqlConnectOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.lang.Integer.parseInt;
import static java.lang.System.getenv;

@DataObject
@JsonGen(publicConverter = false)
public class CrateConnectOptions extends SqlConnectOptions {

  /**
   * @return the {@code options} as CrateDB specific connect options
   */
  public static CrateConnectOptions wrap(SqlConnectOptions options) {
    if (options instanceof CrateConnectOptions) {
      return (CrateConnectOptions) options;
    } else {
      return new CrateConnectOptions(options);
    }
  }

  /**
   * Provide a {@link CrateConnectOptions} configured from a connection URI.
   *
   * @param connectionUri the connection URI to configure from
   * @return a {@link CrateConnectOptions} parsed from the connection URI
   * @throws IllegalArgumentException when the {@code connectionUri} is in an invalid format
   */
  public static CrateConnectOptions fromUri(String connectionUri) throws IllegalArgumentException {
    JsonObject parsedConfiguration = CrateConnectionUriParser.parse(connectionUri);
    return new CrateConnectOptions(parsedConfiguration);
  }

  /**
   * Provide a {@link CrateConnectOptions} configured with environment variables, if the environment variable
   * is not set, then a default value will take precedence over this.
   */
  public static CrateConnectOptions fromEnv() {
    CrateConnectOptions crateConnectOptions = new CrateConnectOptions();

    if (getenv("CRATEHOSTADDR") == null) {
      if (getenv("CRATEHOST") != null) {
        crateConnectOptions.setHost(getenv("CRATEHOST"));
      }
    } else {
      crateConnectOptions.setHost(getenv("CRATEHOSTADDR"));
    }

    if (getenv("CRATEPORT") != null) {
      try {
        crateConnectOptions.setPort(parseInt(getenv("CRATEPORT")));
      } catch (NumberFormatException e) {
        // port will be set to default
      }
    }

    if (getenv("CRATEDATABASE") != null) {
      crateConnectOptions.setDatabase(getenv("CRATEDATABASE"));
    }
    if (getenv("CRATEUSER") != null) {
      crateConnectOptions.setUser(getenv("CRATEUSER"));
    }
    if (getenv("CRATEPASSWORD") != null) {
      crateConnectOptions.setPassword(getenv("CRATEPASSWORD"));
    }
    if (getenv("CRATESSLMODE") != null) {
      crateConnectOptions.setSslMode(SslMode.of(getenv("CRATESSLMODE")));
    }
    return crateConnectOptions;
  }

  public static final String DEFAULT_HOST = "localhost";
  public static int DEFAULT_PORT = 4200;
  public static final String DEFAULT_DATABASE = "doc";
  public static final String DEFAULT_USER = "crate";
  public static final String DEFAULT_PASSWORD = "crate";
  public static final int DEFAULT_PIPELINING_LIMIT = 256;
  public static final SslMode DEFAULT_SSLMODE = SslMode.DISABLE;
  public static final boolean DEFAULT_USE_LAYER_7_PROXY = false;
  public static final Map<String, String> DEFAULT_PROPERTIES;

  static {
    DEFAULT_PROPERTIES = Map.of(
      "application_name", "vertx-crate-client",
      "client_encoding", "utf8",
      "DateStyle", "ISO",
      "extra_float_digits", "2"
    );
  }

  private int pipeliningLimit = DEFAULT_PIPELINING_LIMIT;
  private SslMode sslMode = DEFAULT_SSLMODE;
  private boolean useLayer7Proxy = DEFAULT_USE_LAYER_7_PROXY;

  public CrateConnectOptions() {
    super();
  }

  public CrateConnectOptions(JsonObject json) {
    super(json);
    CrateConnectOptionsConverter.fromJson(json, this);
  }

  public CrateConnectOptions(SqlConnectOptions other) {
    super(other);
    if (other instanceof CrateConnectOptions) {
      CrateConnectOptions opts = (CrateConnectOptions) other;
      pipeliningLimit = opts.pipeliningLimit;
      sslMode = opts.sslMode;
    }
  }

  public CrateConnectOptions(CrateConnectOptions other) {
    super(other);
    pipeliningLimit = other.pipeliningLimit;
    sslMode = other.sslMode;
  }

  @Override
  public CrateConnectOptions setHost(String host) {
    return (CrateConnectOptions) super.setHost(host);
  }

  @Override
  public CrateConnectOptions setPort(int port) {
    return (CrateConnectOptions) super.setPort(port);
  }

  @Override
  public CrateConnectOptions setUser(String user) {
    return (CrateConnectOptions) super.setUser(user);
  }

  @Override
  public CrateConnectOptions setPassword(String password) {
    return (CrateConnectOptions) super.setPassword(password);
  }

  @Override
  public CrateConnectOptions setDatabase(String database) {
    return (CrateConnectOptions) super.setDatabase(database);
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

  @Override
  public CrateConnectOptions setCachePreparedStatements(boolean cachePreparedStatements) {
    return (CrateConnectOptions) super.setCachePreparedStatements(cachePreparedStatements);
  }

  @Override
  public CrateConnectOptions setPreparedStatementCacheMaxSize(int preparedStatementCacheMaxSize) {
    return (CrateConnectOptions) super.setPreparedStatementCacheMaxSize(preparedStatementCacheMaxSize);
  }

  @GenIgnore
  @Override
  public CrateConnectOptions setPreparedStatementCacheSqlFilter(Predicate<String> predicate) {
    return (CrateConnectOptions) super.setPreparedStatementCacheSqlFilter(predicate);
  }

  @Override
  public CrateConnectOptions setPreparedStatementCacheSqlLimit(int preparedStatementCacheSqlLimit) {
    return (CrateConnectOptions) super.setPreparedStatementCacheSqlLimit(preparedStatementCacheSqlLimit);
  }

  @Override
  public CrateConnectOptions setProperties(Map<String, String> properties) {
    return (CrateConnectOptions) super.setProperties(properties);
  }

  @GenIgnore
  @Override
  public CrateConnectOptions addProperty(String key, String value) {
    return (CrateConnectOptions) super.addProperty(key, value);
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

  /**
   * @return whether the client interacts with a layer 7 proxy instead of a server
   */
  @Unstable
  public boolean getUseLayer7Proxy() {
    return useLayer7Proxy;
  }

  /**
   * Set the client to use a layer 7 (application) proxy compatible protocol, set to {@code true} when the client
   * interacts with a layer 7 proxy like PgBouncer instead of a server. Prepared statement caching must be disabled.
   *
   * @param useLayer7Proxy whether to use a layer 7 proxy instead of a server
   * @return a reference to this, so the API can be used fluently
   */
  @Unstable
  public CrateConnectOptions setUseLayer7Proxy(boolean useLayer7Proxy) {
    this.useLayer7Proxy = useLayer7Proxy;
    return this;
  }

  @Override
  public CrateConnectOptions setReconnectAttempts(int attempts) {
    return (CrateConnectOptions)super.setReconnectAttempts(attempts);
  }

  @Override
  public CrateConnectOptions setReconnectInterval(long interval) {
    return (CrateConnectOptions)super.setReconnectInterval(interval);
  }

  @Override
  public CrateConnectOptions setTracingPolicy(TracingPolicy tracingPolicy) {
    return (CrateConnectOptions) super.setTracingPolicy(tracingPolicy);
  }

  @Override
  public CrateConnectOptions setSslOptions(ClientSSLOptions sslOptions) {
    return (CrateConnectOptions) super.setSslOptions(sslOptions);
  }

  /**
   * Initialize with the default options.
   */
  @Override
  protected void init() {
    super.init();
    this.setHost(DEFAULT_HOST);
    this.setPort(DEFAULT_PORT);
    this.setUser(DEFAULT_USER);
    this.setPassword(DEFAULT_PASSWORD);
    this.setDatabase(DEFAULT_DATABASE);
    this.setMetricsName(DEFAULT_METRICS_NAME);
    this.setProperties(new HashMap<>(DEFAULT_PROPERTIES));
  }

  @Override
  public JsonObject toJson() {
    JsonObject json = super.toJson();
    CrateConnectOptionsConverter.toJson(this, json);
    return json;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CrateConnectOptions)) return false;
    if (!super.equals(o)) return false;

    CrateConnectOptions that = (CrateConnectOptions) o;

    if (pipeliningLimit != that.pipeliningLimit) return false;
    if (sslMode != that.sslMode) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + pipeliningLimit;
    result = 31 * result + sslMode.hashCode();
    return result;
  }

  @Override
  public CrateConnectOptions merge(JsonObject other) {
    JsonObject json = toJson();
    json.mergeIn(other);
    return new CrateConnectOptions(json);
  }
}
