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
package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.SslMode;
import io.vertx.core.Future;
import io.vertx.core.http.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.net.*;
import io.vertx.core.net.endpoint.LoadBalancer;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class CrateConnectionFactory {
  protected final HttpClientAgent agent;
  protected final ContextInternal context;
  protected final CrateConnectOptions options;

  private static final Logger LOG = LoggerFactory.getLogger(CrateConnectionFactory.class);

  public CrateConnectionFactory(ContextInternal context, CrateConnectOptions options) {
    this.context = context;
    this.options = options;
    this.agent = context.owner().httpClientBuilder()
      .with(new HttpClientOptions()
        .setSsl(options.getSslMode() != SslMode.DISABLE)
        .setTrustAll(options.getSslMode() == SslMode.TRUST_ALL)
        .setVerifyHost(options.getSslMode() == SslMode.VERIFY_CA || options.getSslMode() == SslMode.VERIFY_FULL)
        .setPipelining(true)
        .setPipeliningLimit(8)
        .setKeepAliveTimeout(60)
        .setProtocolVersion(HttpVersion.HTTP_1_1)
      )
      .with(new PoolOptions()
        .setHttp1MaxSize(12)
        .setMaxWaitQueueSize(256))
      .withLoadBalancer(LoadBalancer.ROUND_ROBIN)
      .build();
  }

  public Future<CrateHttpConnection> connect(HttpConnectOptions httpOptions) {
    LOG.warn("New CrateClient connect");
    boolean cachePreparedStatements = options.getCachePreparedStatements();
    int preparedStatementCacheMaxSize = options.getPreparedStatementCacheMaxSize();
    Predicate<String> preparedStatementCacheSqlFilter = options.getPreparedStatementCacheSqlFilter();
    int pipeliningLimit = options.getPipeliningLimit();
    VertxMetrics vertxMetrics = context.owner().metrics();
    ClientMetrics metrics = vertxMetrics != null
      ? vertxMetrics.createClientMetrics(options.getSocketAddress(), "sql", options.getMetricsName())
      : null;

    CrateHttpConnection conn = new CrateHttpConnection(
      agent, httpOptions, metrics, options,
      cachePreparedStatements, preparedStatementCacheMaxSize,
      preparedStatementCacheSqlFilter, pipeliningLimit, context
    );

    return conn.initSession(context).map(v -> conn);
  }

  public Future<Void> shutdown(long timeout, TimeUnit unit) {
    return agent.shutdown(timeout, unit);
  }
}
