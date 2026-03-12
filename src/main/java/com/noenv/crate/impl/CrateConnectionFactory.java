package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.SslMode;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.http.HttpClientConnection;
import io.vertx.core.http.HttpConnectOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Holds several {@link CrateEndpoint}s (each has its state and its own agent with separate pool
 * and load balancer). Does failover and load balancing across these endpoints using the configured
 * backoff and {@link EndpointSelector}.
 */
public class CrateConnectionFactory {
  private final List<CrateEndpoint> endpoints;
  private final ContextInternal context;
  private final CrateConnectOptions options;
  private final EndpointSelector endpointSelector;

  private static final Logger logger = LoggerFactory.getLogger(CrateConnectionFactory.class);

  public CrateConnectionFactory(ContextInternal context, CrateConnectOptions options) {
    this.context = context;
    this.options = options;
    this.endpointSelector = EndpointSelector.from(options.getLoadBalancer());
    this.endpoints = new ArrayList<>();
    for (SocketAddress sa : options.getEndpoints()) {
      endpoints.add(CrateEndpoint.create(sa, context, options));
    }
  }

  public CrateConnectOptions getOptions() {
    return options;
  }

  public Future<CrateHttpConnection> connect() {
    return tryConnect(options.getFailoverMaxRetries());
  }

  private Future<CrateHttpConnection> tryConnect(int remaining) {
    if (remaining <= 0) {
      return context.failedFuture(new RuntimeException("No healthy endpoints available after failover attempts"));
    }
    var healthy = endpoints.stream().filter(CrateEndpoint::isHealthy).collect(Collectors.toList());
    if (healthy.isEmpty()) {
      return context.failedFuture(new RuntimeException("No healthy endpoints available"));
    }
    CrateEndpoint chosen = endpointSelector.select(healthy);
    HttpClientAgent agent = chosen.getAgent();
    var opts = new HttpConnectOptions()
      .setHost(chosen.getHost())
      .setPort(chosen.getPort())
      .setSsl(options.getSslMode() != SslMode.DISABLE);
    return agent.connect(opts)
      .map(conn -> wrapCrateHttpConnection(conn, chosen))
      .onSuccess(c -> c.initSession(context))
      .recover(err -> {
        logger.warn(String.format("Failed to connect to endpoint %s:%d. Remaining failover attempts: %d. Error: %s", chosen.getHost(), chosen.getPort(), remaining - 1, err.toString()));
        if (CrateFailoverPredicate.isFailoverError(err)) {
          chosen.markUnhealthy(options.getFailoverBackoffMs());
          return tryConnect(remaining - 1);
        }
        return context.failedFuture(err);
      });
  }

  private CrateHttpConnection wrapCrateHttpConnection(HttpClientConnection c, CrateEndpoint endpoint) {
    VertxMetrics vertxMetrics = context.owner().metrics();
    ClientMetrics<?, ?, ?> metrics = vertxMetrics == null
      ? null
      : vertxMetrics.createClientMetrics(c.remoteAddress(), "sql", options.getMetricsName());
    return new CrateHttpConnection(c, metrics, options, context, endpoint);
  }

  public Future<Void> shutdown(long timeout, TimeUnit unit) {
    List<Future<Void>> shutdowns = new ArrayList<>(endpoints.size());
    for (CrateEndpoint e : endpoints) {
      shutdowns.add(e.getAgent().shutdown(timeout, unit));
    }
    return Future.all(shutdowns).mapEmpty();
  }
}
