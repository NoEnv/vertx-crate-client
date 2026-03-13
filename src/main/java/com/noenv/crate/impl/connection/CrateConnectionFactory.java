package com.noenv.crate.impl.connection;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateSessionOptions;
import com.noenv.crate.SslMode;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.http.HttpClientConnection;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpConnectOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;

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
  private final CrateSessionOptions sessionOptions;

  public CrateConnectionFactory(ContextInternal context, CrateConnectOptions options) {
    this(context, options, new CrateSessionOptions());
  }
  public CrateConnectionFactory(ContextInternal context, CrateConnectOptions options, CrateSessionOptions sessionOptions) {
    this.context = context;
    this.options = options;
    this.sessionOptions = sessionOptions;
    this.endpointSelector = EndpointSelector.from(options.getLoadBalancer());
    this.endpoints = new ArrayList<>();
    for (SocketAddress sa : options.getEndpoints()) {
      endpoints.add(CrateEndpoint.create(sa, context, options));
    }
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Connection factory created with %d endpoint(s)", endpoints.size()));
    }
  }

  public CrateConnectOptions getOptions() {
    return options;
  }

  /** Returns the number of endpoints currently considered healthy (for backoff policy: last healthy gets short backoff). */
  public int getHealthyEndpointCount() {
    return (int) endpoints.stream().filter(CrateEndpoint::isHealthy).count();
  }

  public Future<CrateHttpConnection> connect() {
    return tryConnect(options.getFailoverMaxRetries());
  }

  private Future<CrateHttpConnection> tryConnect(int remaining) {
    if (remaining <= 0) {
      logger.error("No healthy endpoints available after exhausting failover attempts");
      return context.failedFuture(new RuntimeException("No healthy endpoints available after failover attempts"));
    }
    var healthy = endpoints.stream().filter(CrateEndpoint::isHealthy).collect(Collectors.toList());
    if (healthy.isEmpty()) {
      logger.error("No healthy endpoints available (all endpoints may be in backoff)");
      return context.failedFuture(new RuntimeException("No healthy endpoints available"));
    }
    CrateEndpoint chosen = endpointSelector.select(healthy);
    // When this is the last healthy endpoint and it fails, use short random backoff (1–10s) so we can retry sooner when all are down
    boolean isLastHealthy = healthy.size() == 1;
    logger.debug(String.format("Failover: connecting to endpoint %s:%d (healthy endpoints: %d, attempts left: %d)",
      chosen.getHost(), chosen.getPort(), healthy.size(), remaining));
    HttpClientAgent agent = chosen.getAgent();
    var opts = new HttpConnectOptions()
      .setHost(chosen.getHost())
      .setPort(chosen.getPort())
      .setSsl(options.getSslMode() != SslMode.DISABLE);
    return agent.connect(opts)
      .map(conn -> wrapCrateHttpConnection(conn, chosen))
      .compose(c -> {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("Connected to %s:%d", chosen.getHost(), chosen.getPort()));
        }
        return c.initSession(context, sessionOptions)
          .map(c);
      })
      .recover(err -> {
        logger.warn(String.format("Failed to connect to endpoint %s:%d. Remaining failover attempts: %d. Error: %s", chosen.getHost(), chosen.getPort(), remaining - 1, err.toString()));
        if (CrateFailoverPredicate.isFailoverError(err)) {
          chosen.markUnhealthy(options.computeFailoverBackoffMs(isLastHealthy));
          logger.debug(String.format("Failover: marked endpoint %s:%d unhealthy, trying next endpoint (attempts left: %d)",
            chosen.getHost(), chosen.getPort(), remaining - 1));
          return tryConnect(remaining - 1);
        }
        return context.failedFuture(err);
      });
  }

  @SuppressWarnings("unchecked")
  private CrateHttpConnection wrapCrateHttpConnection(HttpClientConnection c, CrateEndpoint endpoint) {
    io.vertx.core.spi.metrics.VertxMetrics vertxMetrics = context.owner().metrics();
    ClientMetrics<?, HttpClientRequest, HttpClientResponse> metrics = vertxMetrics == null
      ? null
      : (ClientMetrics<?, HttpClientRequest, HttpClientResponse>)
          vertxMetrics.createClientMetrics(c.remoteAddress(), "sql", options.getMetricsName());
    return new CrateHttpConnection(c, metrics, options, endpoint);
  }

  public Future<Void> shutdown(long timeout, TimeUnit unit) {
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Shutting down connection factory (%d endpoint(s), timeout=%d %s)", endpoints.size(), timeout, unit));
    }
    List<Future<Void>> shutdowns = new ArrayList<>(endpoints.size());
    for (CrateEndpoint e : endpoints) {
      shutdowns.add(e.getAgent().shutdown(timeout, unit));
    }
    return Future.all(shutdowns).mapEmpty();
  }
}
