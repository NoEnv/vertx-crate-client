package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.SslMode;
import com.noenv.crate.resolver.CrateEndpoint;
import com.noenv.crate.resolver.CrateEndpointResolver;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.http.HttpClientConnection;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpConnectOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.AddressResolver;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CrateConnectionFactory {
  protected final HttpClientAgent agent;
  protected final ContextInternal context;
  protected final CrateConnectOptions options;

  public CrateConnectionFactory(ContextInternal context, CrateConnectOptions options) {
    this.context = context;
    this.options = options;
    this.agent = context.owner().httpClientBuilder()
      .withAddressResolver((AddressResolver<SocketAddress>) vertx -> new CrateEndpointResolver(options.getEndpoints()))
      .with(createHttpClientOptions(options))
      .with(options.getHttpPoolOptions())
      .withLoadBalancer(options.getLoadBalancer())
      .build();
  }

  /** Mark the endpoint at the given address as unhealthy for the configured backoff period. */
  public void markEndpointUnhealthy(SocketAddress address) {
    long backoffMs = options.getFailoverBackoffMs();
    for (CrateEndpoint e : options.getEndpoints()) {
      if (e.getAddress().equals(address)) {
        e.markUnhealthy(backoffMs);
        return;
      }
    }
  }

  private HttpClientOptions createHttpClientOptions(CrateConnectOptions options) {
    return new HttpClientOptions()
      .setSsl(options.getSslMode() != SslMode.DISABLE)
      .setTrustAll(options.getSslMode() == SslMode.TRUST_ALL)
      .setVerifyHost(options.getSslMode() == SslMode.VERIFY_CA || options.getSslMode() == SslMode.VERIFY_FULL)
      .setUseAlpn(true)
      .setKeepAliveTimeout(options.getKeepAliveTimeout())
      .setProtocolVersion(options.getHttpVersion())
      .setPipelining(true)
      .setPipeliningLimit(options.getPipeliningLimit());
  }

  public Future<CrateHttpConnection> connect(HttpConnectOptions httpOptions) {
    return agent.connect(httpOptions)
      .map(this::wrapCrateHttpConnection)
      .onSuccess(c -> c.initSession(context));
  }

  /**
   * Try to connect to one of the healthy endpoints, with failover: on failover error
   * mark the endpoint unhealthy and try the next, up to {@link CrateConnectOptions#getFailoverMaxRetries} attempts.
   */
  public Future<CrateHttpConnection> connectWithFailover() {
    return tryConnect(options.getFailoverMaxRetries());
  }

  private Future<CrateHttpConnection> tryConnect(int remaining) {
    if (remaining <= 0) {
      return context.failedFuture(new RuntimeException("No healthy endpoints available after failover attempts"));
    }
    var healthy = options.getEndpoints().stream().filter(CrateEndpoint::isHealthy).collect(Collectors.toList());
    if (healthy.isEmpty()) {
      return context.failedFuture(new RuntimeException("No healthy endpoints available"));
    }
    var first = healthy.get(0);
    var opts = new HttpConnectOptions()
      .setHost(first.getAddress().host())
      .setPort(first.getAddress().port())
      .setSsl(options.getSslMode() != SslMode.DISABLE);
    return agent.connect(opts)
      .map(this::wrapCrateHttpConnection)
      .onSuccess(c -> c.initSession(context))
      .recover(err -> {
        if (CrateFailoverPredicate.isFailoverError(err)) {
          first.markUnhealthy(options.getFailoverBackoffMs());
          return tryConnect(remaining - 1);
        }
        return context.failedFuture(err);
      });
  }

  private CrateHttpConnection wrapCrateHttpConnection(HttpClientConnection c) {
    VertxMetrics vertxMetrics = context.owner().metrics();
    ClientMetrics<?,?,?> metrics = vertxMetrics == null
      ? null
      : vertxMetrics.createClientMetrics(c.remoteAddress(), "sql", options.getMetricsName());
    return new CrateHttpConnection(
      c,
      metrics,
      options,
      context
    );
  }

  public Future<Void> shutdown(long timeout, TimeUnit unit) {
    return agent.shutdown(timeout, unit);
  }
}
