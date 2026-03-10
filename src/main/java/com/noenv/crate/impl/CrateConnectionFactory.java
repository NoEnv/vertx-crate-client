package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.SslMode;
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

public class CrateConnectionFactory {
  protected final HttpClientAgent agent;
  protected final ContextInternal context;
  protected final CrateConnectOptions options;

  public CrateConnectionFactory(ContextInternal context, CrateConnectOptions options) {
    this.context = context;
    this.options = options;
    this.agent = context.owner().httpClientBuilder()
      .withAddressResolver((AddressResolver<SocketAddress>) _ -> new CrateEndpointResolver(options.getEndpoints()))
      .with(createHttpClientOptions(options))
      .with(options.getHttpPoolOptions())
      .withLoadBalancer(options.getLoadBalancer())
      .build();
  }

  private HttpClientOptions createHttpClientOptions(CrateConnectOptions options) {
    return new HttpClientOptions()
      .setSsl(options.getSslMode() != SslMode.DISABLE)
      .setTrustAll(options.getSslMode() == SslMode.TRUST_ALL)
      .setVerifyHost(options.getSslMode() == SslMode.VERIFY_CA || options.getSslMode() == SslMode.VERIFY_FULL)
      .setUseAlpn(true)
      .setKeepAliveTimeout(60) // todo: expose
      .setProtocolVersion(options.getHttpVersion())
      .setPipelining(true)
      .setPipeliningLimit(options.getPipeliningLimit());
  }

  public Future<CrateHttpConnection> connect(HttpConnectOptions httpOptions) {
    return agent.connect(httpOptions)
      .map(this::wrapCrateHttpConnection)
      .onSuccess(c -> c.initSession(context));
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
