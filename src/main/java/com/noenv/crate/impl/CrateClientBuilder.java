package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateConnection;
import io.vertx.core.Vertx;
import io.vertx.core.http.PoolOptions;
import io.vertx.core.internal.ContextInternal;

/**
 * Fluent builder for a pool-backed {@link CrateConnection}.
 */
public class CrateClientBuilder {

  private Vertx vertx;
  private CrateConnectOptions connectOptions;
  private PoolOptions poolOptions = new PoolOptions().setHttp1MaxSize(12);

  public CrateClientBuilder using(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  public CrateClientBuilder connectingTo(CrateConnectOptions options) {
    this.connectOptions = options;
    return this;
  }

  public CrateClientBuilder connectingTo(String host, int port) {
    this.connectOptions = new CrateConnectOptions()
      .setHost(host)
      .setPort(port);
    return this;
  }

  public CrateClientBuilder with(PoolOptions options) {
    this.poolOptions = options;
    return this;
  }

  public CrateConnection build() {
    if (vertx == null) {
      throw new IllegalStateException("Vertx instance is required - call using(vertx)");
    }
    if (connectOptions == null) {
      throw new IllegalStateException("Connect options are required - call connectingTo(options)");
    }
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectionFactory factory = new CrateConnectionFactory(context, connectOptions, poolOptions);
    return new CrateConnectionPoolImpl(factory, context);
  }
}
