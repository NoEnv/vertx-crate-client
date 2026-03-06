package com.noenv.crate;

import com.noenv.crate.impl.CrateClientBuilder;

/**
 * Entry point for building CrateDB clients.
 *
 * <p>Example usage:
 * <pre>{@code
 * CrateConnection connection = CrateBuilder
 *   .pool()
 *   .with(new PoolOptions().setHttp1MaxSize(12))
 *   .connectingTo(new CrateConnectOptions().setHost("localhost").setPort(4200))
 *   .using(vertx)
 *   .build();
 * }</pre>
 */
public interface CrateBuilder {

  /**
   * Provide a builder for a pool-backed {@link CrateConnection}.
   * <p>
   * Example usage: {@code CrateConnection conn = CrateBuilder.pool().connectingTo(connectOptions).using(vertx).build()}
   */
  static CrateClientBuilder pool() {
    return new CrateClientBuilder();
  }
}
