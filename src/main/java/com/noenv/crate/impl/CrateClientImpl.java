package com.noenv.crate.impl;

import com.noenv.crate.CrateClient;
import io.vertx.core.Vertx;

/**
 * The implementation of the {@link com.noenv.crate.CrateClient}.
 *
 * @author Lukas Prettenthaler
 */
public class CrateClientImpl implements CrateClient {

  private final Vertx vertx;

  public CrateClientImpl(final Vertx vertx) {
    this.vertx = vertx;
  }
}
