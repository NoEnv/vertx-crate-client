package com.noenv.crate;

import com.noenv.crate.impl.CrateClientImpl;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;

/**
 * Creates CrateClient instances.
 *
 * @author Lukas Prettenthaler
 */
@VertxGen
public interface CrateClient {

  /**
   * Create a CrateClient.
   *
   * @param vertx the Vert.x instance
   * @return the instance
   */
  static CrateClient create(Vertx vertx) {
    return new CrateClientImpl(vertx);
  }
}
