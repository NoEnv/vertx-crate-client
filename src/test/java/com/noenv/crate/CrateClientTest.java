package com.noenv.crate;

import io.vertx.test.core.VertxTestBase;
import org.junit.Test;
import org.testcontainers.cratedb.CrateDBContainer;

public class CrateClientTest extends VertxTestBase {
  @Test
  public void testConnect() {
    CrateDBContainer cratedb = new CrateDBContainer("crate:6.2.1");
    cratedb.start();
  }
}
