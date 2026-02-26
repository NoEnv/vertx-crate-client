package examples;

import com.noenv.crate.CrateClient;
import io.vertx.core.Vertx;

/**
 * @author Lukas Prettenthaler
 */
public class CrateClientExamples {

  public void example1(Vertx vertx) {
    CrateClient
      .create(vertx);
  }
}
