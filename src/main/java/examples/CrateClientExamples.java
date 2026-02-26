package examples;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.CrateConnection;
import io.vertx.core.Vertx;

/**
 * @author Lukas Prettenthaler
 */
public class CrateClientExamples {

  public void example1(Vertx vertx) {
    CrateConnection
      .connect(vertx, CrateConnectOptions.fromEnv());
  }
}
