package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.connection.CrateEndpoint;
import com.noenv.crate.connection.EndpointSelector;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.endpoint.LoadBalancer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class EndpointSelectorTest {

  @Test
  void roundRobin_selectsInOrder(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    List<CrateEndpoint> healthy = List.of(
      CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "host1"), context, options),
      CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "host2"), context, options),
      CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "host3"), context, options)
    );
    EndpointSelector selector = EndpointSelector.from(LoadBalancer.ROUND_ROBIN);
    // Round-robin cycles through endpoints in order (starting at a random index)
    String a = selector.select(healthy).getHost();
    String b = selector.select(healthy).getHost();
    String c = selector.select(healthy).getHost();
    assertEquals(3, Set.of(a, b, c).size(), "first cycle has 3 distinct hosts");
    assertEquals(a, selector.select(healthy).getHost(), "cycle repeats");
    assertEquals(b, selector.select(healthy).getHost());
    assertEquals(c, selector.select(healthy).getHost());
    ctx.completeNow();
  }

  @Test
  void random_selectsFromList(Vertx vertx, VertxTestContext ctx) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectOptions options = new CrateConnectOptions();
    List<CrateEndpoint> healthy = List.of(
      CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "host1"), context, options),
      CrateEndpoint.create(SocketAddress.inetSocketAddress(4200, "host2"), context, options)
    );
    EndpointSelector selector = EndpointSelector.from(LoadBalancer.RANDOM);
    for (int i = 0; i < 20; i++) {
      CrateEndpoint e = selector.select(healthy);
      assertTrue(e.getHost().equals("host1") || e.getHost().equals("host2"));
    }
    ctx.completeNow();
  }

  @Test
  void select_emptyList_roundRobin_throws(Vertx vertx, VertxTestContext ctx) {
    EndpointSelector selector = EndpointSelector.from(LoadBalancer.ROUND_ROBIN);
    assertThrows(IllegalArgumentException.class, () -> selector.select(List.of()));
    ctx.completeNow();
  }

  @Test
  void select_emptyList_random_throws(Vertx vertx, VertxTestContext ctx) {
    EndpointSelector selector = EndpointSelector.from(LoadBalancer.RANDOM);
    assertThrows(IllegalArgumentException.class, () -> selector.select(List.of()));
    ctx.completeNow();
  }
}
