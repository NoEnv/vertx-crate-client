package com.noenv.crate.impl;

import io.vertx.core.net.endpoint.LoadBalancer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Selects one endpoint from a list of healthy endpoints for connection or request distribution.
 */
public interface EndpointSelector {

  /**
   * Select one endpoint from the given non-empty list of healthy endpoints.
   */
  CrateEndpoint select(List<CrateEndpoint> healthyEndpoints);

  static EndpointSelector from(LoadBalancer loadBalancer) {
    if (loadBalancer == LoadBalancer.RANDOM) {
      return new RandomEndpointSelector();
    }
    return new RoundRobinEndpointSelector();
  }

  final class RoundRobinEndpointSelector implements EndpointSelector {
    private int counter = ThreadLocalRandom.current().nextInt();

    @Override
    public CrateEndpoint select(List<CrateEndpoint> healthyEndpoints) {
      if (healthyEndpoints.isEmpty()) {
        throw new IllegalArgumentException("healthyEndpoints must not be empty");
      }
      int index = Math.floorMod(counter++, healthyEndpoints.size());
      return healthyEndpoints.get(index);
    }
  }

  final class RandomEndpointSelector implements EndpointSelector {
    @Override
    public CrateEndpoint select(List<CrateEndpoint> healthyEndpoints) {
      if (healthyEndpoints.isEmpty()) {
        throw new IllegalArgumentException("healthyEndpoints must not be empty");
      }
      return healthyEndpoints.get(ThreadLocalRandom.current().nextInt(healthyEndpoints.size()));
    }
  }
}
