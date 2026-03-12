package com.noenv.crate.resolver;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.Address;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.endpoint.EndpointBuilder;
import io.vertx.core.spi.endpoint.EndpointResolver;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CrateEndpointResolver implements EndpointResolver<SocketAddress, SocketAddress, CrateLookup, CrateEndpoint> {

  private final List<CrateEndpoint> endpoints;

  public CrateEndpointResolver(List<CrateEndpoint> endpoints) {
    this.endpoints = Objects.requireNonNull(endpoints);
  }

  @Override
  public SocketAddress tryCast(Address address) {
    return (SocketAddress) address;
  }

  @Override
  public SocketAddress addressOf(SocketAddress server) {
    return server;
  }

  @Override
  public Future<CrateLookup> resolve(SocketAddress address, EndpointBuilder<CrateEndpoint, SocketAddress> builder) {
    return Future.succeededFuture(new CrateLookup(address, builder));
  }

  @Override
  public CrateEndpoint endpoint(CrateLookup state) {
    synchronized (state) {
      List<CrateEndpoint> healthy = endpoints.stream().filter(CrateEndpoint::isHealthy).collect(Collectors.toList());
      if (!Objects.equals(state.endpoints, healthy)) {
        EndpointBuilder<CrateEndpoint, SocketAddress> b = state.builder;
        for (CrateEndpoint e : healthy) {
          b = b.addServer(e.getAddress());
        }
        state.endpoints = healthy;
        state.endpoint = b.build();
      }
      return state.endpoint;
    }
  }

  @Override
  public JsonObject propertiesOf(SocketAddress server) {
    return JsonObject.of(); // TODO: health state, server version, etc.
  }

  @Override
  public boolean isValid(CrateLookup state) {
    return endpoints.stream().anyMatch(CrateEndpoint::isHealthy);
  }

  @Override
  public void dispose(CrateLookup data) {
  }

  @Override
  public void close() {
  }
}
