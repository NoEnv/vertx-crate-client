package com.noenv.crate.resolver;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.Address;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.endpoint.EndpointBuilder;
import io.vertx.core.spi.endpoint.EndpointResolver;

import java.util.List;
import java.util.Objects;

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
      if (!Objects.equals(state.endpoints, this.endpoints)) {
        EndpointBuilder<CrateEndpoint, SocketAddress> builder = state.builder;
        for (CrateEndpoint e : endpoints) {
          builder = builder.addServer(e.getAddress());
        }
        state.endpoints = endpoints;
        state.endpoint = builder.build();
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
    return true;
  } // TODO: health check

  @Override
  public void dispose(CrateLookup data) {
  }

  @Override
  public void close() {
  }
}
