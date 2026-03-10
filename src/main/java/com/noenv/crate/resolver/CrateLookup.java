package com.noenv.crate.resolver;

import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.endpoint.EndpointBuilder;

import java.util.Collections;
import java.util.List;

public class CrateLookup{

  final SocketAddress address;
  final EndpointBuilder<CrateEndpoint, SocketAddress> builder;
  List<CrateEndpoint> endpoints;
  CrateEndpoint endpoint;

  CrateLookup(SocketAddress name, EndpointBuilder<CrateEndpoint, SocketAddress> builder) {
    this.endpoints = Collections.singletonList(new CrateEndpoint(name));
    this.address = name;
    this.builder = builder;
  }
}
