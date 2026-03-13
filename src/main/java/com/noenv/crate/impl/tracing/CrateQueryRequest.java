package com.noenv.crate.impl.tracing;

/**
 * A traceable CrateDB request (query or metadata).
 * Used by {@link CrateQueryReporter} and {@link io.vertx.core.spi.tracing.TagExtractor}
 * to attach span tags (e.g. db.system, db.query.text, network.peer.address).
 */
public class CrateQueryRequest {

  final String address;
  final String user;
  final String system;
  final String database;
  final String operation;
  final String sql;

  public CrateQueryRequest(String address, String user, String system, String database,
                           String operation, String sql) {
    this.address = address;
    this.user = user;
    this.system = system;
    this.database = database;
    this.operation = operation != null ? operation : "unknown";
    this.sql = sql != null ? sql : "";
  }

  public String operation() {
    return operation;
  }

  public String sql() {
    return sql;
  }
}
