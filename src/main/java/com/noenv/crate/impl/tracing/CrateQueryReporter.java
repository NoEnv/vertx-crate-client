package com.noenv.crate.impl.tracing;

import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingPolicy;

import java.util.function.BiConsumer;

/**
 * Reporter for CrateDB queries that integrates with Vert.x tracing and metrics SPI.
 * <p>
 * Mirrors the pattern used by vertx-sql-client ({@code QueryReporter}): drives both
 * {@link VertxTracer} (sendRequest / receiveResponse) and {@link ClientMetrics}
 * (requestBegin, requestEnd, responseBegin, responseEnd, requestReset) from a single
 * before() / after() lifecycle.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CrateQueryReporter {

  private static final String OPERATION = "Query";

  enum RequestTags {
    PEER_ADDRESS("network.peer.address", q -> q.address),
    SPAN_KIND("span.kind", q -> "client"),
    DB_USER("db.user", q -> q.user),
    DB_NAMESPACE("db.namespace", q -> q.database),
    DB_QUERY_TEXT("db.query.text", CrateQueryRequest::sql),
    DB_SYSTEM("db.system", q -> q.system),
    ;

    final String name;
    final java.util.function.Function<CrateQueryRequest, String> fn;

    RequestTags(String name, java.util.function.Function<CrateQueryRequest, String> fn) {
      this.name = name;
      this.fn = fn;
    }
  }

  private static final TagExtractor<CrateQueryRequest> REQUEST_TAG_EXTRACTOR = new TagExtractor<>() {
    private final RequestTags[] TAGS = RequestTags.values();

    @Override
    public int len(CrateQueryRequest obj) {
      return TAGS.length;
    }

    @Override
    public String name(CrateQueryRequest obj, int index) {
      return TAGS[index].name;
    }

    @Override
    public String value(CrateQueryRequest obj, int index) {
      return TAGS[index].fn.apply(obj);
    }
  };

  private final VertxTracer tracer;
  private final ClientMetrics metrics;
  private final ContextInternal context;
  private final TracingPolicy tracingPolicy;
  private final String address;
  private final String user;
  private final String system;
  private final String database;

  private Object payload;
  private Object metric;
  private HttpClientResponse response;
  private long bytesRead = -1L;

  public CrateQueryReporter(VertxTracer tracer, ClientMetrics metrics, ContextInternal context,
                            TracingPolicy tracingPolicy, String address, String user, String system, String database) {
    this.tracer = tracer;
    this.metrics = metrics;
    this.context = context;
    this.tracingPolicy = tracingPolicy != null ? tracingPolicy : TracingPolicy.PROPAGATE;
    this.address = address != null ? address : "";
    this.user = user != null ? user : "";
    this.system = system != null ? system : "crate";
    this.database = database != null ? database : "";
  }

  /**
   * Call when the request is about to be sent (once you have the request object).
   * Drives tracer.sendRequest and, if metrics present, metrics.requestBegin / requestEnd.
   */
  public void before(String operation, String sql, HttpClientRequest request) {
    if (tracer != null) {
      CrateQueryRequest req = new CrateQueryRequest(address, user, system, database, operation, sql);
      payload = tracer.sendRequest(context, SpanKind.RPC, tracingPolicy, req, OPERATION,
          (BiConsumer<String, String>) (k, v) -> {}, REQUEST_TAG_EXTRACTOR);
    }
    if (metrics != null && request != null) {
      metric = metrics.requestBegin(operation, request);
      metrics.requestEnd(metric, -1L);
    }
  }

  /**
   * Call from the success path to record response and bytes read for metrics.responseBegin/responseEnd.
   */
  public void setResponse(HttpClientResponse response, long bytesRead) {
    this.response = response;
    this.bytesRead = bytesRead;
  }

  /**
   * Call when the request/response is done (success or failure).
   *
   * @param result  the success payload (e.g. {@link com.noenv.crate.codec.CrateMessage}, {@link com.noenv.crate.impl.connection.CrateDatabaseMetadata})
   *                so the tracer can attach response tags; {@code null} for streams or on failure
   * @param failure the failure when the request failed, or {@code null} on success
   */
  public void after(Object result, Throwable failure) {
    if (tracer != null) {
      tracer.receiveResponse(context, result, payload, failure, TagExtractor.empty());
    }
    if (metrics != null && metric != null) {
      if (failure == null) {
        metrics.responseBegin(metric, response);
        metrics.responseEnd(metric, bytesRead);
      } else {
        metrics.requestReset(metric);
      }
    }
  }
}
