/*
 * Copyright (C) 2026 Christoph Spörk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.noenv.crate.impl.connection;

import com.noenv.crate.CrateException;
import io.vertx.core.dns.DnsException;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.http.HttpClosedException;
import io.vertx.core.json.JsonObject;

/**
 * Classifies errors as either "failover" (trigger reconnect to another endpoint)
 * or "application" (surface to caller, do not failover).
 * <p>
 * Failover: CrateDB 5xxx error codes, HTTP 5xx / gateway errors, connection/timeout/SSL errors.
 * Do not failover: CrateDB 4xxx (client/application errors), ConnectionReset, BrokenPipe (per Python driver).
 */
public final class CrateFailoverPredicate {

  private CrateFailoverPredicate() {
  }

  /** CrateDB: unhandled server error (5000) and execution/shard failures (5001–5006). */
  private static boolean isCrateServerError(int code) {
    return code == 5000
      || (code >= 5001 && code <= 5006);
  }

  /** CrateDB: cluster/node/shard unavailable (5030–5035). */
  private static boolean isCrateClusterUnavailable(int code) {
    return code >= 5030 && code <= 5035;
  }

  /**
   * Returns true if the error should trigger failover (mark endpoint unhealthy and retry on another).
   * Returns false for application errors (e.g. DuplicateKey, syntax, auth) and for
   * ConnectionReset/BrokenPipe (keep endpoint in pool per Python driver).
   */
  public static boolean isFailoverError(Throwable t) {
    if (t == null) {
      return false;
    }
    // ConnectionReset / BrokenPipe: do not mark unhealthy (preserve server in list)
    if (isConnectionResetOrBrokenPipe(t)) {
      return false;
    }
    if (t instanceof CrateException) {
      return isFailoverCrateException((CrateException) t);
    }
    // Network/transport errors that did not reach the server or got 5xx
    if (t instanceof HttpClosedException || t instanceof DnsException || t instanceof ConnectionPoolTooBusyException) {
      return true;
    }
    String msg = t.getMessage();
    if (msg != null) {
      if (msg.contains("Connection refused") || msg.contains("Connection reset") && !isConnectionResetOrBrokenPipe(t)) {
        return true;
      }
      if (msg.contains("timed out") || msg.contains("Timeout")) {
        return true;
      }
    }
    Throwable cause = t.getCause();
    return cause != null && cause != t && isFailoverError(cause);
  }

  /**
   * Returns true if the HTTP response (status + optional JSON error body) indicates a failover error.
   */
  public static boolean isFailoverError(int httpStatus, JsonObject body) {
    // Gateway / server unavailable (explicit)
    if (httpStatus == 502 || httpStatus == 503 || httpStatus == 504 || httpStatus == 509) {
      return true;
    }
    // Any 5xx server error
    if (httpStatus >= 500 && httpStatus < 600) {
      return true;
    }
    // CrateDB error code in body overrides status
    if (body != null && body.containsKey("error")) {
      JsonObject error = body.getJsonObject("error");
      if (error != null && error.containsKey("code")) {
        int code = error.getInteger("code", -1);
        return isCrateServerError(code) || isCrateClusterUnavailable(code);
      }
    }
    return false;
  }

  /**
   * Returns true if the CrateException represents a failover error (5xxx codes or 5xx HTTP).
   */
  public static boolean isFailoverCrateException(CrateException e) {
    int status = e.getHttpStatus();
    if (status == 502 || status == 503 || status == 504 || status == 509) {
      return true;
    }
    if (status >= 500 && status < 600) {
      return true;
    }
    int code = e.getErrorCode();
    return isCrateServerError(code) || isCrateClusterUnavailable(code);
  }

  private static boolean isConnectionResetOrBrokenPipe(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      String cn = c.getClass().getName();
      if (cn.contains("ConnectionResetException") || cn.contains("BrokenPipeException")) {
        return true;
      }
      if (cn.contains("ErrnoException") && c.getMessage() != null) {
        String m = c.getMessage();
        if (m.contains("Connection reset") || m.contains("Broken pipe")) {
          return true;
        }
      }
    }
    return false;
  }
}
