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
package com.noenv.crate.impl;

import com.noenv.crate.CrateException;
import io.vertx.core.dns.DnsException;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.http.HttpClosedException;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;

import static io.vertx.core.dns.DnsResponseCode.NXDOMAIN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrateFailoverPredicateTest {

  @Nested
  class IsFailoverError_Throwable {

    @Test
    void null_returnsFalse() {
      assertFalse(CrateFailoverPredicate.isFailoverError(null));
    }

    @Test
    void CrateException_5xx_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(new CrateException(503, 5030, "Unavailable")));
      assertTrue(CrateFailoverPredicate.isFailoverError(new CrateException(502, -1, "Bad Gateway")));
      assertTrue(CrateFailoverPredicate.isFailoverError(new CrateException(500, 5000, "Server error")));
    }

    @Test
    void CrateException_4xx_returnsFalse() {
      assertFalse(CrateFailoverPredicate.isFailoverError(new CrateException(400, 4000, "Bad request")));
      assertFalse(CrateFailoverPredicate.isFailoverError(new CrateException(409, 4091, "DuplicateKey")));
      assertFalse(CrateFailoverPredicate.isFailoverError(new CrateException(404, 4041, "Not found")));
    }

    @Test
    void ConnectException_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(new ConnectException("Connection refused")));
    }

    @Test
    void HttpClosedException_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(new HttpClosedException("Network error")));
    }

    @Test
    void DnsException_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(new DnsException(NXDOMAIN)));
    }

    @Test
    void ConnectionPoolTooBusyException_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(new ConnectionPoolTooBusyException("Too busy")));
    }

    @Test
    void messageContainsTimeout_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(new RuntimeException("Connection timed out")));
      assertTrue(CrateFailoverPredicate.isFailoverError(new RuntimeException("Timeout after 5s")));
    }

    @Test
    void causeIsFailover_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(
        new RuntimeException("wrapper", new CrateException(503, 5030, "Unavailable"))));
    }
  }

  @Nested
  class IsFailoverError_HttpStatus_Body {

    @Test
    void status502_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(502, null));
    }

    @Test
    void status503_504_509_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(503, null));
      assertTrue(CrateFailoverPredicate.isFailoverError(504, null));
      assertTrue(CrateFailoverPredicate.isFailoverError(509, null));
    }

    @Test
    void status5xx_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(500, null));
      assertTrue(CrateFailoverPredicate.isFailoverError(599, null));
    }

    @Test
    void status4xx_returnsFalse() {
      assertFalse(CrateFailoverPredicate.isFailoverError(400, null));
      assertFalse(CrateFailoverPredicate.isFailoverError(404, null));
      assertFalse(CrateFailoverPredicate.isFailoverError(409, null));
    }

    @Test
    void bodyWithCrateCode_5000_to_5006_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(200, new JsonObject()
        .put("error", new JsonObject().put("code", 5000).put("message", "x"))));
      assertTrue(CrateFailoverPredicate.isFailoverError(200, new JsonObject()
        .put("error", new JsonObject().put("code", 5003).put("message", "x"))));
      assertTrue(CrateFailoverPredicate.isFailoverError(200, new JsonObject()
        .put("error", new JsonObject().put("code", 5006).put("message", "x"))));
    }

    @Test
    void bodyWithCrateCode_5030_to_5035_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverError(200, new JsonObject()
        .put("error", new JsonObject().put("code", 5030).put("message", "x"))));
      assertTrue(CrateFailoverPredicate.isFailoverError(200, new JsonObject()
        .put("error", new JsonObject().put("code", 5032).put("message", "x"))));
    }

    @Test
    void bodyWithCrateCode_4xxx_returnsFalse() {
      assertFalse(CrateFailoverPredicate.isFailoverError(200, new JsonObject()
        .put("error", new JsonObject().put("code", 4091).put("message", "DuplicateKey"))));
      assertFalse(CrateFailoverPredicate.isFailoverError(400, new JsonObject()
        .put("error", new JsonObject().put("code", 4000).put("message", "Syntax"))));
    }
  }

  @Nested
  class IsFailoverCrateException {

    @Test
    void http502_503_504_509_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(502, -1, "x")));
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(503, -1, "x")));
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(504, -1, "x")));
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(509, -1, "x")));
    }

    @Test
    void http5xx_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(500, 5000, "x")));
    }

    @Test
    void crateCode_5000_5006_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(200, 5000, "x")));
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(200, 5003, "x")));
    }

    @Test
    void crateCode_5030_5035_returnsTrue() {
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(200, 5030, "x")));
      assertTrue(CrateFailoverPredicate.isFailoverCrateException(new CrateException(200, 5032, "x")));
    }

    @Test
    void http4xx_returnsFalse() {
      assertFalse(CrateFailoverPredicate.isFailoverCrateException(new CrateException(400, 4000, "x")));
      assertFalse(CrateFailoverPredicate.isFailoverCrateException(new CrateException(409, 4091, "x")));
    }
  }
}
