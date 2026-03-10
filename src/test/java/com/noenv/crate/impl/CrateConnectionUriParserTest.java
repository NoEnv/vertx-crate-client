/*
 * Copyright (C) 2026 Lukas Prettenthaler
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
 *
 */
package com.noenv.crate.impl;

import com.noenv.crate.CrateConnectOptions;
import com.noenv.crate.SslMode;
import com.noenv.crate.resolver.CrateEndpoint;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CrateConnectionUriParserTest {

  @Nested
  class Parse {

    @Test
    void minimalUri_noHosts() {
      JsonObject config = CrateConnectionUriParser.parse("crate://");
      assertNotNull(config);
      assertFalse(config.containsKey("endpoints"));
    }

    @Test
    void singleHost_defaultPort() {
      JsonObject config = CrateConnectionUriParser.parse("crate://localhost");
      assertNotNull(config);
      JsonArray endpoints = config.getJsonArray("endpoints");
      assertNotNull(endpoints);
      assertEquals(1, endpoints.size());
      assertEquals("localhost", endpoints.getJsonObject(0).getString("host"));
      assertEquals(4200, endpoints.getJsonObject(0).getInteger("port"));
    }

    @Test
    void singleHost_withPort() {
      JsonObject config = CrateConnectionUriParser.parse("crate://localhost:4200");
      JsonArray endpoints = config.getJsonArray("endpoints");
      assertNotNull(endpoints);
      assertEquals(1, endpoints.size());
      assertEquals("localhost", endpoints.getJsonObject(0).getString("host"));
      assertEquals(4200, endpoints.getJsonObject(0).getInteger("port"));
    }

    @Test
    void multipleHosts_mixedPorts() {
      JsonObject config = CrateConnectionUriParser.parse("crate://host1:4200,host2,host3:4201");
      JsonArray endpoints = config.getJsonArray("endpoints");
      assertNotNull(endpoints);
      assertEquals(3, endpoints.size());
      assertEquals("host1", endpoints.getJsonObject(0).getString("host"));
      assertEquals(4200, endpoints.getJsonObject(0).getInteger("port"));
      assertEquals("host2", endpoints.getJsonObject(1).getString("host"));
      assertEquals(4200, endpoints.getJsonObject(1).getInteger("port"));
      assertEquals("host3", endpoints.getJsonObject(2).getString("host"));
      assertEquals(4201, endpoints.getJsonObject(2).getInteger("port"));
    }

    @Test
    void withUser() {
      JsonObject config = CrateConnectionUriParser.parse("crate://myuser@localhost:4200");
      assertEquals("myuser", config.getString("user"));
      assertFalse(config.containsKey("password"));
    }

    @Test
    void withUserAndPassword() {
      JsonObject config = CrateConnectionUriParser.parse("crate://myuser:mypass@localhost:4200");
      assertEquals("myuser", config.getString("user"));
      assertEquals("mypass", config.getString("password"));
    }

    @Test
    void withDatabase() {
      JsonObject config = CrateConnectionUriParser.parse("crate://localhost/doc");
      assertEquals("doc", config.getString("database"));
    }

    @Test
    void withParams_sslmode() {
      JsonObject config = CrateConnectionUriParser.parse("crate://localhost?sslmode=verify-ca");
      assertTrue(config.containsKey("sslMode"));
      Object sslMode = config.getValue("sslMode");
      assertTrue(sslMode instanceof SslMode && sslMode == SslMode.VERIFY_CA
        || "VERIFY_CA".equals(sslMode.toString()));
    }

    @Test
    void fullUri_userHostsDatabaseParams() {
      JsonObject config = CrateConnectionUriParser.parse("crate://u:p@h1:4200,h2/doc?sslmode=disable");
      assertEquals("u", config.getString("user"));
      assertEquals("p", config.getString("password"));
      assertEquals("doc", config.getString("database"));
      JsonArray endpoints = config.getJsonArray("endpoints");
      assertNotNull(endpoints);
      assertEquals(2, endpoints.size());
    }

    @Test
    void invalidUri_wrongSyntax_throws() {
      // invalid host (space not allowed) causes validation to throw
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parse("crate://host name:4200"));
    }

    @Test
    void invalidPort_throws() {
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parse("crate://localhost:99999"));
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parse("crate://localhost:0"));
    }

    @Test
    void parse_exactFalse_withoutScheme_returnsNull() {
      assertNull(CrateConnectionUriParser.parse("localhost:4200", false));
    }
  }

  @Nested
  class ParseEndpoints {

    @Test
    void emptyHosts_returnsEmptyList() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://");
      assertNotNull(endpoints);
      assertTrue(endpoints.isEmpty());
    }

    @Test
    void singleHost_defaultPort() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://localhost");
      assertEquals(1, endpoints.size());
      SocketAddress addr = endpoints.getFirst().getAddress();
      assertEquals("localhost", addr.hostName());
      assertEquals(4200, addr.port());
    }

    @Test
    void singleHost_withPort() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://localhost:4201");
      assertEquals(1, endpoints.size());
      assertEquals(4201, endpoints.getFirst().getAddress().port());
    }

    @Test
    void multipleHosts() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://h1:4200,h2,h3:4201");
      assertEquals(3, endpoints.size());
      assertEquals("h1", endpoints.getFirst().getAddress().host());
      assertEquals(4200, endpoints.getFirst().getAddress().port());
      assertEquals("h2", endpoints.get(1).getAddress().host());
      assertEquals(4200, endpoints.get(1).getAddress().port());
      assertEquals("h3", endpoints.get(2).getAddress().host());
      assertEquals(4201, endpoints.get(2).getAddress().port());
    }

    @Test
    void ipv4Host() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://192.168.1.1:4200");
      assertEquals(1, endpoints.size());
      assertEquals("192.168.1.1", endpoints.getFirst().getAddress().host());
      assertEquals(4200, endpoints.getFirst().getAddress().port());
    }

    @Test
    void ipv6Host_withPort() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://[::1]:4200");
      assertEquals(1, endpoints.size());
      assertEquals("::1", endpoints.getFirst().getAddress().host());
      assertEquals(4200, endpoints.getFirst().getAddress().port());
    }

    @Test
    void ipv6Host_defaultPort() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://[2001:db8::1]");
      assertEquals(1, endpoints.size());
      assertTrue(endpoints.getFirst().getAddress().host().contains("2001") || endpoints.getFirst().getAddress().host().equals("2001:db8::1"));
      assertEquals(4200, endpoints.getFirst().getAddress().port());
    }

    @Test
    void withUserInfo_ignoresUserForEndpoints() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://user:pass@host1:4200,host2");
      assertEquals(2, endpoints.size());
      assertEquals("host1", endpoints.getFirst().getAddress().host());
      assertEquals("host2", endpoints.get(1).getAddress().host());
    }

    @Test
    void withPath_ignoresPathForEndpoints() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://host1:4200,host2/doc");
      assertEquals(2, endpoints.size());
    }

    @Test
    void invalidHost_throws() {
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parseEndpoints("crate://host name:4200"));
    }

    @Test
    void invalidPort_throws() {
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parseEndpoints("crate://localhost:99999"));
    }

    @Test
    void invalidUri_throws() {
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parseEndpoints("crate://[::1"));
    }
  }

  @Nested
  class FromUriAndJsonCtor {

    @Test
    void fromUri_setsEndpoints() {
      CrateConnectOptions options = CrateConnectOptions.fromUri("crate://h1:4200,h2/doc");
      List<CrateEndpoint> endpoints = options.getEndpoints();
      assertEquals(2, endpoints.size());
      assertEquals("h1", endpoints.getFirst().getAddress().host());
      assertEquals(4200, endpoints.getFirst().getAddress().port());
      assertEquals("h2", endpoints.get(1).getAddress().host());
      assertEquals(4200, endpoints.get(1).getAddress().port());
    }

    @Test
    void fromUri_setsUserPasswordAndDatabase() {
      CrateConnectOptions options = CrateConnectOptions.fromUri("crate://u:p@localhost/doc");
      assertEquals("u", options.getUser());
      assertEquals("p", options.getPassword());
      assertEquals(1, options.getEndpoints().size());
      assertEquals("localhost", options.getEndpoints().getFirst().getAddress().host());
    }

    @Test
    void optionsFromParsedConfig_endpointsFromJsonArray() {
      JsonObject config = CrateConnectionUriParser.parse("crate://a:4200,b:4201");
      CrateConnectOptions options = new CrateConnectOptions(config);
      List<CrateEndpoint> endpoints = options.getEndpoints();
      assertEquals(2, endpoints.size());
      assertEquals("a", endpoints.getFirst().getAddress().host());
      assertEquals(4200, endpoints.getFirst().getAddress().port());
      assertEquals("b", endpoints.get(1).getAddress().host());
      assertEquals(4201, endpoints.get(1).getAddress().port());
    }
  }

  @Nested
  class HostValidation {

    @Test
    void hostname_withHyphenAndDots() {
      List<CrateEndpoint> endpoints = CrateConnectionUriParser.parseEndpoints("crate://my-server.example.com:4200");
      assertEquals(1, endpoints.size());
      assertEquals("my-server.example.com", endpoints.getFirst().getAddress().host());
    }

    @Test
    void invalid_hostWithSpace_throws() {
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parseEndpoints("crate://host name:4200"));
    }

    @Test
    void invalid_hostWithAtSign_throws() {
      // @ in host is invalid; use URL-encoded %40 so the host segment is "host@bad"
      assertThrows(IllegalArgumentException.class, () ->
        CrateConnectionUriParser.parseEndpoints("crate://host%40bad:4200"));
    }
  }
}
