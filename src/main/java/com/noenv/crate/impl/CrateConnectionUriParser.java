package com.noenv.crate.impl;

import com.noenv.crate.SslMode;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * This is a parser for parsing connection URIs of CrateDB.
 * Based on Crate 6: crate://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
 * <p>
 * Multiple hosts can be given as comma-separated host[:port] pairs, e.g.
 * {@code crate://host1:4200,host2,host3:4201/db} (omitted port defaults to 4200).
 */
public class CrateConnectionUriParser {

  private static final int DEFAULT_PORT = 4200;

  private static final String SCHEME_DESIGNATOR_REGEX = "crate://"; // URI scheme designator
  private static final String USER_INFO_REGEX = "((?<userinfo>[a-zA-Z0-9\\-._~%!*]+(:[a-zA-Z0-9\\-._~%!*]+)?)@)?"; // user name and password
  // hosts: comma-separated host[:port] (e.g. host1:4200,host2 or [::1]:4200,host2)
  private static final String HOSTS_REGEX = "(?<hosts>[^/?#]*)";
  private static final String DATABASE_REGEX = "(/(?<database>[a-zA-Z0-9\\-._~%!*]+))?"; // database name
  private static final String PARAMS_REGEX = "(\\?(?<params>.*))?"; // parameters

  // netloc validation: IPv4, IPv6 (content only, no brackets), or hostname/domain
  private static final Pattern HOST_IPV4_PATTERN = Pattern.compile("^[0-9.]+$");
  private static final Pattern HOST_IPV6_PATTERN = Pattern.compile("^[a-zA-Z0-9:]+$");
  private static final Pattern HOST_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-._~%]+$");

  private static final Pattern SCHEME_DESIGNATOR_PATTERN = Pattern.compile("^" + SCHEME_DESIGNATOR_REGEX);
  private static final Pattern FULL_URI_PATTERN = Pattern.compile("^"
    + SCHEME_DESIGNATOR_REGEX
    + USER_INFO_REGEX
    + HOSTS_REGEX
    + DATABASE_REGEX
    + PARAMS_REGEX
    + "$");

  public static JsonObject parse(String connectionUri) {
    return parse(connectionUri, true);
  }

  public static JsonObject parse(String connectionUri, boolean exact) {
    try {
      Matcher matcher = SCHEME_DESIGNATOR_PATTERN.matcher(connectionUri);
      if (matcher.find() || exact) {
        JsonObject configuration = new JsonObject();
        doParse(connectionUri, configuration);
        return configuration;
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse invalid connection URI: " + connectionUri, e);
    }
  }

  /**
   * Parses the connection URI and returns a list of {@link SocketAddress}s for each host[:port]
   * in the URI. Multiple hosts are given as comma-separated host[:port] pairs; omitted port
   * defaults to 4200.
   *
   * @param connectionUri the connection URI (e.g. {@code crate://host1:4200,host2,host3:4201/db})
   * @return list of endpoint configs, one per host:port; never null, may be empty if URI has no hosts
   * @throws IllegalArgumentException when the URI is invalid
   */
  public static List<SocketAddress> parseEndpoints(String connectionUri) {
    try {
      String rest = connectionUri;
      if (rest.startsWith("crate://")) {
        rest = rest.substring("crate://".length());
      }
      // authority: from start until / or ? or end
      int pathStart = rest.indexOf('/');
      int queryStart = rest.indexOf('?');
      int authorityEnd = pathStart >= 0 ? (queryStart >= 0 ? Math.min(pathStart, queryStart) : pathStart) : (queryStart >= 0 ? queryStart : rest.length());
      String authority = rest.substring(0, authorityEnd);
      // optional userinfo: part before last @ (user and password must not contain @)
      int at = authority.lastIndexOf('@');
      String hostsPart = at >= 0 ? authority.substring(at + 1) : authority;
      if (hostsPart.isEmpty()) {
        return List.of();
      }
      List<SocketAddress> endpoints = new ArrayList<>();
      for (String segment : hostsPart.split(",")) {
        segment = segment.trim();
        if (segment.isEmpty()) {
          continue;
        }
        segment = decodeUrl(segment);
        HostPort hp = parseHostPort(segment, DEFAULT_PORT);
        endpoints.add(SocketAddress.inetSocketAddress(hp.port, hp.host));
      }
      return endpoints;
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse invalid connection URI: " + connectionUri, e);
    }
  }

  private static final class HostPort {
    final String host;
    final int port;

    HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }

  private static HostPort parseHostPort(String segment, int defaultPort) {
    String host;
    int port = defaultPort;
    if (segment.startsWith("[")) {
      int close = segment.indexOf(']');
      if (close < 0) {
        throw new IllegalArgumentException("Invalid IPv6 address in segment: " + segment);
      }
      host = segment.substring(1, close);
      if (close + 1 < segment.length()) {
        if (segment.charAt(close + 1) == ':') {
          port = parseInt(segment.substring(close + 2));
        }
      }
    } else {
      int lastColon = segment.lastIndexOf(':');
      if (lastColon > 0) {
        String suffix = segment.substring(lastColon + 1);
        if (suffix.matches("\\d+")) {
          host = segment.substring(0, lastColon);
          port = parseInt(suffix);
        } else {
          host = segment;
        }
      } else {
        host = segment;
      }
    }
    if (port <= 0 || port > 65535) {
      throw new IllegalArgumentException("Port must be in 1-65535: " + port);
    }
    validateHost(host);
    return new HostPort(host, port);
  }

  private static void validateHost(String host) {
    if (host == null || host.isEmpty()) {
      throw new IllegalArgumentException("Host must not be empty");
    }
    if (host.contains(":")) {
      if (!HOST_IPV6_PATTERN.matcher(host).matches()) {
        throw new IllegalArgumentException("Invalid IPv6 address: " + host);
      }
    } else if (HOST_IPV4_PATTERN.matcher(host).matches()) {
      // valid IPv4 pattern
    } else if (!HOST_NAME_PATTERN.matcher(host).matches()) {
      throw new IllegalArgumentException("Invalid host or domain: " + host);
    }
  }

  // execute the parsing process and store options in the configuration
  private static void doParse(String connectionUri, JsonObject configuration) {
    Matcher matcher = FULL_URI_PATTERN.matcher(connectionUri);

    if (matcher.matches()) {
      // parse the user and password
      parseUserAndPassword(matcher.group("userinfo"), configuration);

      // parse endpoints (host:port list) into config for JSON ctor
      List<SocketAddress> endpoints = parseEndpoints(connectionUri);
      if (!endpoints.isEmpty()) {
        JsonArray arr = new JsonArray();
        for (SocketAddress e : endpoints) {
          arr.add(JsonObject.of("host", e.host(), "port", e.port()));
        }
        configuration.put("endpoints", arr);
      }

      // parse the database name
      parseDatabaseName(matcher.group("database"), configuration);

      // parse the parameters
      parseParameters(matcher.group("params"), configuration);

    } else {
      throw new IllegalArgumentException("Wrong syntax of connection URI");
    }
  }

  private static void parseUserAndPassword(String userInfo, JsonObject configuration) {
    if (userInfo == null || userInfo.isEmpty()) {
      return;
    }
    if (occurExactlyOnce(userInfo, ":")) {
      int index = userInfo.indexOf(":");
      String user = userInfo.substring(0, index);
      if (user.isEmpty()) {
        throw new IllegalArgumentException("Can not only specify the password without a concrete user");
      }
      String password = userInfo.substring(index + 1);
      configuration.put("user", decodeUrl(user));
      configuration.put("password", decodeUrl(password));
    } else if (!userInfo.contains(":")) {
      configuration.put("user", decodeUrl(userInfo));
    } else {
      throw new IllegalArgumentException("Can not use multiple delimiters to delimit user and password");
    }
  }

  private static void parseNetLocation(String hostInfo, JsonObject configuration) {
    if (hostInfo == null || hostInfo.isEmpty()) {
      return;
    }
    parseNetLocationValue(decodeUrl(hostInfo), configuration);
  }

  private static void parsePort(String portInfo, JsonObject configuration) {
    if (portInfo == null || portInfo.isEmpty()) {
      return;
    }
    int port;
    try {
      port = parseInt(decodeUrl(portInfo));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("The port must be a valid integer");
    }
    if (port > 65535 || port <= 0) {
      throw new IllegalArgumentException("The port can only range in 1-65535");
    }
    configuration.put("port", port);
  }

  private static void parseDatabaseName(String databaseInfo, JsonObject configuration) {
    if (databaseInfo == null || databaseInfo.isEmpty()) {
      return;
    }
    configuration.put("database", decodeUrl(databaseInfo));
  }

  private static void parseParameters(String parametersInfo, JsonObject configuration) {
    if (parametersInfo == null || parametersInfo.isEmpty()) {
      return;
    }
    Map<String, String> properties = new HashMap<>();
    for (String parameterPair : parametersInfo.split("&")) {
      if (parameterPair.isEmpty()) {
        continue;
      }
      int indexOfDelimiter = parameterPair.indexOf("=");
      if (indexOfDelimiter < 0) {
        throw new IllegalArgumentException(format("Missing delimiter '=' of parameters \"%s\" in the part \"%s\"", parametersInfo, parameterPair));
      } else {
        String key = parameterPair.substring(0, indexOfDelimiter).toLowerCase();
        String value = decodeUrl(parameterPair.substring(indexOfDelimiter + 1).trim());
        switch (key) {
          case "port":
            parsePort(value, configuration);
            break;
          case "host":
            parseNetLocationValue(value, configuration);
            break;
          case "hostaddr":
            configuration.put("host", value);
            break;
          case "user":
            configuration.put("user", value);
            break;
          case "password":
            configuration.put("password", value);
            break;
          case "dbname":
            configuration.put("database", value);
            break;
          case "sslmode":
            configuration.put("sslMode", SslMode.of(value));
            break;
          default:
            properties.put(key, value);
            break;
        }
      }
    }
    if (!properties.isEmpty()) {
      configuration.put("properties", properties);
    }
  }

  private static void parseNetLocationValue(String hostValue, JsonObject configuration) {
    if (isRegardedAsIpv6Address(hostValue)) {
      configuration.put("host", hostValue.substring(1, hostValue.length() - 1));
    } else {
      configuration.put("host", hostValue);
    }
  }

  private static boolean isRegardedAsIpv6Address(String hostAddress) {
    return hostAddress.startsWith("[") && hostAddress.endsWith("]");
  }

  private static String decodeUrl(String url) {
    return URLDecoder.decode(url, StandardCharsets.UTF_8);
  }

  private static boolean occurExactlyOnce(String uri, String character) {
    return uri.contains(character) && uri.indexOf(character) == uri.lastIndexOf(character);
  }
}
