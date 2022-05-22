/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest.auth;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Pair;

public class OAuth2Util {
  private OAuth2Util() {
  }

  // valid scope tokens are from ascii 0x21 to 0x7E, excluding 0x22 (") and 0x5C (\)
  private static final Pattern VALID_SCOPE_TOKEN = Pattern.compile("^[!-~&&[^\"\\\\]]+$");
  private static final Splitter SCOPE_DELIMITER = Splitter.on(" ");
  private static final Joiner SCOPE_JOINER = Joiner.on(" ");

  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String BEARER_PREFIX = "Bearer ";

  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(":").limit(2).trimResults();
  private static final String GRANT_TYPE = "grant_type";
  private static final String CLIENT_CREDENTIALS = "client_credentials";
  private static final String TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
  private static final String SCOPE = "scope";
  private static final String CATALOG = "catalog";

  // Client credentials flow
  private static final String CLIENT_ID = "client_id";
  private static final String CLIENT_SECRET = "client_secret";

  // Token exchange flow
  private static final String SUBJECT_TOKEN = "subject_token";
  private static final String SUBJECT_TOKEN_TYPE = "subject_token_type";
  private static final String ACTOR_TOKEN = "actor_token";
  private static final String ACTOR_TOKEN_TYPE = "actor_token_type";
  private static final Set<String> VALID_TOKEN_TYPES = Sets.newHashSet(
      "urn:ietf:params:oauth:token-type:access_token",
      "urn:ietf:params:oauth:token-type:refresh_token",
      "urn:ietf:params:oauth:token-type:id_token",
      "urn:ietf:params:oauth:token-type:saml1",
      "urn:ietf:params:oauth:token-type:saml2",
      "urn:ietf:params:oauth:token-type:jwt");

  // response serialization
  private static final String ACCESS_TOKEN = "access_token";
  private static final String TOKEN_TYPE = "token_type";
  private static final String EXPIRES_IN = "expires_in";
  private static final String ISSUED_TOKEN_TYPE = "issued_token_type";
  private static final String REFRESH_TOKEN = "refresh_token";

  public static Map<String, String> authHeaders(String token) {
    if (token != null) {
      return ImmutableMap.of(AUTHORIZATION_HEADER, BEARER_PREFIX + token);
    } else {
      return ImmutableMap.of();
    }
  }

  public static boolean isValidScopeToken(String scopeToken) {
    return VALID_SCOPE_TOKEN.matcher(scopeToken).matches();
  }

  public static List<String> parseScope(String scope) {
    return SCOPE_DELIMITER.splitToList(scope);
  }

  public static String toScope(Iterable<String> scopes) {
    return SCOPE_JOINER.join(scopes);
  }

  public static Map<String, String> tokenExchangeRequest(String subjectToken, String subjectTokenType,
                                                         List<String> scopes) {
    return tokenExchangeRequest(subjectToken, subjectTokenType, null, null, scopes);
  }

  public static Map<String, String> tokenExchangeRequest(String subjectToken, String subjectTokenType,
                                                         String actorToken, String actorTokenType,
                                                         List<String> scopes) {
    Preconditions.checkArgument(VALID_TOKEN_TYPES.contains(subjectTokenType),
        "Invalid token type: %s", subjectTokenType);
    Preconditions.checkArgument(actorToken == null || VALID_TOKEN_TYPES.contains(actorTokenType),
        "Invalid token type: %s", actorTokenType);

    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, TOKEN_EXCHANGE);
    formData.put(SCOPE, toScope(scopes));
    formData.put(SUBJECT_TOKEN, subjectToken);
    formData.put(SUBJECT_TOKEN_TYPE, subjectTokenType);
    if (actorToken != null) {
      formData.put(ACTOR_TOKEN, actorToken);
      formData.put(ACTOR_TOKEN_TYPE, actorTokenType);
    }

    return formData.build();
  }

  private static Pair<String, String> parseCredential(String credential) {
    Preconditions.checkNotNull(credential, "Invalid credential: null");
    List<String> parts = CREDENTIAL_SPLITTER.splitToList(credential);
    switch (parts.size()) {
      case 2:
        // client ID and client secret
        return Pair.of(parts.get(0), parts.get(1));
      case 1:
        // client ID
        return Pair.of(null, parts.get(0));
      default:
        // this should never happen because the credential splitter is limited to 2
        throw new IllegalArgumentException("Invalid credential: " + credential);
    }
  }

  public static Map<String, String> clientCredentialsRequest(String credential, List<String> scopes) {
    Pair<String, String> credentialPair = parseCredential(credential);
    return clientCredentialsRequest(credentialPair.first(), credentialPair.second(), scopes);
  }

  public static Map<String, String> clientCredentialsRequest(String clientId, String clientSecret,
                                                             List<String> scopes) {
    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, CLIENT_CREDENTIALS);
    if (clientId != null) {
      formData.put(CLIENT_ID, clientId);
    }
    formData.put(CLIENT_SECRET, clientSecret);
    formData.put(SCOPE, toScope(Iterables.concat(scopes, ImmutableList.of(CATALOG))));

    return formData.build();
  }

  public static String tokenResponseToJson(OAuthTokenResponse response) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      tokenResponseToJson(response, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static void tokenResponseToJson(OAuthTokenResponse response, JsonGenerator gen) throws IOException {
    response.validate();

    gen.writeStartObject();

    gen.writeStringField(ACCESS_TOKEN, response.token());
    gen.writeStringField(TOKEN_TYPE, response.tokenType());

    if (response.issuedTokenType() != null) {
      gen.writeStringField(ISSUED_TOKEN_TYPE, response.issuedTokenType());
    }

    if (response.expiresInSeconds() != null) {
      gen.writeNumberField(EXPIRES_IN, response.expiresInSeconds());
    }

    if (response.scopes() != null && !response.scopes().isEmpty()) {
      gen.writeStringField(SCOPE, toScope(response.scopes()));
    }

    gen.writeEndObject();
  }

  public static OAuthTokenResponse tokenResponseFromJson(String json) {
    try {
      return tokenResponseFromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static OAuthTokenResponse tokenResponseFromJson(JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse token response from non-object: %s", json);

    OAuthTokenResponse.Builder builder = OAuthTokenResponse.builder()
        .withToken(JsonUtil.getString(ACCESS_TOKEN, json))
        .withTokenType(JsonUtil.getString(TOKEN_TYPE, json))
        .withIssuedTokenType(JsonUtil.getStringOrNull(ISSUED_TOKEN_TYPE, json));

    if (json.has(EXPIRES_IN)) {
      builder.setExpirationInSeconds(JsonUtil.getInt(EXPIRES_IN, json));
    }

    if (json.has(SCOPE)) {
      builder.addScopes(parseScope(JsonUtil.getString(SCOPE, json)));
    }

    return builder.build();
  }
}
