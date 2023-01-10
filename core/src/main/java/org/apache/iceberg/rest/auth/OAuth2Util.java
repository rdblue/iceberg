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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuth2Util {
  private OAuth2Util() {}

  private static final Logger LOG = LoggerFactory.getLogger(OAuth2Util.class);

  // valid scope tokens are from ascii 0x21 to 0x7E, excluding 0x22 (") and 0x5C (\)
  private static final Pattern VALID_SCOPE_TOKEN = Pattern.compile("^[!-~&&[^\"\\\\]]+$");
  private static final Splitter SCOPE_DELIMITER = Splitter.on(" ");
  private static final Joiner SCOPE_JOINER = Joiner.on(" ");

  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String BEARER_PREFIX = "Bearer ";
  private static final String BASIC_PREFIX = "Basic ";

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
  private static final Set<String> VALID_TOKEN_TYPES =
      Sets.newHashSet(
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.REFRESH_TOKEN_TYPE,
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE);

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

  public static Map<String, String> basicAuthHeaders(String credential) {
    if (credential != null) {
      return ImmutableMap.of(
          AUTHORIZATION_HEADER,
          BASIC_PREFIX
              + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8)));
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

  private static OAuthTokenResponse refreshToken(
      RESTClient client,
      Map<String, String> headers,
      String subjectToken,
      String subjectTokenType,
      String scope) {
    Map<String, String> request =
        tokenExchangeRequest(
            subjectToken,
            subjectTokenType,
            scope != null ? ImmutableList.of(scope) : ImmutableList.of());

    OAuthTokenResponse response =
        client.postForm(
            ResourcePaths.tokens(),
            request,
            OAuthTokenResponse.class,
            headers,
            ErrorHandlers.oauthErrorHandler());
    response.validate();

    return response;
  }

  public static OAuthTokenResponse exchangeToken(
      RESTClient client,
      Map<String, String> headers,
      String subjectToken,
      String subjectTokenType,
      String actorToken,
      String actorTokenType,
      String scope) {
    Map<String, String> request =
        tokenExchangeRequest(
            subjectToken,
            subjectTokenType,
            actorToken,
            actorTokenType,
            scope != null ? ImmutableList.of(scope) : ImmutableList.of());

    OAuthTokenResponse response =
        client.postForm(
            ResourcePaths.tokens(),
            request,
            OAuthTokenResponse.class,
            headers,
            ErrorHandlers.oauthErrorHandler());
    response.validate();

    return response;
  }

  public static OAuthTokenResponse fetchToken(
      RESTClient client, Map<String, String> headers, String credential, String scope) {
    Map<String, String> request =
        clientCredentialsRequest(
            credential, scope != null ? ImmutableList.of(scope) : ImmutableList.of());

    OAuthTokenResponse response =
        client.postForm(
            ResourcePaths.tokens(),
            request,
            OAuthTokenResponse.class,
            headers,
            ErrorHandlers.oauthErrorHandler());
    response.validate();

    return response;
  }

  private static Map<String, String> tokenExchangeRequest(
      String subjectToken, String subjectTokenType, List<String> scopes) {
    return tokenExchangeRequest(subjectToken, subjectTokenType, null, null, scopes);
  }

  private static Map<String, String> tokenExchangeRequest(
      String subjectToken,
      String subjectTokenType,
      String actorToken,
      String actorTokenType,
      List<String> scopes) {
    Preconditions.checkArgument(
        VALID_TOKEN_TYPES.contains(subjectTokenType), "Invalid token type: %s", subjectTokenType);
    Preconditions.checkArgument(
        actorToken == null || VALID_TOKEN_TYPES.contains(actorTokenType),
        "Invalid token type: %s",
        actorTokenType);

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
        // client secret
        return Pair.of(null, parts.get(0));
      default:
        // this should never happen because the credential splitter is limited to 2
        throw new IllegalArgumentException("Invalid credential: " + credential);
    }
  }

  private static Map<String, String> clientCredentialsRequest(
      String credential, List<String> scopes) {
    Pair<String, String> credentialPair = parseCredential(credential);
    return clientCredentialsRequest(credentialPair.first(), credentialPair.second(), scopes);
  }

  private static Map<String, String> clientCredentialsRequest(
      String clientId, String clientSecret, List<String> scopes) {
    ImmutableMap.Builder<String, String> formData = ImmutableMap.builder();
    formData.put(GRANT_TYPE, CLIENT_CREDENTIALS);
    if (clientId != null) {
      formData.put(CLIENT_ID, clientId);
    }
    formData.put(CLIENT_SECRET, clientSecret);
    formData.put(SCOPE, toScope(scopes));

    return formData.build();
  }

  public static String tokenResponseToJson(OAuthTokenResponse response) {
    return JsonUtil.generate(gen -> tokenResponseToJson(response, gen), false);
  }

  public static void tokenResponseToJson(OAuthTokenResponse response, JsonGenerator gen)
      throws IOException {
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
    return JsonUtil.parse(json, OAuth2Util::tokenResponseFromJson);
  }

  public static OAuthTokenResponse tokenResponseFromJson(JsonNode json) {
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse token response from non-object: %s", json);

    OAuthTokenResponse.Builder builder =
        OAuthTokenResponse.builder()
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

  /** Class to handle authorization headers and token refresh. */
  public static class AuthSession {
    private static final long MAX_REFRESH_WINDOW_MILLIS = 300_000; // 5 minutes
    private static final long MIN_REFRESH_WAIT_MILLIS = 10;
    private Map<String, String> headers;
    private String token;
    private String tokenType;
    private final String credential;
    private volatile boolean keepRefreshed = true;

    public AuthSession(Map<String, String> baseHeaders, String token, String tokenType) {
      this(baseHeaders, token, tokenType, null);
    }

    public AuthSession(
        Map<String, String> baseHeaders, String token, String tokenType, String credential) {
      this.headers = RESTUtil.merge(baseHeaders, authHeaders(token));
      this.token = token;
      this.tokenType = tokenType;
      this.credential = credential;
    }

    public Map<String, String> headers() {
      return headers;
    }

    public String token() {
      return token;
    }

    public String tokenType() {
      return tokenType;
    }

    public void stopRefreshing() {
      this.keepRefreshed = false;
    }

    public String credential() {
      return credential;
    }

    /**
     * A new {@link AuthSession} with empty headers.
     *
     * @return A new {@link AuthSession} with empty headers.
     */
    public static AuthSession empty() {
      return new AuthSession(ImmutableMap.of(), null, null, null);
    }

    /**
     * Attempt to refresh the session token using the token exchange flow.
     *
     * @param client a RESTClient
     * @return interval to wait before calling refresh again, or null if no refresh is needed
     */
    public Pair<Integer, TimeUnit> refresh(RESTClient client) {
      if (token != null && keepRefreshed) {
        AtomicReference<OAuthTokenResponse> ref = new AtomicReference<>(null);
        boolean isSuccessful =
            Tasks.foreach(ref)
                .suppressFailureWhenFinished()
                .retry(5)
                .onFailure(
                    (holder, err) -> {
                      if (null != credential) {
                        holder.set(
                            refreshToken(
                                client,
                                RESTUtil.merge(headers(), basicAuthHeaders(credential)),
                                token,
                                tokenType,
                                OAuth2Properties.CATALOG_SCOPE));
                      } else {
                        LOG.warn("Failed to refresh token", err);
                      }
                    })
                .exponentialBackoff(
                    COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
                    COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
                    COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
                    2.0 /* exponential */)
                .run(
                    holder ->
                        holder.set(
                            refreshToken(
                                client,
                                headers(),
                                token,
                                tokenType,
                                OAuth2Properties.CATALOG_SCOPE)));

        if (!isSuccessful || ref.get() == null) {
          return null;
        }

        OAuthTokenResponse response = ref.get();
        this.token = response.token();
        this.tokenType = response.issuedTokenType();
        this.headers = RESTUtil.merge(headers, authHeaders(token));

        if (response.expiresInSeconds() != null) {
          return Pair.of(response.expiresInSeconds(), TimeUnit.SECONDS);
        }
      }

      return null;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public static void scheduleTokenRefresh(
        RESTClient client,
        ScheduledExecutorService tokenRefreshExecutor,
        AuthSession session,
        long startTimeMillis,
        long expiresIn,
        TimeUnit unit) {
      // convert expiration interval to milliseconds
      long expiresInMillis = unit.toMillis(expiresIn);
      // how much ahead of time to start the request to allow it to complete
      long refreshWindowMillis = Math.min(expiresInMillis / 10, MAX_REFRESH_WINDOW_MILLIS);
      // how much time to wait before expiration
      long waitIntervalMillis = expiresInMillis - refreshWindowMillis;
      // how much time has already elapsed since the new token was issued
      long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
      // how much time to actually wait
      long timeToWait = Math.max(waitIntervalMillis - elapsedMillis, MIN_REFRESH_WAIT_MILLIS);

      tokenRefreshExecutor.schedule(
          () -> {
            long refreshStartTime = System.currentTimeMillis();
            Pair<Integer, TimeUnit> expiration = session.refresh(client);
            if (expiration != null) {
              scheduleTokenRefresh(
                  client,
                  tokenRefreshExecutor,
                  session,
                  refreshStartTime,
                  expiration.first(),
                  expiration.second());
            }
          },
          timeToWait,
          TimeUnit.MILLISECONDS);
    }

    public static AuthSession sessionFromToken(
        RESTClient client,
        ScheduledExecutorService tokenRefreshExecutor,
        String token,
        String credential,
        Long defaultExpirationMillis,
        AuthSession parent) {
      Optional<JWT> jwt = JWT.of(token);

      if (jwt.isPresent() && jwt.get().isExpired()) {
        Preconditions.checkState(
            null != credential, "Credential is required to refresh expired token.");

        // we add the credential to the Authorization header and perform a token exchange to
        // refresh the expired token
        AuthSession session =
            new AuthSession(OAuth2Util.basicAuthHeaders(credential), null, null, credential);

        return AuthSession.sessionFromTokenExchange(
            client,
            tokenRefreshExecutor,
            token,
            OAuth2Properties.ACCESS_TOKEN_TYPE,
            session,
            OAuth2Properties.CATALOG_SCOPE);
      } else {
        AuthSession session =
            new AuthSession(
                parent.headers(), token, OAuth2Properties.ACCESS_TOKEN_TYPE, credential);
        Long expiresInMillis = jwt.map(JWT::expiresInMillis).orElse(defaultExpirationMillis);

        if (null != expiresInMillis) {
          scheduleTokenRefresh(
              client,
              tokenRefreshExecutor,
              session,
              System.currentTimeMillis(),
              expiresInMillis,
              TimeUnit.MILLISECONDS);
        }

        return session;
      }
    }

    public static AuthSession sessionFromFetchedToken(
        RESTClient client,
        ScheduledExecutorService tokenRefreshExecutor,
        OAuthTokenResponse response,
        long startTimeMillis,
        AuthSession parent) {
      AuthSession session =
          new AuthSession(
              parent.headers(), response.token(), response.issuedTokenType(), parent.credential());
      if (response.expiresInSeconds() != null) {
        scheduleTokenRefresh(
            client,
            tokenRefreshExecutor,
            session,
            startTimeMillis,
            response.expiresInSeconds(),
            TimeUnit.SECONDS);
      }

      return session;
    }

    public static AuthSession sessionFromTokenExchange(
        RESTClient client,
        ScheduledExecutorService tokenRefreshExecutor,
        String token,
        String tokenType,
        AuthSession parent,
        String scope) {
      long startTimeMillis = System.currentTimeMillis();
      OAuthTokenResponse response =
          exchangeToken(
              client,
              parent.headers(),
              token,
              tokenType,
              parent.token(),
              parent.tokenType(),
              scope);
      return sessionFromFetchedToken(
          client, tokenRefreshExecutor, response, startTimeMillis, parent);
    }
  }
}
