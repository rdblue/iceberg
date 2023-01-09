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

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.immutables.value.Value;

@Value.Immutable
public abstract class JWT {

  public abstract String token();

  public abstract long expiresAtEpochMillis();

  public long expiresInMillis() {
    return expiresAtEpochMillis() - Instant.now().toEpochMilli();
  }

  public boolean isExpired() {
    return expiresAtEpochMillis() <= Instant.now().toEpochMilli();
  }

  public static Optional<JWT> of(String token) {
    try {
      Preconditions.checkArgument(null != token, "Invalid JWT: null");
      String[] parts = token.split("\\.");
      Preconditions.checkArgument(parts.length == 3, "Invalid JWT: %s", token);

      // Parse the payload JSON
      JsonNode jsonNode = JsonUtil.mapper().readTree(Base64.getUrlDecoder().decode(parts[1]));
      Long expiresAt = JsonUtil.getLongOrNull("exp", jsonNode);
      return Optional.of(
          ImmutableJWT.builder()
              .token(token)
              .expiresAtEpochMillis(
                  null == expiresAt
                      ? Long.MAX_VALUE
                      : Instant.ofEpochSecond(expiresAt).toEpochMilli())
              .build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
