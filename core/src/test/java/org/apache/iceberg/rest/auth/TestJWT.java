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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.Test;

public class TestJWT {

  @Test
  public void invalidToken() {
    assertThat(JWT.of(null)).isNotPresent();
    assertThat(JWT.of("token")).isNotPresent();
  }

  @Test
  public void tokenExpirationInPast() {
    // expires at epoch second = 1
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjF9.gQADTbdEv-rpDWKSkGLbmafyB5UUjTdm9B_1izpuZ6E";
    Optional<JWT> jwtToken = JWT.of(token);
    assertThat(jwtToken).isPresent();
    assertThat(jwtToken.get().isExpired()).isTrue();
    assertThat(jwtToken.get().expiresAtEpochMillis()).isEqualTo(1000L);
    assertThat(jwtToken.get().expiresInMillis()).isLessThan(0);
  }

  @Test
  public void tokenExpirationInFuture() {
    // expires at epoch second = 19999999999
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE5OTk5OTk5OTk5fQ._3k92KJi2NTyTG6V1s2mzJ__GiQtL36DnzsZSkBdYPw";
    Optional<JWT> jwtToken = JWT.of(token);
    assertThat(jwtToken).isPresent();
    assertThat(jwtToken.get().isExpired()).isFalse();
    assertThat(jwtToken.get().expiresAtEpochMillis()).isEqualTo(19999999999000L);
    assertThat(jwtToken.get().expiresInMillis()).isGreaterThan(1);
  }

  @Test
  public void tokenWithoutExpiration() {
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
    Optional<JWT> jwtToken = JWT.of(token);
    assertThat(jwtToken).isPresent();
    assertThat(jwtToken.get().isExpired()).isFalse();
    assertThat(jwtToken.get().expiresAtEpochMillis()).isEqualTo(Long.MAX_VALUE);
    assertThat(jwtToken.get().expiresInMillis()).isGreaterThan(1);
  }
}
