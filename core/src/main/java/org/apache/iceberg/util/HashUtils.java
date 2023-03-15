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
package org.apache.iceberg.util;

import java.nio.charset.StandardCharsets;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;

public class HashUtils {

  private HashUtils() {}

  private static final HashFunction hashFunction = Hashing.sha1();
  private static final String allChars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  /**
   * Compute hash containing alphanumeric characters
   *
   * @param fileName name of the file
   * @return hash generated
   */
  public static String computeHash(String fileName) {
    Preconditions.checkState(fileName != null, "fileName cannot be null");
    byte[] messageDigest =
        hashFunction.hashBytes(fileName.getBytes(StandardCharsets.UTF_8)).asBytes();

    StringBuilder hash = new StringBuilder();
    for (int i = 0; i < 8; ++i) {
      hash.append(allChars.charAt((messageDigest[i] % 62 + 62) % 62));
    }

    return hash.toString();
  }
}
