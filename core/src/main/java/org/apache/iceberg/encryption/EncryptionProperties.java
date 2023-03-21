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
package org.apache.iceberg.encryption;

public class EncryptionProperties {

  private EncryptionProperties() {}

  public static final String ENCRYPTION_TABLE_KEY = "encryption.table.key.id";

  public static final String ENCRYPTION_DEK_LENGTH = "encryption.data.key.length";
  public static final int ENCRYPTION_DEK_LENGTH_DEFAULT = 16;

  public static final int ENCRYPTION_AAD_LENGTH_DEFAULT = 16;

  /** Implementation of the KMS client for envelope encryption */
  public static final String ENCRYPTION_KMS_CLIENT_IMPL = "encryption.kms.client-impl";
}