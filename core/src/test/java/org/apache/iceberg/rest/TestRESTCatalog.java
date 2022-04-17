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

package org.apache.iceberg.rest;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRESTCatalog<C extends Catalog & SupportsNamespaces> extends CatalogTests<C> {
  @TempDir
  public Path temp;

  private C catalog;
  private JdbcCatalog backendCatalog;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void createCatalog() {
    File warehouse = temp.toFile();
    Configuration conf = new Configuration();

    this.backendCatalog = new JdbcCatalog();
    backendCatalog.setConf(conf);
    Map<String, String> backendCatalogProperties = ImmutableMap.of(
        CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath(),
        CatalogProperties.URI, "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""),
        JdbcCatalog.PROPERTY_PREFIX + "username", "user",
        JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    backendCatalog.initialize("backend", backendCatalogProperties);

    Map<String, String> testHeaders = ImmutableMap.of("header", "value");

    RESTCatalogAdapter adaptor = new RESTCatalogAdapter(backendCatalog) {
      @Override
      public <T extends RESTResponse> T execute(RESTCatalogAdapter.HTTPMethod method, String path, Object body,
                                                Class<T> responseType, Map<String, String> headers,
                                                Consumer<ErrorResponse> errorHandler) {
        Assertions.assertEquals(headers, testHeaders, "Should pass headers through");
        return super.execute(method, path, body, responseType, headers, errorHandler);
      }
    };

    RESTCatalog restCatalog = new RESTCatalog((config) -> adaptor);
    restCatalog.setConf(conf);

    this.catalog = (C) restCatalog.withHeaders(testHeaders);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored"));
  }

  @AfterEach
  public void closeCatalog() throws IOException {
    if (catalog != null && catalog instanceof Closeable) {
      ((Closeable) catalog).close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }
  }

  @Override
  protected C catalog() {
    return catalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  /* RESTCatalog specific tests */

  @Test
  public void testConfigRoute() throws IOException {
    RESTClient testClient = new RESTClient() {
      @Override
      public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        throw new UnsupportedOperationException("Should not be called for testConfigRoute");
      }

      @Override
      public <T extends RESTResponse> T delete(String path, Class<T> responseType, Map<String, String> headers,
                                               Consumer<ErrorResponse> errorHandler) {
        throw new UnsupportedOperationException("Should not be called for testConfigRoute");
      }

      @Override
      public <T extends RESTResponse> T get(String path, Class<T> responseType, Map<String, String> headers,
                                            Consumer<ErrorResponse> errorHandler) {
        return (T) ConfigResponse
            .builder()
            .withDefaults(ImmutableMap.of(CatalogProperties.CLIENT_POOL_SIZE, "1"))
            .withOverrides(ImmutableMap.of(CatalogProperties.CACHE_ENABLED, "false"))
            .build();
      }

      @Override
      public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
                                             Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        throw new UnsupportedOperationException("Should not be called for testConfigRoute");
      }

      @Override
      public void close() {
      }
    };

    RESTCatalog restCat = new RESTCatalog((config) -> testClient);
    Map<String, String> initialConfig = ImmutableMap.of(
        CatalogProperties.URI, "http://localhost:8080",
        CatalogProperties.CACHE_ENABLED, "true");

    restCat.setConf(new Configuration());
    restCat.initialize("prod", initialConfig);

    Assert.assertEquals("Catalog properties after initialize should use the server's override properties",
        "false", restCat.properties().get(CatalogProperties.CACHE_ENABLED));

    Assert.assertEquals("Catalog after initialize should use the server's default properties if not specified",
        "1", restCat.properties().get(CatalogProperties.CLIENT_POOL_SIZE));
    restCat.close();
  }

  @Test
  public void testInitializeWithBadArguments() throws IOException {
    RESTCatalog restCat = new RESTCatalog();
    AssertHelpers.assertThrows("Configuration passed to initialize cannot be null",
        IllegalArgumentException.class,
        "Invalid configuration: null",
        () -> restCat.initialize("prod", null));

    AssertHelpers.assertThrows("Configuration passed to initialize must have uri",
        IllegalArgumentException.class,
        "REST Catalog server URI is required",
        () -> restCat.initialize("prod", ImmutableMap.of()));

    restCat.close();
  }
}
