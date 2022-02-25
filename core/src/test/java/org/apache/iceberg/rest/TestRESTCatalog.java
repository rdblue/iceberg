/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class TestRESTCatalog extends CatalogTests<RESTCatalog> {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private RESTCatalog restCatalog;

  @Before
  public void createCatalog() throws IOException {
    File warehouse = temp.newFolder();

    JdbcCatalog backendCatalog = new JdbcCatalog();
    backendCatalog.setConf(new Configuration());
    Map<String, String> backendCatalogProperties = ImmutableMap.of(
        CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath(),
        CatalogProperties.URI, "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""),
        JdbcCatalog.PROPERTY_PREFIX + "username", "user",
        JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    backendCatalog.initialize("backend", backendCatalogProperties);

    RESTCatalogAdaptor adaptor = new RESTCatalogAdaptor(backendCatalog, ErrorHandlers.defaultErrorHandler());

    this.restCatalog = new RESTCatalog(adaptor);
    restCatalog.initialize("prod", ImmutableMap.of());
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }
}
