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

package com.netflix.iceberg.spark.source;

import java.io.File;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.spark.sql.catalog.v2.CaseInsensitiveStringMap;
import org.apache.spark.sql.catalyst.TableIdentifier;

public class TestCatalog extends SparkCatalog {
  private File catalogPath = null;

  @Override
  protected Table create(TableIdentifier ident, Schema schema, PartitionSpec spec,
                         Map<String, String> properties) {
    File location = new File(catalogPath, ident.unquotedString());
    return TestTables.create(location, ident.unquotedString(), schema, spec);
  }

  @Override
  protected Table load(TableIdentifier ident) {
    return TestTables.load(ident.unquotedString());
  }

  @Override
  protected boolean drop(TableIdentifier ident) {
    return TestTables.drop(ident.unquotedString());
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);
    this.catalogPath = new File(options.get("path"));
  }
}
