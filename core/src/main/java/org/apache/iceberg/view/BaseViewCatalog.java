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

package org.apache.iceberg.view;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public interface BaseViewCatalog extends ViewCatalog {

  boolean isValidIdentifier(TableIdentifier identifier);

  String defaultWarehouseLocation(TableIdentifier identifier);

  ViewOperations newViewOps(TableIdentifier identifier);

  default View loadView(TableIdentifier identifier) {
    View result;
    if (isValidIdentifier(identifier)) {
      ViewOperations ops = newViewOps(identifier);
      if (ops.current() == null) {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      } else {
        result = new BaseView(newViewOps(identifier), ViewUtil.fullViewName(name(), identifier));
      }
    } else {
      throw new NoSuchViewException("Invalid view identifier: %s", identifier);
    }

    return result;
  }

  @Override
  default ViewBuilder buildView(TableIdentifier identifier) {
    return new BaseViewBuilder(this, identifier);
  }

  class BaseViewBuilder implements ViewBuilder {
    private final BaseViewCatalog catalog;
    private final TableIdentifier identifier;
    private final ImmutableViewVersion.Builder viewVersionBuilder = ImmutableViewVersion.builder();
    private final List<ViewRepresentation> viewRepresentations = Lists.newArrayList();
    private Schema schema;
    private final Map<String, String> properties = Maps.newHashMap();

    public BaseViewBuilder(BaseViewCatalog catalog, TableIdentifier identifier) {
      Preconditions.checkArgument(
          catalog.isValidIdentifier(identifier), "Invalid view identifier: %s", identifier);
      this.catalog = catalog;
      this.identifier = identifier;
    }

    @Override
    public ViewBuilder withSchema(Schema newSchema) {
      this.schema = newSchema;
      viewVersionBuilder.schemaId(newSchema.schemaId());
      return this;
    }

    @Override
    public ViewBuilder withQuery(String dialect, String sql) {
      viewRepresentations.add(
          ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
      return this;
    }

    @Override
    public ViewBuilder withDefaultCatalog(String catalog) {
      viewVersionBuilder.defaultCatalog(catalog);
      return this;
    }

    @Override
    public ViewBuilder withDefaultNamespace(Namespace namespace) {
      viewVersionBuilder.defaultNamespace(namespace);
      return this;
    }

    @Override
    public ViewBuilder withProperties(Map<String, String> newProperties) {
      this.properties.putAll(newProperties);
      return this;
    }

    @Override
    public ViewBuilder withProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    @Override
    public View create() {
      ViewOperations ops = catalog.newViewOps(identifier);
      if (null != ops.current()) {
        throw new AlreadyExistsException("View already exists: %s", identifier);
      }

      long timestampMillis = System.currentTimeMillis();

      ViewVersion viewVersion =
          viewVersionBuilder
              .versionId(1)
              .addAllRepresentations(viewRepresentations)
              .timestampMillis(timestampMillis)
              .putSummary("operation", "create")
              .build();

      ViewMetadata.Builder builder =
          ViewMetadata.builder()
              .setProperties(properties)
              .setLocation(catalog.defaultWarehouseLocation(identifier));

      if (null != schema) {
        builder.addSchema(schema);
      }

      ViewMetadata viewMetadata =
          builder.addVersion(viewVersion).setCurrentVersionId(viewVersion.versionId()).build();

      try {
        ops.commit(null, viewMetadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("View was created concurrently: %s", identifier);
      }

      return new BaseView(ops, ViewUtil.fullViewName(catalog.name(), identifier));
    }

    @Override
    public View replace() {
      ViewOperations ops = catalog.newViewOps(identifier);
      if (null == ops.current()) {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      }

      ViewMetadata metadata = ops.current();

      long timestampMillis = System.currentTimeMillis();

      ViewVersion viewVersion =
          viewVersionBuilder
              .versionId(metadata.currentVersionId() + 1)
              .addAllRepresentations(viewRepresentations)
              .timestampMillis(timestampMillis)
              .putSummary("operation", "replace")
              .build();

      ViewMetadata.Builder builder = ViewMetadata.buildFrom(metadata).setProperties(properties);

      if (null != schema) {
        builder.addSchema(schema);
      }

      ViewMetadata replacement =
          builder.addVersion(viewVersion).setCurrentVersionId(viewVersion.versionId()).build();

      try {
        ops.commit(metadata, replacement);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("View was updated concurrently: %s", identifier);
      }

      return new BaseView(ops, ViewUtil.fullViewName(catalog.name(), identifier));
    }

    @Override
    public View createOrReplace() {
      if (null == catalog.newViewOps(identifier).current()) {
        return create();
      } else {
        return replace();
      }
    }
  }
}
