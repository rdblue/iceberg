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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.CatalogWithViews;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreCatalog implements CatalogWithViews {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);

  private MetricsReporter metricsReporter;

  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table result;
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() == null) {
        // the identifier may be valid for both tables and metadata tables
        if (isValidMetadataIdentifier(identifier)) {
          result = loadMetadataTable(identifier);

        } else {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }

      } else {
        result = new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
      }

    } else if (isValidMetadataIdentifier(identifier)) {
      result = loadMetadataTable(identifier);

    } else {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }

    LOG.info("Table loaded by catalog: {}", result);
    return result;
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    Preconditions.checkArgument(
        identifier != null && isValidIdentifier(identifier), "Invalid identifier: %s", identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a table");

    // Throw an exception if this table already exists in the catalog.
    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    TableOperations ops = newTableOps(identifier);
    InputFile metadataFile = ops.io().newInputFile(metadataFileLocation);
    TableMetadata metadata = TableMetadataParser.read(ops.io(), metadataFile);
    ops.commit(null, metadata);

    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new BaseMetastoreCatalogTableBuilder(identifier, schema);
  }

  private Table loadMetadataTable(TableIdentifier identifier) {
    String tableName = identifier.name();
    MetadataTableType type = MetadataTableType.from(tableName);
    if (type != null) {
      TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      TableOperations ops = newTableOps(baseTableIdentifier);
      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", baseTableIdentifier);
      }

      return MetadataTableUtils.createMetadataTableInstance(
          ops, name(), baseTableIdentifier, identifier, type);
    } else {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }
  }

  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null
        && isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }

  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    // by default allow all identifiers
    return true;
  }

  protected Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  protected abstract TableOperations newTableOps(TableIdentifier tableIdentifier);

  protected abstract String defaultWarehouseLocation(TableIdentifier tableIdentifier);

  protected class BaseMetastoreCatalogTableBuilder implements TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final Map<String, String> tableProperties = Maps.newHashMap();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public BaseMetastoreCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid table identifier: %s", identifier);

      this.identifier = identifier;
      this.schema = schema;
      this.tableProperties.putAll(tableDefaultProperties());
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        tableProperties.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      tableProperties.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
      }

      return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
    }

    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      return Transactions.createTableTransaction(identifier.toString(), ops, metadata);
    }

    @Override
    public Transaction replaceTransaction() {
      return newReplaceTableTransaction(false);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return newReplaceTableTransaction(true);
    }

    private Transaction newReplaceTableTransaction(boolean orCreate) {
      TableOperations ops = newTableOps(identifier);
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      TableMetadata metadata;
      tableProperties.putAll(tableOverrideProperties());
      if (ops.current() != null) {
        String baseLocation = location != null ? location : ops.current().location();
        metadata =
            ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, tableProperties);
      } else {
        String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
        metadata =
            TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(identifier.toString(), ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(identifier.toString(), ops, metadata);
      }
    }

    /**
     * Get default table properties set at Catalog level through catalog properties.
     *
     * @return default table properties specified in catalog properties
     */
    private Map<String, String> tableDefaultProperties() {
      Map<String, String> tableDefaultProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_DEFAULT_PREFIX);
      LOG.info(
          "Table properties set at catalog level through catalog properties: {}",
          tableDefaultProperties);
      return tableDefaultProperties;
    }

    /**
     * Get table properties that are enforced at Catalog level through catalog properties.
     *
     * @return default table properties enforced through catalog properties
     */
    private Map<String, String> tableOverrideProperties() {
      Map<String, String> tableOverrideProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_OVERRIDE_PREFIX);
      LOG.info(
          "Table properties enforced at catalog level through catalog properties: {}",
          tableOverrideProperties);
      return tableOverrideProperties;
    }
  }

  protected static String fullTableName(String catalogName, TableIdentifier identifier) {
    StringBuilder sb = new StringBuilder();

    if (catalogName.contains("/") || catalogName.contains(":")) {
      // use / for URI-like names: thrift://host:port/db.table
      sb.append(catalogName);
      if (!catalogName.endsWith("/")) {
        sb.append("/");
      }
    } else {
      // use . for non-URI named catalogs: prod.db.table
      sb.append(catalogName).append(".");
    }

    for (String level : identifier.namespace().levels()) {
      sb.append(level).append(".");
    }

    sb.append(identifier.name());

    return sb.toString();
  }

  private MetricsReporter metricsReporter() {
    if (metricsReporter == null) {
      metricsReporter = CatalogUtil.loadMetricsReporter(properties());
    }

    return metricsReporter;
  }

  protected ViewOperations newViewOps(TableIdentifier identifier) {
    throw new UnsupportedOperationException("Not implemented: newViewOps");
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    throw new UnsupportedOperationException("Not implemented: listViews");
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    View result;
    if (isValidIdentifier(identifier)) {
      ViewOperations ops = newViewOps(identifier);
      if (ops.current() == null) {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      } else {
        result = new BaseView(newViewOps(identifier), fullTableName(name(), identifier));
      }
    } else {
      throw new NoSuchViewException("Invalid view identifier: %s", identifier);
    }

    LOG.info("View loaded by catalog: {}", result);
    return result;
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    throw new UnsupportedOperationException("Not implemented: dropView");
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Not implemented: renameView");
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return new BaseViewBuilder(identifier);
  }

  protected class BaseViewBuilder implements ViewBuilder {
    private final TableIdentifier identifier;
    private final ImmutableViewVersion.Builder viewVersionBuilder = ImmutableViewVersion.builder();
    private final List<ViewRepresentation> viewRepresentations = Lists.newArrayList();
    private Schema schema;
    private final Map<String, String> properties = Maps.newHashMap();

    public BaseViewBuilder(TableIdentifier identifier) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid view identifier: %s", identifier);
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
      ViewOperations ops = newViewOps(identifier);
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
              .setLocation(defaultWarehouseLocation(identifier));

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

      return new BaseView(ops, fullTableName(name(), identifier));
    }

    @Override
    public View replace() {
      ViewOperations ops = newViewOps(identifier);
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

      return new BaseView(ops, fullTableName(name(), identifier));
    }

    @Override
    public View createOrReplace() {
      if (null == newViewOps(identifier).current()) {
        return create();
      } else {
        return replace();
      }
    }
  }
}
