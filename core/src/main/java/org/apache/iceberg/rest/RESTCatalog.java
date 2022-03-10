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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.DropNamespaceResponse;
import org.apache.iceberg.rest.responses.DropTableResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

public class RESTCatalog implements Catalog, SupportsNamespaces, Configurable<Configuration> {
  private final RESTClient client;
  private String catalogName = null;
  private Map<String, String> properties = null;
  private Object conf = null;
  private FileIO io = null;

  RESTCatalog(RESTClient client) {
    this.client = client;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name;
    this.properties = properties;
    String ioImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.io = CatalogUtil.loadFileIO(ioImpl != null ? ioImpl : ResolvingFileIO.class.getName(), properties, conf);
  }

  @Override
  public void setConf(Configuration newConf) {
    this.conf = newConf;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    String ns = RESTUtil.urlEncode(namespace);
    ListTablesResponse response = client
        .get("v1/namespaces/" + ns + "/tables", ListTablesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.identifiers();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String tablePath = tablePath(identifier);
    // TODO: support purge flag
    DropTableResponse response = client.delete(
        tablePath, DropTableResponse.class, ErrorHandlers.tableErrorHandler());
    return response.isDropped();
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {

  }

  private LoadTableResponse loadInternal(TableIdentifier identifier) {
    String tablePath = tablePath(identifier);
    return client.get(tablePath, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    LoadTableResponse response = loadInternal(identifier);

    // TODO: pass a customized client
    FileIO tableIO = tableIO(response.config());
    return new BaseTable(
        new RESTTableOperations(client, tablePath(identifier), tableIO, response.tableMetadata()),
        fullTableName(identifier));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(namespace)
        .setProperties(metadata)
        .build();

    // for now, ignore the response because there is no way to return it
    client.post("v1/namespaces", request, CreateNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Preconditions.checkArgument(namespace.isEmpty(), "Cannot list namespaces under parent: %s", namespace);
    // String joined = NULL.join(namespace.levels());
    ListNamespacesResponse response = client
        .get("v1/namespaces", ListNamespacesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.namespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    String ns = RESTUtil.urlEncode(namespace);
    // TODO: rename to LoadNamespaceResponse?
    GetNamespaceResponse response = client
        .get("v1/namespaces/" + ns, GetNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    String ns = RESTUtil.urlEncode(namespace);
    DropNamespaceResponse response = client
        .delete("v1/namespaces/" + ns, DropNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.isDropped();
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    String ns = RESTUtil.urlEncode(namespace);
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .updateAll(properties)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        "v1/namespaces/" + ns + "/properties", request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    String ns = RESTUtil.urlEncode(namespace);
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .removeAll(properties)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        "v1/namespaces/" + ns + "/properties", request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.removed().isEmpty();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new Builder(identifier, schema);
  }

  private class Builder implements TableBuilder {
    private final TableIdentifier ident;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = null;
    private SortOrder writeOrder = null;
    private String location = null;

    private Builder(TableIdentifier ident, Schema schema) {
      this.ident = ident;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec tableSpec) {
      this.spec = tableSpec;
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder tableWriteOrder) {
      this.writeOrder = tableWriteOrder;
      return this;
    }

    @Override
    public TableBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      this.propertiesBuilder.putAll(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      this.propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      String ns = RESTUtil.urlEncode(ident.namespace());
      CreateTableRequest request = CreateTableRequest.builder()
          .withName(ident.name())
          .withSchema(schema)
          .withPartitionSpec(spec)
          .withWriteOrder(writeOrder)
          .withLocation(location)
          .setProperties(propertiesBuilder.build())
          .build();

      LoadTableResponse response = client.post(
          "v1/namespaces/" + ns + "/tables", request, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());

      String tablePath = tablePath(ident);
      FileIO tableIO = tableIO(response.config());

      return new BaseTable(
          new RESTTableOperations(client, tablePath, tableIO, response.tableMetadata()),
          fullTableName(ident));
    }

    @Override
    public Transaction createTransaction() {
      LoadTableResponse response = stageCreate();
      String fullName = fullTableName(ident);

      String tablePath = tablePath(ident);
      FileIO tableIO = tableIO(response.config());
      TableMetadata meta = response.tableMetadata();

      return Transactions.createTableTransaction(
          fullName,
          new RESTTableOperations(
              client, tablePath, tableIO, RESTTableOperations.UpdateType.CREATE, createChanges(meta), meta),
          meta);
    }

    @Override
    public Transaction replaceTransaction() {
      LoadTableResponse response = loadInternal(ident);
      String fullName = fullTableName(ident);

      String tablePath = tablePath(ident);
      FileIO tableIO = tableIO(response.config());
      TableMetadata base = response.tableMetadata();

      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata replacement = base.buildReplacement(
          schema,
          spec != null ? spec : PartitionSpec.unpartitioned(),
          writeOrder != null ? writeOrder : SortOrder.unsorted(),
          location != null ? location : base.location(),
          properties);

      ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

      if (replacement.changes().stream().noneMatch(MetadataUpdate.SetCurrentSchema.class::isInstance)) {
        // ensure there is a change to set the current schema
        changes.add(new MetadataUpdate.SetCurrentSchema(replacement.currentSchemaId()));
      }

      if (replacement.changes().stream().noneMatch(MetadataUpdate.SetDefaultPartitionSpec.class::isInstance)) {
        // ensure there is a change to set the default spec
        changes.add(new MetadataUpdate.SetDefaultPartitionSpec(replacement.defaultSpecId()));
      }

      if (replacement.changes().stream().noneMatch(MetadataUpdate.SetDefaultSortOrder.class::isInstance)) {
        // ensure there is a change to set the default sort order
        changes.add(new MetadataUpdate.SetDefaultSortOrder(replacement.defaultSortOrderId()));
      }

      return Transactions.replaceTableTransaction(
          fullName,
          new RESTTableOperations(
              client, tablePath, tableIO, RESTTableOperations.UpdateType.REPLACE, changes.build(), base),
          replacement);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // return a create or a replace transaction, depending on whether the table exists
      // deciding whether to create or replace can't be determined on the service because schema field IDs are assigned
      // at this point and then used in data and metadata files. because create and replace will assign different
      // field IDs, they must be determined before any writes occur
      try {
        return replaceTransaction();
      } catch (NoSuchTableException e) {
        return createTransaction();
      }
    }

    private LoadTableResponse stageCreate() {
      String ns = RESTUtil.urlEncode(ident.namespace());
      Map<String, String> properties = propertiesBuilder.build();

      CreateTableRequest request = CreateTableRequest.builder()
          .withName(ident.name())
          .withSchema(schema)
          .withPartitionSpec(spec)
          .withWriteOrder(writeOrder)
          .withLocation(location)
          .setProperties(properties)
          .build();

      // TODO: will this be a specific route or a modified create?
      return client.post(
          "v1/namespaces/" + ns + "/stageCreate", request, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
    }
  }

  private static List<MetadataUpdate> createChanges(TableMetadata meta) {
    ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

    Schema schema = meta.schema();
    changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
    changes.add(new MetadataUpdate.SetCurrentSchema(-1));

    PartitionSpec spec = meta.spec();
    if (spec != null && spec.isPartitioned()) {
      changes.add(new MetadataUpdate.AddPartitionSpec(spec));
      changes.add(new MetadataUpdate.SetDefaultPartitionSpec(-1));
    }

    SortOrder order = meta.sortOrder();
    if (order != null && order.isSorted()) {
      changes.add(new MetadataUpdate.AddSortOrder(order));
      changes.add(new MetadataUpdate.SetDefaultSortOrder(-1));
    }

    String location = meta.location();
    if (location != null) {
      changes.add(new MetadataUpdate.SetLocation(location));
    }

    Map<String, String> properties = meta.properties();
    if (properties != null && !properties.isEmpty()) {
      changes.add(new MetadataUpdate.SetProperties(properties));
    }

    return changes.build();
  }

  private String fullTableName(TableIdentifier ident) {
    return String.format("%s.%s", catalogName, ident);
  }

  private static String tablePath(TableIdentifier ident) {
    return "v1/namespaces/" + RESTUtil.urlEncode(ident.namespace()) + "/tables/" + ident.name();
  }

  private FileIO tableIO(Map<String, String> conf) {
    if (conf.isEmpty()) {
      return io; // reuse the FileIO since config is the same
    } else {
      Map<String, String> fullConf = Maps.newHashMap(properties);
      properties.putAll(conf);
      String ioImpl = fullConf.get(CatalogProperties.FILE_IO_IMPL);
      return CatalogUtil.loadFileIO(ioImpl != null ? ioImpl : ResolvingFileIO.class.getName(), fullConf, this.conf);
    }
  }
}
