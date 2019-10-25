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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.v2.CaseInsensitiveStringMap;
import org.apache.spark.sql.catalog.v2.PartitionTransform;
import org.apache.spark.sql.catalog.v2.PartitionTransforms;
import org.apache.spark.sql.catalog.v2.TableCatalog;
import org.apache.spark.sql.catalog.v2.TableChange;
import org.apache.spark.sql.catalog.v2.TableChange.RemoveProperty;
import org.apache.spark.sql.catalog.v2.TableChange.SetProperty;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkCatalog implements TableCatalog {
  enum TableType {
    DATA,
    FILES,
    ENTRIES,
    HISTORY,
    SNAPSHOTS,
    MANIFESTS,
    PARTITIONS
  }

  protected abstract Catalog catalog();

  protected abstract org.apache.iceberg.catalog.TableIdentifier toIceberg(TableIdentifier ident);

  private String name = null;
  private SparkSession spark = null;

  protected SparkSession lazySparkSession() {
    if (spark == null) {
      this.spark = SparkSession.builder().getOrCreate();
    }
    return spark;
  }

  private SparkTable loadInternal(TableIdentifier ident) throws NoSuchTableException {
    Table table;
    try {
      table = catalog().loadTable(toIceberg(ident));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident.database().get(), ident.table());
    }

    if (table == null) {
      throw new NoSuchTableException(ident.database().get(), ident.table());
    }

    return new SparkTable(table, lazySparkSession());
  }

  @Override
  public org.apache.spark.sql.catalog.v2.Table loadTable(TableIdentifier ident)
      throws NoSuchTableException {

    TableRef ref = TableRef.parse(ident.table());
    SparkTable sourceTable = loadInternal(new TableIdentifier(ref.table(), ident.database()));
    Long snapshotId = null;
    Long asOfTimestamp = null;
    if (ref.at() != null) {
      if (sourceTable.table().snapshot(ref.at()) != null) {
        snapshotId = ref.at();
      } else {
        Preconditions.checkArgument(ref.at() < System.currentTimeMillis(),
            "Invalid timestamp: %s is in the future", ref.at());
        asOfTimestamp = ref.at();
      }
    }

    switch (ref.type()) {
      case DATA:
        if (ref.at() != null) {
          return new SparkTable(sourceTable.table(), lazySparkSession(), snapshotId, asOfTimestamp);
        } else {
          return sourceTable;
        }
      case PARTITIONS:
        return new SparkPartitionsTable(sourceTable.table(), snapshotId, asOfTimestamp);
      case MANIFESTS:
        return new SparkManifestsTable(sourceTable.table(), snapshotId, asOfTimestamp);
      case HISTORY:
        return new SparkHistoryTable(sourceTable.table());
      case SNAPSHOTS:
        return new SparkSnapshotsTable(sourceTable.table());
      default:
        throw new IllegalArgumentException("Unknown table type: " + ref.type());
    }
  }

  @Override
  public SparkTable refreshTable(TableIdentifier ident) throws NoSuchTableException {
    // refresh the table in Spark's catalog
    lazySparkSession().catalog().refreshTable(ident.quotedString());
    // refresh the Iceberg table
    SparkTable table = loadInternal(ident);
    table.table().refresh();
    return table;
  }

  @Override
  public SparkTable createTable(TableIdentifier ident, StructType tableType,
                                List<PartitionTransform> partitions,
                                Map<String, String> properties) throws TableAlreadyExistsException {
    if (tableExists(ident)) {
      throw new TableAlreadyExistsException(ident.database().get(), ident.table());
    }

    Schema schema = SparkSchemaUtil.convert(tableType);
    PartitionSpec spec = convert(schema, partitions);

    return new SparkTable(catalog().createTable(toIceberg(ident), schema, spec, properties), lazySparkSession());
  }

  @Override
  public SparkTable alterTable(TableIdentifier ident, List<TableChange> changes)
      throws NoSuchTableException {
    SetProperty setLocation = null;
    SetProperty setSnapshotId = null;
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();

    for (TableChange change : changes) {
      if (change instanceof SetProperty) {
        SetProperty set = (SetProperty) change;
        if ("location".equalsIgnoreCase(set.property())) {
          setLocation = set;
        } else if ("current-snapshot-id".equalsIgnoreCase(set.property())) {
          setSnapshotId = set;
        } else {
          propertyChanges.add(set);
        }
      } else if (change instanceof RemoveProperty) {
        propertyChanges.add(change);
      } else {
        schemaChanges.add(change);
      }
    }

    SparkTable table = loadInternal(ident);

    // if updating the table snapshot, perform that update first in case it fails
    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId.value());
      table.table().rollback().toSnapshotId(newSnapshotId).commit();
    }

    // use a transaction to apply all the remaining changes at one time
    Transaction transaction = table.table().newTransaction();

    if (setLocation != null) {
      transaction.updateLocation()
          .setLocation(setLocation.value())
          .commit();
    }

    if (!propertyChanges.isEmpty()) {
      applyPropertyChanges(transaction.table(), propertyChanges);
    }

    if (!schemaChanges.isEmpty()) {
      applySchemaChanges(transaction.table(), schemaChanges);
    }

    transaction.commitTransaction();

    return table;
  }

  private void applyPropertyChanges(Table table, List<TableChange> changes) {
    UpdateProperties pendingUpdate = table.updateProperties();

    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        TableChange.SetProperty set = (TableChange.SetProperty) change;
        pendingUpdate.set(set.property(), set.value());

      } else if (change instanceof TableChange.RemoveProperty) {
        TableChange.RemoveProperty remove = (TableChange.RemoveProperty) change;
        pendingUpdate.remove(remove.property());

      } else {
        throw new UnsupportedOperationException("Unsupported table change: " + change);
      }
    }

    pendingUpdate.commit();
  }

  private void applySchemaChanges(Table table, List<TableChange> changes) {
    UpdateSchema pendingUpdate = table.updateSchema();

    for (TableChange change : changes) {
      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        Type type = SparkSchemaUtil.convert(add.type());
        pendingUpdate.addColumn(add.parent(), add.name(), type, add.comment());

      } else if (change instanceof TableChange.UpdateColumn) {
        TableChange.UpdateColumn update = (TableChange.UpdateColumn) change;
        Type newType = SparkSchemaUtil.convert(update.newDataType());
        Preconditions.checkArgument(newType.isPrimitiveType(),
            "Cannot update '%s', not a primitive type: %s", update.name(), update.newDataType());
        pendingUpdate.updateColumn(update.name(), newType.asPrimitiveType());

      } else if (change instanceof TableChange.UpdateColumnComment) {
        TableChange.UpdateColumnComment update = (TableChange.UpdateColumnComment) change;
        pendingUpdate.updateColumnDoc(update.name(), update.newComment());

      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
        pendingUpdate.renameColumn(rename.name(), rename.newName());

      } else if (change instanceof TableChange.DeleteColumn) {
        TableChange.DeleteColumn delete = (TableChange.DeleteColumn) change;
        pendingUpdate.deleteColumn(delete.name());

      } else {
        throw new UnsupportedOperationException("Unsupported table change: " + change);
      }
    }

    pendingUpdate.commit();
  }

  @Override
  public boolean dropTable(TableIdentifier ident) {
    return catalog().dropTable(toIceberg(ident));
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  private PartitionSpec convert(Schema schema, List<PartitionTransform> partitioning) {
    if (partitioning == null || partitioning.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (PartitionTransform partition : partitioning) {
      Preconditions.checkArgument(partition.references().length == 1,
          "Cannot convert transform with more than one column reference: " + partition);
      String colName = partition.references()[0];
      if (partition instanceof PartitionTransforms.Identity) {
        builder.identity(colName);
      } else if (partition instanceof PartitionTransforms.Bucket) {
        builder.bucket(colName, ((PartitionTransforms.Bucket) partition).numBuckets());
      } else if (partition instanceof PartitionTransforms.Year) {
        builder.year(colName);
      } else if (partition instanceof PartitionTransforms.Month) {
        builder.month(colName);
      } else if (partition instanceof PartitionTransforms.Date) {
        builder.day(colName);
      } else if (partition instanceof PartitionTransforms.DateAndHour) {
        builder.hour(colName);
      }
      // else if (partition instanceof PartitionTransforms.Apply) {
      //   builder.add(schema.findField(colName).fieldId(), colName, partition.name());
      // }
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return "IcebergCatalog(name=" + name + ")";
  }

  static class TableRef {
    private static final Logger LOG = LoggerFactory.getLogger(TableRef.class);

    private static final Pattern TABLE_PATTERN = Pattern.compile(
        "(?<table>(?:[^$@_]|_[^$@_])+)" +
            "(?:(?:@|__)(?<ver1>\\d+))?" +
            "(?:(?:\\$|__)(?<type>(?:history|snapshots|manifests|partitions|files|entries))" +
            "(?:(?:@|__)(?<ver2>\\d+))?)?");

    static TableRef parse(String rawName) {
      Matcher match = TABLE_PATTERN.matcher(rawName);
      if (match.matches()) {
        try {
          String table = match.group("table");
          String typeStr = match.group("type");
          String ver1 = match.group("ver1");
          String ver2 = match.group("ver2");

          TableType type;
          if (typeStr != null) {
            type = TableType.valueOf(typeStr.toUpperCase(Locale.ROOT));
          } else {
            type = TableType.DATA;
          }

          Long version;
          if (type == TableType.DATA ||
              type == TableType.PARTITIONS ||
              type == TableType.MANIFESTS ||
              type == TableType.FILES ||
              type == TableType.ENTRIES) {
            Preconditions.checkArgument(ver1 == null || ver2 == null,
                "Cannot specify two versions");
            if (ver1 != null) {
              version = Long.parseLong(ver1);
            } else if (ver2 != null) {
              version = Long.parseLong(ver2);
            } else {
              version = null;
            }
          } else {
            Preconditions.checkArgument(ver1 == null && ver2 == null,
                "Cannot use version with table type %s: %s", typeStr, rawName);
            version = null;
          }

          return new TableRef(table, type, version);

        } catch (IllegalArgumentException e) {
          LOG.warn("Failed to parse table name, using {}: {}", rawName, e.getMessage());
        }
      }

      return new TableRef(rawName, TableType.DATA, null);
    }

    private final String table;
    private final TableType type;
    private final Long at;

    private TableRef(String table, TableType type, Long at) {
      this.table = table;
      this.type = type;
      this.at = at;
    }

    public String table() {
      return table;
    }

    public TableType type() {
      return type;
    }

    public Long at() {
      return at;
    }
  }
}
