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
package org.apache.iceberg.delta;

import com.google.common.collect.ImmutableList;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDeltaTable {
  private static final Configuration CONF = new Configuration();
  private static final Engine ENGINE = DefaultEngine.create(CONF);

  private static final StructType SCHEMA =
      new StructType(
          ImmutableList.of(
              new StructField(
                  "id",
                  LongType.LONG,
                  false), //,
                  //FieldMetadata.builder().putLong("delta.columnMapping.id", 1).build()),
              new StructField(
                  "data",
                  StringType.STRING,
                  true))); //,
                  //FieldMetadata.builder().putLong("delta.columnMapping.id", 2).build())));

  private Table createDeltaTable(File tempDir) {
    Table deltaTable = Table.forPath(ENGINE, tempDir.toString());

    Transaction create =
        deltaTable
            .createTransactionBuilder(ENGINE, IcebergBuild.fullVersion(), Operation.CREATE_TABLE)
            .withSchema(ENGINE, SCHEMA)
            .withPartitionColumns(ENGINE, ImmutableList.of())
            .build(ENGINE);

    create.commit(ENGINE, CloseableIterable.emptyIterable());

    SnapshotImpl snapshot = (SnapshotImpl) deltaTable.getLatestSnapshot(ENGINE);
    Protocol initProtocol = snapshot.getProtocol();
    Protocol versionUpdate =
        new Protocol(
            Math.max(2, initProtocol.getMinReaderVersion()),
            Math.max(7, initProtocol.getMinWriterVersion()),
            initProtocol.getReaderFeatures(),
            initProtocol.getWriterFeatures());

    deltaTable
        .createTransactionBuilder(ENGINE, IcebergBuild.fullVersion(), Operation.MANUAL_UPDATE)
        .build(ENGINE)
        .commit(
            ENGINE,
            InMemoryCloseableIterable.of(
                ImmutableList.of(SingleAction.createProtocolSingleAction(versionUpdate.toRow()))));

    Metadata initMetadata = snapshot.getMetadata();
    Map<String, String> config = Maps.newHashMap(initMetadata.getConfiguration());
    config.put(ColumnMapping.COLUMN_MAPPING_MODE_KEY, ColumnMapping.COLUMN_MAPPING_MODE_NAME);
    config.put("delta.columnMapping.maxColumnId", "2");

    Metadata withColumnMapping =
        new Metadata(
            initMetadata.getId(),
            initMetadata.getName(),
            initMetadata.getDescription(),
            initMetadata.getFormat(),
            initMetadata.getSchemaString(),
            initMetadata.getSchema(),
            initMetadata.getPartitionColumns(),
            initMetadata.getCreatedTime(),
            VectorUtils.stringStringMapValue(config));

    deltaTable
        .createTransactionBuilder(ENGINE, IcebergBuild.fullVersion(), Operation.MANUAL_UPDATE)
        .build(ENGINE)
        .commit(
            ENGINE,
            InMemoryCloseableIterable.of(
                ImmutableList.of(
                    SingleAction.createMetadataSingleAction(withColumnMapping.toRow()))));

    return deltaTable;
  }

  private DataFile writeParquetFile(DeltaTable table, String name) throws IOException {
    String location =
        table.locationProvider().newDataLocation(FileFormat.PARQUET.addExtension(name));

    DataWriter<Record> writer =
        Parquet.writeData(table.io().newOutputFile(location))
            .forTable(table)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build();

    try (writer) {
      GenericRecord record = GenericRecord.create(table.schema());

      String LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
      for (long i = 0; i < 100; i += 1) {
        record.setField("id", i);
        int index = (int) i % LETTERS.length();
        record.setField("data", LETTERS.substring(index, index + 1));

        writer.write(record);
      }
    }

    return writer.toDataFile();
  }

  @Test
  public void testDeltaTableLoad() {
    DeltaTable table =
        new DeltaTable(TableIdentifier.of("table"), CONF, "/home/blue/tmp/warehouse/default_test");
    table.schemas();
    List<ScanTask> tasks =
        Lists.newArrayList(
            table.newBatchScan().filter(Expressions.lessThan("id", 3)).planFiles().iterator());
    table.currentSnapshot().snapshotId();
  }

  @Test
  public void testDeltaTableCreate(@TempDir File tempDir) throws IOException {
    tempDir.delete();

    Table deltaTable = createDeltaTable(tempDir);

    DeltaTable table = new DeltaTable(TableIdentifier.of("table"), CONF, tempDir.toString());

    DataFile fileA = writeParquetFile(table, "A");
    DataFile fileB = writeParquetFile(table, "B");

    table.newAppend().appendFile(fileA).appendFile(fileB).commit();

    table.refresh();

    List<ScanTask> tasks = Lists.newArrayList(table.newBatchScan().planFiles().iterator());

    Snapshot latest = deltaTable.getLatestSnapshot(ENGINE);
  }
}
