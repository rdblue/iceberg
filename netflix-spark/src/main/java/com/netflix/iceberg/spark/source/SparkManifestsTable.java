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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkManifestsTable extends StaticTable<ManifestFile> {
  private static final StructType PARTITION_SUMMARY_SCHEMA = new StructType(new StructField[] {
      new StructField("contains_null", BooleanType$.MODULE$, false, Metadata.empty()),
      new StructField("lower_bound", StringType$.MODULE$, true, Metadata.empty()),
      new StructField("upper_bound", StringType$.MODULE$, true, Metadata.empty()),
      });

  private static final StructType SCHEMA = new StructType(new StructField[] {
      new StructField("path", StringType$.MODULE$, false, Metadata.empty()),
      new StructField("length", LongType$.MODULE$, false, Metadata.empty()),
      new StructField("partition_spec_id", IntegerType$.MODULE$, false, Metadata.empty()),
      new StructField("added_snapshot_id", LongType$.MODULE$, true, Metadata.empty()),
      new StructField("added_data_files_count", IntegerType$.MODULE$, false, Metadata.empty()),
      new StructField("existing_data_files_count", IntegerType$.MODULE$, false, Metadata.empty()),
      new StructField("deleted_data_files_count", IntegerType$.MODULE$, false, Metadata.empty()),
      new StructField("partitions", new ArrayType(PARTITION_SUMMARY_SCHEMA, false), true,
                      Metadata.empty())
  });

  public SparkManifestsTable(Table table, Long snapshotId, Long asOfTimestamp) {
    super(SCHEMA, snapshot(table, snapshotId, asOfTimestamp).manifests(),
          manifest -> SparkManifestsTable.manifestFileToInternalRow(table.spec(), manifest));
  }

  private static Snapshot snapshot(Table table, Long snapshotId, Long asOfTimestamp) {
    if (snapshotId != null) {
      table.snapshot(snapshotId);

    } else if (asOfTimestamp != null) {
      Long lastSnapshotId = null;
      for (HistoryEntry logEntry : table.history()) {
        if (logEntry.timestampMillis() <= asOfTimestamp) {
          lastSnapshotId = logEntry.snapshotId();
        }
      }
      if (lastSnapshotId != null) {
        return table.snapshot(lastSnapshotId);
      }
    }

    return table.currentSnapshot();
  }

  private static InternalRow manifestFileToInternalRow(PartitionSpec spec, ManifestFile manifest) {
    return new GenericInternalRow(new Object[] {
        UTF8String.fromString(manifest.path()),
        manifest.length(),
        manifest.partitionSpecId(),
        manifest.snapshotId(),
        manifest.addedFilesCount(),
        manifest.existingFilesCount(),
        manifest.deletedFilesCount(),
        partitionsToArrayData(spec, manifest.partitions())
    });
  }

  @SuppressWarnings("unchecked")
  private static ArrayData partitionsToArrayData(PartitionSpec spec, List<PartitionFieldSummary> partitions) {
    List<InternalRow> rows = Lists.newArrayListWithExpectedSize(spec.fields().size());
    for (int i = 0; i < spec.fields().size(); i += 1) {
      ManifestFile.PartitionFieldSummary summary = partitions.get(i);
      rows.add(new GenericInternalRow(new Object[]{
          summary.containsNull(),
          UTF8String.fromString(spec.fields().get(i).transform().toHumanString(
              Conversions.fromByteBuffer(spec.partitionType().fields().get(i).type(), summary.lowerBound()))),
          UTF8String.fromString(spec.fields().get(i).transform().toHumanString(
              Conversions.fromByteBuffer(spec.partitionType().fields().get(i).type(), summary.upperBound())))
      }));
    }
    return new GenericArrayData((List) rows);
  }
}
