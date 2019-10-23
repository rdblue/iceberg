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

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType$;

public class SparkHistoryTable extends StaticTable<HistoryEntry> {
  private static final StructType SCHEMA = new StructType(new StructField[] {
      new StructField("made_current_at", TimestampType$.MODULE$, false, Metadata.empty()),
      new StructField("snapshot_id", LongType$.MODULE$, false, Metadata.empty()),
      new StructField("parent_id", LongType$.MODULE$, true, Metadata.empty()),
      new StructField("is_current_ancestor", BooleanType$.MODULE$, false, Metadata.empty())
  });

  public SparkHistoryTable(Table table) {
    super(SCHEMA, table.history(), SparkHistoryTable.convertHistoryEntryFunc(table));
  }

  private static Function<HistoryEntry, InternalRow> convertHistoryEntryFunc(Table table) {
    Map<Long, Snapshot> snapshots = Maps.newHashMap();
    for (Snapshot snap : table.snapshots()) {
      snapshots.put(snap.snapshotId(), snap);
    }

    List<Long> ancestorIds = SnapshotUtil.currentAncestors(table);

    return historyEntry -> {
      long snapshotId = historyEntry.snapshotId();
      Snapshot snap = snapshots.get(snapshotId);
      return new GenericInternalRow(new Object[] {
          historyEntry.timestampMillis() * 1000,
          historyEntry.snapshotId(),
          snap != null ? snap.parentId() : null,
          ancestorIds.contains(snapshotId)
      });
    };
  }
}
