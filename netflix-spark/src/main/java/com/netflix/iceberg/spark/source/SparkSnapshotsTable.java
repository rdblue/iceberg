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
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType$;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkSnapshotsTable extends StaticTable<Snapshot> {
  private static final StructType SCHEMA = new StructType(new StructField[] {
      new StructField("committed_at", TimestampType$.MODULE$, false, Metadata.empty()),
      new StructField("snapshot_id", LongType$.MODULE$, false, Metadata.empty()),
      new StructField("parent_id", LongType$.MODULE$, true, Metadata.empty()),
      new StructField("operation", StringType$.MODULE$, true, Metadata.empty()),
      new StructField("manifest_list", StringType$.MODULE$, true, Metadata.empty()),
      new StructField("summary", new MapType(StringType$.MODULE$, StringType$.MODULE$, false),
          true, Metadata.empty())
  });

  public SparkSnapshotsTable(Table table) {
    super(SCHEMA, table.snapshots(), SparkSnapshotsTable::snapToInternalRow);
  }

  private static InternalRow snapToInternalRow(Snapshot snap) {
    return new GenericInternalRow(new Object[] {
        snap.timestampMillis() * 1000,
        snap.snapshotId(),
        snap.parentId(),
        UTF8String.fromString(snap.operation()),
        snap.manifestListLocation() != null ?
            UTF8String.fromString(snap.manifestListLocation()) :
            null,
        internalStringMap(snap.summary())
    });
  }

  private static MapData internalStringMap(Map<String, String> map) {
    if (map == null) {
      return null;
    }

    List<Object> keys = Lists.newArrayListWithExpectedSize(map.size());
    List<Object> values = Lists.newArrayListWithExpectedSize(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      keys.add(UTF8String.fromString(entry.getKey()));
      values.add(UTF8String.fromString(entry.getValue()));
    }

    return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
  }
}
