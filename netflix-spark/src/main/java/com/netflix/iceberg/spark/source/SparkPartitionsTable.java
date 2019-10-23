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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.iceberg.spark.SparkSchemaUtil.convert;

public class SparkPartitionsTable extends StaticTable<SparkPartitionsTable.Partition> {

  public SparkPartitionsTable(Table table, Long snapshotId, Long asOfTimestamp) {
    super(schema(table.spec().partitionType()),
        partitions(table, snapshotId, asOfTimestamp),
        converter(table.spec().partitionType()));
  }

  private static Function<Partition, InternalRow> converter(Types.StructType partitionType) {
    StructLikeToRowConverter partitionConverter = new StructLikeToRowConverter(partitionType);
    return partition -> new GenericInternalRow(new Object[] {
        partitionConverter.apply(partition.key),
        partition.recordCount,
        partition.fileCount
    });
  }

  private static Iterable<Partition> partitions(Table table, Long snapshotId, Long asOfTimestamp) {
    PartitionSet partitions = new PartitionSet();
    TableScan scan = table.newScan();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    if (asOfTimestamp != null) {
      scan = scan.asOfTime(asOfTimestamp);
    }

    for (FileScanTask task : scan.planFiles()) {
      partitions.get(task.file().partition()).update(task.file());
    }

    return partitions.all();
  }

  private static StructType schema(Types.StructType partitionType) {
    return new StructType(new StructField[] {
        new StructField("partition", convert(partitionType), false, Metadata.empty()),
        new StructField("record_count", LongType$.MODULE$, false, Metadata.empty()),
        new StructField("file_count", IntegerType$.MODULE$, false, Metadata.empty())
    });
  }

  static class PartitionSet {
    private final Map<StructLikeWrapper, Partition> partitions = Maps.newHashMap();
    private final StructLikeWrapper reused = StructLikeWrapper.wrap(null);

    Partition get(StructLike key) {
      Partition partition = partitions.get(reused.set(key));
      if (partition == null) {
        partition = new Partition(key);
        partitions.put(StructLikeWrapper.wrap(key), partition);
      }
      return partition;
    }

    Iterable<Partition> all() {
      return partitions.values();
    }
  }

  static class Partition {
    private final StructLike key;
    private long recordCount;
    private int fileCount;

    Partition(StructLike key) {
      this.key = key;
      this.recordCount = 0;
      this.fileCount = 0;
    }

    void update(DataFile file) {
      this.recordCount += file.recordCount();
      this.fileCount += 1;
    }
  }

  static class StructLikeToRowConverter implements Function<StructLike, InternalRow> {
    private final DataType[] types;
    private final Class<?>[] javaTypes;

    StructLikeToRowConverter(Types.StructType struct) {
      StructField[] fields = ((StructType) SparkSchemaUtil.convert(struct)).fields();

      this.javaTypes = new Class<?>[fields.length];
      this.types = new DataType[fields.length];

      for (int index = 0; index < fields.length; index += 1) {
        javaTypes[index] = struct.fields().get(index).type().asPrimitiveType().typeId().javaClass();
        types[index] = fields[index].dataType();
      }
    }

    @Override
    public InternalRow apply(StructLike tuple) {
      GenericInternalRow row = new GenericInternalRow(new Object[types.length]);
      for (int i = 0; i < types.length; i += 1) {
        Object value = tuple.get(i, javaTypes[i]);
        if (value != null) {
          row.update(i, convert(value, types[i]));
        } else {
          row.setNullAt(i);
        }
      }

      return row;
    }

    /**
     * Converts the objects into instances used by Spark's InternalRow.
     *
     * @param value a data value
     * @param type the Spark data type
     * @return the value converted to the representation expected by Spark's InternalRow.
     */
    private static Object convert(Object value, DataType type) {
      if (type instanceof StringType) {
        return UTF8String.fromString(value.toString());
      } else if (type instanceof BinaryType) {
        ByteBuffer buffer = (ByteBuffer) value;
        return buffer.get(new byte[buffer.remaining()]);
      } else if (type instanceof DecimalType) {
        return Decimal.fromDecimal(value);
      }
      return value;
    }
  }
}
