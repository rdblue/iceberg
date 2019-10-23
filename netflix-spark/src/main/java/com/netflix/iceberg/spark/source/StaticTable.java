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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import jersey.repackaged.com.google.common.collect.Iterables;
import org.apache.spark.sql.catalog.v2.PartitionTransform;
import org.apache.spark.sql.catalog.v2.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;

public class StaticTable<T> implements Table, ReadSupport {
  private final StructType schema;
  private final InternalRow[] rows;

  public StaticTable(StructType schema, Iterable<T> values, Function<T, InternalRow> transform) {
    this.schema = schema;
    this.rows = Lists.newArrayList(Iterables.transform(values, transform::apply))
        .toArray(new InternalRow[0]);
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public List<PartitionTransform> partitioning() {
    return null;
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return new StaticReader(schema, rows);
  }

  private static class StaticReader implements DataSourceReader {
    private final StructType schema;
    private final InternalRow[] rows;

    private StaticReader(StructType schema, InternalRow[] rows) {
      this.schema = schema;
      this.rows = rows;
    }

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      return ImmutableList.of(new StaticInputPartition(rows));
    }
  }

  private static class StaticInputPartition implements InputPartition<InternalRow> {
    private final InternalRow[] rows;

    private StaticInputPartition(InternalRow[] rows) {
      this.rows = rows;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
      return new InputPartitionReader<InternalRow>() {
        private int index = -1;

        @Override
        public boolean next() {
          index += 1;
          return index < rows.length;
        }

        @Override
        public InternalRow get() {
          return rows[index];
        }

        @Override
        public void close() {
        }
      };
    }
  }
}
