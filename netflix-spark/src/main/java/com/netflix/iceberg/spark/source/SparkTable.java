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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.v2.PartitionTransform;
import org.apache.spark.sql.catalog.v2.PartitionTransforms;
import org.apache.spark.sql.catalog.v2.Table;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DeleteSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import static org.apache.iceberg.spark.source.IcebergSource.validatePartitionTransforms;
import static org.apache.iceberg.spark.source.IcebergSource.validateWriteSchema;

class SparkTable implements Table, ReadSupport, WriteSupport, DeleteSupport {
  private final org.apache.iceberg.Table table;
  private final SparkSession spark;
  private final Long snapshotId;
  private final Long asOfTimestamp;
  private Map<String, String> lazyProperties = null;
  private StructType lazySchema = null;
  private List<PartitionTransform> lazyPartitioning = null;

  SparkTable(org.apache.iceberg.Table table, SparkSession spark) {
    this(table, spark, null, null);
  }

  SparkTable(
      org.apache.iceberg.Table table, SparkSession spark,
      Long snapshotId, Long asOfTimestamp) {
    this.table = table;
    this.spark = spark;
    this.snapshotId = snapshotId;
    this.asOfTimestamp = asOfTimestamp;
  }

  public org.apache.iceberg.Table table() {
    return table;
  }

  @Override
  public Map<String, String> properties() {
    if (lazyProperties == null) {
      this.lazyProperties = ImmutableMap.copyOf(table.properties());
    }
    return lazyProperties;
  }

  @Override
  public StructType schema() {
    if (lazySchema == null) {
      this.lazySchema = SparkSchemaUtil.convert(table.schema());
    }
    return lazySchema;
  }

  @Override
  public List<PartitionTransform> partitioning() {
    if (lazyPartitioning == null) {
      this.lazyPartitioning = ImmutableList.copyOf(convert(table.spec()));
    }
    return lazyPartitioning;
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    String caseSensitive = spark.conf().get("spark.sql.caseSensitive", "true");
    return new Reader(table, Boolean.parseBoolean(caseSensitive), options, snapshotId, asOfTimestamp);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String writeUUID, StructType writeSchema,
                                                 SaveMode mode, DataSourceOptions options) {
    validateWriteSchema(table.schema(), writeSchema);
    validatePartitionTransforms(table.spec());

    return Optional.of(new Writer(table, options, mode == SaveMode.Overwrite,
        spark.sparkContext().applicationId(), spark.conf().get("spark.wap.id", null)));
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    table.newDelete()
        .deleteFromRowFilter(convert(filters))
        .commit();
  }

  @Override
  public String toString() {
    return table.toString();
  }

  private static Expression convert(Filter[] filters) {
    Expression filterExpr = Expressions.alwaysTrue();

    for (Filter filter : filters) {
      filterExpr = Expressions.and(filterExpr, SparkFilters.convert(filter));
    }

    return filterExpr;
  }

  private static List<PartitionTransform> convert(PartitionSpec spec) {
    return PartitionSpecVisitor.visit(spec.schema(), spec,
        new PartitionSpecVisitor<PartitionTransform>() {
          @Override
          public PartitionTransform identity(String sourceName, int sourceId) {
            return PartitionTransforms.identity(sourceName);
          }

          @Override
          public PartitionTransform bucket(String sourceName, int sourceId, int width) {
            return PartitionTransforms.bucket(width, sourceName);
          }

          @Override
          public PartitionTransform truncate(String sourceName, int sourceId, int width) {
            return PartitionTransforms.apply("truncate[" + width + "]", sourceName);
          }

          @Override
          public PartitionTransform year(String sourceName, int sourceId) {
            return PartitionTransforms.year(sourceName);
          }

          @Override
          public PartitionTransform month(String sourceName, int sourceId) {
            return PartitionTransforms.month(sourceName);
          }

          @Override
          public PartitionTransform day(String sourceName, int sourceId) {
            return PartitionTransforms.date(sourceName);
          }

          @Override
          public PartitionTransform hour(String sourceName, int sourceId) {
            return PartitionTransforms.hour(sourceName);
          }
        });
  }
}
