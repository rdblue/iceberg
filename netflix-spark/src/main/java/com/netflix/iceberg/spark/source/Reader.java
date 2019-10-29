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
import com.netflix.iceberg.spark.SparkExpressions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.Reader.ReadTask;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownCatalystFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;

class Reader implements DataSourceReader, SupportsPushDownCatalystFilters,
    SupportsPushDownRequiredColumns, SupportsReportStatistics {

  private static final org.apache.spark.sql.catalyst.expressions.Expression[] NO_EXPRS =
      new org.apache.spark.sql.catalyst.expressions.Expression[0];

  private final Table table;
  private final Long snapshotId;
  private final Long asOfTimestamp;
  private final Map<String, String> options;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final FileIO fileIo;
  private final EncryptionManager encryptionManager;
  private final boolean caseSensitive;
  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private org.apache.spark.sql.catalyst.expressions.Expression[] pushedExprs = NO_EXPRS;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  Reader(Table table, boolean caseSensitive, DataSourceOptions options, Long snapshotId, Long asOfTimestamp) {
    this.table = table;
    this.snapshotId = options.get("snapshot-id").map(Long::parseLong).orElse(snapshotId);
    this.asOfTimestamp = options.get("as-of-timestamp").map(Long::parseLong).orElse(asOfTimestamp);
    this.options = options.asMap();
    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    // look for split behavior overrides in options
    this.splitSize = options.get("split-size").map(Long::parseLong).orElse(null);
    this.splitLookback = options.get("lookback").map(Integer::parseInt).orElse(null);
    this.splitOpenFileCost = options.get("file-open-cost").map(Long::parseLong).orElse(null);

    this.schema = table.schema();
    this.fileIo = table.io();
    this.encryptionManager = table.encryption();
    this.caseSensitive = caseSensitive;
  }

  private Schema lazySchema() {
    if (schema == null) {
      if (requestedSchema != null) {
        this.schema = SparkSchemaUtil.prune(table.schema(), requestedSchema);
      } else {
        this.schema = table.schema();
      }
    }
    return schema;
  }

  private StructType lazyType() {
    if (type == null) {
      this.type = SparkSchemaUtil.convert(lazySchema());
    }
    return type;
  }

  @Override
  public StructType readSchema() {
    return lazyType();
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    List<InputPartition<InternalRow>> readTasks = Lists.newArrayList();
    for (CombinedScanTask task : tasks()) {
      readTasks.add(
          new ReadTask(task, tableSchemaString, expectedSchemaString, fileIo, encryptionManager, caseSensitive));
    }

    return readTasks;
  }

  @Override
  public org.apache.spark.sql.catalyst.expressions.Expression[] pushCatalystFilters(
      org.apache.spark.sql.catalyst.expressions.Expression[] filters) {
    this.tasks = null; // invalidate cached tasks, if present

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<org.apache.spark.sql.catalyst.expressions.Expression> pushed =
        Lists.newArrayListWithExpectedSize(filters.length);

    for (org.apache.spark.sql.catalyst.expressions.Expression filter : filters) {
      Expression expr = SparkExpressions.convert(filter);
      if (expr != null) {
        expressions.add(expr);
        pushed.add(filter);
      }
    }

    this.filterExpressions = expressions;
    this.pushedExprs = pushed.toArray(new org.apache.spark.sql.catalyst.expressions.Expression[0]);

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public org.apache.spark.sql.catalyst.expressions.Expression[] pushedCatalystFilters() {
    return pushedExprs;
  }

  @Override
  public void pruneColumns(StructType newRequestedSchema) {
    this.requestedSchema = newRequestedSchema;

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;
  }

  @Override
  public Statistics getStatistics() {
    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  private List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive)
          .project(lazySchema());

      if (!options.isEmpty()) {
        for (Map.Entry<String, String> option : options.entrySet()) {
          scan = scan.option(option.getKey(), option.getValue());
        }
      }

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (splitSize != null) {
        scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.toString());
      }

      if (splitLookback != null) {
        scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookback.toString());
      }

      if (splitOpenFileCost != null) {
        scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.toString());
      }

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
        this.tasks = Lists.newArrayList(tasksIterable);
      }  catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table, lazySchema().asStruct(), filterExpressions, caseSensitive);
  }
}
