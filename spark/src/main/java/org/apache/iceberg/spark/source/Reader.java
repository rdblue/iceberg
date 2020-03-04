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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Reader implements DataSourceReader, SupportsScanColumnarBatch, SupportsPushDownFilters,
    SupportsPushDownRequiredColumns, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

  private static final Filter[] NO_FILTERS = new Filter[0];
  private static final ImmutableSet<String> LOCALITY_WHITELIST_FS = ImmutableSet.of("hdfs");

  private final Table table;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final boolean caseSensitive;
  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;
  private final boolean localityPreferred;
  private final int batchSize;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks
  private Boolean enableBatchRead = null; // cache variable for enabling batched reads

  Reader(
      Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
      boolean caseSensitive, DataSourceOptions options) {
    this.table = table;
    this.snapshotId = options.get("snapshot-id").map(Long::parseLong).orElse(null);
    this.asOfTimestamp = options.get("as-of-timestamp").map(Long::parseLong).orElse(null);
    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = options.get("start-snapshot-id").map(Long::parseLong).orElse(null);
    this.endSnapshotId = options.get("end-snapshot-id").map(Long::parseLong).orElse(null);
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either snapshot-id or " +
                "as-of-timestamp is specified");
      }
    } else {
      if (startSnapshotId == null && endSnapshotId != null) {
        throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
      }
    }

    // look for split behavior overrides in options
    this.splitSize = options.get("split-size").map(Long::parseLong).orElse(null);
    this.splitLookback = options.get("lookback").map(Integer::parseInt).orElse(null);
    this.splitOpenFileCost = options.get("file-open-cost").map(Long::parseLong).orElse(null);

    if (io.getValue() instanceof HadoopFileIO) {
      String scheme = "no_exist";
      try {
        Configuration conf = new Configuration(SparkSession.active().sparkContext().hadoopConfiguration());
        // merge hadoop config set on table
        mergeIcebergHadoopConfs(conf, table.properties());
        // merge hadoop config passed as options and overwrite the one on table
        mergeIcebergHadoopConfs(conf, options.asMap());
        FileSystem fs = new Path(table.location()).getFileSystem(conf);
        scheme = fs.getScheme().toLowerCase(Locale.ENGLISH);
      } catch (IOException ioe) {
        LOG.warn("Failed to get Hadoop Filesystem", ioe);
      }
      this.localityPreferred = options.get("locality").map(Boolean::parseBoolean)
          .orElse(LOCALITY_WHITELIST_FS.contains(scheme));
    } else {
      this.localityPreferred = false;
    }

    this.schema = table.schema();
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.caseSensitive = caseSensitive;

    boolean enableBatchReadsConfig =
        options.get("iceberg.read.parquet-vectorization.enabled").map(Boolean::parseBoolean).orElse(true);
    if (!enableBatchReadsConfig) {
      enableBatchRead = Boolean.FALSE;
    }
    Optional<String> numRecordsPerBatchOpt = options.get("iceberg.read.parquet-vectorization.batch-size");
    this.batchSize = numRecordsPerBatchOpt.map(Integer::parseInt).orElse(VectorizedArrowReader.DEFAULT_BATCH_SIZE);
  }

  private Schema lazySchema() {
    if (schema == null) {
      if (requestedSchema != null) {
        // the projection should include all columns that will be returned, including those only used in filters
        this.schema = SparkSchemaUtil.prune(table.schema(), requestedSchema, filterExpression(), caseSensitive);
      } else {
        this.schema = table.schema();
      }
    }
    return schema;
  }

  private Expression filterExpression() {
    if (filterExpressions != null) {
      return filterExpressions.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
    }
    return Expressions.alwaysTrue();
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

  /**
   * This is called in the Spark Driver when data is to be materialized into {@link ColumnarBatch}
   */
  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    Preconditions.checkState(enableBatchRead != null && enableBatchRead, "Batched reads not enabled");
    Preconditions.checkState(batchSize > 0, "Invalid batch size");
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    List<InputPartition<ColumnarBatch>> readTasks = Lists.newArrayList();
    for (CombinedScanTask task : tasks()) {
      readTasks.add(
          new ColumnarBatchReadTask(task, tableSchemaString, expectedSchemaString,
              io, encryptionManager, caseSensitive, localityPreferred, batchSize));
    }
    LOG.info("Batching input partitions with {} tasks.", readTasks.size());

    return readTasks;
  }

  /**
   * This is called in the Spark Driver when data is to be materialized into {@link InternalRow}
   */
  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    List<InputPartition<InternalRow>> readTasks = Lists.newArrayList();
    for (CombinedScanTask task : tasks()) {
      readTasks.add(
          new InternalRowReadTask(task, tableSchemaString, expectedSchemaString, io, encryptionManager,
              caseSensitive, localityPreferred));
    }

    return readTasks;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    this.tasks = null; // invalidate cached tasks, if present

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        expressions.add(expr);
        pushed.add(filter);
      }
    }

    this.filterExpressions = expressions;
    this.pushedFilters = pushed.toArray(new Filter[0]);

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType newRequestedSchema) {
    this.requestedSchema = newRequestedSchema;

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;
  }

  @Override
  public Statistics estimateStatistics() {
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

  @Override
  public boolean enableBatchRead() {
    return lazyCheckEnableBatchRead();
  }

  private boolean lazyCheckEnableBatchRead() {
    if (enableBatchRead == null) {
      boolean allParquetFileScanTasks =
          tasks().stream()
              .allMatch(combinedScanTask -> !combinedScanTask.isDataTask() && combinedScanTask.files()
                  .stream()
                  .allMatch(fileScanTask -> fileScanTask.file().format().equals(
                      FileFormat.PARQUET)));
      if (!allParquetFileScanTasks) {
        this.enableBatchRead = false;
        return false;
      }

      int numColumns = lazySchema().columns().size();
      if (numColumns == 0) {
        this.enableBatchRead = false;
        return false;
      }

      boolean projectIdentityPartitionColumn =
          tasks().stream()
              .anyMatch(combinedScanTask -> combinedScanTask.files()
                  .stream()
                  .anyMatch(fileScanTask -> !fileScanTask.spec().identitySourceIds().isEmpty()));
      if (projectIdentityPartitionColumn) {
        this.enableBatchRead = false;
        return false;
      }

      // Enable batched reads only if all requested columns are primitive otherwise revert to row-based reads
      this.enableBatchRead = lazySchema().columns().stream().allMatch(c -> c.type().isPrimitiveType());
    }
    return enableBatchRead;
  }

  private static void mergeIcebergHadoopConfs(
      Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("hadoop."))
        .forEach(key -> baseConf.set(key.replaceFirst("hadoop.", ""), options.get(key)));
  }

  private List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive)
          .project(lazySchema());

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (startSnapshotId != null) {
        if (endSnapshotId != null) {
          scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
        } else {
          scan = scan.appendsAfter(startSnapshotId);
        }
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
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, caseSensitive=%s, batchedReads=%s)",
        table, lazySchema().asStruct(), filterExpressions, caseSensitive, enableBatchRead());
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private abstract static class BaseReadTask<T> implements Serializable, InputPartition<T> {
    final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    final Broadcast<FileIO> io;
    final Broadcast<EncryptionManager> encryptionManager;
    final boolean caseSensitive;
    private final boolean localityPreferred;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;
    private transient String[] preferredLocations;

    private BaseReadTask(CombinedScanTask task, String tableSchemaString, String expectedSchemaString,
        Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
        boolean caseSensitive, boolean localityPreferred) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.io = io;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
      this.localityPreferred = localityPreferred;
      this.preferredLocations = getPreferredLocations();
    }

    @Override
    public String[] preferredLocations() {
      return preferredLocations;
    }

    Schema lazyTableSchema() {
      if (tableSchema == null) {
        this.tableSchema = SchemaParser.fromJson(tableSchemaString);
      }
      return tableSchema;
    }

    Schema lazyExpectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }

    private String[] getPreferredLocations() {
      if (!localityPreferred) {
        return new String[0];
      }

      Configuration conf = SparkSession.active().sparkContext().hadoopConfiguration();
      return Util.blockLocations(task, conf);
    }
  }

  private static class InternalRowReadTask extends BaseReadTask<InternalRow> {

    private InternalRowReadTask(
        CombinedScanTask task, String tableSchemaString, String expectedSchemaString,
        Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
        boolean caseSensitive, boolean localityPreferred) {
      super(task, tableSchemaString, expectedSchemaString, io, encryptionManager, caseSensitive, localityPreferred);
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
      return new RowDataReader(task, lazyTableSchema(), lazyExpectedSchema(), io.value(),
          encryptionManager.value(), caseSensitive);
    }
  }

  /**
   * Organizes input data into [InputPartition]s for Vectorized [ColumnarBatch] reads
   */
  private static class ColumnarBatchReadTask extends BaseReadTask<ColumnarBatch> {
    private final int batchSize;

    ColumnarBatchReadTask(
        CombinedScanTask task, String tableSchemaString, String expectedSchemaString, Broadcast<FileIO> fileIo,
        Broadcast<EncryptionManager> encryptionManager, boolean caseSensitive, boolean localityPreferred, int size) {
      super(task, tableSchemaString, expectedSchemaString, fileIo, encryptionManager, caseSensitive, localityPreferred);
      this.batchSize = size;
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {
      return new BatchDataReader(task, lazyTableSchema(), lazyExpectedSchema(), io.value(),
          encryptionManager.value(), caseSensitive, batchSize);
    }
  }

  private static class StructLikeInternalRow implements StructLike {
    private final DataType[] types;
    private InternalRow row = null;

    StructLikeInternalRow(StructType struct) {
      this.types = new DataType[struct.size()];
      StructField[] fields = struct.fields();
      for (int i = 0; i < fields.length; i += 1) {
        types[i] = fields[i].dataType();
      }
    }

    public StructLikeInternalRow setRow(InternalRow row) {
      this.row = row;
      return this;
    }

    @Override
    public int size() {
      return types.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(row.get(pos, types[pos]));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Not implemented: set");
    }
  }
}
