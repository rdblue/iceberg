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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.source.Writer.TaskCommit;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

// TODO: parameterize DataSourceWriter with subclass of WriterCommitMessage
class Writer implements DataSourceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final Table table;
  private final FileFormat format;
  private final FileIO fileIo;
  private final EncryptionManager encryptionManager;
  private final String genieId;
  private final String applicationId;
  private final String wapId;
  private final long targetFileSize;

  // write configuration. append is the default.
  private boolean replacePartitions;
  private boolean overwriteByFilter = false;
  private Expression overwriteFilter = null;

  Writer(Table table, DataSourceOptions options, boolean replacePartitions,
         String genieId, String applicationId, String wapId) {
    this.table = table;
    this.format = getFileFormat(table.properties(), options);
    this.fileIo = table.io();
    this.encryptionManager = table.encryption();
    this.replacePartitions = replacePartitions;
    this.genieId = genieId;
    this.applicationId = applicationId;
    this.wapId = wapId;

    long tableTargetFileSize = PropertyUtil.propertyAsLong(
        table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetFileSize = options.getLong("target-file-size-bytes", tableTargetFileSize);
  }

  private FileFormat getFileFormat(Map<String, String> tableProperties, DataSourceOptions options) {
    Optional<String> formatOption = options.get("write-format");
    String formatString = formatOption
        .orElse(tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private boolean isWapTable() {
    return Boolean.parseBoolean(table.properties().getOrDefault(
        TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
  }

  @Override
  public boolean supportsReplacePartitions() {
    return true;
  }

  @Override
  public void replacePartitions() {
    Preconditions.checkState(!overwriteByFilter,
        "Cannot replace partitions dynamically and overwrite by filter in the same write.");
    this.replacePartitions = true;
  }

  @Override
  public boolean supportsOverwrite() {
    return true;
  }

  @Override
  public void overwrite(Filter[] filters) {
    Preconditions.checkState(!replacePartitions,
        "Cannot replace partitions dynamically and overwrite by filter in the same write.");
    this.overwriteByFilter = true;

    if (filters.length > 0) {
      Expression deleteExpr = Expressions.alwaysTrue();
      for (Filter filter : filters) {
        deleteExpr = Expressions.and(deleteExpr, SparkFilters.convert(filter));
      }
      this.overwriteFilter = deleteExpr;
    } else {
      this.overwriteFilter = Expressions.alwaysFalse(); // no matching filters, so don't delete
    }
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new WriterFactory(
        table.spec(), format, table.locationProvider(), table.properties(), fileIo, encryptionManager, targetFileSize);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (replacePartitions) {
      replacePartitions(messages);
    } else if (overwriteByFilter) {
      overwrite(messages);
    } else {
      append(messages);
    }
  }

  protected void commitOperation(SnapshotUpdate<?> operation, int numFiles, String description) {
    LOG.info("Committing {} with {} files to table {}", description, numFiles, table);
    if (genieId != null) {
      operation.set("genie-id", genieId);
    }

    if (applicationId != null) {
      operation.set("spark.app.id", applicationId);
    }

    if (isWapTable() && wapId != null) {
      // write-audit-publish is enabled for this table and job
      // stage the changes without changing the current snapshot
      operation.set("wap.id", wapId);
      operation.stageOnly();
    }

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  private void append(WriterCommitMessage[] messages) {
    AppendFiles append = table.newAppend();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    commitOperation(append, numFiles, "append");
  }

  private void overwrite(WriterCommitMessage[] messages) {
    OverwriteFiles overwrite = table.newOverwrite();

    overwrite.overwriteByRowFilter(overwriteFilter);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      overwrite.addFile(file);
    }

    commitOperation(overwrite, numFiles, "overwrite " + overwriteFilter.toString());
  }

  private void replacePartitions(WriterCommitMessage[] messages) {
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      dynamicOverwrite.addFile(file);
    }

    commitOperation(dynamicOverwrite, numFiles, "dynamic partition overwrite");
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    Tasks.foreach(files(messages))
        .retry(propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .throwFailureWhenFinished()
        .run(file -> {
          fileIo.deleteFile(file.path().toString());
        });
  }

  protected Table table() {
    return table;
  }

  private Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return Iterables.concat(Iterables.transform(Arrays.asList(messages), message -> message != null ?
          ImmutableList.copyOf(((TaskCommit) message).files()) :
          ImmutableList.of()));
    }
    return ImmutableList.of();
  }

  private int propertyAsInt(String property, int defaultValue) {
    Map<String, String> properties = table.properties();
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(properties.get(property));
    }
    return defaultValue;
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, format=%s)", table, format);
  }

  private static class WriterFactory extends org.apache.iceberg.spark.source.Writer.WriterFactory
      implements DataWriterFactory<InternalRow> {

    WriterFactory(PartitionSpec spec, FileFormat format, LocationProvider locations,
                  Map<String, String> properties, FileIO fileIo, EncryptionManager encryptionManager,
                  long targetFileSize) {
      super(spec, format, locations, properties, fileIo, encryptionManager, targetFileSize);
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
      return createDataWriter(partitionId, attemptNumber, 0);
    }
  }
}
