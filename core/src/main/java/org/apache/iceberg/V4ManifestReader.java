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
package org.apache.iceberg;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.RestoreColumns;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;

/** Reader that reads a v4+ manifest file as {@link TrackedFile}s. */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {
  private static final Set<Integer> REQUIRED_COLUMN_IDS =
      ImmutableSet.of(
          Tracking.STATUS.fieldId(), // needed to filter live files
          MetadataColumns.ROW_POSITION.fieldId(), // needed to apply metadata DVs
          TrackedFile.CONTENT_TYPE.fieldId(), // needed for content filtering
          TrackedFile.RECORD_COUNT.fieldId()); // needed for first_row_id assignment and filtering

  static Builder builder(
      InputFile file, Schema tableSchema, Map<Integer, PartitionSpec> specsById) {
    return new Builder(file, tableSchema, specsById);
  }

  private final InputFile file;
  private final Schema readSchema;
  private final String tableLocation;
  private final Map<Integer, Pair<Evaluator, StructProjection>> partitionFilters; // by spec ID
  private final boolean includeAll;
  private final Set<Integer> requestedStatsFieldIds;
  private final ScanMetrics scanMetrics;

  private V4ManifestReader(
      InputFile file,
      Schema readSchema,
      String tableLocation,
      Map<Integer, Pair<Evaluator, StructProjection>> partitionFilters,
      boolean includeAll,
      Set<Integer> requestedStatsFieldIds,
      ScanMetrics scanMetrics) {
    this.file = file;
    this.readSchema = readSchema;
    this.tableLocation = tableLocation;
    this.partitionFilters = partitionFilters;
    this.includeAll = includeAll;
    this.requestedStatsFieldIds = requestedStatsFieldIds;
    this.scanMetrics = scanMetrics;
  }

  @VisibleForTesting
  Schema readSchema() {
    return readSchema;
  }

  /** Returns copies of the tracked files that match this reader's configured filters. */
  @Override
  public CloseableIterator<TrackedFile> iterator() {
    CloseableIterable<TrackedFile> entries = CloseableIterable.transform(open(), this::prepare);
    if (!partitionFilters.isEmpty()) {
      // manifests have no partition, so the partition filter cannot apply to them
      entries =
          CloseableIterable.filter(entries, entry -> isManifest(entry) || matchesPartition(entry));
    }

    if (!includeAll) {
      entries = CloseableIterable.filter(entries, entry -> entry.tracking().isLive());
    }

    return CloseableIterable.transform(entries, this::copyResolved).iterator();
  }

  private boolean matchesPartition(TrackedFile trackedFile) {
    Integer specId = trackedFile.specId();
    if (specId == null) {
      // a file without a spec is not partitioned and may match the filter
      return true;
    }

    Pair<Evaluator, StructProjection> partitionFilter = partitionFilters.get(specId);
    if (partitionFilter == null) {
      // the row filter does not project to a partition filter for this spec
      return true;
    }

    Evaluator evaluator = partitionFilter.first();
    StructProjection projection = partitionFilter.second();
    boolean matches = evaluator.eval(projection.wrap(trackedFile.partition()));
    if (!matches) {
      incrementSkipCount(trackedFile.contentType());
    }

    return matches;
  }

  private void incrementSkipCount(FileContent content) {
    switch (content) {
      case DATA -> scanMetrics.skippedDataFiles().increment();
      case EQUALITY_DELETES -> scanMetrics.skippedDeleteFiles().increment();
      case DATA_MANIFEST -> scanMetrics.skippedDataManifests().increment();
      case DELETE_MANIFEST -> scanMetrics.skippedDeleteManifests().increment();
      default -> throw new UnsupportedOperationException("Unsupported content type: " + content);
    }
  }

  private CloseableIterable<TrackedFile> open() {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(
        format != null, "Cannot determine format of manifest: %s", file.location());

    InternalData.ReadBuilder readBuilder =
        InternalData.read(format, file)
            .project(readSchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .reuseContainers();

    Types.NestedField partitionField = readSchema.findField(TrackedFile.PARTITION_ID);
    if (partitionField != null && partitionField.type().isStructType()) {
      readBuilder.setCustomType(TrackedFile.PARTITION_ID, PartitionData.class);
    }

    // content_stats is missing from the read schema when no stats are read
    Types.NestedField statsField = readSchema.findField(TrackedFile.CONTENT_STATS_ID);
    if (statsField != null && statsField.type().isStructType()) {
      readBuilder.setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class);
      for (Types.NestedField fieldStats : statsField.type().asStructType().fields()) {
        readBuilder.setCustomType(fieldStats.fieldId(), FieldStatsStruct.class);
      }
    }

    CloseableIterable<TrackedFile> reader = readBuilder.build();
    addCloseable(reader);
    return reader;
  }

  private TrackedFile prepare(TrackedFile trackedFile) {
    Tracking tracking = trackedFile.tracking();
    // manifestLocation is not stored in the manifest; the reader fills it in
    if (tracking instanceof TrackingStruct) {
      ((TrackingStruct) tracking).setManifestLocation(file.location());
    }

    return trackedFile;
  }

  // resolves stored locations against the table location
  private TrackedFile copyResolved(TrackedFile trackedFile) {
    TrackedFileStruct copy = (TrackedFileStruct) trackedFile.copyWithStats(requestedStatsFieldIds);
    if (copy.location() != null) {
      copy.setLocation(LocationUtil.resolveLocation(tableLocation, copy.location()));
    }

    DeletionVector dv = copy.deletionVector();
    if (dv != null && dv.location() != null) {
      ((DeletionVectorStruct) dv)
          .setLocation(LocationUtil.resolveLocation(tableLocation, dv.location()));
    }

    return copy;
  }

  private static boolean isManifest(TrackedFile trackedFile) {
    FileContent content = trackedFile.contentType();
    return content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST;
  }

  static class Builder {
    private final InputFile file;
    private final Schema tableSchema;
    private final Types.StructType unionPartitionType;
    private final Map<Integer, PartitionSpec> specsById;
    private String tableLocation = null;
    private Expression rowFilter = Expressions.alwaysTrue();
    private boolean caseSensitive = true;
    private boolean includeAll = false;
    private boolean scanPlanning = false;
    private Set<String> requestedColumns = null;
    private Schema requestedProjection = null;
    private Set<Integer> requestedStatsFieldIds = ImmutableSet.of();
    private MetricsConfig metricsConfig = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();

    private Builder(InputFile file, Schema tableSchema, Map<Integer, PartitionSpec> specsById) {
      Preconditions.checkArgument(tableSchema != null, "Invalid table schema: null");
      this.file = file;
      this.tableSchema = tableSchema;
      this.specsById = specsById;
      this.unionPartitionType = Partitioning.unionPartitionTypes(specsById.values());
    }

    /**
     * Sets the table location used to resolve relative paths.
     *
     * @param tableLocation active table location
     * @return this for method chaining
     */
    Builder tableLocation(String tableLocation) {
      Preconditions.checkArgument(tableLocation != null, "Invalid table location: null");
      this.tableLocation = tableLocation;
      return this;
    }

    /** Sets a filter; files that cannot match the expression are skipped. */
    Builder filter(Expression expr) {
      Preconditions.checkArgument(expr != null, "Invalid filter: null");
      this.rowFilter = expr;
      return this;
    }

    Builder caseSensitive(boolean isCaseSensitive) {
      this.caseSensitive = isCaseSensitive;
      return this;
    }

    /** Returns all entries without filtering by {@link Tracking#isLive() liveness}. */
    Builder includeAll() {
      this.includeAll = true;
      return this;
    }

    /** Configures the reader to select the minimal fields needed for scan planning. */
    Builder forScanPlanning() {
      Preconditions.checkState(
          requestedColumns == null && requestedProjection == null,
          "Cannot use forScanPlanning() with select(Iterable<String>) or project(Schema)");
      this.scanPlanning = true;
      return this;
    }

    /** Selects columns to read by name; fields needed by the reader are always read. */
    Builder select(String... newColumns) {
      Preconditions.checkArgument(newColumns != null, "Invalid columns: null");
      return select(Arrays.asList(newColumns));
    }

    /** Selects columns to read by name; fields needed by the reader are always read. */
    Builder select(Iterable<String> newColumns) {
      Preconditions.checkArgument(newColumns != null, "Invalid columns: null");
      Preconditions.checkState(
          !scanPlanning, "Cannot use select(Iterable<String>) with forScanPlanning()");
      Preconditions.checkState(
          requestedProjection == null,
          "Cannot use select(Iterable<String>) with project(Schema)");
      this.requestedColumns = ImmutableSet.copyOf(newColumns);
      return this;
    }

    /** Sets the exact schema to read; used in place of {@link #select(Iterable)}. */
    Builder project(Schema newProjection) {
      Preconditions.checkArgument(newProjection != null, "Invalid projection: null");
      Preconditions.checkState(!scanPlanning, "Cannot use project(Schema) with forScanPlanning()");
      Preconditions.checkState(
          requestedColumns == null, "Cannot use project(Schema) with select(Iterable<String>)");
      this.requestedProjection = newProjection;
      return this;
    }

    /**
     * Reads content stats for the given table field IDs instead of for every field. Stats for
     * fields referenced by the {@link #filter(Expression) filter} are always read.
     *
     * <p>Passing no field IDs reads only the stats that the filter needs.
     */
    Builder projectStats(int... fieldIds) {
      Preconditions.checkArgument(fieldIds != null, "Invalid field IDs: null");
      return projectStats(ArrayUtil.toIntList(fieldIds));
    }

    /**
     * Reads content stats for the given table field IDs instead of for every field. Stats for
     * fields referenced by the {@link #filter(Expression) filter} are always read.
     *
     * <p>Passing an empty iterable reads only the stats that the filter needs.
     */
    Builder projectStats(Iterable<Integer> fieldIds) {
      Preconditions.checkArgument(fieldIds != null, "Invalid field IDs: null");
      this.requestedStatsFieldIds = ImmutableSet.copyOf(fieldIds);
      return this;
    }

    /** Sets the metrics config that determines which stats the manifest holds. */
    Builder metricsConfig(MetricsConfig newMetricsConfig) {
      Preconditions.checkArgument(newMetricsConfig != null, "Invalid metrics config: null");
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    Builder scanMetrics(ScanMetrics newScanMetrics) {
      Preconditions.checkArgument(newScanMetrics != null, "Invalid scan metrics: null");
      this.scanMetrics = newScanMetrics;
      return this;
    }

    V4ManifestReader build() {
      Map<Integer, Pair<Evaluator, StructProjection>> partitionFilters = projectFilters();

      return new V4ManifestReader(
          file,
          readSchema(!partitionFilters.isEmpty()),
          tableLocation,
          partitionFilters,
          includeAll,
          requestedStatsFieldIds,
          scanMetrics);
    }

    private Map<Integer, Pair<Evaluator, StructProjection>> projectFilters() {
      Map<Integer, Pair<Evaluator, StructProjection>> evaluatorAndProjections = Maps.newHashMap();
      if (rowFilter != Expressions.alwaysTrue() && !unionPartitionType.fields().isEmpty()) {
        for (PartitionSpec spec : specsById.values()) {
          Expression partFilter = Projections.inclusive(spec, caseSensitive).project(rowFilter);
          if (partFilter != Expressions.alwaysTrue()) {
            Evaluator evaluator = new Evaluator(spec.partitionType(), partFilter, caseSensitive);
            StructProjection projection =
                StructProjection.create(unionPartitionType, spec.partitionType());
            evaluatorAndProjections.put(spec.specId(), Pair.of(evaluator, projection));
          }
        }
      }

      return evaluatorAndProjections;
    }

    private Schema readSchema(boolean includePartition) {
      if (scanPlanning) {
        Types.StructType statsProjection = StatsUtil.statsReadSchema(tableSchema, statsFieldIds());
        return TypeUtil.replaceFieldTypes(
            TrackedFile.schema(unionPartitionType, statsProjection),
            ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.SCAN_TYPE));
      }

      Preconditions.checkState(metricsConfig != null, "Metrics config is required");

      Types.StructType tableStatsSchema = StatsUtil.statsWriteSchema(tableSchema, metricsConfig);
      Schema tableManifestSchema =
          TypeUtil.replaceFieldTypes(
              TrackedFile.schema(unionPartitionType, tableStatsSchema),
              ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.BASE_TYPE));

      if (requestedProjection != null) {
        return RestoreColumns.restore(
            tableManifestSchema, requestedProjection, idsToRestore(includePartition));
      }

      if (requestedColumns != null) {
        Schema projection =
            caseSensitive
                ? tableManifestSchema.select(requestedColumns)
                : tableManifestSchema.caseInsensitiveSelect(requestedColumns);

        return RestoreColumns.restore(
            tableManifestSchema, projection, idsToRestore(includePartition));
      }

      return tableManifestSchema;
    }

    /** Return a set of manifest field IDs that should be projected. */
    private Set<Integer> idsToRestore(boolean includePartition) {
      Set<Integer> ids = Sets.newHashSet(REQUIRED_COLUMN_IDS);

      // project the full stats struct that corresponds to each field for which stats are needed
      statsFieldIds().stream().map(StatsUtil::toBaseId).forEach(ids::add);

      if (includePartition) {
        ids.add(TrackedFile.SPEC_ID.fieldId());
        ids.add(TrackedFile.PARTITION_ID);
      }

      return ids;
    }

    /** Return a set of table field IDs for which stats are needed. */
    private Set<Integer> statsFieldIds() {
      Set<Integer> filterFieldIds =
          Binder.boundReferences(tableSchema.asStruct(), rowFilter, caseSensitive);
      return Sets.union(filterFieldIds, requestedStatsFieldIds);
    }
  }
}
