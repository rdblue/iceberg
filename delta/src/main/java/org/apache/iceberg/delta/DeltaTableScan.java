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
package org.apache.iceberg.delta;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.ScanImpl;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.Pair;

class DeltaTableScan extends SimpleScan<DeltaTableScan> {
  private final Table deltaTable;
  private final Engine deltaEngine;

  DeltaTableScan(DeltaTable table, Table deltaTable, Engine deltaEngine) {
    super(table);
    this.deltaTable = deltaTable;
    this.deltaEngine = deltaEngine;
  }

  protected DeltaTableScan(DeltaTableScan toCopy) {
    super(toCopy);
    this.deltaTable = toCopy.deltaTable;
    this.deltaEngine = toCopy.deltaEngine;
  }

  @Override
  protected DeltaTableScan self() {
    return this;
  }

  @Override
  protected DeltaTableScan copy(DeltaTableScan toCopy) {
    return new DeltaTableScan(this);
  }

  @Override
  public CloseableIterable<ScanTask> planFiles() {
    Schema schema = schema();
    PartitionSpec spec = table().spec();
    Snapshot deltaSnapshot = ((DeltaSnapshot) snapshot()).deltaSnapshot();

    Predicate deltaFilter =
        DeltaExpressionUtil.convert(Binder.bind(schema.asStruct(), filter(), isCaseSensitive()));

    ScanImpl scan =
        (ScanImpl)
            deltaSnapshot.getScanBuilder(deltaEngine).withFilter(deltaEngine, deltaFilter).build();

    String schemaString = SchemaParser.toJson(schema);
    String specString = PartitionSpecParser.toJson(spec);
    ResidualEvaluator residualEval = ResidualEvaluator.of(spec, filter(), isCaseSensitive());

    CloseableIterable<FilteredColumnarBatch> batches =
        CloseableIterable.fromLambda(() -> scan.getScanFiles(deltaEngine, true));

    CloseableIterable<CloseableIterable<Pair<DataFile, DeleteFile>>> fileBatches =
        CloseableIterable.transform(
            batches, batch -> DeltaFileUtil.files(table().location(), schema, spec, batch));

    CloseableIterable<Pair<DataFile, DeleteFile>> files = CloseableIterable.concat(fileBatches);

    return CloseableIterable.combine(
        DeltaFileUtil.asTasks(schemaString, specString, residualEval, files), batches);
  }
}
