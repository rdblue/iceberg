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

package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;

public class TestSparkParquetFallbackToDictionaryEncodingForVectorizedReader extends TestSparkParquetVectorizedReader {
  @Override
  public List<GenericData.Record> generateData(Schema schema) {
    return RandomData.generateListWithFallBackDictionaryEncoding(schema, 200000, 0L, 0.05f);
  }

  @Override
  FileAppender<GenericData.Record> getParquetWriter(Schema schema, File testFile) throws IOException {
    return Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .set(TableProperties.PARQUET_DICT_SIZE_BYTES, "512000")
        .build();
  }

}
