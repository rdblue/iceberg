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

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;

public class TestSparkParquetWriter extends AvroDataTest {
  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    writeAndValidate(writeSchema, expectedSchema, 100);
  }

  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          required(1, "roots", Types.LongType.get()),
          optional(3, "lime", Types.ListType.ofRequired(4, Types.DoubleType.get())),
          required(
              5,
              "strict",
              Types.StructType.of(
                  required(9, "tangerine", Types.StringType.get()),
                  optional(
                      6,
                      "hopeful",
                      Types.StructType.of(
                          required(7, "steel", Types.FloatType.get()),
                          required(8, "lantern", Types.DateType.get()))),
                  optional(10, "vehement", Types.LongType.get()))),
          optional(
              11,
              "metamorphosis",
              Types.MapType.ofRequired(
                  12, 13, Types.StringType.get(), Types.TimestampType.withZone())),
          required(
              14,
              "winter",
              Types.ListType.ofOptional(
                  15,
                  Types.StructType.of(
                      optional(16, "beet", Types.DoubleType.get()),
                      required(17, "stamp", Types.FloatType.get()),
                      optional(18, "wheeze", Types.StringType.get())))),
          optional(
              19,
              "renovate",
              Types.MapType.ofRequired(
                  20,
                  21,
                  Types.StringType.get(),
                  Types.StructType.of(
                      optional(22, "jumpy", Types.DoubleType.get()),
                      required(23, "koala", Types.UUIDType.get()),
                      required(24, "couch rope", Types.IntegerType.get())))),
          optional(2, "slide", Types.StringType.get()),
          required(25, "flush", Types.VariantType.get()));

  @Test
  public void testCorrectness() throws IOException {
    writeAndValidate(COMPLEX_SCHEMA, COMPLEX_SCHEMA, 50_000);
  }

  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema, int numRows)
      throws IOException {
    Iterable<InternalRow> records = RandomData.generateSpark(writeSchema, numRows, 19981);

    OutputFile outputFile = new InMemoryOutputFile();

    try (FileAppender<InternalRow> writer =
        Parquet.write(outputFile)
            .schema(writeSchema)
            .createWriterFunc(
                (schema, msgType) ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(schema), msgType))
            .build()) {
      writer.addAll(records);
    }

    try (CloseableIterable<InternalRow> reader =
        Parquet.read(outputFile.toInputFile())
            .project(expectedSchema)
            .createReaderFunc(SparkParquetReaders::buildReader)
            .build()) {
      Iterator<InternalRow> expected = records.iterator();
      Iterator<InternalRow> rows = reader.iterator();
      for (int i = 0; i < numRows; i += 1) {
        assertThat(rows).as("Should have expected number of rows").hasNext();
        TestHelpers.assertEquals(expectedSchema, expected.next(), rows.next());
      }
      assertThat(rows).as("Should not have extra rows").isExhausted();
    }
  }

  @Test
  public void testFpp() throws IOException, NoSuchFieldException, IllegalAccessException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "id_long", Types.LongType.get()));

    OutputFile outputFile = new InMemoryOutputFile();
    try (FileAppender<InternalRow> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX + "id", "0.05")
            .createWriterFunc(
                (icebergSchema, msgType) ->
                    SparkParquetWriters.buildWriter(
                        SparkSchemaUtil.convert(icebergSchema), msgType))
            .build()) {
      // Using reflection to access the private 'props' field in ParquetWriter
      Field propsField = writer.getClass().getDeclaredField("props");
      propsField.setAccessible(true);
      ParquetProperties props = (ParquetProperties) propsField.get(writer);
      MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "test");
      ColumnDescriptor descriptor = parquetSchema.getColumnDescription(new String[] {"id"});
      double fpp = props.getBloomFilterFPP(descriptor).getAsDouble();
      assertThat(fpp).isEqualTo(0.05);
    }
  }
}
