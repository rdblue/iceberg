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
package org.apache.iceberg.avro;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestReadDefaultValues {

  @TempDir
  public Path temp;

  private static final Object[][] TYPES_WITH_DEFAULTS =
      new Object[][] {
        {Types.BooleanType.get(), "true"},
        {Types.IntegerType.get(), "1"},
        {Types.LongType.get(), "9999999"},
        {Types.FloatType.get(), "1.23"},
        {Types.DoubleType.get(), "123.456"},
        {Types.DateType.get(), "\"2007-12-03\""},
        {Types.TimeType.get(), "\"10:15:30\""},
        {Types.TimestampType.withoutZone(), "\"2007-12-03T10:15:30\""},
        {Types.TimestampType.withZone(), "\"2007-12-03T10:15:30+00:00\""},
        {Types.StringType.get(), "\"foo\""},
        {Types.UUIDType.get(), "\"eb26bdb1-a1d8-4aa6-990e-da940875492c\""},
        {Types.FixedType.ofLength(2), "\"111f\""},
        {Types.BinaryType.get(), "\"0000ff\""},
        {Types.DecimalType.of(9, 4), "\"123.4500\""},
        {Types.DecimalType.of(9, 0), "\"2\""},
        // Avro doesn't support negative scale
        // {Types.DecimalType.of(9, -20), "\"2E+20\""},
        // Nested type defaults are not currently allowed
        // {Types.ListType.ofOptional(1, Types.IntegerType.get()), "[1, 2, 3]"},
//        {
//          Types.MapType.ofOptional(2, 3, Types.IntegerType.get(), Types.StringType.get()),
//          "{\"keys\": [1, 2], \"values\": [\"foo\", \"bar\"]}"
//        },
//        {
//          Types.StructType.of(
//              required(4, "f1", Types.IntegerType.get()),
//              optional(5, "f2", Types.StringType.get())),
//          "{\"4\": 1, \"5\": \"bar\"}"
//        },
//        // deeply nested complex types
//        {
//          Types.ListType.ofOptional(
//              6,
//              Types.StructType.of(
//                  required(7, "f1", Types.IntegerType.get()),
//                  optional(8, "f2", Types.StringType.get()))),
//          "[{\"7\": 1, \"8\": \"bar\"}, {\"7\": 2, \"8\": " + "\"foo\"}]"
//        },
//        {
//          Types.MapType.ofOptional(
//              9,
//              10,
//              Types.IntegerType.get(),
//              Types.StructType.of(
//                  required(11, "f1", Types.IntegerType.get()),
//                  optional(12, "f2", Types.StringType.get()))),
//          "{\"keys\": [1, 2], \"values\": [{\"11\": 1, \"12\": \"bar\"}, {\"11\": 2, \"12\": \"foo\"}]}"
//        },
//        {
//          Types.StructType.of(
//              required(
//                  13,
//                  "f1",
//                  Types.StructType.of(
//                      optional(14, "ff1", Types.IntegerType.get()),
//                      optional(15, "ff2", Types.StringType.get()))),
//              optional(
//                  16,
//                  "f2",
//                  Types.StructType.of(
//                      optional(17, "ff1", Types.StringType.get()),
//                      optional(18, "ff2", Types.IntegerType.get())))),
//          "{\"13\": {\"14\": 1, \"15\": \"bar\"}, \"16\": {\"17\": \"bar\", \"18\": 1}}"
//        },
      };

  @Test
  public void testDefaultAppliedWhenMissingColumn() throws IOException {
    for (Object[] typeWithDefault : TYPES_WITH_DEFAULTS) {
      Type type = (Type) typeWithDefault[0];
      String defaultValueJson = (String) typeWithDefault[1];
      Object defaultValue = SingleValueParser.fromJson(type, defaultValueJson);

      // note that this schema does not have column "defaulted"
      Schema writerSchema = new Schema(required(999, "written", Types.IntegerType.get()));

      File testFile = temp.resolve("test.avro").toFile();
      testFile.delete();

      try (FileAppender<Record> writer =
               Avro.write(Files.localOutput(testFile))
                   .schema(writerSchema)
                   .createWriterFunc(GenericAvroWriter::create)
                   .named("test")
                   .build()) {
        Record record = new Record(AvroSchemaUtil.convert(writerSchema.asStruct()));
        record.put(0, 1);
        writer.add(record);
      }

      Schema readerSchema =
          new Schema(
              Types.NestedField.required("written").withId(999).ofType(Types.IntegerType.get()).build(),
              Types.NestedField.optional("defaulted").withId(1000).ofType(type).withDefault(defaultValue).build());

      Record expectedRecord = new Record(AvroSchemaUtil.convert(readerSchema.asStruct()));
      expectedRecord.put(0, 1);
      expectedRecord.put(1, defaultValue);

      List<Record> rows;
      try (AvroIterable<Record> reader =
          Avro.read(Files.localInput(testFile))
              .project(readerSchema)
              .createResolvingReader(schema -> GenericAvroReader.create(schema))
              .build()) {
        rows = Lists.newArrayList(reader);
      }

      AvroTestHelpers.assertEquals(readerSchema.asStruct(), expectedRecord, rows.get(0));
    }
  }

  @Test
  public void testDefaultDoesNotOverrideExplicitValue() throws IOException {
    for (Object[] typeAndDefault : TYPES_WITH_DEFAULTS) {
      Type type = (Type) typeAndDefault[0];
      String defaultValueJson = (String) typeAndDefault[1];
      Object defaultValue = SingleValueParser.fromJson(type, defaultValueJson);

      Schema readerSchema =
          new Schema(
              Types.NestedField.required("written_1").withId(999).ofType(Types.IntegerType.get()).build(),
              Types.NestedField.optional("written_2").withId(1000).ofType(type).withDefault(defaultValue).build());

      // Create a record with null value for the column with default value
      Record expectedRecord = new Record(AvroSchemaUtil.convert(readerSchema.asStruct()));
      expectedRecord.put(0, 1);
      expectedRecord.put(1, null);

      File testFile = temp.resolve("test.avro").toFile();
      testFile.delete();

      try (FileAppender<Record> writer =
          Avro.write(Files.localOutput(testFile))
              .schema(readerSchema)
              .createWriterFunc(GenericAvroWriter::create)
              .named("test")
              .build()) {
        writer.add(expectedRecord);
      }

      List<Record> rows;
      try (AvroIterable<Record> reader =
          Avro.read(Files.localInput(testFile))
              .project(readerSchema)
              .createReaderFunc(GenericAvroReader::create)
              .build()) {
        rows = Lists.newArrayList(reader);
      }

      // Existence of default value should not affect the read result
      AvroTestHelpers.assertEquals(readerSchema.asStruct(), expectedRecord, rows.get(0));
    }
  }
}
