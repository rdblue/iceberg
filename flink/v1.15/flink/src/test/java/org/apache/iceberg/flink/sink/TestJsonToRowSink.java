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

package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.flink.util.JsonToRowConverter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestJsonToRowSink {
  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final Schema SUPPORTED_PRIMITIVES =
      new Schema(
          optional(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          optional(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          optional(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          optional(108, "ts", Types.TimestampType.withoutZone()),
          optional(110, "s", Types.StringType.get()),
          optional(113, "bytes", Types.BinaryType.get()),
          optional(115, "dec_11_2", Types.DecimalType.of(11, 2)));

  private Configuration conf;
  private String warehouse;
  private TableIdentifier ident = TableIdentifier.of("ns", "table");
  private Catalog catalog;
  private StreamExecutionEnvironment env;

  @Before
  public void before() throws IOException {
    File folder = TEMPORARY_FOLDER.newFolder();

    this.conf = new Configuration();
    this.warehouse = folder.getAbsolutePath();
    this.catalog = new HadoopCatalog(conf, warehouse);
    this.env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(2)
            .setMaxParallelism(2);
  }

  private BoundedTestSource<String> createBoundedSource(String... rows) {
    return new BoundedTestSource<>(Arrays.stream(rows).toArray(String[]::new));
  }

  @Test
  public void testSupportedTypes() throws Exception {
    Table table = catalog.buildTable(ident, SUPPORTED_PRIMITIVES).create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row =
        "{"
            + "\"b\": true,"
            + "\"i\": 34,"
            + "\"l\": 35,"
            + "\"f\": 3.6,"
            + "\"d\": 3.7,"
            + "\"date\": \"2022-09-21\","
            + "\"ts\": \"2022-09-21T13:46\","
            + "\"s\": \"iceberg\","
            + "\"bytes\": \"aWNlYmVyZw==\","
            + "\"dec_11_2\": \"34.56\""
            + "}";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    expected.setField("b", true);
    expected.setField("i", 34);
    expected.setField("l", 35L);
    expected.setField("f", 3.6f);
    expected.setField("d", 3.7d);
    expected.setField("date", LocalDate.parse("2022-09-21"));
    expected.setField("ts", LocalDateTime.parse("2022-09-21T13:46:00.000000"));
    expected.setField("s", "iceberg");
    expected.setField("bytes", ByteBuffer.wrap("iceberg".getBytes(StandardCharsets.UTF_8)));
    expected.setField("dec_11_2", new BigDecimal("34.56"));

    validate(table, expected);
  }

  @Test
  public void testNullValues() throws Exception {
    Table table = catalog.buildTable(ident, SUPPORTED_PRIMITIVES).create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row =
        "{"
            + "\"i\": null,"
            + "\"date\": null,"
            + "\"ts\": null,"
            + "\"bytes\": null,"
            + "\"dec_11_2\": null"
            + "}";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    expected.setField("b", null);
    expected.setField("i", null);
    expected.setField("l", null);
    expected.setField("f", null);
    expected.setField("d", null);
    expected.setField("date", null);
    expected.setField("ts", null);
    expected.setField("s", null);
    expected.setField("bytes", null);
    expected.setField("dec_11_2", null);

    validate(table, expected);
  }

  @Test
  public void testArray() throws Exception {
    Table table =
        catalog
            .buildTable(
                ident,
                new Schema(
                    optional(1, "arr", Types.ListType.ofOptional(2, Types.IntegerType.get()))))
            .create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row = "{ \"arr\": [1, 2, 4] }";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    expected.setField("arr", ImmutableList.of(1, 2, 4));

    validate(table, expected);
  }

  @Test
  public void testArrayOfStructs() throws Exception {
    Table table =
        catalog
            .buildTable(
                ident,
                new Schema(
                    optional(
                        1,
                        "arr",
                        Types.ListType.ofOptional(
                            2,
                            Types.StructType.of(
                                required(3, "id", Types.IntegerType.get()),
                                required(4, "data", Types.StringType.get()))))))
            .create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row =
        "{ \"arr\": [ "
            + "{ \"id\": 1, \"data\": \"a\" }, "
            + "{ \"id\": 2, \"data\": \"b\" }, "
            + "{ \"id\": 3, \"data\": \"c\" } ] }";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    GenericRecord inner =
        GenericRecord.create(table.schema().findField("arr.element").type().asStructType());
    expected.setField(
        "arr",
        ImmutableList.of(
            inner.copy(ImmutableMap.of("id", 1, "data", "a")),
            inner.copy(ImmutableMap.of("id", 2, "data", "b")),
            inner.copy(ImmutableMap.of("id", 3, "data", "c"))));

    validate(table, expected);
  }

  @Test
  public void testMap() throws Exception {
    Table table =
        catalog
            .buildTable(
                ident,
                new Schema(
                    optional(
                        1,
                        "map",
                        Types.MapType.ofOptional(
                            2, 3, Types.StringType.get(), Types.StringType.get()))))
            .create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row = "{ \"map\": { \"id\": 34, \"data\": \"iceberg\" } }";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    expected.setField("map", ImmutableMap.of("id", "34", "data", "iceberg"));

    validate(table, expected);
  }

  @Test
  public void testMapWithIntegerKeys() throws Exception {
    Table table =
        catalog
            .buildTable(
                ident,
                new Schema(
                    optional(
                        1,
                        "map",
                        Types.MapType.ofOptional(
                            2, 3, Types.IntegerType.get(), Types.StringType.get()))))
            .create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row = "{ \"map\": { \"0\": 34, \"1\": \"iceberg\" } }";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    expected.setField("map", ImmutableMap.of(0, "34", 1, "iceberg"));

    validate(table, expected);
  }

  @Test
  public void testStruct() throws Exception {
    Table table =
        catalog
            .buildTable(
                ident,
                new Schema(
                    optional(
                        1,
                        "struct",
                        Types.StructType.of(
                            required(2, "id", Types.LongType.get()),
                            optional(3, "data", Types.StringType.get())))))
            .create();

    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    String row = "{ \"struct\": { \"id\": 34, \"data\": \"iceberg\" } }";

    write(table, row);

    GenericRecord expected = GenericRecord.create(table.schema());
    GenericRecord inner =
        GenericRecord.create(table.schema().findField("struct").type().asStructType());
    inner.setField("id", 34L);
    inner.setField("data", "iceberg");
    expected.setField("struct", inner);

    validate(table, expected);
  }

  private void write(Table table, String... jsonRows) throws Exception {
    Schema schema = table.schema();
    NameMapping mapping =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    CatalogLoader catalogLoader =
        CatalogLoader.hadoop("test", conf, ImmutableMap.of("warehouse", warehouse));
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, ident);

    DataStream<String> stream =
        env.addSource(createBoundedSource(jsonRows), TypeInformation.of(String.class));
    DataStream<RowData> rows =
        stream.map(
            new MapFunction<String, RowData>() {
              @Override
              public RowData map(String value) {
                return JsonToRowConverter.convert(schema, mapping, value);
              }
            },
            FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(schema)));

    FlinkSink.forRowData(rows).table(table).tableLoader(tableLoader).writeParallelism(2).append();

    env.execute("Test JSON to row conversion");

    table.refresh();
  }

  private void validate(Table table, Record expected) throws Exception {
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      Record record = Iterables.getOnlyElement(records);
      DataTestHelpers.assertEquals(table.schema().asStruct(), expected, record);
    }
  }
}
