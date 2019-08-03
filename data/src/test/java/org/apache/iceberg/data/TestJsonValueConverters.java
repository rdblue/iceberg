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

package org.apache.iceberg.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.sun.jersey.core.util.Base64;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestJsonValueConverters {
  private static final long ABOVE_INT_MAX = 2 * ((long) Integer.MAX_VALUE);
  private static final double ABOVE_FLOAT_MAX = 2 * ((double) Float.MAX_VALUE);

  @Test
  public void testBooleanConverter() {
    JsonValueConverters.Converter<Boolean> converter = JsonValueConverters.BooleanConverter.INSTANCE;
    Assert.assertTrue(converter.convert("true", null));
    Assert.assertTrue(converter.convert("True", null));
    Assert.assertFalse(converter.convert("false", null));
    Assert.assertFalse(converter.convert("faLSE", null));
    Assert.assertNull(converter.convert("true!", null));
    Assert.assertNull(converter.convert("faalse", null));
    Assert.assertNull(converter.convert("", null));
    Assert.assertNull(converter.convert(0, null));
    Assert.assertNull(converter.convert(1, null));
  }

  @Test
  public void testIntegerConverter() {
    JsonValueConverters.Converter<Integer> converter = JsonValueConverters.IntegerConverter.INSTANCE;
    Assert.assertEquals(123, (int) converter.convert("123", null));
    Assert.assertEquals(123, (int) converter.convert(123, null));
    Assert.assertEquals(123, (int) converter.convert(123L, null));
    Assert.assertEquals(123, (int) converter.convert(123.1F, null));
    Assert.assertEquals(123, (int) converter.convert(123.1D, null));
    Assert.assertNull(converter.convert(ABOVE_INT_MAX, null));
    Assert.assertNull(converter.convert(ABOVE_FLOAT_MAX, null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testLongConverter() {
    JsonValueConverters.Converter<Long> converter = JsonValueConverters.LongConverter.INSTANCE;
    Assert.assertEquals(123L, (long) converter.convert("123", null));
    Assert.assertEquals(123L, (long) converter.convert(123, null));
    Assert.assertEquals(123L, (long) converter.convert(123L, null));
    Assert.assertEquals(123L, (long) converter.convert(123.1F, null));
    Assert.assertEquals(123L, (long) converter.convert(123.1D, null));
    Assert.assertEquals(ABOVE_INT_MAX, (long) converter.convert(ABOVE_INT_MAX, null));
    Assert.assertNull(converter.convert(ABOVE_FLOAT_MAX, null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testFloatConverter() {
    JsonValueConverters.Converter<Float> converter = JsonValueConverters.FloatConverter.INSTANCE;
    Assert.assertEquals(123.0F, converter.convert("123", null), 0.0001);
    Assert.assertEquals(123.0F, converter.convert(123, null), 0.0001);
    Assert.assertEquals(123.0F, converter.convert(123L, null), 0.0001);
    Assert.assertEquals(123.1F, converter.convert(123.1F, null), 0.0001);
    Assert.assertEquals(123.1F, converter.convert(123.1D, null), 0.0001);
    Assert.assertEquals((float) ABOVE_INT_MAX, converter.convert(ABOVE_INT_MAX, null), 0.0001);
    Assert.assertNull(converter.convert(ABOVE_FLOAT_MAX, null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a.1", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testDoubleConverter() {
    JsonValueConverters.Converter<Double> converter = JsonValueConverters.DoubleConverter.INSTANCE;
    Assert.assertEquals(123.0D, converter.convert("123", null), 0.0001);
    Assert.assertEquals(123.0D, converter.convert(123, null), 0.0001);
    Assert.assertEquals(123.0D, converter.convert(123L, null), 0.0001);
    Assert.assertEquals(123.1D, converter.convert(123.1F, null), 0.0001);
    Assert.assertEquals(123.1D, converter.convert(123.1D, null), 0.0001);
    Assert.assertEquals(ABOVE_INT_MAX, converter.convert(ABOVE_INT_MAX, null), 0.0001);
    Assert.assertEquals(ABOVE_FLOAT_MAX, converter.convert(ABOVE_FLOAT_MAX, null), 0.0001);
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a.1", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testDateConverter() {
    JsonValueConverters.Converter<Integer> converter = JsonValueConverters.DateConverter.INSTANCE;
    String dateString = "2019-08-02";
    Literal<Integer> lit = Literal.of(dateString).to(Types.DateType.get());
    int dateOrdinal = lit.value();

    Assert.assertEquals(dateOrdinal, (int) converter.convert(dateOrdinal, null));
    Assert.assertEquals(dateOrdinal, (int) converter.convert((long) dateOrdinal, null));
    Assert.assertEquals(dateOrdinal, (int) converter.convert((float) dateOrdinal, null));
    Assert.assertEquals(dateOrdinal, (int) converter.convert((double) dateOrdinal, null));
    Assert.assertEquals(dateOrdinal, (int) converter.convert(dateString, null));
    Assert.assertNull(converter.convert(String.valueOf(dateOrdinal), null));
    Assert.assertNull(converter.convert(ABOVE_INT_MAX, null));
    Assert.assertNull(converter.convert(ABOVE_FLOAT_MAX, null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testTimeConverter() {
    JsonValueConverters.Converter<Long> converter = JsonValueConverters.TimeConverter.INSTANCE;
    String timeString = "16:59:01.109521";
    Literal<Long> lit = Literal.of(timeString).to(Types.TimeType.get());
    long timeOrdinal = lit.value();

    Assert.assertEquals(timeOrdinal, (long) converter.convert(timeOrdinal, null));
    Assert.assertEquals(timeOrdinal, (long) converter.convert(timeString, null));
    Assert.assertNull(converter.convert((float) timeOrdinal, null));
    Assert.assertNull(converter.convert((double) timeOrdinal, null));
    Assert.assertNull(converter.convert(String.valueOf(timeOrdinal), null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testTimestampConverter() {
    JsonValueConverters.Converter<Long> converter = JsonValueConverters.TimestampConverter.INSTANCE;
    String timestampString = "2019-08-02T16:59:01.109521";
    String timestamptzString = "2019-08-02T16:59:01.109521-08:00";
    Literal<Long> lit = Literal.of(timestampString).to(Types.TimestampType.withoutZone());
    long timestampOrdinal = lit.value();

    Assert.assertEquals(timestampOrdinal, (long) converter.convert(timestampOrdinal, null));
    Assert.assertEquals(timestampOrdinal, (long) converter.convert(timestampString, null));
    Assert.assertNull(converter.convert((float) timestampOrdinal, null));
    Assert.assertNull(converter.convert((double) timestampOrdinal, null));
    Assert.assertNull(converter.convert(String.valueOf(timestampOrdinal), null));
    Assert.assertNull(converter.convert(timestamptzString, null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testTimestamptzConverter() {
    JsonValueConverters.Converter<Long> converter = JsonValueConverters.TimestamptzConverter.INSTANCE;
    String timestampString = "2019-08-02T16:59:01.109521";
    String timestamptzString = "2019-08-02T16:59:01.109521-08:00";
    Literal<Long> lit = Literal.of(timestamptzString).to(Types.TimestampType.withZone());
    long timestampOrdinal = lit.value();

    Assert.assertEquals(timestampOrdinal, (long) converter.convert(timestampOrdinal, null));
    Assert.assertEquals(timestampOrdinal, (long) converter.convert(timestamptzString, null));
    Assert.assertNull(converter.convert((float) timestampOrdinal, null));
    Assert.assertNull(converter.convert((double) timestampOrdinal, null));
    Assert.assertNull(converter.convert(String.valueOf(timestampOrdinal), null));
    Assert.assertNull(converter.convert(timestampString, null));
    Assert.assertNull(converter.convert(null, null));
    Assert.assertNull(converter.convert("3a", null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
  }

  @Test
  public void testStringConverter() {
    JsonValueConverters.Converter<String> converter = JsonValueConverters.StringConverter.INSTANCE;

    Assert.assertEquals("", converter.convert("", null));
    Assert.assertEquals("3a", converter.convert("3a", null));
    Assert.assertEquals("true", converter.convert(true, null));
    Assert.assertEquals("123", converter.convert(123, null));
    Assert.assertEquals("123", converter.convert(123L, null));
    Assert.assertEquals("123.1", converter.convert(123.1F, null));
    Assert.assertEquals("123.1", converter.convert(123.1D, null));
    Assert.assertEquals("{\"test\":\"value\"}", converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertEquals("[\"a\",\"b\",\"c\"]", converter.convert(ImmutableList.of("a", "b", "c"), null));
    Assert.assertEquals("[{\"a\":1},{\"b\":2},{\"c\":3}]", converter.convert(
        ImmutableList.of(ImmutableMap.of("a", 1), ImmutableMap.of("b", 2), ImmutableMap.of("c", 3)), null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testUUIDConverter() {
    JsonValueConverters.Converter<UUID> converter = JsonValueConverters.UUIDConverter.INSTANCE;
    UUID uuid = UUID.randomUUID();

    Assert.assertEquals(uuid, converter.convert(uuid.toString(), null));
    Assert.assertNull(converter.convert(uuid, null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
    Assert.assertNull(converter.convert("", null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testFixedConverter() {
    JsonValueConverters.Converter<ByteBuffer> converter = new JsonValueConverters.FixedConverter(3);
    String base64 = new String(Base64.encode("abc"), StandardCharsets.UTF_8);
    ByteBuffer expected = ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals(0, Comparators.unsignedBytes().compare(expected, converter.convert(base64, null)));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
    Assert.assertNull(converter.convert("a!c", null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testBinaryConverter() {
    JsonValueConverters.Converter<ByteBuffer> converter = JsonValueConverters.BinaryConverter.INSTANCE;
    String base64 = new String(Base64.encode("abc"), StandardCharsets.UTF_8);
    ByteBuffer expected = ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals(0, Comparators.unsignedBytes().compare(expected, converter.convert(base64, null)));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
    Assert.assertNull(converter.convert("a!c", null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testDecimalConverter() {
    JsonValueConverters.Converter<BigDecimal> converter = new JsonValueConverters.BigDecimalConverter(2);

    Assert.assertEquals(new BigDecimal("34.00"), converter.convert(34, null));
    Assert.assertEquals(new BigDecimal("34.00"), converter.convert(34L, null));
    Assert.assertEquals(new BigDecimal("34.12"), converter.convert(34.12F, null));
    Assert.assertEquals(new BigDecimal("34.12"), converter.convert(34.12D, null));
    Assert.assertEquals(new BigDecimal("34.12"), converter.convert("34.12", null));
    Assert.assertNull(converter.convert(false, null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test"), null));
    Assert.assertNull(converter.convert("a!c", null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testListConverter() {
    JsonValueConverters.Converter<Collection<Integer>> converter = new JsonValueConverters.ListConverter<>(
        JsonValueConverters.IntegerConverter.INSTANCE);

    Assert.assertEquals(ImmutableList.of(), converter.convert(ImmutableList.of(), null));
    Assert.assertEquals(ImmutableList.of(1, 2, 3), converter.convert(ImmutableList.of(1, 2, 3), null));
    Assert.assertEquals(ImmutableList.of(1, 2, 3), converter.convert(ImmutableList.of("1", "2", "3"), null));
    Assert.assertNull(converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert("a!c", null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testMapConverter() {
    JsonValueConverters.Converter<Map<String, Integer>> converter = new JsonValueConverters.MapConverter<>(
        JsonValueConverters.StringConverter.INSTANCE,
        JsonValueConverters.IntegerConverter.INSTANCE);

    Assert.assertEquals(ImmutableMap.of(), converter.convert(ImmutableMap.of(), null));
    Assert.assertEquals(
        ImmutableMap.of("a", 1, "b", 2, "c", 3),
        converter.convert(ImmutableMap.of("a", 1, "b", 2, "c", 3), null));

    // use LinkedHashMap to have a predictable traversal order
    Map<String, Integer> dupMap = Maps.newLinkedHashMap();
    dupMap.put("a", 1);
    dupMap.put("a", 2);
    Assert.assertEquals(ImmutableMap.of("a", 2), converter.convert(dupMap, null));

    Assert.assertEquals(
        ImmutableMap.of("a", 1, "b", 2, "c", 3),
        converter.convert(ImmutableMap.of("a", "1", "b", "2", "c", "3"), null));

    Map<String, Integer> expected = Maps.newHashMap();
    expected.put("test", null);

    Assert.assertEquals(expected, converter.convert(ImmutableMap.of("test", "value"), null));
    Assert.assertNull(converter.convert(ImmutableList.of("test", "value"), null));
    Assert.assertNull(converter.convert("a!c", null));
    Assert.assertNull(converter.convert(null, null));
  }

  @Test
  public void testStructConverter() {
    Types.StructType struct = Types.StructType.of(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(3, "other_properties", Types.MapType.ofRequired(10, 11,
            Types.StringType.get(),
            Types.StringType.get())),
        Types.NestedField.required(2, "data", Types.StringType.get()),
        Types.NestedField.required(8, "non_null", Types.FloatType.get())
    );
    JsonValueConverters.Converter<Record> converter = new JsonValueConverters.StructConverter(
        struct,
        ImmutableMap.of(
            0, new JsonValueConverters.FirstKeyConverter<>(
                ImmutableSet.of("obj_id", "id"), JsonValueConverters.LongConverter.INSTANCE),
            2, new JsonValueConverters.FirstKeyConverter<>(
                ImmutableSet.of("data"), JsonValueConverters.StringConverter.INSTANCE),
            3, new JsonValueConverters.FirstKeyConverter<>(
                ImmutableSet.of("key"), JsonValueConverters.FloatConverter.INSTANCE)),
        1);

    Map<Object, Object> source = Maps.newLinkedHashMap();
    source.put("id", 2);
    source.put("obj_id", 4);
    source.put("data", "abc");
    source.put("other", "value");
    source.put("key", ""); // will be converted to null

    GenericRecord expected = GenericRecord.create(struct);
    expected.setField("id", 4L);
    expected.setField("data", "abc");
    expected.setField("other_properties", ImmutableMap.of(
        "other", "value",
        "id", "2"));
    expected.setField("non_null", null); // does not respect field nullability!

    Assert.assertEquals(expected.toString(), converter.convert(source, null).toString());

    source = Maps.newLinkedHashMap();
    source.put("test", "value");

    GenericRecord empty = GenericRecord.create(struct);
    empty.setField("other_properties", ImmutableMap.of("test", "value"));

    Assert.assertEquals(empty, converter.convert(source, null));

    source = Maps.newLinkedHashMap();
    source.put("id", "2");
    source.put("data", "def");

    GenericRecord idFallback = GenericRecord.create(struct);
    idFallback.setField("id", 2L);
    idFallback.setField("data", "def");
    idFallback.setField("other_properties", ImmutableMap.of());

    Assert.assertEquals(idFallback, converter.convert(source, null));

    Assert.assertNull(converter.convert(false, null));
    Assert.assertNull(converter.convert(1, null));
    Assert.assertNull(converter.convert(ImmutableList.of("test", "value"), null));
    Assert.assertNull(converter.convert("a!c", null));
    Assert.assertNull(converter.convert(null, null));
  }
}
