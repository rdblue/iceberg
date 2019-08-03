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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonValueConverters {
  private static final Logger LOG = LoggerFactory.getLogger(JsonValueConverters.class);
  private static final String OTHER_PROPERTIES = "other_properties";

  private JsonValueConverters() {
  }

  @SuppressWarnings("unchecked")
  public static Converter<GenericRecord> get(Schema schema, NameMapping mapping) {
    return (Converter<GenericRecord>) TypeUtil.visit(schema, new ConverterBuilder(mapping));
  }

  interface Converter<T> extends Serializable {
    T convert(Object obj, Object reuse);
  }

  private static class ConverterBuilder extends TypeUtil.SchemaVisitor<Converter<?>> {
    private final NameMapping mapping;

    private ConverterBuilder(NameMapping mapping) {
      this.mapping = mapping;
    }

    @Override
    public Converter<?> schema(Schema schema, Converter<?> structResult) {
      return structResult;
    }

    @Override
    public Converter<?> struct(Types.StructType struct, List<Converter<?>> fieldConverters) {
      Map<Integer, Converter<?>> converters = Maps.newHashMap();
      List<Types.NestedField> fields = struct.fields();
      Integer otherPropertiesPosition = null;

      for (int pos = 0; pos < fields.size(); pos += 1) {
        Types.NestedField field = fields.get(pos);
        // add all converters except for other_properties
        if (OTHER_PROPERTIES.equalsIgnoreCase(field.name())) {
          otherPropertiesPosition = pos;
        } else {
          converters.put(pos, fieldConverters.get(pos));
        }
      }

      return new StructConverter(struct, converters, otherPropertiesPosition);
    }

    @Override
    public Converter<?> field(Types.NestedField field, Converter<?> fieldConverter) {
      MappedField structMapping = mapping.find(Lists.newArrayList(fieldNames().descendingIterator()));
      if (structMapping.nestedMapping() == null) {
        LOG.warn("Missing mapping for struct field: {}", field);
        return NullConverter.INSTANCE;
      }

      MappedField mappedField = structMapping.nestedMapping().field(field.fieldId());
      if (mappedField != null) {
        return new FirstKeyConverter<>(mappedField.names(), fieldConverter);
      } else {
        return NullConverter.INSTANCE;
      }
    }

    @Override
    public Converter<?> list(Types.ListType list, Converter<?> elementConverter) {
      MappedField listMapping = mapping.find(Lists.newArrayList(fieldNames().descendingIterator()));
      if (listMapping.nestedMapping() == null) {
        LOG.warn("Missing mapping for list: {}", list);
        return NullConverter.INSTANCE;
      }

      MappedField mappedElement = listMapping.nestedMapping().field(list.elementId());
      if (mappedElement != null) {
        return new ListConverter(elementConverter);
      } else {
        LOG.warn("Missing element mapping for list: {}", list);
        return NullConverter.INSTANCE;
      }
    }

    @Override
    public Converter<?> map(Types.MapType map, Converter<?> keyConverter, Converter<?> valueConverter) {
      MappedField mapMapping = mapping.find(Lists.newArrayList(fieldNames().descendingIterator()));
      if (mapMapping.nestedMapping() == null) {
        LOG.warn("Missing mapping for map: {}", map);
        return NullConverter.INSTANCE;
      }

      MappedField mappedKey = mapMapping.nestedMapping().field(map.keyId());
      MappedField mappedValue = mapMapping.nestedMapping().field(map.valueId());
      if (mappedKey != null && mappedValue != null) {
        return new MapConverter(keyConverter, valueConverter);
      } else {
        LOG.warn("Missing key or value mapping for map: {}", map);
        return NullConverter.INSTANCE;
      }
    }

    @Override
    public Converter<?> primitive(Type.PrimitiveType primitive) {
      switch (primitive.typeId()) {
        case BOOLEAN:
          return BooleanConverter.INSTANCE;
        case INTEGER:
          return IntegerConverter.INSTANCE;
        case LONG:
          return LongConverter.INSTANCE;
        case FLOAT:
          return FloatConverter.INSTANCE;
        case DOUBLE:
          return DoubleConverter.INSTANCE;
        case DATE:
          return DateConverter.INSTANCE;
        case TIME:
          return TimeConverter.INSTANCE;
        case TIMESTAMP:
          if (((Types.TimestampType) primitive).shouldAdjustToUTC()) {
            return TimestamptzConverter.INSTANCE;
          } else {
            return TimestampConverter.INSTANCE;
          }
        case STRING:
          return StringConverter.INSTANCE;
        case UUID:
          return UUIDConverter.INSTANCE;
        case FIXED:
          return new FixedConverter(((Types.FixedType) primitive).length());
        case BINARY:
          return BinaryConverter.INSTANCE;
        case DECIMAL:
          return new BigDecimalConverter(((Types.DecimalType) primitive).scale());
        default:
          LOG.warn("Cannot convert values for unknown type: {}", primitive);
          return NullConverter.INSTANCE;
      }
    }
  }

  static class NullConverter implements Converter<Void> {
    static final NullConverter INSTANCE = new NullConverter();

    private NullConverter() {
    }

    @Override
    public Void convert(Object node, Object reuse) {
      return null;
    }
  }

  static class BooleanConverter implements Converter<Boolean> {
    static final BooleanConverter INSTANCE = new BooleanConverter();

    private BooleanConverter() {
    }

    @Override
    public Boolean convert(Object obj, Object ignored) {
      if (obj instanceof Boolean) {
        return (Boolean) obj;

      } else if (obj instanceof String) {
        if ("true".equalsIgnoreCase((String) obj)) {
          return true;
        } else if ("false".equalsIgnoreCase((String) obj)) {
          return false;
        }
      }

      return null;
    }
  }

  static class IntegerConverter implements Converter<Integer> {
    static final IntegerConverter INSTANCE = new IntegerConverter();

    private IntegerConverter() {
    }

    @Override
    public Integer convert(Object obj, Object reuse) {
      try {
        if (obj instanceof Integer) {
          return (Integer) obj;

        } else if (obj instanceof Long) {
          long num = (Long) obj;
          if (num <= Integer.MAX_VALUE && num >= Integer.MIN_VALUE) {
            return (int) num;
          } else {
            return null;
          }

        } else if (obj instanceof Float) {
          float num = (Float) obj;
          if (num <= Integer.MAX_VALUE && num >= Integer.MIN_VALUE) {
            return (int) num;
          } else {
            return null;
          }

        } else if (obj instanceof Double) {
          double num = (Double) obj;
          if (num <= Integer.MAX_VALUE && num >= Integer.MIN_VALUE) {
            return (int) num;
          } else {
            return null;
          }

        } else if (obj instanceof String) {
          return Integer.valueOf((String) obj);

        } else {
          return null;
        }

      } catch (NumberFormatException ignored) {
        return null;
      }
    }
  }

  static class LongConverter implements Converter<Long> {
    static final LongConverter INSTANCE = new LongConverter();

    private LongConverter() {
    }

    @Override
    public Long convert(Object obj, Object reuse) {
      try {
        if (obj instanceof Integer) {
          return ((Integer) obj).longValue();

        } else if (obj instanceof Long) {
          return (Long) obj;

        } else if (obj instanceof Float) {
          float num = (Float) obj;
          if (num <= Long.MAX_VALUE && num >= Long.MIN_VALUE) {
            return (long) num;
          } else {
            return null;
          }

        } else if (obj instanceof Double) {
          double num = (Double) obj;
          if (num <= Long.MAX_VALUE && num >= Long.MIN_VALUE) {
            return (long) num;
          } else {
            return null;
          }

        } else if (obj instanceof String) {
          return Long.valueOf((String) obj);

        } else {
          return null;
        }

      } catch (NumberFormatException ignored) {
        return null;
      }
    }
  }

  static class FloatConverter implements Converter<Float> {
    static final FloatConverter INSTANCE = new FloatConverter();

    private FloatConverter() {
    }

    @Override
    public Float convert(Object obj, Object reuse) {
      try {
        if (obj instanceof Double) {
          double num = (Double) obj;
          if (num <= Float.MAX_VALUE && num >= -Float.MAX_VALUE) {
            return (float) num;
          } else {
            return null;
          }
        } else if (obj instanceof Number) {
          return ((Number) obj).floatValue();
        } else if (obj instanceof String) {
          return Float.valueOf((String) obj);
        } else {
          return null;
        }

      } catch (NumberFormatException ignored) {
        return null;
      }
    }
  }

  static class DoubleConverter implements Converter<Double> {
    static final DoubleConverter INSTANCE = new DoubleConverter();

    private DoubleConverter() {
    }

    @Override
    public Double convert(Object obj, Object reuse) {
      try {
        if (obj instanceof Number) {
          return ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
          return Double.valueOf((String) obj);
        } else {
          return null;
        }

      } catch (NumberFormatException ignored) {
        return null;
      }
    }
  }

  static class DateConverter implements Converter<Integer> {
    static final DateConverter INSTANCE = new DateConverter();
    static final Types.DateType DATE_TYPE = Types.DateType.get();

    private DateConverter() {
    }

    @Override
    public Integer convert(Object obj, Object reuse) {
      try {
        // reuse conversions from numbers or strings
        if (obj instanceof Number) {
          Integer ordinal = IntegerConverter.INSTANCE.convert(obj, null);
          if (ordinal != null) {
            Literal<Integer> lit = Literal.of(ordinal).to(DATE_TYPE);
            return lit.value();
          } else {
            return null;
          }
        } else if (obj instanceof CharSequence) {
          // throws DateTimeParseException if conversion fails
          Literal<Integer> lit = Literal.of((CharSequence) obj).to(DATE_TYPE);
          return lit.value();
        } else {
          return null;
        }

      } catch (DateTimeParseException ignored) {
        return null;
      }
    }
  }

  static class TimeConverter implements Converter<Long> {
    static final TimeConverter INSTANCE = new TimeConverter();
    static final Types.TimeType TIME_TYPE = Types.TimeType.get();

    private TimeConverter() {
    }

    @Override
    public Long convert(Object obj, Object reuse) {
      try {
        // reuse conversions from numbers or strings
        if (obj instanceof Integer || obj instanceof Long) {
          Long ordinal = LongConverter.INSTANCE.convert(obj, null);
          if (ordinal != null) {
            Literal<Long> lit = Literal.of(ordinal).to(TIME_TYPE);
            return lit.value();
          } else {
            return null;
          }
        } else if (obj instanceof CharSequence) {
          // throws DateTimeParseException if conversion fails
          Literal<Long> lit = Literal.of((CharSequence) obj).to(TIME_TYPE);
          return lit.value();
        } else {
          return null;
        }

      } catch (DateTimeParseException ignored) {
        return null;
      }
    }
  }

  static class TimestamptzConverter implements Converter<Long> {
    static final TimestamptzConverter INSTANCE = new TimestamptzConverter();
    static final Types.TimestampType TIMESTAMPTZ_TYPE = Types.TimestampType.withZone();

    private TimestamptzConverter() {
    }

    @Override
    public Long convert(Object obj, Object reuse) {
      try {
        // reuse conversions from numbers or strings
        if (obj instanceof Integer || obj instanceof Long) {
          Long ordinal = LongConverter.INSTANCE.convert(obj, null);
          if (ordinal != null) {
            Literal<Long> lit = Literal.of(ordinal).to(TIMESTAMPTZ_TYPE);
            return lit.value();
          } else {
            return null;
          }
        } else if (obj instanceof CharSequence) {
          // throws DateTimeParseException if conversion fails
          Literal<Long> lit = Literal.of((CharSequence) obj).to(TIMESTAMPTZ_TYPE);
          return lit.value();
        } else {
          return null;
        }

      } catch (DateTimeParseException ignored) {
        return null;
      }
    }
  }

  static class TimestampConverter implements Converter<Long> {
    static final TimestampConverter INSTANCE = new TimestampConverter();
    static final Types.TimestampType TIMESTAMP_TYPE = Types.TimestampType.withoutZone();

    private TimestampConverter() {
    }

    @Override
    public Long convert(Object obj, Object reuse) {
      try {
        // reuse conversions from numbers or strings
        if (obj instanceof Integer || obj instanceof Long) {
          Long ordinal = LongConverter.INSTANCE.convert(obj, null);
          if (ordinal != null) {
            Literal<Long> lit = Literal.of(ordinal).to(TIMESTAMP_TYPE);
            return lit.value();
          } else {
            return null;
          }
        } else if (obj instanceof CharSequence) {
          // throws DateTimeParseException if conversion fails
          Literal<Long> lit = Literal.of((CharSequence) obj).to(TIMESTAMP_TYPE);
          return lit.value();
        } else {
          return null;
        }

      } catch (DateTimeParseException ignored) {
        return null;
      }
    }
  }

  static class StringConverter implements Converter<String> {
    static final StringConverter INSTANCE = new StringConverter();
    private static final ObjectMapper OM = new ObjectMapper();

    private StringConverter() {
    }

    @Override
    public String convert(Object obj, Object reuse) {
      if (obj instanceof Map || obj instanceof Collection) {
        try {
          return OM.writeValueAsString(obj);
        } catch (IOException ignored) {
          // fall through to return null
        }

      } else if (obj != null) {
        return obj.toString();
      }

      return null;
    }
  }

  static class UUIDConverter implements Converter<UUID> {
    static final UUIDConverter INSTANCE = new UUIDConverter();

    private UUIDConverter() {
    }

    @Override
    public UUID convert(Object obj, Object reuse) {
      if (obj instanceof String) {
        try {
          return UUID.fromString((String) obj);
        } catch (IllegalArgumentException ignored) {
          return null;
        }
      }

      return null;
    }
  }

  static class FixedConverter implements Converter<ByteBuffer> {
    private final int length;

    FixedConverter(int length) {
      this.length = length;
    }

    @Override
    public ByteBuffer convert(Object obj, Object reuse) {
      if (obj instanceof String) {
        String base64 = (String) obj;

        if (!base64.isEmpty()) {
          try {
            // throws IllegalArgumentException if conversion fails
            byte[] bytes = Base64.getDecoder().decode(base64);
            return ByteBuffer.wrap(bytes, 0, Math.min(bytes.length, length));
          } catch (IllegalArgumentException ignored) {
            // fall through to return null
          }
        }
      }

      return null;
    }
  }

  static class BinaryConverter implements Converter<ByteBuffer> {
    static final BinaryConverter INSTANCE = new BinaryConverter();

    private BinaryConverter() {
    }

    @Override
    public ByteBuffer convert(Object obj, Object reuse) {
      if (obj instanceof String) {
        String base64 = (String) obj;

        if (!base64.isEmpty()) {
          try {
            // throws IllegalArgumentException if conversion fails
            return ByteBuffer.wrap(Base64.getDecoder().decode(base64));
          } catch (IllegalArgumentException ignored) {
            // fall through to return null
          }
        }
      }

      return null;
    }
  }

  static class BigDecimalConverter implements Converter<BigDecimal> {
    private final int scale;

    BigDecimalConverter(int scale) {
      this.scale = scale;
    }

    @Override
    public BigDecimal convert(Object obj, Object reuse) {
      if (obj instanceof Integer || obj instanceof Long || obj instanceof Short || obj instanceof Byte) {
        return new BigDecimal(((Number) obj).longValue()).setScale(scale, RoundingMode.HALF_DOWN);

      } else if (obj instanceof Float || obj instanceof Double) {
        return new BigDecimal(((Number) obj).doubleValue()).setScale(scale, RoundingMode.HALF_DOWN);

      } if (obj instanceof String) {
        try {
          // throws NumberFormatException if the value is not a BigDecimal
          BigDecimal decimal = new BigDecimal((String) obj);
          return decimal.setScale(scale, RoundingMode.HALF_DOWN);
        } catch (NumberFormatException ignored) {
          // fall through to return null
        }
      }

      return null;
    }
  }

  static class StructConverter implements Converter<Record> {
    private final Types.StructType type;
    private final Map<Integer, Converter<?>> converters;
    private final Integer otherPropertiesPos;

    StructConverter(Types.StructType type, Map<Integer, Converter<?>> converters, Integer otherPropertiesPos) {
      this.type = type;
      this.converters = converters;
      this.otherPropertiesPos = otherPropertiesPos;
    }

    @Override
    public Record convert(Object obj, Object reuse) {
      if (obj instanceof Map) {
        GenericRecord record = GenericRecord.create(type);

        converters.forEach((pos, converter) ->
            record.set(pos, converter.convert(obj, null)));

        if (otherPropertiesPos != null) {
          Map<String, String> otherProperties = OtherPropertiesConverter.INSTANCE.convert(obj, null);
          record.set(otherPropertiesPos, otherProperties);
        }

        return record;
      }

      LOG.warn("Cannot convert non-object to {}: {}", type, obj);
      return null;
    }
  }

  static class OtherPropertiesConverter implements Converter<Map<String, String>> {
    static final OtherPropertiesConverter INSTANCE = new OtherPropertiesConverter();

    private OtherPropertiesConverter() {
    }

    @Override
    public Map<String, String> convert(Object obj, Object reuse) {
      Map<String, String> otherProperties = Maps.newHashMap();

      ((Map<?, ?>) obj).forEach((key, value) -> otherProperties.put(
          StringConverter.INSTANCE.convert(key, null),
          StringConverter.INSTANCE.convert(value, null)));

      return otherProperties;
    }
  }

  static class FirstKeyConverter<T> implements Converter<T> {
    private final Iterable<String> names;
    private final Converter<T> valueConverter;

    FirstKeyConverter(Iterable<String> names, Converter<T> valueConverter) {
      this.names = names;
      this.valueConverter = valueConverter;
    }

    @Override
    public T convert(Object node, Object reuse) {
      if (node instanceof Map) {
        for (String name : names) {
          Object value = ((Map) node).remove(name);
          if (value != null) {
            return valueConverter.convert(value, null);
          }
        }
      }

      return null;
    }
  }

  static class ListConverter<T> implements Converter<Collection<T>> {
    private final Converter<T> elementConverter;

    ListConverter(Converter<T> elementConverter) {
      this.elementConverter = elementConverter;
    }

    @Override
    public Collection<T> convert(Object node, Object reuse) {
      if (node instanceof Collection) {
        List<T> result = Lists.newArrayList();
        ((Collection<?>) node).forEach(elementNode ->
            result.add(elementConverter.convert(elementNode, null)));
        return result;
      }

      LOG.warn("Cannot convert non-array to list: {}", node);
      return null;
    }
  }

  static class MapConverter<K, V> implements Converter<Map<K, V>> {
    private final Converter<K> keyConverter;
    private final Converter<V> valueConverter;

    MapConverter(Converter<K> keyConverter, Converter<V> valueConverter) {
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public Map<K, V> convert(Object obj, Object reuse) {
      if (obj instanceof Map) {
        Map<K, V> result = Maps.newHashMap();
        ((Map<?, ?>) obj).forEach((key, value) -> result.put(
            keyConverter.convert(key, null),
            valueConverter.convert(value, null)));

        return result;
      }

      LOG.warn("Cannot convert non-map to map: {}", obj);
      return null;
    }
  }
}
