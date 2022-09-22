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

package org.apache.iceberg.flink.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToRowConverter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonToRowConverter.class);
  private static final Pattern DATE = Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d");
  private static final Pattern TIMESTAMP =
      Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d(:\\d\\d(.\\d{1,6})?)?");
  private static final Pattern TIMESTAMPTZ =
      Pattern.compile(
          "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d(:\\d\\d(.\\d{1,6})?)?[-+]\\d\\d:\\d\\d");

  public static RowData convert(Schema schema, NameMapping mapping, String json) {
    return convert(schema, mapping, parse(json));
  }

  public static RowData convert(Schema schema, NameMapping mapping, JsonNode node) {
    return convert(schema.asStruct(), mapping.asMappedFields(), node);
  }

  private static JsonNode parse(String json) {
    try {
      return JsonUtil.mapper().readValue(json, JsonNode.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Object convert(Type type, MappedFields mapping, JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return null;
    }

    try {
      switch (type.typeId()) {
        case BOOLEAN:
          if (node.isBoolean()) {
            return node.asBoolean();
          } else {
            return null;
          }
        case INTEGER:
          if (node.isIntegralNumber() && node.canConvertToInt()) {
            return node.asInt();
          } else {
            return null;
          }
        case LONG:
          if (node.isIntegralNumber() && node.canConvertToLong()) {
            return node.asLong();
          } else {
            return null;
          }
        case FLOAT:
          if (node.isNumber() && node.canConvertToInt()) {
            return node.floatValue();
          } else {
            return null;
          }
        case DOUBLE:
          if (node.isNumber() && node.canConvertToLong()) {
            return node.doubleValue();
          } else {
            return null;
          }
        case DATE:
          return convertDate(node);
        case TIMESTAMP:
          return convertTimestamp(node);
        case STRING:
          return StringData.fromString(node.asText());
        case FIXED:
          if (node.isTextual()) {
            // throws IllegalArgumentException if conversion fails
            int len = ((Types.FixedType) type).length();
            byte[] bytes = Base64.getDecoder().decode(node.asText());
            if (bytes.length == len) {
              return bytes;
            } else {
              return Arrays.copyOf(bytes, len);
            }
          } else {
            return null;
          }
        case BINARY:
          if (node.isTextual()) {
            // throws IllegalArgumentException if conversion fails
            return Base64.getDecoder().decode(node.asText());
          } else {
            return null;
          }
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) type;
          if (node.isTextual()) {
            // throws NumberFormatException if conversion fails
            BigDecimal decimal =
                new BigDecimal(node.asText())
                    .setScale(((Types.DecimalType) type).scale(), RoundingMode.HALF_DOWN);
            return DecimalData.fromBigDecimal(
                decimal, decimalType.precision(), decimalType.scale());
          } else if (node.isFloatingPointNumber() || node.isIntegralNumber()) {
            BigDecimal decimal =
                new BigDecimal(node.asDouble())
                    .setScale(((Types.DecimalType) type).scale(), RoundingMode.HALF_DOWN);
            return DecimalData.fromBigDecimal(
                decimal, decimalType.precision(), decimalType.scale());
          } else {
            return null;
          }
        case STRUCT:
          return convert(type.asStructType(), mapping, node);
        case LIST:
          return convert(type.asListType(), mapping, node);
        case MAP:
          return convert(type.asMapType(), mapping, node);
        default:
          LOG.warn("Cannot convert value to unsupported type: {}", type);
          return null;
      }
    } catch (IllegalArgumentException | DateTimeParseException ignored) {
      // NumberFormatException is an IllegalArgumentException
      return null;
    }
  }

  private static RowData convert(Types.StructType struct, MappedFields mapping, JsonNode node) {
    if (!node.isObject()) {
      LOG.warn("Cannot convert non-object to {}: {}", struct, node);
      return null;
    }

    if (mapping == null) {
      LOG.warn("Invalid field mapping for type {}: null", struct);
      return null;
    }

    List<Types.NestedField> fields = struct.fields();
    GenericRowData record = new GenericRowData(fields.size());

    for (int pos = 0; pos < fields.size(); pos += 1) {
      Types.NestedField field = struct.fields().get(pos);
      int id = field.fieldId();
      MappedField mappedField = mapping.field(id);
      if (mappedField == null) {
        record.setField(pos, null);
        continue; // this field has no data
      }

      for (String name : mappedField.names()) {
        if (node.has(name)) {
          record.setField(pos, convert(field.type(), mappedField.nestedMapping(), node.get(name)));
          break; // use the first available name
        }
      }
    }

    return record;
  }

  private static ArrayData convert(Types.ListType list, MappedFields mapping, JsonNode node) {
    if (!node.isArray()) {
      LOG.warn("Cannot convert non-array to {}: {}", list, node);
      return null;
    }

    if (mapping == null || mapping.fields().size() != 1) {
      LOG.warn("Invalid field mapping for type {}: {}", list, mapping);
      return null;
    }

    MappedField elementField = mapping.fields().get(0);
    if (elementField.id() != list.elementId()) {
      LOG.warn(
          "Incorrect element ID in mapping: {} != {} (expected)",
          elementField.id(),
          list.elementId());
    }

    List<Object> result = Lists.newArrayList();
    node.elements()
        .forEachRemaining(
            elementNode ->
                result.add(convert(list.elementType(), elementField.nestedMapping(), elementNode)));

    return new GenericArrayData(result.toArray());
  }

  private static MapData convert(Types.MapType map, MappedFields mapping, JsonNode node) {
    if (!node.isObject()) {
      LOG.warn("Cannot convert non-object to {}: {}", map, node);
      return null;
    }

    if (mapping.fields().size() != 2) {
      LOG.warn("Invalid field mapping for type {}: {}", map, mapping);
      return null;
    }

    MappedField keyField = mapping.fields().get(0);
    if (keyField.id() != map.keyId()) {
      LOG.warn("Incorrect key ID in mapping: {} != {} (expected)", keyField.id(), map.keyId());
    }

    MappedField valueField = mapping.fields().get(1);
    if (valueField.id() != map.valueId()) {
      LOG.warn("Incorrect value ID in mapping: {} != {} (expected)", keyField.id(), map.valueId());
    }

    Map<Object, Object> result = Maps.newHashMap();
    node.fields()
        .forEachRemaining(
            entry ->
                result.put(
                    convert(map.keyType(), entry.getKey()),
                    convert(map.valueType(), valueField.nestedMapping(), entry.getValue())));

    return new GenericMapData(result);
  }

  private static Object convert(Type type, String string) {
    if (string == null) {
      return null;
    }

    try {
      switch (type.typeId()) {
        case BOOLEAN:
          if ("true".equalsIgnoreCase(string)) {
            return true;
          } else if ("false".equalsIgnoreCase(string)) {
            return false;
          } else {
            return null;
          }
        case INTEGER:
          return Integer.parseInt(string);
        case LONG:
          return Long.parseLong(string);
        case FLOAT:
          return Float.parseFloat(string);
        case DOUBLE:
          return Double.parseDouble(string);
        case DATE:
          return DateTimeUtil.isoDateToDays(string);
        case TIMESTAMP:
          return convertTimestamp(string);
        case STRING:
          return StringData.fromString(string);
        case FIXED:
          // throws IllegalArgumentException if conversion fails
          int len = ((Types.FixedType) type).length();
          byte[] bytes = Base64.getDecoder().decode(string);
          if (bytes.length == len) {
            return bytes;
          } else {
            return Arrays.copyOf(bytes, len);
          }
        case BINARY:
          // throws IllegalArgumentException if conversion fails
          return Base64.getDecoder().decode(string);
        case DECIMAL:
          // throws NumberFormatException if conversion fails
          BigDecimal decimal = new BigDecimal(string);
          return decimal.setScale(((Types.DecimalType) type).scale(), RoundingMode.HALF_DOWN);
        default:
          LOG.warn("Cannot convert value to unsupported type: {}", type);
          return null;
      }
    } catch (IllegalArgumentException | DateTimeParseException ignored) {
      // NumberFormatException is an IllegalArgumentException
      return null;
    }
  }

  private static Integer convertDate(JsonNode node) {
    if (node.isIntegralNumber() && node.canConvertToInt()) {
      return node.asInt();
    } else if (node.isTextual()) {
      String date = node.asText();
      if (DATE.matcher(date).matches()) {
        return DateTimeUtil.isoDateToDays(date);
      }
    }

    return null;
  }

  private static LocalDateTime convertTimestamp(String timestamp) {
    if (TIMESTAMP.matcher(timestamp).matches()) {
      return LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    } else if (TIMESTAMPTZ.matcher(timestamp).matches()) {
      return LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME);
    } else if (DATE.matcher(timestamp).matches()) {
      return LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE);
    }

    return null;
  }

  private static TimestampData convertTimestamp(JsonNode node) {
    if (node.isIntegralNumber() && node.canConvertToLong()) {
      // assumes incoming values are in micros. this could be more robust
      return TimestampData.fromLocalDateTime(DateTimeUtil.timestampFromMicros(node.asLong()));
    } else if (node.isTextual()) {
      return TimestampData.fromLocalDateTime(convertTimestamp(node.asText()));
    }

    return null;
  }
}
