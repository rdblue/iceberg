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

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

class DeltaTypeToType extends DeltaTypeVisitor<Type> {
  private static final String DEFAULT_VALUE_KEY = "CURRENT_DEFAULT";
  private static final String FIELD_ID_KEY = "delta.columnMapping.id";
  private static final Joiner DOT = Joiner.on(".");

  private final Deque<String> fieldNames = Lists.newLinkedList();
  private final StructType root;
  private final NameMapping mapping;
  private final AtomicInteger nextId;

  DeltaTypeToType() {
    this.root = null;
    this.mapping = null;
    this.nextId = new AtomicInteger(0);
  }

  DeltaTypeToType(StructType root) {
    this(root, null);
  }

  DeltaTypeToType(StructType root, NameMapping mapping) {
    this.root = root;
    this.mapping = mapping;
    // TODO: plumb through lastAssignedFieldId
    this.nextId = new AtomicInteger(root.fields().size() + 1);
  }

  private int assignOrFindId(String name, TypeUtil.NextID nextId) {
    if (mapping != null) {
      MappedField field = mapping.find(fieldName(name));
      if (field != null) {
        return field.id();
      }
    }

    return nextId.get();
  }

  private int assignOrFindId(String name) {
    return assignOrFindId(name, nextId::getAndIncrement);
  }

  @Override
  @SuppressWarnings("ReferenceEquality")
  public Type struct(StructType struct, List<Type> types) {
    List<StructField> fields = struct.fields();
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean isRoot = root == struct;
    for (int i = 0; i < fields.size(); i += 1) {
      StructField field = fields.get(i);
      FieldMetadata metadata = field.getMetadata();

      Object deltaId = metadata.get(FIELD_ID_KEY);
      int id;
      if (deltaId instanceof Number) {
        id = ((Number) deltaId).intValue();
      } else if (isRoot) {
        // for new conversions, use ordinals for ids in the root struct
        int pos = i; // make i effectively final
        id = assignOrFindId(field.getName(), () -> pos);
      } else {
        id = assignOrFindId(field.getName());
      }

      String doc = metadata.contains("comment") ? metadata.get("comment").toString() : null;

      // TODO: add this as the write default
      Object currentDefault = metadata.get(DEFAULT_VALUE_KEY);

      if (field.isNullable()) {
        newFields.add(Types.NestedField.optional(id, field.getName(), types.get(i), doc));
      } else {
        newFields.add(Types.NestedField.required(id, field.getName(), types.get(i), doc));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(StructField field, Type typeResult) {
    return typeResult;
  }

  @Override
  public Type array(ArrayType array, Type elementType) {
    int elementId = assignOrFindId("element");
    if (array.containsNull()) {
      return Types.ListType.ofOptional(elementId, elementType);
    } else {
      return Types.ListType.ofRequired(elementId, elementType);
    }
  }

  @Override
  public Type map(MapType map, Type keyType, Type valueType) {
    int keyId = assignOrFindId("key");
    int valueId = assignOrFindId("value");
    if (map.isValueContainsNull()) {
      return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
    } else {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public Type atomic(DataType atomic) {
    if (atomic instanceof BooleanType) {
      return Types.BooleanType.get();

    } else if (atomic instanceof IntegerType
        || atomic instanceof ShortType
        || atomic instanceof ByteType) {
      return Types.IntegerType.get();

    } else if (atomic instanceof LongType) {
      return Types.LongType.get();

    } else if (atomic instanceof FloatType) {
      return Types.FloatType.get();

    } else if (atomic instanceof DoubleType) {
      return Types.DoubleType.get();

    } else if (atomic instanceof StringType) {
      return Types.StringType.get();

    } else if (atomic instanceof DateType) {
      return Types.DateType.get();

    } else if (atomic instanceof TimestampType) {
      return Types.TimestampType.withZone();

    } else if (atomic instanceof TimestampNTZType) {
      return Types.TimestampType.withoutZone();

    } else if (atomic instanceof DecimalType) {
      return Types.DecimalType.of(
          ((DecimalType) atomic).getPrecision(), ((DecimalType) atomic).getScale());
    } else if (atomic instanceof BinaryType) {
      return Types.BinaryType.get();
    }

    throw new ValidationException("Not a supported type: %s", atomic.toString());
  }

  private String fieldName(String name) {
    return DOT.join(Iterables.concat(fieldNames, ImmutableList.of(name)));
  }

  @Override
  public void beforeField(StructField field) {
    fieldNames.addLast(field.getName());
  }

  @Override
  public void afterField(StructField field) {
    fieldNames.removeLast();
  }
}
