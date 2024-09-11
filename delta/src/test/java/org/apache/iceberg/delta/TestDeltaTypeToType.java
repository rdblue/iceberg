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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeltaTypeToType {
  private static final String OPTIONAL_BOOLEAN_TYPE = "testNullableBoolType";
  private static final String REQUIRED_BINARY_TYPE = "testRequiredBinaryType";
  private static final String DOUBLE_ARRAY_TYPE = "testNullableArrayType";
  private static final String STRUCT_ARRAY_TYPE = "testStructArrayType";
  private static final String INNER_ATOMIC_SCHEMA = "testInnerAtomicSchema";
  private static final String STRING_LONG_MAP_TYPE = "testStringLongMap";
  private static final String NULL_TYPE = "testNullType";
  private StructType deltaAtomicSchema;
  private StructType deltaNestedSchema;
  private StructType deltaShallowNullTypeSchema;
  private StructType deltaNullTypeSchema;

  @BeforeEach
  public void constructDeltaLakeSchema() {
    deltaAtomicSchema =
        new StructType()
            .add(OPTIONAL_BOOLEAN_TYPE, BooleanType.BOOLEAN)
            .add(REQUIRED_BINARY_TYPE, BinaryType.BINARY, false);
    deltaNestedSchema =
        new StructType()
            .add(INNER_ATOMIC_SCHEMA, deltaAtomicSchema)
            .add(DOUBLE_ARRAY_TYPE, new ArrayType(DoubleType.DOUBLE, true), false)
            .add(STRUCT_ARRAY_TYPE, new ArrayType(deltaAtomicSchema, true), false)
            .add(STRING_LONG_MAP_TYPE, new MapType(StringType.STRING, LongType.LONG, false), false);
    deltaNullTypeSchema =
        new StructType()
            .add(INNER_ATOMIC_SCHEMA, deltaAtomicSchema)
            .add(DOUBLE_ARRAY_TYPE, new ArrayType(DoubleType.DOUBLE, true), false)
            .add(STRING_LONG_MAP_TYPE, new MapType(StringType.STRING, LongType.LONG, false), false);
    deltaShallowNullTypeSchema = new StructType().add(NULL_TYPE, StringType.STRING, false);
  }

  @Test
  public void testAtomicTypeConversion() {
    Type converted =
        DeltaTypeVisitor.visit(deltaAtomicSchema, new DeltaTypeToType(deltaAtomicSchema));
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType(OPTIONAL_BOOLEAN_TYPE))
        .isInstanceOf(Types.BooleanType.class);
    assertThat(convertedSchema.findField(OPTIONAL_BOOLEAN_TYPE).isOptional()).isTrue();
    assertThat(convertedSchema.findType(REQUIRED_BINARY_TYPE)).isInstanceOf(Types.BinaryType.class);
    assertThat(convertedSchema.findField(REQUIRED_BINARY_TYPE).isRequired()).isTrue();
  }

  @Test
  public void testNestedTypeConversion() {
    Type converted =
        DeltaTypeVisitor.visit(deltaNestedSchema, new DeltaTypeToType(deltaNestedSchema));
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType(INNER_ATOMIC_SCHEMA)).isInstanceOf(Types.StructType.class);
    assertThat(convertedSchema.findField(INNER_ATOMIC_SCHEMA).isOptional()).isTrue();
    assertThat(
            convertedSchema
                .findType(INNER_ATOMIC_SCHEMA)
                .asStructType()
                .fieldType(OPTIONAL_BOOLEAN_TYPE))
        .isInstanceOf(Types.BooleanType.class);
    assertThat(
            convertedSchema
                .findType(INNER_ATOMIC_SCHEMA)
                .asStructType()
                .fieldType(REQUIRED_BINARY_TYPE))
        .isInstanceOf(Types.BinaryType.class);
    assertThat(
            convertedSchema
                .findType(INNER_ATOMIC_SCHEMA)
                .asStructType()
                .field(REQUIRED_BINARY_TYPE)
                .isRequired())
        .isTrue();
    assertThat(convertedSchema.findType(STRING_LONG_MAP_TYPE)).isInstanceOf(Types.MapType.class);
    assertThat(convertedSchema.findType(STRING_LONG_MAP_TYPE).asMapType().keyType())
        .isInstanceOf(Types.StringType.class);
    assertThat(convertedSchema.findType(STRING_LONG_MAP_TYPE).asMapType().valueType())
        .isInstanceOf(Types.LongType.class);
    assertThat(convertedSchema.findType(DOUBLE_ARRAY_TYPE)).isInstanceOf(Types.ListType.class);
    assertThat(convertedSchema.findField(DOUBLE_ARRAY_TYPE).isRequired()).isTrue();
    assertThat(convertedSchema.findType(DOUBLE_ARRAY_TYPE).asListType().isElementOptional())
        .isTrue();
    assertThat(convertedSchema.findType(STRUCT_ARRAY_TYPE)).isInstanceOf(Types.ListType.class);
    assertThat(convertedSchema.findField(STRUCT_ARRAY_TYPE).isRequired()).isTrue();
    assertThat(convertedSchema.findType(STRUCT_ARRAY_TYPE).asListType().isElementOptional())
        .isTrue();
    assertThat(convertedSchema.findType(STRUCT_ARRAY_TYPE).asListType().elementType())
        .isInstanceOf(Types.StructType.class);
    assertThat(
            convertedSchema
                .findType(STRUCT_ARRAY_TYPE)
                .asListType()
                .elementType()
                .asStructType()
                .fieldType(OPTIONAL_BOOLEAN_TYPE))
        .isInstanceOf(Types.BooleanType.class);
    assertThat(
            convertedSchema
                .findType(STRUCT_ARRAY_TYPE)
                .asListType()
                .elementType()
                .asStructType()
                .field(OPTIONAL_BOOLEAN_TYPE)
                .isOptional())
        .isTrue();
    assertThat(
            convertedSchema
                .findType(STRUCT_ARRAY_TYPE)
                .asListType()
                .elementType()
                .asStructType()
                .fieldType(REQUIRED_BINARY_TYPE))
        .isInstanceOf(Types.BinaryType.class);
    assertThat(
            convertedSchema
                .findType(STRUCT_ARRAY_TYPE)
                .asListType()
                .elementType()
                .asStructType()
                .field(REQUIRED_BINARY_TYPE)
                .isRequired())
        .isTrue();
  }

  private static class NullType extends DataType {
    @Override
    public String toJson() {
      return "void";
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof NullType;
    }

    @Override
    public String toString() {
      return "void";
    }
  }

  @Test
  public void testNullTypeConversion() {
    assertThatThrownBy(
            () ->
                DeltaTypeVisitor.visit(
                    deltaNullTypeSchema, new DeltaTypeToType(deltaNullTypeSchema)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format("Not a supported type: %s", new NullType()));
    assertThatThrownBy(
            () ->
                DeltaTypeVisitor.visit(
                    deltaShallowNullTypeSchema, new DeltaTypeToType(deltaShallowNullTypeSchema)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format("Not a supported type: %s", new NullType()));
  }
}
