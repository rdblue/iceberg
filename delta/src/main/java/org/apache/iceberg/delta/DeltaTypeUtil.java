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

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Utility methods for working with Delta types. */
public class DeltaTypeUtil {
  private DeltaTypeUtil() {}

  /**
   * Convert a Delta table schema to Iceberg.
   *
   * @param struct a Delta table's StructType
   * @return an Iceberg StructType
   */
  public static Types.StructType convert(StructType struct) {
    return DeltaTypeVisitor.visit(struct, new DeltaTypeToType(struct)).asStructType();
  }

  public static DataType convert(Type.PrimitiveType type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case INTEGER:
        return IntegerType.INTEGER;
      case LONG:
        return LongType.LONG;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case DATE:
        return DateType.DATE;
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return TimestampType.TIMESTAMP;
        } else {
          return TimestampNTZType.TIMESTAMP_NTZ;
        }
      case STRING:
      case UUID:
        return StringType.STRING;
      case BINARY:
      case FIXED:
        return BinaryType.BINARY;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        return new DecimalType(decimal.precision(), decimal.scale());
    }

    throw new UnsupportedOperationException("Cannot convert type: " + type);
  }
}
