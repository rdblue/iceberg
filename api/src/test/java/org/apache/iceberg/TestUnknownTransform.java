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

package org.apache.iceberg;

import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestUnknownTransform {
  @Test
  @SuppressWarnings("unchecked")
  public void testUnknownTransform() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "before", Types.DateType.get()),
        Types.NestedField.required(3, "after", Types.DateType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .add(3, "after_unsupported", "unsupported")
        .build();

    Transform<Object, ?> unknown = (Transform<Object, ?>) spec.fields().get(0).transform();
    TestHelpers.assertThrows("Should throw if apply is called",
        UnsupportedOperationException.class, "Cannot apply unsupported transform: unsupported",
        () -> unknown.apply(null));

    Assert.assertTrue("Should support transform for source type", unknown.canTransform(Types.DateType.get()));
    Assert.assertFalse("Should not support transform for other types", unknown.canTransform(Types.StringType.get()));

    BoundPredicate<Object> predicate = (BoundPredicate<Object>)
        Expressions.lessThan("before", 0).bind(schema.asStruct(), true);
    Assert.assertNull("Should return null for project", unknown.project("before", predicate));
    Assert.assertNull("Should return null for project", unknown.projectStrict("before", predicate));
    Assert.assertEquals("Should return string result type",
        Types.StringType.get(), unknown.getResultType(Types.DateType.get()));
  }

  @Test
  public void testUnknownTransformEquals() {
    Assert.assertEquals("Should support equality by type and transform name",
        Transforms.fromString(Types.DateType.get(), "unsupported"),
        Transforms.fromString(Types.DateType.get(), "unsupported"));
    Assert.assertNotEquals("Should support equality by type and transform name",
        Transforms.fromString(Types.DateType.get(), "unsupported"),
        Transforms.fromString(Types.DateType.get(), "different"));
    Assert.assertNotEquals("Should support equality by type and transform name",
        Transforms.fromString(Types.DateType.get(), "unsupported"),
        Transforms.fromString(Types.IntegerType.get(), "unsupported"));
  }
}
