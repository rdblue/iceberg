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
package org.apache.iceberg.expressions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.DefaultValueParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.JsonUtil;

public class ExpressionParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String VALUES = "values";
  private static final String TRANSFORM = "transform";
  private static final String TERM = "term";
  private static final String LEFT = "left";
  private static final String RIGHT = "right";
  private static final String CHILD = "child";
  private static final String REFERENCE = "reference";
  private static final String LITERAL = "literal";

  private ExpressionParser() {}

  public static String toJson(Expression expression) {
    return toJson(expression, false);
  }

  public static String toJson(Expression expression, boolean pretty) {
    return JsonUtil.generate(
        gen ->
            ExpressionVisitors.visit(expression, new JsonGeneratorVisitor(gen)),
        pretty);
  }

  private static class JsonGeneratorVisitor extends ExpressionVisitors.CustomOrderExpressionVisitor<Void> {
    private final JsonGenerator gen;

    private JsonGeneratorVisitor(JsonGenerator gen) {
      this.gen = gen;
    }

    /** A convenience method to make code more readable by calling {@code toJson} instead of {@code get()} */
    private void toJson(Supplier<Void> child) {
      child.get();
    }

    @FunctionalInterface
    private interface Task {
      void run() throws IOException;
    }

    private Void generate(Task task) {
      try {
        task.run();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return null;
    }

    @Override
    public Void alwaysTrue() {
      return generate(() -> gen.writeBoolean(true));
    }

    @Override
    public Void alwaysFalse() {
      return generate(() -> gen.writeBoolean(false));
    }

    @Override
    public Void not(Supplier<Void> child) {
      return generate(() -> {
        gen.writeStartObject();
        gen.writeStringField(TYPE, "not");
        gen.writeFieldName(CHILD);
        toJson(child);
        gen.writeEndObject();
      });
    }

    @Override
    public Void and(Supplier<Void> left, Supplier<Void> right) {
      return generate(() -> {
        gen.writeStartObject();
        gen.writeStringField(TYPE, "and");
        gen.writeFieldName(LEFT);
        toJson(left);
        gen.writeFieldName(RIGHT);
        toJson(right);
        gen.writeEndObject();
      });
    }

    @Override
    public Void or(Supplier<Void> left, Supplier<Void> right) {
      return generate(() -> {
        gen.writeStartObject();
        gen.writeStringField(TYPE, "or");
        gen.writeFieldName(LEFT);
        toJson(left);
        gen.writeFieldName(RIGHT);
        toJson(right);
        gen.writeEndObject();
      });
    }

    @Override
    public <T> Void predicate(BoundPredicate<T> pred) {
      return generate(() -> {
        gen.writeStartObject();

        gen.writeStringField(TYPE, operationType(pred.op()));
        gen.writeFieldName(TERM);
        term(pred.term());

        if (pred.isLiteralPredicate()) {
          gen.writeFieldName(VALUE);
          DefaultValueParser.toJson(pred.term().type(), pred.asLiteralPredicate().literal().value(), gen);
        } else if (pred.isSetPredicate()) {
          gen.writeArrayFieldStart(VALUES);
          for (T value : pred.asSetPredicate().literalSet()) {
            DefaultValueParser.toJson(pred.term().type(), value, gen);
          }
          gen.writeEndArray();
        }

        gen.writeEndObject();
      });
    }

    private String operationType(Expression.Operation op) {
      return op.toString().replaceAll("_", "-").toLowerCase(Locale.ROOT);
    }

    private <T> void term(BoundTerm<T> term) throws IOException {
      if (term instanceof BoundTransform) {
        transform((BoundTransform<?, ?>) term);
        return;
      } else if (term instanceof BoundReference) {
        gen.writeString(((BoundReference<?>) term).name());
        return;
      }

      throw new UnsupportedOperationException("Cannot write unsupported term: " + term);
    }

    private void transform(BoundTransform<?, ?> term) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, TRANSFORM);
      gen.writeStringField(TRANSFORM, term.transform().toString());
      gen.writeStringField(TERM, term.ref().name());
      gen.writeEndObject();
    }
  }

  public static Expression fromJson(String json) {
    return fromJson(json, null);
  }

  public static Expression fromJson(JsonNode json) {
    return fromJson(json, null);
  }

  public static Expression fromJson(String json, Schema schema) {
    return JsonUtil.parse(json, node -> fromJson(node, schema));
  }

  static Expression fromJson(JsonNode json, Schema schema) {
    Preconditions.checkArgument(null != json, "Cannot parse expression from null object");
    // check for constant expressions
    if (json.isBoolean()) {
      if (json.asBoolean()) {
        return Expressions.alwaysTrue();
      } else {
        return Expressions.alwaysFalse();
      }
    }

    Preconditions.checkArgument(
        json.isObject(), "Cannot parse expression from non-object: %s", json);

    String type = JsonUtil.getString(TYPE, json);
    if (type.equalsIgnoreCase(LITERAL)) {
      if (JsonUtil.getBool(VALUE, json)) {
        return Expressions.alwaysTrue();
      } else {
        return Expressions.alwaysFalse();
      }
    }

    Expression.Operation op = fromType(type);
    switch (op) {
      case NOT:
        return Expressions.not(fromJson(JsonUtil.get(CHILD, json), schema));
      case AND:
        return Expressions.and(fromJson(JsonUtil.get(LEFT, json), schema), fromJson(JsonUtil.get(RIGHT, json), schema));
      case OR:
        return Expressions.or(fromJson(JsonUtil.get(LEFT, json), schema), fromJson(JsonUtil.get(RIGHT, json), schema));
    }

    return predicateFromJson(op, json, schema);
  }

  private static Expression.Operation fromType(String type) {
    return Expression.Operation.valueOf(type.replaceAll("-", "_").toUpperCase(Locale.ROOT));
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> predicateFromJson(Expression.Operation op, JsonNode node, Schema schema) {
    UnboundTerm<T> term = term(JsonUtil.get(TERM, node), schema);

    Function<JsonNode, T> convertValue;
    if (schema != null) {
      BoundTerm<?> bound = term.bind(schema.asStruct(), false);
      convertValue = valueNode -> (T) DefaultValueParser.fromJson(bound.type(), valueNode);
    } else {
      convertValue = valueNode -> (T) ExpressionParser.asObject(valueNode);
    }

    if (node.has(VALUE)) {
      // must be a literal predicate
      Object value = literal(JsonUtil.get(VALUE, node), convertValue);
      return Expressions.predicate(op, term, (Iterable<T>) ImmutableList.of(value));

    } else if (node.has(VALUES)) {
      // must be a set predicate
      JsonNode valuesNode = JsonUtil.get(VALUES, node);
      Preconditions.checkArgument(valuesNode.isArray(), "Cannot parse literals from non-array: %s", valuesNode);
      return Expressions.predicate(
          op, term,
          Iterables.transform(((ArrayNode) valuesNode)::elements, valueNode -> literal(valueNode, convertValue)));

    } else {
      // must be a unary predicate
      return Expressions.predicate(op, term);
    }
  }

  private static <T> T literal(JsonNode valueNode, Function<JsonNode, T> toValue) {
    if (valueNode.isObject() && valueNode.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, valueNode);
      Preconditions.checkArgument(type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      return toValue.apply(JsonUtil.get(VALUE, valueNode));
    }

    // the node is a directly embedded literal value
    return toValue.apply(valueNode);
  }

  private static Object asObject(JsonNode node) {
    if (node.isIntegralNumber() && node.canConvertToLong()) {
      return node.asLong();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isFloatingPointNumber()) {
      return node.asDouble();
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else {
      throw new IllegalArgumentException("Cannot convert JSON to literal: " + node);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> term(JsonNode node, Schema schema) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          return Expressions.ref(JsonUtil.getString(TERM, node));
        case TRANSFORM:
          UnboundTerm<T> child = term(JsonUtil.get(TERM, node), schema);
          Type termType = schema.findType(child.ref().name());
          String transform = JsonUtil.getString(TRANSFORM, node);
          return (UnboundTerm<T>) Expressions.transform(child.ref().name(), Transforms.fromString(termType, transform));
        default:
          throw new IllegalArgumentException("Cannot parse type as a reference: " + type);
      }
    }

    throw new IllegalArgumentException("Cannot parse reference (requires string or object): " + node);
  }
}
