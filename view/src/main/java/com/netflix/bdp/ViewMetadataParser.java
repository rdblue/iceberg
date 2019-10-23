package com.netflix.bdp;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.SchemaParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class ViewMetadataParser {
  static ViewMetadata fromJson(JsonNode node) {
    return ViewMetadata.builder()
        .sql(node.get("sql").asText())
        .schema(Optional.ofNullable(node.get("schema"))
            .map(SchemaParser::fromJson)
            .orElse(ViewMetadata.EMPTY_SCHEMA))
        .comment(Optional.ofNullable(node.get("comment"))
            .map(JsonNode::asText)
            .orElse(""))
        .createVersion(Optional.ofNullable(node.get("createVersion"))
            .map(JsonNode::asText)
            .orElse(""))
        .sessionCatalog(Optional.ofNullable(node.get("sessionCatalog"))
            .map(JsonNode::asText)
            .orElse(""))
        .sessionNamespace(Optional.ofNullable(node.get("sessionNamespace"))
            .map((childNode) ->
                StreamSupport.stream(childNode.spliterator(), false)
                    .map(JsonNode::asText)
                    .collect(Collectors.toList()))
            .orElse(Collections.emptyList()))
        .owner(Optional.ofNullable(node.get("owner"))
            .map(JsonNode::asText)
            .orElse(""))
        .runAsInvoker(Optional.ofNullable(node.get("runAsInvoker"))
            .map(JsonNode::asBoolean)
            .orElse(View.DEFAULT_RUNASINVOKER))
        .build();
  }

  static void toJson(ViewMetadata view, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("sql", view.sql());
    generator.writeFieldName("schema");
    SchemaParser.toJson(view.schema(), generator);
    generator.writeStringField("comment", view.comment());
    generator.writeStringField("createVersion", view.createVersion());
    generator.writeStringField("sessionCatalog", view.sessionCatalog());
    generator.writeFieldName("sessionNamespace");
    generator.writeStartArray();
    for (String space : view.sessionNamespace()) {
      generator.writeString(space);
    }
    generator.writeEndArray();
    generator.writeStringField("owner", view.owner());
    generator.writeBooleanField("runAsInvoker", view.runAsInvoker());
    generator.writeEndObject();
  }
}
