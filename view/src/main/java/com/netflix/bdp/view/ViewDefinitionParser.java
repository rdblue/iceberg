package com.netflix.bdp.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.SchemaParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class ViewDefinitionParser {
    static ViewDefinition fromJson(JsonNode node) {
        return new BaseViewDefinition(node.get("sql").asText(),
                Optional.ofNullable(node.get("schema"))
                        .map(SchemaParser::fromJson)
                        .orElse(ViewDefinition.EMPTY_SCHEMA),
                Optional.ofNullable(node.get("sessionCatalog"))
                        .map(JsonNode::asText)
                        .orElse(""),
                Optional.ofNullable(node.get("sessionNamespace"))
                        .map((childNode) ->
                                StreamSupport.stream(childNode.spliterator(), false)
                                        .map(JsonNode::asText)
                                        .collect(Collectors.toList()))
                        .orElse(Collections.emptyList()));
    }

    static void toJson(ViewDefinition view, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("sql", view.sql());
        generator.writeFieldName("schema");
        SchemaParser.toJson(view.schema(), generator);
        generator.writeStringField("sessionCatalog", view.sessionCatalog());
        generator.writeFieldName("sessionNamespace");
        generator.writeStartArray();
        for (String space : view.sessionNamespace()) {
            generator.writeString(space);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }
}
