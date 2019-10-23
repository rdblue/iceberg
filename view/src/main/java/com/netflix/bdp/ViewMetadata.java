package com.netflix.bdp;

import lombok.Builder;
import lombok.Value;
import org.apache.iceberg.Schema;

import java.util.Collections;
import java.util.List;

/**
 * The default View implementation.
 */
@Value
@Builder(builderClassName = "Builder")
public class ViewMetadata implements View {

  static final Schema EMPTY_SCHEMA = new Schema();

  String sql;
  @lombok.Builder.Default Schema schema = EMPTY_SCHEMA;
  @lombok.Builder.Default String comment = "";
  @lombok.Builder.Default String createVersion = "";
  @lombok.Builder.Default String sessionCatalog = "";
  @lombok.Builder.Default List<String> sessionNamespace = Collections.emptyList();
  @lombok.Builder.Default String owner = "";
  @lombok.Builder.Default boolean runAsInvoker = DEFAULT_RUNASINVOKER;
}
