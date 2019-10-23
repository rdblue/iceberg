package com.netflix.bdp;

import org.apache.iceberg.Schema;

import java.util.List;

/**
 * Interface for view definition.
 */
public interface View {

  /**
   * Default runAsInvoker value.
   */
  boolean DEFAULT_RUNASINVOKER = false;

  /**
   * Returns the view query SQL text.
   *
   * @return the view query SQL text
   */
  String sql();

  /**
   * Returns the view query output schema.
   *
   * @return the view query output schema
   */
  Schema schema();

  /**
   * Returns the view comment.
   *
   * @return the view comment
   */
  String comment();

  /**
   * Returns the version of the query engine where the view is created.
   *
   * @return the version of the query engine where the view is created
   */
  String createVersion();

  /**
   * Returns the session catalog when the view is created.
   *
   * @return the session catalog
   */
  String sessionCatalog();

  /**
   * Returns the session namespace when the view is created.
   *
   * @return the session namespace
   */
  List<String> sessionNamespace();

  /**
   * Returns the owner.
   *
   * @return the owner
   */
  String owner();

  /**
   * Returns whether to run the view as the invoker.
   *
   * @return whether to run the view as the invoker
   */
  boolean runAsInvoker();
}
