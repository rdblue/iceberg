package com.netflix.bdp;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;

import java.io.IOException;

/**
 * Methods for working with views.
 */
public interface Views {

  /**
   * Create a view without replacing any existing view.
   *
   * @param viewIdentifier the view identifier
   * @param definition     the view definition
   * @return the definition of the newly created view
   * @throws AlreadyExistsException If the path already exists
   * @throws RuntimeIOException     If the implementation throws an {@link IOException}
   */
  default View create(String viewIdentifier, View definition) {
    return create(viewIdentifier, definition, false);
  }

  /**
   * Creates a view.
   *
   * @param viewIdentifier the view identifier
   * @param definition     the view definition
   * @param replace        whether to replace any existing view
   * @return the definition of the newly created view
   * @throws AlreadyExistsException If the view already exists
   * @throws RuntimeIOException     If the implementation throws an {@link IOException}
   */
  View create(String viewIdentifier, View definition, boolean replace);

  /**
   * Loads a view by name.
   *
   * @param viewIdentifier the view identifier
   * @return the view definition
   * @throws NotFoundException If the view does not exist
   * @throws RuntimeIOException  If the implementation throws an {@link IOException}
   */
  View load(String viewIdentifier);

  /**
   * Drops a view.
   *
   * @param viewIdentifier the view identifier
   * @throws RuntimeIOException If the implementation throws an {@link IOException}
   */
  void drop(String viewIdentifier);

}
