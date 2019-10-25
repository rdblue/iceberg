package com.netflix.iceberg.view;

import java.util.Map;

/**
 * Sets the properties of Hadoop Configuration.
 */
public interface HadoopConfigurable {

  /**
   * Sets the properties of the Hadoop Configuration.
   *
   * @param props the properties
   */
  void setConf(Iterable<Map.Entry<String, String>> props);
}
