package com.netflix.iceberg.view;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;

import java.util.Map;

/**
 * The Views implementation based on Hadoop file system.
 */
@Data
public class SimpleHadoopViews extends BaseViews implements HadoopConfigurable {

  private final Configuration conf;

  public static SimpleHadoopViews of() {
    return new SimpleHadoopViews(new Configuration());
  }

  private SimpleHadoopViews(Configuration conf) {
    super(new HadoopFileIO(conf));
    this.conf = conf;
  }

  @Override
  public void setConf(Iterable<Map.Entry<String, String>> props) {
    props.forEach((entry) -> conf.set(entry.getKey(), entry.getValue()));
  }
}
