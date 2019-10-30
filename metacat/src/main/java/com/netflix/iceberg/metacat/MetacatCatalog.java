package com.netflix.iceberg.metacat;

import com.netflix.iceberg.spark.source.SparkCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.v2.CaseInsensitiveStringMap;

public class MetacatCatalog extends SparkCatalog {
  private static final String CATALOG_OPTION = "catalog";

  private String defaultDB = null;
  private String catalogName = null;
  private Catalog icebergCatalog = null;

  @Override
  protected Catalog catalog() {
    return icebergCatalog;
  }

  @Override
  protected TableIdentifier toIceberg(org.apache.spark.sql.catalyst.TableIdentifier ident) {
    if (ident.database().isDefined()) {
      return TableIdentifier.of(catalogName, ident.database().get(), ident.table());
    } else {
      return TableIdentifier.of(catalogName, defaultDB, ident.table());
    }
  }

  public Table load(String db, String table) {
    return icebergCatalog.loadTable(TableIdentifier.of(catalogName, db, table));
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);

    SparkSession spark = lazySparkSession();
    Configuration conf = new Configuration(spark.sparkContext().hadoopConfiguration());

    boolean cacheEnabled = Boolean.parseBoolean(options.getOrDefault("cache-enabled", "true"));

    String hadoopHiveEnv = spark.sparkContext().hadoopConfiguration()
        .get("spark.sql.hive.env", "prod");
    String hiveEnv = spark.sparkContext().conf()
        .get("spark.sql.hive.env", hadoopHiveEnv);
    String defaultCatalog = spark.sparkContext().conf()
        .get("spark.sql.metacat.write.catalog", hiveEnv + "hive");
    this.catalogName = options.get(CATALOG_OPTION);
    if (catalogName == null) {
      catalogName = defaultCatalog;
    }

    String metacatUri = options.get("metacat-uri");
    if (metacatUri != null) {
      conf.set("netflix.metacat.host", metacatUri);
    }

    Catalog catalog = new MetacatIcebergCatalog(conf, spark.sparkContext().applicationId(), "spark");

    this.icebergCatalog = cacheEnabled ? CachingCatalog.wrap(catalog) : catalog;
  }

  public String toString() {
    return "MetacatCatalog(name=" + name() + ", catalog=" + catalogName + ")";
  }
}
