package com.netflix.iceberg.spark.source;

import com.google.common.base.Preconditions;
import org.apache.iceberg.Table;
import com.netflix.iceberg.metacat.MetacatCatalog;
import com.netflix.iceberg.metacat.MetacatIcebergCatalog;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.v2.CatalogProvider;
import org.apache.spark.sql.catalog.v2.TableCatalog;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import scala.Option;

public class IcebergMetacatSource extends IcebergSource
{
  private static final String CATALOG_OPTION = "catalog";
  private static final String DATABASE_OPTION = "database";
  private static final String TABLE_OPTION = "table";

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  protected Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> table = options.get(TABLE_OPTION);
    Preconditions.checkArgument(table.isPresent(), "Required option 'table' is missing");

    SparkSession session = lazySparkSession();

    String hiveEnv = conf.get("spark.sql.hive.env", "prod");
    String defaultCatalog = session.sparkContext().conf()
        .get("spark.sql.metacat.write.catalog", hiveEnv + "hive");

    String catalog = options.get(CATALOG_OPTION).orElse(defaultCatalog);
    String db = options.get(DATABASE_OPTION).orElse(session.catalog().currentDatabase());

    // try to use the current session's TableCatalog instead of loading directly so that a shared
    // Table instance from the cache is used, if possible
    MetacatCatalog sparkCatalog = catalog(session, catalog, "iceberg");
    if (sparkCatalog != null) {
      return sparkCatalog.load(db, table.get());
    }

    // if no table catalog was found, fall back to loading the table with a new MetacatTables
    Namespace namespace = Namespace.of(catalog, db);
    TableIdentifier identifier = TableIdentifier.of(namespace, table.get());
    MetacatIcebergCatalog icebergCatalog = new MetacatIcebergCatalog(conf, session.sparkContext().applicationId());
    return icebergCatalog.loadTable(identifier);
  }

  private static MetacatCatalog catalog(SparkSession session, String... catalogNames) {
    for (String catalog : catalogNames) {
      try {
        CatalogProvider provider = session.catalog(Option.apply(catalog));
        if (provider instanceof TableCatalog) {
          return (MetacatCatalog) provider;
        }
      } catch (Exception e) {
        // try the next name
      }
    }
    return null;
  }
}
