package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.netflix.iceberg.spark.source.SparkCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.v2.CaseInsensitiveStringMap;
import org.apache.spark.sql.catalyst.TableIdentifier;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetacatCatalog extends SparkCatalog
{
  private static final String CATALOG_OPTION = "catalog";

  private final Cache<String, Table> tableCache = CacheBuilder.newBuilder().softValues().build();
  private MetacatIcebergCatalog icebergCatalog = null;
  private String defaultDB = null;
  private String catalog = null;

  @Override
  protected Table create(TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> properties) {
    AtomicBoolean created = new AtomicBoolean(false);
    Table table;
    try {
      table = tableCache.get(key(ident), () -> {
        created.set(true);
        return icebergCatalog.createTable(toTableIdentifier(ident), schema, spec, properties);
      });
    } catch (UncheckedExecutionException | ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
      throw new RuntimeException("Failed to load table: " + key(ident), e.getCause());
    }

    if (!created.get()) {
      throw new AlreadyExistsException("Table already exists: %s.%s", database(ident), ident.table());
    }

    return table;
  }

  @Override
  protected Table load(TableIdentifier ident) {
    try {
      return tableCache.get(key(ident), () -> icebergCatalog.loadTable(toTableIdentifier(ident)));
    } catch (UncheckedExecutionException | ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
      throw new RuntimeException("Failed to load table: " + key(ident), e.getCause());
    }
  }

  public Table load(String db, String table) {
    String key = db + "." + table;
    try {
      return tableCache.get(key, () -> icebergCatalog.loadTable(toTableIdentifier(db, table)));
    } catch (UncheckedExecutionException | ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
      throw new RuntimeException(String.format("Failed to load table: %s.%s", db, table), e.getCause());
    }
  }

  @Override
  protected boolean drop(TableIdentifier ident) {
    Preconditions.checkArgument(ident.database().isDefined(),
        "Cannot drop: no database in identifier: %s", ident.unquotedString());
    boolean dropped = icebergCatalog.dropTable(toTableIdentifier(ident), false);
    tableCache.invalidate(key(ident));
    return dropped;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);

    SparkSession session = SparkSession.builder().getOrCreate();
    Configuration conf = new Configuration(session.sparkContext().hadoopConfiguration());

    String hadoopHiveEnv = session.sparkContext().hadoopConfiguration()
        .get("spark.sql.hive.env", "prod");
    String hiveEnv = session.sparkContext().conf()
        .get("spark.sql.hive.env", hadoopHiveEnv);
    String defaultCatalog = session.sparkContext().conf()
        .get("spark.sql.metacat.write.catalog", hiveEnv + "hive");
    this.catalog = options.get(CATALOG_OPTION);
    if (catalog == null) {
      catalog = defaultCatalog;
    }

    String metacatUri = options.get("metacat-uri");
    if (metacatUri != null) {
      conf.set("netflix.metacat.host", metacatUri);
    }

    this.icebergCatalog = new MetacatIcebergCatalog(conf, session.sparkContext().applicationId());
  }

  private org.apache.iceberg.catalog.TableIdentifier toTableIdentifier(TableIdentifier sparkIdentifier) {
    Namespace namespace = Namespace.of(catalog, database(sparkIdentifier));
    org.apache.iceberg.catalog.TableIdentifier identifier = org.apache.iceberg.catalog.TableIdentifier.of(namespace, sparkIdentifier.table());
    return identifier;
  }

  private org.apache.iceberg.catalog.TableIdentifier toTableIdentifier(String db, String tableName) {
    Namespace namespace = Namespace.of(catalog, db);
    org.apache.iceberg.catalog.TableIdentifier identifier = org.apache.iceberg.catalog.TableIdentifier.of(namespace, tableName);
    return identifier;
  }

  public String toString() {
    return "MetacatCatalog(name=" + name() + ", catalog=" + catalog + ")";
  }

  private String key(TableIdentifier ident) {
    return database(ident) + "." + ident.table();
  }

  private String database(TableIdentifier ident) {
    if (ident.database().isDefined()) {
      return ident.database().get();
    }
    return defaultDB;
  }
}
