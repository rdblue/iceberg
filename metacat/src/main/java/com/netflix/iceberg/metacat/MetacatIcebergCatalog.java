package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.KSGatewayListener;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.shaded.feign.Retryer;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

public class MetacatIcebergCatalog extends BaseMetastoreCatalog {

  private static boolean initialized = false;

  private static void initialize(String appName, String appId, Configuration conf) {
    if (!MetacatIcebergCatalog.initialized) {
      synchronized (MetacatIcebergCatalog.class) {
        if (!MetacatIcebergCatalog.initialized) {
          MetacatIcebergCatalog.initialized = true;
          KSGatewayListener.initialize(appName, appId, conf);
        }
      }
    }
  }

  private Configuration conf;
  private final String metacatHost;
  private final String jobId;
  private final String user;
  private final String appName;
  private final Client dbClient;

  public MetacatIcebergCatalog(Configuration conf, String appName) {
    this(conf, null, appName);
  }

  public MetacatIcebergCatalog(Configuration conf, String appId, String appName) {
    this.conf = conf;
    this.metacatHost = conf.get("netflix.metacat.host");
    this.jobId = conf.get("genie.job.id");
    this.user = Util.getUser();
    this.appName = appName;
    this.dbClient = newClient();

    MetacatIcebergCatalog.initialize(appName, appId, conf);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    return new MetacatClientOps(conf, newClient(), catalog, database, tableIdentifier.name());
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);

    DatabaseDto dbInfo = dbClient.getApi().getDatabase(catalog, database,
        false, /* omit user metadata */
        false /* omit table names */ );

    if (dbInfo.getUri() != null) {
      return dbInfo.getUri() + "/" + tableIdentifier.name();
    }

    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format("%s/%s.db/%s", warehouseLocation, database, tableIdentifier.name());
  }

  @Override
  public org.apache.iceberg.Table createTable(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
    Preconditions.checkArgument(isValidIdentifier(identifier),
        "Identifiers must be catalog.database.table: %s", identifier);
    return super.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public org.apache.iceberg.Table loadTable(TableIdentifier identifier) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Identifiers must be catalog.database.table: %s", identifier);
    }
    return super.loadTable(identifier);
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    if (!isValidIdentifier(tableIdentifier)) {
      throw new NoSuchTableException("Identifiers must be catalog.database.table: %s", tableIdentifier);
    }

    if (purge) {
      // TODO: ensure purge isn't set by default
      throw new UnsupportedOperationException("Metacat does not support purging data");
    }

    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    String tableName = tableIdentifier.name();

    newClient().getApi().deleteTable(catalog, database, tableName);
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Identifiers must be catalog.database.table: %s", from);
    }
    Preconditions.checkArgument(isValidIdentifier(to), "Identifiers must be catalog.database.table: %s", to);

    String fromCatalog = from.namespace().level(0);
    String toCatalog = to.namespace().level(0);
    Preconditions.checkArgument(fromCatalog.equals(toCatalog),
        "Cannot move table between catalogs: from=%s and to=%s", fromCatalog, toCatalog);

    String fromDatabase = from.namespace().level(1);
    String toDatabase = to.namespace().level(1);
    Preconditions.checkArgument(fromDatabase.equals(toDatabase),
        "Cannot move table between databases: from=%s and to=%s", fromDatabase, toDatabase);

    String fromTableName = from.name();
    String toTableName = to.name();
    if(fromTableName.equals(toTableName)) {
      return;
    }

    newClient().getApi().renameTable(fromCatalog, fromDatabase, fromTableName, toTableName);
  }

  private boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.hasNamespace() && tableIdentifier.namespace().levels().length == 2;
  }

  private Client newClient() {
    return Client.builder()
        .withClientAppName(appName)
        .withJobId(jobId)
        .withHost(metacatHost)
        .withUserName(user)
        .withDataTypeContext("hive")
        .withRetryer(new Retryer.Default())
        .build();
  }
}
