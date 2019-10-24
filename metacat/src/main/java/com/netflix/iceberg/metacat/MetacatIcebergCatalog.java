package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.KSGatewayListener;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.shaded.feign.Retryer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

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

  public MetacatIcebergCatalog(Configuration conf, String appName) {
    this(conf, null, appName);
  }

  public MetacatIcebergCatalog(Configuration conf, String appId, String appName) {
    this.conf = conf;
    this.metacatHost = conf.get("netflix.metacat.host");
    this.jobId = conf.get("genie.job.id");
    this.user = System.getProperty("user.name"); // TODO: BDC-42: use a better user name
    this.appName = appName;
    MetacatIcebergCatalog.initialize(appName, appId, conf);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);

    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);

    return new MetacatClientOps(conf, newClient(), catalog, database, tableIdentifier.name());
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);

    String database = tableIdentifier.namespace().level(1);

    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format("%s/%s.db/%s", warehouseLocation, database, tableIdentifier.name());
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    validateTableIdentifier(tableIdentifier);

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
    validateTableIdentifier(from);
    validateTableIdentifier(to);

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

  private static void validateTableIdentifier(TableIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier.hasNamespace(),
        "Table identifier should have namespace: %s", tableIdentifier);
    Preconditions.checkArgument(tableIdentifier.namespace().levels().length == 2,
        "Table identifier should be catalog and database: %s", tableIdentifier);
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
