package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.shaded.feign.Retryer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

import static com.google.common.base.Preconditions.checkArgument;

public class MetacatIcebergCatalog extends BaseMetastoreCatalog {

  private Configuration conf;
  private final String metacatHost;
  private final String jobid;
  private final String user;
  private final String appName;

  public MetacatIcebergCatalog(Configuration conf, String appName) {
    this.conf = conf;
    this.metacatHost = conf.get("netflix.metacat.host");
    this.jobid = conf.get("genie.job.id");
    this.user = System.getProperty("user.name");
    this.appName = appName;
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

    if (purge == true) {
      throw new UnsupportedOperationException("With Metacat we do not support purging data.");
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
    checkArgument(fromCatalog.equals(toCatalog), String.format("catalogs must be same, from=%s and to=%s",
        fromCatalog, toCatalog));

    String fromDatabase = from.namespace().level(1);
    String toDatabase = to.namespace().level(1);
    checkArgument(fromDatabase.equals(toDatabase), String.format("databases must be same, from=%s and to=%s",
        fromDatabase, toDatabase));

    String fromTableName = from.name();
    String toTableName = to.name();
    if(fromTableName.equals(toTableName)) {
      return;
    }

    newClient().getApi().renameTable(fromCatalog, fromDatabase, fromTableName, toTableName);
  }

  private static void validateTableIdentifier(TableIdentifier tableIdentifier) {
    checkArgument(tableIdentifier.hasNamespace(), "tableIdentifier should have namespace");
    checkArgument(tableIdentifier.namespace().levels().length == 2, "namespace should have catalog.schema");
  }

  private Client newClient() {
    return Client.builder()
        .withClientAppName(appName)
        .withJobId(jobid)
        .withHost(metacatHost)
        .withUserName(user)
        .withDataTypeContext("hive")
        .withRetryer(new Retryer.Default())
        .build();
  }
}
