package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.netflix.iceberg.KSGatewayListener;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.shaded.feign.Retryer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @deprecated from 0.6.0 use {@link MetacatIcebergCatalog}
 */

@Deprecated
public class MetacatTables implements Tables
{
    private static final Splitter DOT = Splitter.on('.').limit(3);

    private static boolean initialized = false;

    private final Configuration conf;
    private final String catalog;
    private final MetacatIcebergCatalog icebergCatalog;
    private final String metacatHost;
    private final String jobid;
    private final String user;
    private final String appName;
    private final Client dbClient;

    public MetacatTables(Configuration conf, String appName, String catalog) {
        this(conf, appName, null, catalog);
    }

    public MetacatTables(Configuration conf, String appName, String appId, String catalog) {
        this.conf = conf;
        this.metacatHost = conf.get("netflix.metacat.host");
        this.jobid = conf.get("genie.job.id");
        this.user = System.getProperty("user.name");
        // NETFLIX_APP may contain app type; could add as a default.
        this.appName = appName;
        this.catalog = catalog;
        this.dbClient = newClient();

        MetacatTables.initialize(appName, appId, conf);
        icebergCatalog = new MetacatIcebergCatalog(conf, appName);
    }

    private static void initialize(String appName, String appId, Configuration conf) {
        if (!MetacatTables.initialized) {
            synchronized (MetacatTables.class) {
                if (!MetacatTables.initialized) {
                    MetacatTables.initialized = true;
                    KSGatewayListener.initialize(appName, appId, conf);
                }
            }
        }
    }

    @Override
  public Table create(Schema schema, PartitionSpec spec, Map<String, String> properties, String tableIdentifier) {
        List<String> parts = DOT.splitToList(tableIdentifier);
        return icebergCatalog.createTable(toTableIdentifier(parts), schema, spec, properties);
    }

    @Override
    public Table load(String tableIdentifier) {
        List<String> parts = DOT.splitToList(tableIdentifier);
        return icebergCatalog.loadTable(toTableIdentifier(parts));
    }

    public boolean drop(String database, String table) {
        Preconditions.checkArgument(database != null && !"default".equals(database),
                "Cannot drop tables from the default database.");
        return icebergCatalog.dropTable(TableIdentifier.of(Namespace.of(catalog, database), table), false);
    }

    public BaseMetastoreTableOperations newTableOps(Configuration conf,
            String database, String table) {
        return new MetacatClientOps(conf, newClient(), catalog, database, table);
    }

    private TableIdentifier toTableIdentifier(List<String> parts)
    {
        switch (parts.size()) {
            case 3:
                return TableIdentifier.of(Namespace.of(parts.get(0), parts.get(1)), parts.get(2));
            case 2:
                return TableIdentifier.of(Namespace.of(catalog, parts.get(0)), parts.get(1));
            case 1:
                return TableIdentifier.of(catalog, "default", parts.get(1));
            default:
                throw new IllegalArgumentException("Metacat only allows up to 3 levels in naming: metacatcatalog.schema.table" +
                        " found =" + Arrays.toString(parts.toArray()));
        }
    }

    private Client newClient()
    {
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