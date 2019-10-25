package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.netflix.bdp.view.BaseMetastoreViewOperations;
import com.netflix.bdp.view.BaseMetastoreViews;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.shaded.feign.Retryer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;

import static com.google.common.base.Preconditions.checkArgument;

public class MetacatViewCatalog extends BaseMetastoreViews {

    private Configuration conf;
    private final String metacatHost;
    private final String jobid;
    private final String user;
    private final String appName;
    private final Client dbClient;

    public MetacatViewCatalog(Configuration conf, String appName) {
        super(conf);
        this.conf = conf;
        this.metacatHost = conf.get("netflix.metacat.host");
        this.jobid = conf.get("genie.job.id");
        this.user = System.getProperty("user.name");
        this.appName = appName;
        this.dbClient = newClient();
    }


    protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
        validateTableIdentifier(viewIdentifier);

        String catalog = viewIdentifier.namespace().level(0);
        String database = viewIdentifier.namespace().level(1);

        DatabaseDto dbInfo = dbClient.getApi().getDatabase(catalog, database,
                false, /* omit user metadata */
                false /* omit table names */ );

        if (dbInfo.getUri() != null) {
            return dbInfo.getUri() + "/" + viewIdentifier.name();
        }


        String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
        Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
        return String.format("%s/%s.db/%s", warehouseLocation, database, viewIdentifier.name());
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

    protected BaseMetastoreViewOperations newViewOps(TableIdentifier viewIdentifier) {
        validateTableIdentifier(viewIdentifier);

        String catalog = viewIdentifier.namespace().level(0);
        String database = viewIdentifier.namespace().level(1);

        return new MetacatViewClientOps(conf, newClient(), catalog, database, viewIdentifier.name());
    }

    private static void validateTableIdentifier(TableIdentifier viewIdentifier) {
        checkArgument(viewIdentifier.hasNamespace(), "viewIdentifier should have namespace");
        checkArgument(viewIdentifier.namespace().levels().length == 2, "namespace should have catalog.schema");
    }
}
