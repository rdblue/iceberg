
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import com.netflix.iceberg.metacat.MetacatIcebergCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.junit.Ignore;

public class ClientTest
{
    @Ignore
    public void testHadoopTables()
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:/");
        conf.set("fs.s3.impl", "com.facebook.presto.s3fs.PrestoS3FileSystem");
        conf.set("fs.s3n.impl", "com.facebook.presto.s3fs.PrestoS3FileSystem");
        conf.set("aws.iam.role.arn.default", "arn:aws:iam::219382154434:role/s3_all_with_vault");
        conf.set("presto.s3.use-instance-credentials", "false");
        conf.set("presto.s3.credentials-provider", "com.netflix.hadoop.aws.ConfigurableCredentialsProvider");
        conf.set("netflix.metacat.host", "http://metacat.dynprod.netflix.net:7001");

        Catalog catalog = new MetacatIcebergCatalog(conf, "rblue-test");
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("prodhive", "rblue"), "t");
        for (Record record : IcebergGenerics.read(catalog.loadTable(tableIdentifier))
                .reuseContainers()
                .build()) {
            System.out.println(record.toString());
        }
    }
}
