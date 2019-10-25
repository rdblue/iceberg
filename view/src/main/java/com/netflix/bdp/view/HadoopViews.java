package com.netflix.bdp.view;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.HashMap;
import java.util.Map;

/**
 * The Views implementation Based on FileIO.
 */
@Slf4j
@Data
public class HadoopViews implements Views, Configurable {
    private Configuration conf;

    public HadoopViews(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Loads the View object from a FileSystem path location.
     *
     * @param location a path URI (e.g. hdfs:///warehouse/my_view/)
     * @return view implementation
     */
    @Override
    public View load(String location) {
        ViewOperations ops = newViewOps(location);
        if (ops.current() == null) {
            throw new NoSuchTableException("View does not exist at location: " + location);
        }

        return new BaseView(ops, location);
    }

    /**
     * Loads the view definition from a FileSystem path location
     */
    @Override
    public ViewDefinition loadDefinition(String location) {
        ViewOperations ops = newViewOps(location);
        if (ops.current() == null) {
            throw new NoSuchTableException("View does not exist at location: " + location);
        }
        return ops.current().definition();
    }

    /**
     * Create a view using the FileSystem implementation, resolved from
     * location.
     *
     * @param location     a path URI (e.g. hdfs:///warehouse/my_view)
     * @param viewDefinition contains the view schema, sql, session catalog/namespace etc
     */
    @Override
    public void create(String location, ViewDefinition viewDefinition, Map<String, String> properties) {
        ViewOperations ops = newViewOps(location);
        if (ops.current() != null) {
            throw new AlreadyExistsException("View already exists at location: " + location);
        }

        int parentId = -1;

        Map<String, String> summaryProps = new HashMap<>();
        summaryProps.put(CommonViewConstants.OPERATION, DDLOperations.CREATE);
        summaryProps.put(CommonViewConstants.GENIE_ID, "N/A");
        summaryProps.put(CommonViewConstants.ENGINE_VERSION, "HadoopEngine");
        VersionSummary summary = new VersionSummary(summaryProps);

        Map<String, String> jsonProperties = new HashMap<>();
        BaseVersion version = new BaseVersion(1, parentId, System.currentTimeMillis(), summary, viewDefinition);

        ViewVersionMetadata metadata = ViewVersionMetadata.newViewVersionMetadata(version, location, viewDefinition,
                jsonProperties);
        ops.commit(ops.current(), metadata, properties);
    }

    @Override
    public void replace(String location, ViewDefinition definition, Map<String, String> properties) {
        ViewOperations ops = newViewOps(location);
        if (ops.current() == null) {
            throw new AlreadyExistsException("View is expected to exist at location: " + location);
        }

        ViewVersionMetadata prevViewVersionMetadata = ops.current();
        Preconditions.checkState(prevViewVersionMetadata.versions().size() > 0, "Version history not found");
        int parentId = prevViewVersionMetadata.currentVersionId();

        Map<String, String> summaryProps = new HashMap<>();
        summaryProps.put(CommonViewConstants.OPERATION, DDLOperations.REPLACE);
        summaryProps.put(CommonViewConstants.GENIE_ID, "N/A");
        summaryProps.put(CommonViewConstants.ENGINE_VERSION, "HadoopEngine");
        VersionSummary summary = new VersionSummary(summaryProps);

        Map<String, String> viewJsonProperties = new HashMap<>();

        BaseVersion version = new BaseVersion(parentId + 1, parentId, System.currentTimeMillis(),
                summary, definition);

        ViewVersionMetadata viewVersionMetadata = ViewVersionMetadata.newViewVersionMetadata(version, location,
                definition, prevViewVersionMetadata, viewJsonProperties);
        ops.commit(prevViewVersionMetadata, viewVersionMetadata, properties);
    }

    @Override
    public void drop(String location) {
        ViewOperations ops = newViewOps(location);
        ops.drop(location);
    }

    private ViewOperations newViewOps(String location) {
        return new HadoopViewOperations(new Path(location), conf);
    }


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}