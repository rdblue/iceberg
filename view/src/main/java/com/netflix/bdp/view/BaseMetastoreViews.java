package com.netflix.bdp.view;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
public abstract class BaseMetastoreViews implements Views {
    private final Configuration conf;

    public BaseMetastoreViews(Configuration conf) {
        this.conf = conf;
    }

    protected abstract BaseMetastoreViewOperations newViewOps(TableIdentifier viewName);

    protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
        throw new UnsupportedOperationException("Implementation for 'defaultWarehouseLocation' not provided.");
    }

    @Override
    public void create(String viewIdentifier, ViewDefinition definition, Map<String, String> properties) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() != null) {
            throw new AlreadyExistsException("View already exists: " + viewName);
        }

        String location = defaultWarehouseLocation(viewName);
        int parentId = -1;

        Map<String, String> summaryProps = getSummaryProperties(DDLOperations.CREATE, properties);
        VersionSummary summary = new VersionSummary(summaryProps);

        // Remove the version summary properties from table properties
        Map<String, String> metacatProperties = getMetacataProperties(properties, summaryProps);

        Map<String, String> viewJsonProperties = new HashMap<>();
        BaseVersion version = new BaseVersion(1, parentId, System.currentTimeMillis(), summary, definition);

        ViewVersionMetadata viewVersionMetadata = ViewVersionMetadata.newViewVersionMetadata(version, location,
                definition, viewJsonProperties);

        ops.commit(null, viewVersionMetadata, metacatProperties);
    }

    @Override
    public void replace(String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() == null) {
            throw new AlreadyExistsException("View " + viewName.toString() + " is expected to exist");
        }

        ViewVersionMetadata prevViewVersionMetadata = ops.current();
        Preconditions.checkState(prevViewVersionMetadata.versions().size() > 0, "Version history not found");
        int parentId = prevViewVersionMetadata.currentVersionId();

        Map<String, String> summaryProps = getSummaryProperties(DDLOperations.REPLACE, properties);
        VersionSummary summary = new VersionSummary(summaryProps);

        // Remove the version summary properties from table properties
        Map<String, String> metacatProperties = getMetacataProperties(properties, summaryProps);

        Map<String, String> viewJsonProperties = new HashMap<>();

        BaseVersion version = new BaseVersion(parentId + 1, parentId, System.currentTimeMillis(),
                summary, viewDefinition);

        String location = prevViewVersionMetadata.location();
        ViewVersionMetadata viewVersionMetadata = ViewVersionMetadata.newViewVersionMetadata(version, location,
                viewDefinition, prevViewVersionMetadata, viewJsonProperties);
        ops.commit(prevViewVersionMetadata, viewVersionMetadata, metacatProperties);
    }

    @Override
    public void drop(String viewIdentifier) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        ops.drop(viewName.toString());
    }

    @Override
    public View load(String viewIdentifier) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() == null) {
            throw new NoSuchTableException("View does not exist: " + viewName);
        }
        return new BaseView(ops, viewName.toString());
    }

    @Override
    public ViewDefinition loadDefinition(String viewIdentifier) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() == null) {
            throw new NoSuchTableException("View does not exist: " + viewName);
        }
        return ops.current().definition();
    }

    private TableIdentifier toCatalogTableIdentifier(String tableIdentifier) {
        List<String> namespace = Lists.newArrayList();
        Iterable<String> parts = Splitter.on(".").split(tableIdentifier);

        String lastPart = "";
        for (String part : parts) {
            if (!lastPart.isEmpty()) {
                namespace.add(lastPart);
            }
            lastPart = part;
        }

        Preconditions.checkState(namespace.size() == 2, "Catalog and schema are expected in the namespace.");

        return TableIdentifier.of(Namespace.of(namespace.toArray(new String[namespace.size()])), lastPart);
    }

    private Map<String, String> getSummaryProperties(String operation, Map<String, String> properties) {
        HashMap<String, String> summaryProperties = new HashMap<>();
        String genieId = properties.get(CommonViewConstants.GENIE_ID);
        if (genieId == null) {
            summaryProperties.put(CommonViewConstants.GENIE_ID, "N/A");
        } else {
            summaryProperties.put(CommonViewConstants.GENIE_ID, genieId);
        }

        String engineVersion = properties.get(CommonViewConstants.ENGINE_VERSION);
        if (engineVersion == null) {
            summaryProperties.put(CommonViewConstants.ENGINE_VERSION, "N/A");
        } else {
            summaryProperties.put(CommonViewConstants.ENGINE_VERSION, engineVersion);
        }
        summaryProperties.put(CommonViewConstants.OPERATION, operation);
        return summaryProperties;
    }

    private Map<String, String> getMetacataProperties(Map<String, String> properties, Map<String, String> summaryProperties) {
        HashMap<String, String> metacatProperties = new HashMap<>(properties);
        metacatProperties.keySet().removeAll(summaryProperties.keySet());
        return metacatProperties;
    }
}
