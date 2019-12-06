package com.netflix.bdp.view;

import com.google.common.base.Objects;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public abstract class BaseMetastoreViewOperations implements ViewOperations {
    private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreViewOperations.class);

    public static final String METADATA_LOCATION_PROP = "metadata_location";
    public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";

    public static final String METADATA_FOLDER_NAME = "metadata";

    private ViewVersionMetadata currentMetadata = null;
    private String currentMetadataLocation = null;
    private boolean shouldRefresh = true;
    private int version = -1;

    @Override
    public ViewVersionMetadata current() {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }

    public String currentMetadataLocation() {
        return currentMetadataLocation;
    }

    public int currentVersion() {
        return version;
    }

    protected void requestRefresh() {
        this.shouldRefresh = true;
    }

    protected String writeNewMetadata(ViewVersionMetadata metadata, int version) {
        String newViewMetadataFilePath = newViewMetadataFilePath(metadata, version);
        OutputFile newMetadataLocation = io().newOutputFile(newViewMetadataFilePath);

        // write the new metadata
        // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
        // always unique because it includes a UUID.
        ViewVersionMetadataParser.write(metadata, newMetadataLocation);
        return newViewMetadataFilePath;
    }

    protected void refreshFromMetadataLocation(String newLocation, Predicate<Exception> shouldRetry,
                                               int numRetries) {
        // use null-safe equality check because new tables have a null metadata location
        if (!Objects.equal(currentMetadataLocation, newLocation)) {
            LOG.info("Refreshing table metadata from new version: {}", newLocation);

            AtomicReference<ViewVersionMetadata> newMetadata = new AtomicReference<>();
            Tasks.foreach(newLocation)
                    .retry(numRetries).exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
                    .throwFailureWhenFinished()
                    .shouldRetryTest(shouldRetry)
                    .run(metadataLocation -> newMetadata.set(
                            ViewVersionMetadataParser.read(io().newInputFile(metadataLocation))));
            this.currentMetadata = newMetadata.get();
            this.currentMetadataLocation = newLocation;
            this.version = parseVersion(newLocation);
        }
        this.shouldRefresh = false;
    }

    private String metadataFileLocation(ViewVersionMetadata metadata, String filename) {
        return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }

    private String newViewMetadataFilePath(ViewVersionMetadata meta, int newVersion) {
        return metadataFileLocation(meta,
                String.format("%05d-%s%s", newVersion, UUID.randomUUID(), ".metadata.json"));
    }

    private static int parseVersion(String metadataLocation) {
        int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
        int versionEnd = metadataLocation.indexOf('-', versionStart);
        try {
            return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
        } catch (NumberFormatException e) {
            LOG.warn("Unable to parse version from metadata location: " + metadataLocation);
            return -1;
        }
    }
}
