package com.netflix.bdp.view;

import org.apache.iceberg.io.FileIO;

import java.util.Map;

/**
 * SPI interface to abstract view metadata access and updates.
 */
public interface ViewOperations {

    /**
     * Return the currently loaded view metadata, without checking for updates.
     *
     * @return view version metadata
     */
    ViewVersionMetadata current();

    /**
     * Return the current view metadata after checking for updates.
     *
     * @return view version metadata
     */
    ViewVersionMetadata refresh();

    /**
     * Drops the view specified by the 'viewIdentifier' parameter.
     * Used only by the test infrastructure such as HadoopViewOperations and TestViews.
     * Metacat views are dropped using direct hive 'drop' object calls.
     * @param viewIdentifier specifies the name/location of the view.
     */
    void drop(String viewIdentifier);

    /**
     * Replace the base metadata with a new version.
     * <p>
     * This method should implement and document atomicity guarantees.
     * <p>
     * Implementations must check that the base metadata is current to avoid overwriting updates.
     * Once the atomic commit operation succeeds, implementations must not perform any operations that
     * may fail because failure in this method cannot be distinguished from commit failure.
     *
     * @param base       view metadata on which changes were based
     * @param metadata   new view metadata with updates
     * @param properties Version property genie-id of the operation,
     *                   as well as table properties such as owner, table type,
     *                   common view flag etc.
     */
    void commit(ViewVersionMetadata base, ViewVersionMetadata metadata, Map<String, String> properties);

    /**
     * @return a {@link FileIO} to read and write table data and metadata files
     */
    FileIO io();
}
