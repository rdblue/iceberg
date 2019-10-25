package com.netflix.bdp.view;

import java.util.List;
import java.util.Map;

/**
 * Base {@link View} implementation.
 * <p>
 * This can be extended by providing a {@link ViewOperations} to the constructor.
 */
public class BaseView implements View, HasViewOperations {
    private final ViewOperations ops;
    private final String name;

    public BaseView(ViewOperations ops, String name) {
        this.ops = ops;
        this.name = name;
    }

    @Override
    public ViewOperations operations() {
        return ops;
    }

    @Override
    public Version currentVersion() {
        return ops.current().currentVersion();
    }

    @Override
    public Version version(int versionId) {
        return ops.current().version(versionId);
    }

    @Override
    public Iterable<Version> versions() {
        return ops.current().versions();
    }

    @Override
    public List<HistoryEntry> history() {
        return ops.current().history();
    }


    @Override
    public String toString() {
        return name;
    }

    @Override
    public UpdateProperties updateProperties() {
        return new PropertiesUpdate(ops);
    }

    @Override
    public Map<String, String> properties() {
        return ops.current().properties();
    }

}
