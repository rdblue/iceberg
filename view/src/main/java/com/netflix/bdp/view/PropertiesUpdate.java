package com.netflix.bdp.view;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.netflix.bdp.view.ViewProperties.*;

class PropertiesUpdate implements UpdateProperties {
    private final ViewOperations ops;
    private final Map<String, String> updates = Maps.newHashMap();
    private final Set<String> removals = Sets.newHashSet();
    private ViewVersionMetadata base;

    PropertiesUpdate(ViewOperations ops) {
        this.ops = ops;
        this.base = ops.current();
    }

    @Override
    public UpdateProperties set(String key, String value) {
        Preconditions.checkNotNull(key, "Key cannot be null");
        Preconditions.checkNotNull(key, "Value cannot be null");
        Preconditions.checkArgument(!removals.contains(key),
                "Cannot remove and update the same key: %s", key);

        updates.put(key, value);

        return this;
    }

    @Override
    public UpdateProperties remove(String key) {
        Preconditions.checkNotNull(key, "Key cannot be null");
        Preconditions.checkArgument(!updates.keySet().contains(key),
                "Cannot remove and update the same key: %s", key);

        removals.add(key);

        return this;
    }

    @Override
    public Map<String, String> apply() {
        this.base = ops.refresh();

        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : base.properties().entrySet()) {
            if (!removals.contains(entry.getKey())) {
                newProperties.put(entry.getKey(), entry.getValue());
            }
        }

        newProperties.putAll(updates);

        return newProperties;
    }

    @Override
    public void commit() {
        Tasks.foreach(ops)
                .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
                .exponentialBackoff(
                        base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                        base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                        base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                        2.0 /* exponential */)
                .onlyRetryOn(CommitFailedException.class)
                .run(taskOps -> {
                    Map<String, String> newProperties = apply();
                    ViewVersionMetadata updated = base.replaceProperties(newProperties);
                    taskOps.commit(base, updated, new HashMap<>());
                });
    }
}
