package com.netflix.bdp.view;

import org.apache.iceberg.Schema;

import java.util.List;

/**
 * SQL metadata for a view
 */
class BaseViewDefinition implements ViewDefinition {
    private final String sql;
    private final Schema schema;
    private final String sessionCatalog;
    private final List<String> sessionNamespace;

    public BaseViewDefinition(String sql, Schema schema,
                              String sessionCatalog, List<String> sessionNamespace) {
        this.sql = sql;
        this.schema = schema;
        this.sessionCatalog = sessionCatalog;
        this.sessionNamespace = sessionNamespace;
    }

    @Override
    public String sql() {
        return sql;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public String sessionCatalog() {
        return sessionCatalog;
    }

    @Override
    public List<String> sessionNamespace() {
        return sessionNamespace;
    }

}