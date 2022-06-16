package com.ansilo.connectors.result;

import java.sql.ResultSetMetaData;

/**
 * The JDBC row structure wrapper class
 */
public class JdbcRowStructure {
    /**
     * The inner JDBC result set metadata
     */
    private ResultSetMetaData metaData;

    /**
     * Initialises the row structure
     */
    public JdbcRowStructure(ResultSetMetaData metaData) {
        this.metaData = metaData;
    }
}
