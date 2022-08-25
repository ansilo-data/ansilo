package com.ansilo.connectors.result;

import com.ansilo.connectors.data.DataType;

/**
 * Represents the metadata of a column in a JDBC result set.
 */
public class JdbcRowColumnInfo {
    /**
     * The name of the column
     */
    private String name;

    /**
     * The data type of the column
     */
    private DataType dataType;

    public JdbcRowColumnInfo(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getDataTypeId() {
        return dataType.getTypeId();
    }
}
