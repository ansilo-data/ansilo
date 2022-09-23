package com.ansilo.connectors.teradata.mapping;

import java.sql.*;
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * Teradata JDBC data mapping
 */
public class TeradataJdbcDataMapping extends JdbcDataMapping {
    static {
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    };

    @Override
    public DataType getColumnDataType(ResultSet resultSet, int index) throws Exception {
        return super.getColumnDataType(resultSet, index);
    }
}
