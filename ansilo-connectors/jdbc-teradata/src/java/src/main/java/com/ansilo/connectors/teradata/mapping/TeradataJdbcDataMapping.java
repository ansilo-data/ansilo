package com.ansilo.connectors.teradata.mapping;

import java.sql.*;
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;
import com.teradata.jdbc.jdbc_4.TDResultSet;

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

    @Override
    public String getUtf8String(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }

    @Override
    public void bindUtf8String(PreparedStatement statement, int index, String data)
            throws Exception {
        statement.setString(index, data);
    }
}
