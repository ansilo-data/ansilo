package com.ansilo.connectors.teradata.mapping;

import java.sql.*;
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.data.DateTimeWithTzDataType;
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
        var type = resultSet.getMetaData().getColumnType(index);
        var typeName = resultSet.getMetaData().getColumnTypeName(index);

        if (type == Types.TIMESTAMP && typeName.toUpperCase().contains("TIME ZONE")) {
            return new DateTimeWithTzDataType();
        }

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
