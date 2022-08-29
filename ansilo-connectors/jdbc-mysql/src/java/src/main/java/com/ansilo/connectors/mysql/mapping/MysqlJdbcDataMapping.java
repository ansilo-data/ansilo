package com.ansilo.connectors.mysql.mapping;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.data.*;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * Mysql JDBC data mapping
 */
public class MysqlJdbcDataMapping extends JdbcDataMapping {
    @Override
    public DataType getColumnDataType(ResultSet resultSet, int index) throws Exception {
        var typeName = resultSet.getMetaData().getColumnTypeName(index);

        switch (typeName.toUpperCase()) {
            case "TINYINT UNSIGNED":
                return new UInt8DataType();

            case "SMALLINT UNSIGNED":
                return new UInt16DataType();

            case "INT UNSIGNED":
                return new UInt32DataType();

            case "BIGINT UNSIGNED":
                return new UInt64DataType();

            case "TIMESTAMP":
                return new DateTimeWithTzDataType();

            case "JSON":
                return new JsonDataType();

            default:
                break;
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
