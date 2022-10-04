package com.ansilo.connectors.mssql.mapping;

import java.sql.ResultSet;
import java.sql.Types;
import com.ansilo.connectors.data.*;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * Mssql JDBC data mapping
 */
public class MssqlJdbcDataMapping extends JdbcDataMapping {
    @Override
    public DataType getColumnDataType(ResultSet resultSet, int index) throws Exception {
        var colType = resultSet.getMetaData().getColumnType(index);
        var typeName = resultSet.getMetaData().getColumnTypeName(index);

        if (colType == microsoft.sql.Types.GUID || typeName.equalsIgnoreCase("uniqueidentifier")) {
            return new UuidDataType();
        }

        if (colType == microsoft.sql.Types.DATETIME
                || colType == microsoft.sql.Types.SMALLDATETIME) {
            return new DateTimeDataType();
        }

        if (colType == microsoft.sql.Types.DATETIMEOFFSET) {
            return new DateTimeWithTzDataType();
        }

        if (colType == Types.TINYINT) {
            return new MssqlUInt8DataType();
        }

        if (colType == Types.VARCHAR || colType == Types.CHAR || colType == Types.LONGVARCHAR) {
            return new MssqlStringDataType();
        }

        return super.getColumnDataType(resultSet, index);
    }
}
