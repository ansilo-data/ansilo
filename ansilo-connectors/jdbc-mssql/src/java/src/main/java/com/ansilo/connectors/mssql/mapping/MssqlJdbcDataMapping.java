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

        if (colType == Types.VARCHAR || colType == Types.CHAR) {
            return new MssqlStringDataType();
        }

        return super.getColumnDataType(resultSet, index);
    }
}
