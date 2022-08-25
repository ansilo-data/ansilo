package com.ansilo.connectors.mapping;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import com.ansilo.connectors.data.DataType;

/**
 * SQLite jdbc type mappings.
 */
public class SqliteJdbcDataMapping extends JdbcDataMapping {

    @Override
    public int getJdbcType(int dataType) throws Exception {
        if (dataType == DataType.TYPE_UTF8_STRING) {
            return Types.VARCHAR;
        }

        return super.getJdbcType(dataType);
    }

    @Override
    public void bindUtf8String(PreparedStatement statement, int index, String data)
            throws Exception {
        statement.setString(index, data);
    }

    @Override
    public String getUtf8String(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }

    @Override
    public void bindJson(PreparedStatement statement, int index, String data) throws Exception {
        statement.setString(index, data);
    }

    @Override
    public String getJson(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }
}
