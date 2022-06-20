package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The nvarchar data type
 */
public class NVarcharDataType implements JdbcStreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_NVARCHAR;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int colIndex) throws Exception {
        var string = resultSet.getNString(colIndex);

        if (string == null) {
            return null;
        }

        return new ByteArrayInputStream(StandardCharsets.UTF_8.encode(string).array());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, Object value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.NVARCHAR);
        } else {
            statement.setNString(index, (String) value);
        }
    }
}
