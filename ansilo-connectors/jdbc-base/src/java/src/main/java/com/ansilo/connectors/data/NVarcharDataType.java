package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
        String string = resultSet.getNString(colIndex);

        if (string == null) {
            return null;
        }

        var buff = StandardCharsets.UTF_8.encode(string);
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.NVARCHAR);
        } else {
            statement.setNString(index, StandardCharsets.UTF_8.decode(buff).toString());
        }
    }
}
