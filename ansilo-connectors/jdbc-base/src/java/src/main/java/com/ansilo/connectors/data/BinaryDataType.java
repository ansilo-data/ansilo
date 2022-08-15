package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The binary data type
 */
public class BinaryDataType implements JdbcStreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_BINARY;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int colIndex) throws Exception {
        var data = resultSet.getBinaryStream(colIndex);

        if (resultSet.wasNull()) {
            return null;
        }

        return data;
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.BINARY);
        } else {
            var bytes = new byte[buff.remaining()];
            buff.get(bytes);
            statement.setBinaryStream(index, new ByteArrayInputStream(bytes));
        }
    }
}
