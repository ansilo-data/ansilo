package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The bool data type
 */
public class BoolDataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_BOOLEAN;
    }

    @Override
    public int getFixedSize() {
        return 2;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        var val = resultSet.getBoolean(colIndex);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);

        buff.put(val ? (byte) 1 : (byte) 0);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.BOOLEAN);
        } else {
            boolean val = buff.get() != 0;
            statement.setBoolean(index, val);
        }
    }
}
