package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The int64 data type
 */
public class Int64DataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_BIGINT;
    }

    @Override
    public int getFixedSize() {
        return 9;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        var val = resultSet.getLong(colIndex);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putLong(val);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;
        
        if (isNull) {
            statement.setNull(index, Types.BIGINT);
        } else {
            statement.setLong(index, buff.getLong());
        }
    }
}
