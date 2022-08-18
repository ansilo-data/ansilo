package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The int32 data type
 */
public class Int32DataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_INTEGER;
    }

    @Override
    public int getFixedSize() {
        return 5;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        int val = resultSet.getInt(colIndex);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putInt(val);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;
        
        if (isNull) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, buff.getInt());
        }
    }
}