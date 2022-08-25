package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The int8 data type
 */
public class Int8DataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_INT8;
    }

    @Override
    public int getFixedSize() {
        return 2;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int index)
            throws Exception {
        var val = resultSet.getByte(index);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.put(val);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;
        
        if (isNull) {
            statement.setNull(index, Types.TINYINT);
        } else {
            statement.setByte(index, buff.get());
        }
    }
}
