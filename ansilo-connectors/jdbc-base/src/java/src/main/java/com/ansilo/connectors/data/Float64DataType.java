package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The float64 data type
 */
public class Float64DataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_FLOAT64;
    }

    @Override
    public int getFixedSize() {
        return 9;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int index)
            throws Exception {
        var val = resultSet.getDouble(index);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putDouble(val);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;
        
        if (isNull) {
            statement.setNull(index, Types.DOUBLE);
        } else {
            statement.setDouble(index, buff.getDouble());
        }
    }
}
