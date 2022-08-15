package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The int16 data type
 */
public class Int16DataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_SMALLINT;
    }

    @Override
    public int getFixedSize() {
        return 3;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        var val = resultSet.getShort(colIndex);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putShort(val);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;
        
        if (isNull) {
            statement.setNull(index, Types.SMALLINT);
        } else {
            statement.setShort(index, buff.getShort());
        }
    }
}
