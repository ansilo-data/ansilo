package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The uint32 data type
 */
public class UInt32DataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_UINT32;
    }

    @Override
    public int getFixedSize() {
        return 5;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getUInt32(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putInt((int) (long) val);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindUInt32(statement, index, Integer.toUnsignedLong((int) buff.getInt()));
        }
    }
}
