package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The uint8 data type
 */
public class UInt8DataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_UINT8;
    }

    @Override
    public int getFixedSize() {
        return 2;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getUInt8(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.put((byte) (short) val);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindUInt8(statement, index, (short)Byte.toUnsignedInt((byte)buff.get()));
        }
    }
}
