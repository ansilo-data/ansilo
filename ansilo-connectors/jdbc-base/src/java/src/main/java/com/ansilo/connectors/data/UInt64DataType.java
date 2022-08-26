package com.ansilo.connectors.data;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The uint64 data type
 */
public class UInt64DataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_UINT64;
    }

    @Override
    public int getFixedSize() {
        return 9;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getUInt64(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putLong(val.longValue());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindUInt64(statement, index,
                    new BigInteger(Long.toUnsignedString(buff.getLong())));
        }
    }
}
