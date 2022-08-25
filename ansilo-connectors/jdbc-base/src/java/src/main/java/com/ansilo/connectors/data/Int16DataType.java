package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The int16 data type
 */
public class Int16DataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_INT16;
    }

    @Override
    public int getFixedSize() {
        return 3;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getInt16(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putShort(val);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindInt16(statement, index, buff.getShort());
        }
    }
}
