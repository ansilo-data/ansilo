package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The bool data type
 */
public class BoolDataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_BOOLEAN;
    }

    @Override
    public int getFixedSize() {
        return 2;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getBool(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.put(val ? (byte) 1 : (byte) 0);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            boolean data = buff.get() != 0;
            mapping.bindBool(statement, index, data);
        }
    }
}
