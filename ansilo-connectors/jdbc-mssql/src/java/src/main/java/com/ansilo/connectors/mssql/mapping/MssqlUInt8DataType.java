package com.ansilo.connectors.mssql.mapping;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.data.FixedSizeDataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The int8 data type
 */
public class MssqlUInt8DataType implements FixedSizeDataType {
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
        var val = mapping.getInt8(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.put(val);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindInt8(statement, index, buff.get());
        }
    }
}
