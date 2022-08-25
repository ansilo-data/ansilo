package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The null data type
 */
public class NullDataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_NULL;
    }

    @Override
    public int getFixedSize() {
        return 1;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int index)
            throws Exception {
        buff.put((byte) 0);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        // Consume non-null byte
        byte nonNull = buff.get();
        assert nonNull == 0;

        statement.setNull(index, Types.NULL);
    }
}
