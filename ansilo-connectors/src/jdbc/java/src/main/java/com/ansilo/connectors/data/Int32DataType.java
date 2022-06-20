package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The int32 data type
 */
public class Int32DataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_INTEGER;
    }

    @Override
    public int getFixedSize() {
        return 5;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        int val = resultSet.getInt(colIndex);
        buff.put(resultSet.wasNull() ? (byte) 0 : 1);

        // Note: we write the int directly to the byte buffer here
        // without worrying about endianess.
        // This is fine if we assume the reader of the buffer is on the same host.
        // In the current version this assumption hosts as postgres is run in the same container
        // In future versions perhaps we have to revise this assumption if we start supporting
        // running
        // postgres on another host.
        buff.putInt(val);
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, (Integer) value);
        }
    }
}
