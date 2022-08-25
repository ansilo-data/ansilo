package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

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
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        buff.put((byte) 0);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        // Consume non-null byte
        byte nonNull = buff.get();
        assert nonNull == 0;

        mapping.bindNull(statement, index, this.getTypeId());
    }
}
