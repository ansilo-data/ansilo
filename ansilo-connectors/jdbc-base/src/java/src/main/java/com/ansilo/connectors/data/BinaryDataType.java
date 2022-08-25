package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The binary data type
 */
public class BinaryDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_BINARY;
    }

    @Override
    public InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int index)
            throws Exception {
        return mapping.getBinary(resultSet, index);
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            var bytes = new byte[buff.remaining()];
            buff.get(bytes);
            mapping.bindBinary(statement, index, new ByteArrayInputStream(bytes));
        }
    }
}
