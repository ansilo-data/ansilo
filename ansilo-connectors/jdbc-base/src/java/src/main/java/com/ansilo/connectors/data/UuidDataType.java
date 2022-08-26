package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The uuid data type
 */
public class UuidDataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_UUID;
    }

    @Override
    public int getFixedSize() {
        return 17;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getUuid(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        // Write UUID in big endian order
        buff.putLong(val.getMostSignificantBits());
        buff.putLong(val.getLeastSignificantBits());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            var msb = buff.getLong();
            var lsb = buff.getLong();
            var uuid = new UUID(msb, lsb);
            mapping.bindUuid(statement, index, uuid);
        }
    }
}
