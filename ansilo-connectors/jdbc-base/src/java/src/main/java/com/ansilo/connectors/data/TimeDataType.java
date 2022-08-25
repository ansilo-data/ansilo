package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalTime;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The time data type
 */
public class TimeDataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_TIME;
    }

    @Override
    public int getFixedSize() {
        return 8;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getTime(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.put((byte) val.getHour());
        buff.put((byte) val.getMinute());
        buff.put((byte) val.getSecond());
        buff.putInt(val.getNano());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            var hour = buff.get();
            var minute = buff.get();
            var second = buff.get();
            var nano = buff.getInt();
            mapping.bindTime(statement, index, LocalTime.of(hour, minute, second, nano));
        }
    }
}
