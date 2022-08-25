package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;

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
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int index)
            throws Exception {
        var val = resultSet.getTime(index);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        var time = val.toLocalTime();
        buff.put((byte) 1);
        buff.put((byte) time.getHour());
        buff.put((byte) time.getMinute());
        buff.put((byte) time.getSecond());
        buff.putInt(time.getNano());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.TIME);
        } else {
            var hour = buff.get();
            var minute = buff.get();
            var second = buff.get();
            var nano = buff.getInt();
            statement.setTime(index, Time.valueOf(LocalTime.of(hour, minute, second, nano)));
        }
    }
}
