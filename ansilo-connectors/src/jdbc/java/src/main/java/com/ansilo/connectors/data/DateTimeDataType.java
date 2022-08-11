package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;

/**
 * The date/time data type
 */
public class DateTimeDataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_TIMESTAMP;
    }

    @Override
    public int getFixedSize() {
        return 15;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        var val = resultSet.getTimestamp(colIndex);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        var dt = val.toLocalDateTime();
        buff.put((byte) 1);
        buff.putInt(dt.getYear());
        buff.put((byte) dt.getMonthValue());
        buff.put((byte) dt.getDayOfMonth());
        buff.put((byte) dt.getHour());
        buff.put((byte) dt.getMinute());
        buff.put((byte) dt.getSecond());
        buff.putInt(dt.getNano());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            var year = buff.getInt();
            var month = buff.get();
            var day = buff.get();
            var hour = buff.get();
            var minute = buff.get();
            var second = buff.get();
            var nano = buff.getInt();
            statement.setTimestamp(index, Timestamp
                    .valueOf(LocalDateTime.of(year, month, day, hour, minute, second, nano)));
        }
    }
}
