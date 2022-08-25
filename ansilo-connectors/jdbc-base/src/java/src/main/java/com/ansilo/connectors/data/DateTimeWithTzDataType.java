package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * The date/time with timezone data type
 */
public class DateTimeWithTzDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_DATE_TIME_WITH_TZ;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int index) throws Exception {
        var val = resultSet.getTimestamp(index);

        if (resultSet.wasNull()) {
            return null;
        }

        var buff = ByteBuffer.allocate(16);
        buff.order(ByteOrder.BIG_ENDIAN);

        var dt = val.toInstant().atZone(ZoneId.of("UTC"));
        buff.putInt(dt.getYear());
        buff.put((byte) dt.getMonthValue());
        buff.put((byte) dt.getDayOfMonth());
        buff.put((byte) dt.getHour());
        buff.put((byte) dt.getMinute());
        buff.put((byte) dt.getSecond());
        buff.putInt(dt.getNano());
        buff.put(StandardCharsets.UTF_8.encode("UTC"));

        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            var year = buff.getInt();
            var month = buff.get();
            var day = buff.get();
            var hour = buff.get();
            var minute = buff.get();
            var second = buff.get();
            var nano = buff.getInt();
            var tz = StandardCharsets.UTF_8.decode(buff).toString();
            statement.setTimestamp(index, Timestamp.from(ZonedDateTime
                    .of(year, month, day, hour, minute, second, nano, ZoneId.of(tz)).toInstant()));
        }
    }
}
