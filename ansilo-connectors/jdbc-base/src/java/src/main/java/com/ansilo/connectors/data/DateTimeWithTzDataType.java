package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The date/time with timezone data type
 */
public class DateTimeWithTzDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_DATE_TIME_WITH_TZ;
    }

    @Override
    public InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int index)
            throws Exception {
        var val = mapping.getDateTimeWithTz(resultSet, index);

        if (val == null) {
            return null;
        }

        var buff = ByteBuffer.allocate(16);
        buff.order(ByteOrder.BIG_ENDIAN);

        buff.putInt(val.getYear());
        buff.put((byte) val.getMonthValue());
        buff.put((byte) val.getDayOfMonth());
        buff.put((byte) val.getHour());
        buff.put((byte) val.getMinute());
        buff.put((byte) val.getSecond());
        buff.putInt(val.getNano());
        buff.put(StandardCharsets.UTF_8.encode("UTC"));

        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            var year = buff.getInt();
            var month = buff.get();
            var day = buff.get();
            var hour = buff.get();
            var minute = buff.get();
            var second = buff.get();
            var nano = buff.getInt();
            var tz = StandardCharsets.UTF_8.decode(buff).toString();
            mapping.bindDateTimeWithTz(statement, index,
                    ZonedDateTime.of(year, month, day, hour, minute, second, nano, ZoneId.of(tz)));
        }
    }
}
