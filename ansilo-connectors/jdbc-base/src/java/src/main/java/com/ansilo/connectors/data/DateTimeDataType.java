package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The date/time data type
 */
public class DateTimeDataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_DATE_TIME;
    }

    @Override
    public int getFixedSize() {
        return 14;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception {
        var val = mapping.getDateTime(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putInt(val.getYear());
        buff.put((byte) val.getMonthValue());
        buff.put((byte) val.getDayOfMonth());
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
            var year = buff.getInt();
            var month = buff.get();
            var day = buff.get();
            var hour = buff.get();
            var minute = buff.get();
            var second = buff.get();
            var nano = buff.getInt();
            mapping.bindDateTime(statement, index,
                    LocalDateTime.of(year, month, day, hour, minute, second, nano));
        }
    }
}
