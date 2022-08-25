package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The date data type
 */
public class DateDataType implements FixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_DATE;
    }

    @Override
    public int getFixedSize() {
        return 7;
    }

    @Override
    public void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet, int index)
            throws Exception {
        var val = mapping.getDate(resultSet, index);

        if (val == null) {
            buff.put((byte) 0);
            return;
        }

        buff.put((byte) 1);
        buff.putInt(val.getYear());
        buff.put((byte) val.getMonthValue());
        buff.put((byte) val.getDayOfMonth());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index, ByteBuffer buff)
            throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            var year = buff.getInt();
            var month = buff.get();
            var day = buff.get();
            mapping.bindDate(statement, index, LocalDate.of(year, month, day));
        }
    }
}
