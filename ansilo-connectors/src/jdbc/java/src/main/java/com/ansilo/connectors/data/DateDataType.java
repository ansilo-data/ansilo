package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * The date data type
 */
public class DateDataType implements JdbcFixedSizeDataType {
    @Override
    public int getTypeId() {
        return TYPE_DATE;
    }

    @Override
    public int getFixedSize() {
        return 7;
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex)
            throws Exception {
        var val = resultSet.getDate(colIndex);
        if (resultSet.wasNull()) {
            buff.put((byte) 0);
            return;
        }

        var date = val.toLocalDate();
        buff.put((byte) 1);
        buff.putInt(date.getYear());
        buff.put((byte) date.getMonthValue());
        buff.put((byte) date.getDayOfMonth());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.DATE);
        } else {
            var year = buff.getInt();
            var month = buff.get();
            var day = buff.get();
            statement.setDate(index, Date.valueOf(LocalDate.of(year, month, day)));
        }
    }
}
