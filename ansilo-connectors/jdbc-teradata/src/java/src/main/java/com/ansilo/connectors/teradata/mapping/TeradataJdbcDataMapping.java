package com.ansilo.connectors.teradata.mapping;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.data.DateTimeWithTzDataType;
import com.ansilo.connectors.data.Float64DataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * Teradata JDBC data mapping
 */
public class TeradataJdbcDataMapping extends JdbcDataMapping {
    static {
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    };

    @Override
    public DataType getColumnDataType(ResultSet resultSet, int index) throws Exception {
        var type = resultSet.getMetaData().getColumnType(index);
        var typeName = resultSet.getMetaData().getColumnTypeName(index);

        if (type == Types.TIMESTAMP && typeName.toUpperCase().contains("TIME ZONE")) {
            return new DateTimeWithTzDataType();
        }

        if (type == Types.FLOAT) {
            return new Float64DataType();
        }

        return super.getColumnDataType(resultSet, index);
    }


    @Override
    public int getJdbcType(int dataType) throws Exception {
        if (dataType == DataType.TYPE_UTF8_STRING) {
            return Types.VARCHAR;
        }

        if (dataType == DataType.TYPE_DATE_TIME_WITH_TZ) {
            return Types.TIMESTAMP;
        }

        return super.getJdbcType(dataType);
    }


    // Teradata driver doesn't support NString
    @Override
    public String getUtf8String(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }

    @Override
    public void bindUtf8String(PreparedStatement statement, int index, String data)
            throws Exception {
        statement.setString(index, data);
    }

    @Override
    public String getJson(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }

    @Override
    public void bindJson(PreparedStatement statement, int index, String data) throws Exception {
        statement.setString(index, data);
    }

    @Override
    public ZonedDateTime getDateTimeWithTz(ResultSet resultSet, int index) throws Exception {
        // From teradata jdbc docs:
        // First, the TIMESTAMP WITH TIME ZONE value is converted to a TIMESTAMP value by truncating
        // the time zone field.
        // Separately, the time zone field is stored in the Calendar argument's TimeZone.
        // Then, the TIMESTAMP value is converted to a Timestamp object such that the TIMESTAMP
        // value matches what the Timestamp object's toString method would print.

        var cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        var data = resultSet.getTimestamp(index, cal);

        if (data == null) {
            return null;
        }

        return data.toLocalDateTime().atZone(cal.getTimeZone().toZoneId())
                .withZoneSameInstant(ZoneId.of("UTC"));
    }

    @Override
    public void bindDateTimeWithTz(PreparedStatement statement, int index, ZonedDateTime data)
            throws Exception {
        statement.setTimestamp(index,
                data == null ? null : java.sql.Timestamp.from(data.toInstant()),
                Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    }

    @Override
    public void bindBinary(PreparedStatement statement, int index, InputStream data)
            throws Exception {
        var bytes = data.readAllBytes();
        var length = bytes.length;
        var stream = new ByteArrayInputStream(bytes);

        statement.setBinaryStream(index, stream, length);
    }
}
