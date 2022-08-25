package com.ansilo.connectors.mapping;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import com.ansilo.connectors.data.*;

/**
 * Maps ansilo data types to the driver-specific data types.
 * 
 * Default implementations of "sane" mappings are provided below but can be overridden for each
 * driver as required.
 */
public class JdbcDataMapping {
    /**
     * Gets the data type for the column on the supplied result set.
     * 
     * @throws Exception
     */
    public DataType getColumnDataType(ResultSet resultSet, int index) throws Exception {
        int jdbcType = resultSet.getMetaData().getColumnType(index);
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return new BoolDataType();

            case Types.TINYINT:
                return new Int8DataType();

            case Types.SMALLINT:
                return new Int16DataType();

            case Types.INTEGER:
                return new Int32DataType();

            case Types.BIGINT:
                return new Int64DataType();

            case Types.FLOAT:
            case Types.REAL:
                return new Float32DataType();

            case Types.DOUBLE:
                return new Float64DataType();

            case Types.DECIMAL:
            case Types.NUMERIC:
                return new DecimalDataType();

            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.ROWID:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.LONGNVARCHAR:
                return new Utf8StringDataType();

            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return new BinaryDataType();

            case Types.DATE:
                return new DateDataType();

            case Types.TIME:
                return new TimeDataType();

            case Types.TIMESTAMP:
                return new DateTimeDataType();

            case Types.TIMESTAMP_WITH_TIMEZONE:
                return new DateTimeWithTzDataType();

            case Types.JAVA_OBJECT:
            case Types.STRUCT:
            case Types.SQLXML:
            case Types.OTHER:
            case Types.ARRAY:
                return new JsonDataType();

            case Types.NULL:
                return new NullDataType();

            default:
                throw new SQLException(String.format("Unknown JDBC type: %d", jdbcType));
        }
    }

    /**
     * Maps the supplied ansilo data type to the relevant JDBC type.
     * 
     * This is currently used to bind null values.
     * 
     * @throws Exception
     */
    public int getJdbcType(int dataType) throws Exception {
        switch (dataType) {
            case DataType.TYPE_INT8:
                return Types.TINYINT;
            case DataType.TYPE_INT16:
                return Types.SMALLINT;
            case DataType.TYPE_INT32:
                return Types.INTEGER;
            case DataType.TYPE_INT64:
                return Types.BIGINT;
            case DataType.TYPE_FLOAT32:
                return Types.FLOAT;
            case DataType.TYPE_FLOAT64:
                return Types.DOUBLE;
            case DataType.TYPE_DECIMAL:
                return Types.DECIMAL;
            case DataType.TYPE_DATE:
                return Types.DATE;
            case DataType.TYPE_TIME:
                return Types.TIME;
            case DataType.TYPE_DATE_TIME:
                return Types.TIMESTAMP;
            case DataType.TYPE_BINARY:
                return Types.BINARY;
            case DataType.TYPE_NULL:
                return Types.NULL;
            case DataType.TYPE_BOOLEAN:
                return Types.BOOLEAN;
            case DataType.TYPE_UTF8_STRING:
                return Types.NVARCHAR;
            case DataType.TYPE_DATE_TIME_WITH_TZ:
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case DataType.TYPE_JSON:
                return Types.VARCHAR;

            default:
                throw new RuntimeException(String.format("Unknown data type id: %d", dataType));
        }
    }

    /**
     * Reads the value from the result set.
     */
    public InputStream getBinary(ResultSet resultSet, int index) throws Exception {
        return resultSet.getBinaryStream(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindBinary(PreparedStatement statement, int index, InputStream data)
            throws Exception {
        statement.setBinaryStream(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public boolean getBool(ResultSet resultSet, int index) throws Exception {
        return resultSet.getBoolean(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindBool(PreparedStatement statement, int index, boolean data) throws Exception {
        statement.setBoolean(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public LocalDate getDate(ResultSet resultSet, int index) throws Exception {
        return resultSet.getDate(index).toLocalDate();
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindDate(PreparedStatement statement, int index, LocalDate data) throws Exception {
        statement.setDate(index, java.sql.Date.valueOf(data));
    }

    /**
     * Reads the value from the result set.
     */
    public LocalTime getTime(ResultSet resultSet, int index) throws Exception {
        return resultSet.getTime(index).toLocalTime();
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindTime(PreparedStatement statement, int index, LocalTime data) throws Exception {
        statement.setTime(index, java.sql.Time.valueOf(data));
    }

    /**
     * Reads the value from the result set.
     */
    public LocalDateTime getDateTime(ResultSet resultSet, int index) throws Exception {
        return resultSet.getTimestamp(index).toLocalDateTime();
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindDateTime(PreparedStatement statement, int index, LocalDateTime data)
            throws Exception {
        statement.setTimestamp(index, java.sql.Timestamp.valueOf(data));
    }

    /**
     * Reads the value from the result set.
     */
    public ZonedDateTime getDateTimeWithTz(ResultSet resultSet, int index) throws Exception {
        return ZonedDateTime.ofInstant(resultSet.getTimestamp(index).toInstant(), ZoneId.of("UTC"));
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindDateTimeWithTz(PreparedStatement statement, int index, ZonedDateTime data)
            throws Exception {
        statement.setTimestamp(index, java.sql.Timestamp.from(data.toInstant()));
    }

    /**
     * Reads the value from the result set.
     */
    public String getUtf8String(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindUtf8String(PreparedStatement statement, int index, String data)
            throws Exception {
        statement.setString(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public String getJson(ResultSet resultSet, int index) throws Exception {
        return resultSet.getString(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindJson(PreparedStatement statement, int index, String data) throws Exception {
        statement.setString(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public float getFloat32(ResultSet resultSet, int index) throws Exception {
        return resultSet.getFloat(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindFloat32(PreparedStatement statement, int index, float data) throws Exception {
        statement.setFloat(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public double getFloat64(ResultSet resultSet, int index) throws Exception {
        return resultSet.getDouble(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindFloat64(PreparedStatement statement, int index, double data) throws Exception {
        statement.setDouble(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public byte getInt8(ResultSet resultSet, int index) throws Exception {
        return resultSet.getByte(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindInt8(PreparedStatement statement, int index, byte data) throws Exception {
        statement.setByte(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public short getInt16(ResultSet resultSet, int index) throws Exception {
        return resultSet.getShort(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindInt16(PreparedStatement statement, int index, short data) throws Exception {
        statement.setShort(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public int getInt32(ResultSet resultSet, int index) throws Exception {
        return resultSet.getInt(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindInt32(PreparedStatement statement, int index, int data) throws Exception {
        statement.setInt(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public long getInt64(ResultSet resultSet, int index) throws Exception {
        return resultSet.getLong(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindInt64(PreparedStatement statement, int index, long data) throws Exception {
        statement.setLong(index, data);
    }

    /**
     * Reads the value from the result set.
     */
    public BigDecimal getDecimal(ResultSet resultSet, int index) throws Exception {
        return resultSet.getBigDecimal(index);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindDecimal(PreparedStatement statement, int index, BigDecimal data)
            throws Exception {
        statement.setBigDecimal(index, data);
    }

    /**
     * Binds the value to the prepared statement.
     */
    public void bindNull(PreparedStatement statement, int index, int dataType) throws Exception {
        statement.setNull(index, this.getJdbcType(dataType));
    }
}
