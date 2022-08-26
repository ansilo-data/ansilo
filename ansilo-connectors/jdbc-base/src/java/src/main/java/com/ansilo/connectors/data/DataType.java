package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The interface of the JDBC data type.
 * 
 * Used for converting data from JDBC values to our rust connector.
 * 
 * @see ansilo-connectors/src/jdbc/data.rs
 */
public interface DataType {
    public static final int MODE_FIXED = 1;
    public static final int MODE_STREAM = 2;

    /**
     * If you update these constants make sure to update the rust constants:
     * 
     * @see ansilo-connectors/jdbc-base/src/data.rs
     */
    public static final int TYPE_INT8 = 1;
    public static final int TYPE_UINT8 = 2;
    public static final int TYPE_INT16 = 3;
    public static final int TYPE_UINT16 = 4;
    public static final int TYPE_INT32 = 5;
    public static final int TYPE_UINT32 = 6;
    public static final int TYPE_INT64 = 7;
    public static final int TYPE_UINT64 = 8;
    public static final int TYPE_FLOAT32 = 9;
    public static final int TYPE_FLOAT64 = 10;
    public static final int TYPE_DECIMAL = 11;
    public static final int TYPE_DATE = 12;
    public static final int TYPE_TIME = 13;
    public static final int TYPE_DATE_TIME = 14;
    public static final int TYPE_DATE_TIME_WITH_TZ = 15;
    public static final int TYPE_BINARY = 16;
    public static final int TYPE_NULL = 17;
    public static final int TYPE_BOOLEAN = 18;
    public static final int TYPE_UTF8_STRING = 19;
    public static final int TYPE_JSON = 20;
    public static final int TYPE_UUID = 21;

    /**
     * Gets the read mode of the data type
     */
    public int getReadMode();

    /**
     * Get's a unique ID of the data type.
     * 
     * This is used to map the data type between java <-> rust implementations
     */
    public int getTypeId();

    /**
     * Binds the supplied value to the prepared statement.
     */
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception;

    /**
     * Creates a new data type instance from the type id as defined by the TYPE_* constants above
     */
    public static DataType createFromTypeId(int dataTypeId) throws SQLException {
        switch (dataTypeId) {
            case TYPE_BOOLEAN:
                return new BoolDataType();

            case TYPE_INT8:
                return new Int8DataType();

            case TYPE_INT16:
                return new Int16DataType();

            case TYPE_INT32:
                return new Int32DataType();

            case TYPE_INT64:
                return new Int64DataType();

            case TYPE_FLOAT32:
                return new Float32DataType();

            case TYPE_FLOAT64:
                return new Float64DataType();

            case TYPE_DECIMAL:
                return new DecimalDataType();

            case TYPE_UTF8_STRING:
                return new Utf8StringDataType();

            case TYPE_BINARY:
                return new BinaryDataType();

            case TYPE_DATE:
                return new DateDataType();

            case TYPE_TIME:
                return new TimeDataType();

            case TYPE_DATE_TIME:
                return new DateTimeDataType();

            case TYPE_DATE_TIME_WITH_TZ:
                return new DateTimeWithTzDataType();

            case TYPE_JSON:
                return new JsonDataType();

            case TYPE_NULL:
                return new NullDataType();

            default:
                throw new SQLException(String.format("Unknown data type id: %d", dataTypeId));
        }
    }


    /**
     * Creates a new data type instance from the type id as defined by the TYPE_* constants above
     */
    public static String typeName(int dataTypeId) {
        switch (dataTypeId) {
            case TYPE_INT8:
                return "INT8";
            case TYPE_INT16:
                return "INT16";
            case TYPE_INT32:
                return "INT32";
            case TYPE_INT64:
                return "INT64";
            case TYPE_FLOAT32:
                return "FLOAT32";
            case TYPE_FLOAT64:
                return "FLOAT64";
            case TYPE_DECIMAL:
                return "DECIMAL";
            case TYPE_DATE:
                return "DATE";
            case TYPE_TIME:
                return "TIME";
            case TYPE_DATE_TIME:
                return "DATE_TIME";
            case TYPE_BINARY:
                return "BINARY";
            case TYPE_NULL:
                return "NULL";
            case TYPE_BOOLEAN:
                return "BOOLEAN";
            case TYPE_UTF8_STRING:
                return "UTF8_STRING";
            case TYPE_DATE_TIME_WITH_TZ:
                return "DATE_TIME_WITH_TZ";
            case TYPE_JSON:
                return "JSON";

            default:
                throw new RuntimeException(String.format("Unknown data type id: %d", dataTypeId));
        }
    }
}
