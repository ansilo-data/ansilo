package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

/**
 * The interface of the JDBC data type.
 * 
 * Used for converting data from JDBC values to our rust connector.
 * 
 * @see ansilo-connectors/src/jdbc/data.rs
 */
public interface JdbcDataType {
    public static final int MODE_FIXED = 1;
    public static final int MODE_STREAM = 2;

    /**
     * If you update these constants make sure to update the rust constants:
     * 
     * @see ansilo-connectors/src/jdbc/data.rs
     */
    public static final int TYPE_BIT = 1;
    public static final int TYPE_TINYINT = 2;
    public static final int TYPE_SMALLINT = 3;
    public static final int TYPE_INTEGER = 4;
    public static final int TYPE_BIGINT = 5;
    public static final int TYPE_FLOAT = 6;
    public static final int TYPE_REAL = 7;
    public static final int TYPE_DOUBLE = 8;
    public static final int TYPE_NUMERIC = 9;
    public static final int TYPE_DECIMAL = 10;
    public static final int TYPE_CHAR = 11;
    public static final int TYPE_VARCHAR = 12;
    public static final int TYPE_LONGVARCHAR = 32;
    public static final int TYPE_DATE = 13;
    public static final int TYPE_TIME = 14;
    public static final int TYPE_TIMESTAMP = 15;
    public static final int TYPE_BINARY = 16;
    public static final int TYPE_NULL = 17;
    public static final int TYPE_JAVA_OBJECT = 18;
    public static final int TYPE_DISTINCT = 19;
    public static final int TYPE_STRUCT = 20;
    public static final int TYPE_ARRAY = 21;
    public static final int TYPE_BLOB = 22;
    public static final int TYPE_CLOB = 23;
    public static final int TYPE_BOOLEAN = 24;
    public static final int TYPE_NCHAR = 25;
    public static final int TYPE_NVARCHAR = 26;
    public static final int TYPE_LONGNVARCHAR = 27;
    public static final int TYPE_NCLOB = 28;
    public static final int TYPE_SQLXML = 29;
    public static final int TYPE_TIME_WITH_TIMEZONE = 30;
    public static final int TYPE_TIMESTAMP_WITH_TIMEZONE = 31;
    public static final int TYPE_JSON = 33;

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
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException;

    /**
     * Creates a new data type instance
     */
    public static JdbcDataType createFromJdbcType(int sqlType) throws SQLException {
        switch (sqlType) {
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
                return new VarcharDataType();

            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.LONGNVARCHAR:
                return new NVarcharDataType();

            case Types.BINARY:
            case Types.BLOB:
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
                return new JsonDataType();

            case Types.NULL:
                return new NullDataType();

            default:
                throw new SQLException(String.format("Unknown sql type: %d", sqlType));
        }
    }

    /**
     * Creates a new data type instance from the type id as defined by the TYPE_* constants above
     */
    public static JdbcDataType createFromTypeId(int dataTypeId) throws SQLException {
        switch (dataTypeId) {
            case TYPE_BIT:
            case TYPE_BOOLEAN:
                return new BoolDataType();

            case TYPE_TINYINT:
                return new Int8DataType();

            case TYPE_SMALLINT:
                return new Int16DataType();

            case TYPE_INTEGER:
                return new Int32DataType();

            case TYPE_BIGINT:
                return new Int64DataType();

            case TYPE_FLOAT:
            case TYPE_REAL:
                return new Float32DataType();

            case TYPE_DOUBLE:
                return new Float64DataType();

            case TYPE_DECIMAL:
            case TYPE_NUMERIC:
                return new DecimalDataType();

            case TYPE_CHAR:
            case TYPE_CLOB:
            case TYPE_VARCHAR:
            case TYPE_LONGVARCHAR:
                return new VarcharDataType();

            case TYPE_NVARCHAR:
            case TYPE_NCHAR:
            case TYPE_NCLOB:
            case TYPE_LONGNVARCHAR:
                return new NVarcharDataType();

            case TYPE_BINARY:
            case TYPE_BLOB:
                return new BinaryDataType();

            case TYPE_DATE:
                return new DateDataType();

            case TYPE_TIME:
                return new TimeDataType();

            case TYPE_TIMESTAMP:
                return new DateTimeDataType();

            case TYPE_TIMESTAMP_WITH_TIMEZONE:
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
            case TYPE_BIT:
                return "BIT";
            case TYPE_TINYINT:
                return "TINYINT";
            case TYPE_SMALLINT:
                return "SMALLINT";
            case TYPE_INTEGER:
                return "INTEGER";
            case TYPE_BIGINT:
                return "BIGINT";
            case TYPE_FLOAT:
                return "FLOAT";
            case TYPE_REAL:
                return "REAL";
            case TYPE_DOUBLE:
                return "DOUBLE";
            case TYPE_NUMERIC:
                return "NUMERIC";
            case TYPE_DECIMAL:
                return "DECIMAL";
            case TYPE_CHAR:
                return "CHAR";
            case TYPE_VARCHAR:
                return "VARCHAR";
            case TYPE_LONGVARCHAR:
                return "LONGVARCHAR";
            case TYPE_DATE:
                return "DATE";
            case TYPE_TIME:
                return "TIME";
            case TYPE_TIMESTAMP:
                return "TIMESTAMP";
            case TYPE_BINARY:
                return "BINARY";
            case TYPE_NULL:
                return "NULL";
            case TYPE_JAVA_OBJECT:
                return "JAVA_OBJECT";
            case TYPE_DISTINCT:
                return "DISTINCT";
            case TYPE_STRUCT:
                return "STRUCT";
            case TYPE_ARRAY:
                return "ARRAY";
            case TYPE_BLOB:
                return "BLOB";
            case TYPE_CLOB:
                return "CLOB";
            case TYPE_BOOLEAN:
                return "BOOLEAN";
            case TYPE_NCHAR:
                return "NCHAR";
            case TYPE_NVARCHAR:
                return "NVARCHAR";
            case TYPE_LONGNVARCHAR:
                return "LONGNVARCHAR";
            case TYPE_NCLOB:
                return "NCLOB";
            case TYPE_SQLXML:
                return "SQLXML";
            case TYPE_TIME_WITH_TIMEZONE:
                return "TIME_WITH_TIMEZONE";
            case TYPE_TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP_WITH_TIMEZONE";
            case TYPE_JSON:
                return "JSON";

            default:
                throw new RuntimeException(String.format("Unknown data type id: %d", dataTypeId));
        }
    }
}
