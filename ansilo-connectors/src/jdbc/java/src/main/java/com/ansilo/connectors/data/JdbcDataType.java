package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * The interface of the JDBC data type.
 * 
 * Used for converting data from JDBC values to our rust connector.
 * 
 * @see ansilo-connectors/src/jdbc/data.rs
 * @see https://docs.oracle.com/cd/E19830-01/819-4721/beajw/index.html
 */
public interface JdbcDataType {
    public static final int MODE_FIXED = 1;
    public static final int MODE_STREAM = 2;

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
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff) throws SQLException;

    /**
     * Creates a new data type instance
     */
    public static JdbcDataType createFromJdbcType(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return new VarcharDataType();

            case Types.NVARCHAR:
            case Types.NCHAR:
                return new NVarcharDataType();

            case Types.INTEGER:
                return new Int32DataType();

            default:
                throw new SQLException(String.format("Unknown sql type: %d", sqlType));
        }
    }

    /**
     * Creates a new data type instance from the type id as defined by the TYPE_* constants above
     */
    public static JdbcDataType createFromTypeId(int dataTypeId) throws SQLException {
        switch (dataTypeId) {
            case TYPE_INTEGER:
                return new Int32DataType();

            case TYPE_VARCHAR:
                return new VarcharDataType();

            case TYPE_NVARCHAR:
                return new NVarcharDataType();

            default:
                throw new SQLException(String.format("Unknown data type id: %d", dataTypeId));
        }
    }
}
