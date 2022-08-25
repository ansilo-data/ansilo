package com.ansilo.connectors.query;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import com.ansilo.connectors.data.DataType;

/**
 * A JDBC query paremeter.
 */
public class JdbcParameter {
    /**
     * The index of the query parameter
     */
    private int index;

    /**
     * The data type of the query parameter
     */
    private DataType dataType;

    /**
     * If the parameter has a constant value, then the byte buffer containing the value
     */
    private ByteBuffer constantValueBuffer;

    private JdbcParameter(int index, DataType dataType, ByteBuffer constantValueBuffer) {
        this.index = index;
        this.dataType = dataType;
        this.constantValueBuffer = constantValueBuffer;
    }

    /**
     * Creates a JDBC query parameter
     */
    public static JdbcParameter createDynamic(int index, DataType dataType) {
        return new JdbcParameter(index, dataType, null);
    }

    /**
     * Creates a JDBC query parameter
     * 
     * @throws SQLException
     */
    public static JdbcParameter createDynamic(int index, int dataTypeId) throws SQLException {
        return createDynamic(index, DataType.createFromTypeId(dataTypeId));
    }

    /**
     * Creates a JDBC query parameter with a constant value.
     */
    public static JdbcParameter createConstant(int index, DataType dataType,
            ByteBuffer buffer) {
        return new JdbcParameter(index, dataType, buffer);
    }

    /**
     * Creates a JDBC query parameter with a constant value.
     */
    public static JdbcParameter createConstant(int index, int dataTypeId, ByteBuffer buffer)
            throws SQLException {
        return createConstant(index, DataType.createFromTypeId(dataTypeId), buffer);
    }

    /**
     * Creates a JDBC query parameter with a constant value and creates a copy of the supplied byte
     * buffer.
     */
    public static JdbcParameter createConstantCopied(int index, DataType dataType,
            ByteBuffer buffer) {
        var copy = ByteBuffer.allocate(buffer.capacity());
        copy.put(buffer);
        copy.limit(buffer.limit());
        copy.rewind();
        return new JdbcParameter(index, dataType, copy);
    }

    /**
     * Creates a JDBC query parameter with a constant value.
     */
    public static JdbcParameter createConstantCopied(int index, int dataTypeId, ByteBuffer buffer)
            throws SQLException {
        return createConstantCopied(index, DataType.createFromTypeId(dataTypeId), buffer);
    }

    public int getIndex() {
        return index;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isConstant() {
        return constantValueBuffer != null;
    }

    public ByteBuffer getConstantValueBuffer() {
        return constantValueBuffer;
    }

}
