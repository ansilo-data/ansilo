package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.ResultSet;

/**
 * A fixed size data type.
 */
public interface JdbcFixedSizeDataType extends JdbcDataType {
    @Override
    default int getReadMode() {
        return JdbcDataType.MODE_FIXED;
    }

    /**
     * Gets the number of bytes of the data type
     */
    int getFixedSize();

    /**
     * Copies the value into the supplied byte buffer from the supplied result set.
     */
    void writeToByteBuffer(ByteBuffer buff, ResultSet resultSet, int colIndex) throws Exception;
}
