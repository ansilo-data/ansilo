package com.ansilo.connectors.data;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * A fixed size data type.
 */
public interface FixedSizeDataType extends DataType {
    @Override
    default int getReadMode() {
        return DataType.MODE_FIXED;
    }

    /**
     * Gets the number of bytes of the data type
     */
    int getFixedSize();

    /**
     * Copies the value into the supplied byte buffer from the supplied result set.
     */
    void writeToByteBuffer(JdbcDataMapping mapping, ByteBuffer buff, ResultSet resultSet,
            int index) throws Exception;
}
