package com.ansilo.connectors.data;

import java.io.InputStream;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * A stream data type.
 */
public interface StreamDataType extends DataType {
    @Override
    default int getReadMode() {
        return DataType.MODE_STREAM;
    }

    /**
     * Gets the input stream from the result set.
     */
    InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int colIndex)
            throws Exception;
}
