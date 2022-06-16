package com.ansilo.connectors.data;

import java.io.InputStream;
import java.sql.ResultSet;

/**
 * A stream data type.
 */
public interface JdbcStreamDataType extends JdbcDataType {
    @Override
    default int getReadMode() {
        return JdbcDataType.MODE_STREAM;
    }

    /**
     * Gets the input stream from the result set.
     */
    InputStream getStream(ResultSet resultSet, int colIndex) throws Exception;
}
