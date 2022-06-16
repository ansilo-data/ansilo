package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;

/**
 * The varchar data type
 */
public class VarcharDataType implements JdbcStreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_VARCHAR;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int colIndex) throws Exception {
        var string = resultSet.getString(colIndex);

        if (string == null) {
            return null;
        }

        return new ByteArrayInputStream(StandardCharsets.UTF_8.encode(string).array());
    }
}
