package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The nvarchar data type
 */
public class Utf8StringDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_UTF8_STRING;
    }

    @Override
    public InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int index)
            throws Exception {
        String string = mapping.getUtf8String(resultSet, index);

        if (string == null) {
            return null;
        }

        var buff = StandardCharsets.UTF_8.encode(string);
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindUtf8String(statement, index,
                    StandardCharsets.UTF_8.decode(buff).toString());
        }
    }
}
