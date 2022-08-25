package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The json data type
 */
public class JsonDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_JSON;
    }

    @Override
    public InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int index)
            throws Exception {
        var data = mapping.getJson(resultSet, index);

        if (data == null) {
            return null;
        }

        var buff = StandardCharsets.UTF_8.encode(data);
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            var data = StandardCharsets.UTF_8.decode(buff).toString();
            mapping.bindJson(statement, index, data);
        }
    }
}
