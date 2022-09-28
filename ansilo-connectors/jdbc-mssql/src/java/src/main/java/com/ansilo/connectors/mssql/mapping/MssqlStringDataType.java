package com.ansilo.connectors.mssql.mapping;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.ansilo.connectors.data.StreamDataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;

public class MssqlStringDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_UTF8_STRING;
    }

    @Override
    public InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int index)
            throws Exception {
        String string = resultSet.getString(index);

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
            statement.setString(index, StandardCharsets.UTF_8.decode(buff).toString());
        }
    }
}
