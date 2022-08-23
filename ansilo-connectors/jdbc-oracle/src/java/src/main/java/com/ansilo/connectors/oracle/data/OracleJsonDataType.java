package com.ansilo.connectors.oracle.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import com.ansilo.connectors.data.JdbcStreamDataType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * The json data type
 */
public class OracleJsonDataType implements JdbcStreamDataType {
    private static Gson gson = new GsonBuilder().serializeNulls().create();

    @Override
    public int getTypeId() {
        return TYPE_JSON;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int colIndex) throws Exception {
        String json = resultSet.getString(colIndex);

        if (json == null) {
            return null;
        }

        var buff = StandardCharsets.UTF_8.encode(json);
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            var json = StandardCharsets.UTF_8.decode(buff).toString();
            statement.setString(index, json);
        }
    }
}
