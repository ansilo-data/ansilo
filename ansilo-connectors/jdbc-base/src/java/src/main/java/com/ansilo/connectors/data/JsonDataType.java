package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * The json data type
 */
public class JsonDataType implements JdbcStreamDataType {
    private static Gson gson = new GsonBuilder().serializeNulls().create();

    @Override
    public int getTypeId() {
        return TYPE_JSON;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int colIndex) throws Exception {
        Object obj = resultSet.getObject(colIndex);

        if (obj == null) {
            return null;
        }

        var json = gson.toJson(obj);
        var buff = StandardCharsets.UTF_8.encode(json);
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.JAVA_OBJECT);
        } else {
            var json = StandardCharsets.UTF_8.decode(buff).toString();
            var parsed = gson.fromJson(json, JsonElement.class);
            statement.setObject(index, parsed);
        }
    }
}
