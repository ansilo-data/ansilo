package com.ansilo.connectors.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * The decimal data type
 */
public class DecimalDataType implements StreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_DECIMAL;
    }

    @Override
    public InputStream getStream(JdbcDataMapping mapping, ResultSet resultSet, int index)
            throws Exception {
        var val = mapping.getDecimal(resultSet, index);

        if (val == null) {
            return null;
        }

        var buff = StandardCharsets.UTF_8.encode(val.toPlainString());
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(JdbcDataMapping mapping, PreparedStatement statement, int index,
            ByteBuffer buff) throws Exception {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            mapping.bindNull(statement, index, this.getTypeId());
        } else {
            mapping.bindDecimal(statement, index,
                    new BigDecimal(StandardCharsets.UTF_8.decode(buff).toString()));
        }
    }
}
