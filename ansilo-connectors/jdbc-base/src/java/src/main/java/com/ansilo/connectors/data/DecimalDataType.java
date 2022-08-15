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

/**
 * The decimal data type
 */
public class DecimalDataType implements JdbcStreamDataType {
    @Override
    public int getTypeId() {
        return TYPE_DECIMAL;
    }

    @Override
    public InputStream getStream(ResultSet resultSet, int colIndex) throws Exception {
        var val = resultSet.getBigDecimal(colIndex);

        if (val == null) {
            return null;
        }

        var buff = StandardCharsets.UTF_8.encode(val.toPlainString());
        return new ByteArrayInputStream(buff.array(), 0, buff.limit());
    }

    @Override
    public void bindParam(PreparedStatement statement, int index, ByteBuffer buff)
            throws SQLException {
        boolean isNull = buff.get() == 0;

        if (isNull) {
            statement.setNull(index, Types.DECIMAL);
        } else {
            statement.setBigDecimal(index,
                    new BigDecimal(StandardCharsets.UTF_8.decode(buff).toString()));
        }
    }
}
