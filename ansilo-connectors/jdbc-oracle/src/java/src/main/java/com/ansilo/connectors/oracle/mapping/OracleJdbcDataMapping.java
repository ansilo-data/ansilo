package com.ansilo.connectors.oracle.mapping;

import java.sql.ResultSet;
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.data.DateTimeWithTzDataType;
import com.ansilo.connectors.data.Float32DataType;
import com.ansilo.connectors.data.Float64DataType;
import com.ansilo.connectors.data.JsonDataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;

/**
 * Oracle JDBC data mapping
 */
public class OracleJdbcDataMapping extends JdbcDataMapping {
    @Override
    public DataType getColumnDataType(ResultSet resultSet, int index) throws Exception {
        var type = resultSet.getMetaData().getColumnType(index);

        if (type == oracle.sql.TypeDescriptor.TYPECODE_BFLOAT) {
            return new Float32DataType();
        }

        if (type == oracle.sql.TypeDescriptor.TYPECODE_BDOUBLE) {
            return new Float64DataType();
        }

        // JSON data type constant not defined in library
        if (type == 2016) {
            return new JsonDataType();
        }

        // TIMESTAMP WITH [LOCAL] TIME ZONE data type constant not defined in library
        if (type == -101 || type == -102) {
            return new DateTimeWithTzDataType();
        }

        return super.getColumnDataType(resultSet, index);
    }
}
