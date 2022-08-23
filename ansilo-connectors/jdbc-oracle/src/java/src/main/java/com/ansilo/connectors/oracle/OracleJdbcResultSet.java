package com.ansilo.connectors.oracle;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import com.ansilo.connectors.data.DateTimeWithTzDataType;
import com.ansilo.connectors.data.Float32DataType;
import com.ansilo.connectors.data.Float64DataType;
import com.ansilo.connectors.data.JdbcDataType;
import com.ansilo.connectors.oracle.data.OracleJsonDataType;
import com.ansilo.connectors.result.JdbcResultSet;

/**
 * Oracle JDBC result set
 */
public class OracleJdbcResultSet extends JdbcResultSet {

    public OracleJdbcResultSet(ResultSet resultSet) throws SQLException {
        super(resultSet);
    }

    @Override
    protected JdbcDataType getDataType(ResultSetMetaData metadata, int index) throws SQLException {
        var type = metadata.getColumnType(index);

        if (type == oracle.sql.TypeDescriptor.TYPECODE_BFLOAT) {
            return new Float32DataType();
        }

        if (type == oracle.sql.TypeDescriptor.TYPECODE_BDOUBLE) {
            return new Float64DataType();
        }

        // JSON data type constant not defined in library
        if (type == 2016) {
            return new OracleJsonDataType();
        }

        // TIMESTAMP WITH [LOCAL] TIME ZONE data type constant not defined in library
        if (type == -101 || type == -102) {
            return new DateTimeWithTzDataType();
        }

        return super.getDataType(metadata, index);
    }
}
