package com.ansilo.connectors.oracle;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.ansilo.connectors.query.JdbcParameter;
import com.ansilo.connectors.query.JdbcPreparedQuery;
import com.ansilo.connectors.result.JdbcResultSet;

/**
 * Oracle JDBC Prepared Query type
 */
public class OracleJdbcPreparedQuery extends JdbcPreparedQuery {

    public OracleJdbcPreparedQuery(PreparedStatement preparedStatement,
            List<JdbcParameter> parameters) {
        super(preparedStatement, parameters);
    }

    @Override
    protected JdbcResultSet newResultSet(ResultSet innerResultSet) throws SQLException {
        return new OracleJdbcResultSet(innerResultSet);
    }
}
