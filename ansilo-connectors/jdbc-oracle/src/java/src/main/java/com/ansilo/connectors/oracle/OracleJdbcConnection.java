package com.ansilo.connectors.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import com.ansilo.connectors.JdbcConnection;
import com.ansilo.connectors.query.JdbcParameter;
import com.ansilo.connectors.query.JdbcPreparedQuery;

/**
 * The Oracle JDBC Connection class.
 */
public class OracleJdbcConnection extends JdbcConnection {

    public OracleJdbcConnection(String jdbcUrl, Properties jdbcProps) throws SQLException {
        super(jdbcUrl, jdbcProps);
    }

    @Override
    protected JdbcPreparedQuery newPreparedQuery(List<JdbcParameter> parameters,
            PreparedStatement statement) {
        return new OracleJdbcPreparedQuery(statement, parameters);
    }
}
