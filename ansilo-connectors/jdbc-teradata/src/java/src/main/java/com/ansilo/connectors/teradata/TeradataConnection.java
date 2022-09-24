package com.ansilo.connectors.teradata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import com.ansilo.connectors.JdbcConnection;
import com.ansilo.connectors.mapping.JdbcDataMapping;
import com.ansilo.connectors.query.JdbcParameter;
import com.ansilo.connectors.query.JdbcPreparedQuery;

public class TeradataConnection extends JdbcConnection {

    public TeradataConnection(String jdbcUrl, Properties jdbcProps, JdbcDataMapping mapping)
            throws SQLException {
        super(jdbcUrl, jdbcProps, mapping);
    }

    public TeradataConnection(Connection connection, JdbcDataMapping mapping) {
        super(connection, mapping);
    }

    @Override
    protected JdbcPreparedQuery newPreparedQuery(List<JdbcParameter> parameters,
            PreparedStatement statement, String query) {
        return new TeradataPreparedQuery(this.mapping, statement, parameters, query);
    }

    @Override
    public void close() throws SQLException {
        try {
            super.close();
        } catch (SQLException e) {
            // Swallow connection already closed exception
            // ([TeraJDBC 17.20.00.09] [Error 1095] [SQLState HY000] Cannot call a method on closed
            // connection)
            if (e.getErrorCode() == 1095) {
                return;
            }

            throw e;
        }
    }
}
