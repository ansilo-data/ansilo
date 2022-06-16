package com.ansilo.connectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import com.ansilo.connectors.params.JdbcParameter;
import com.ansilo.connectors.result.JdbcResultSet;

/**
 * The JDBC Connection wrapper class.
 * 
 * This acts an entrypoint called from our rust code to initialise the JDBC connection.
 */
public class JdbcConnection {
    /**
     * The actual JDBC connection instance
     */
    private Connection connection;

    /**
     * Initialises the JDBC connection
     * 
     * @param jdbcUrl
     * @param props
     * @throws SQLException
     */
    public JdbcConnection(String jdbcUrl, Map<String, String> props) throws SQLException {
        var jdbcProps = new Properties();
        jdbcProps.putAll(props);

        // TODO: logging
        this.connection = DriverManager.getConnection(jdbcUrl, jdbcProps);
    }

    /**
     * Executes the supplied query, returning the result set.
     * 
     * @param query
     * @param params
     * @return
     */
    public JdbcResultSet execute(String query, Map<Integer, JdbcParameter> params) throws SQLException {
        var statement = this.connection.prepareStatement(query);
        
        for (Map.Entry<Integer, JdbcParameter> param : params.entrySet()) {
            // TODO
            // param.getValue().bind(statement, param.getKey());
        }

        return new JdbcResultSet(statement.executeQuery());
    }

    /**
     * Closes the connection
     * @throws SQLException
     */
    public void close() throws SQLException {
        this.connection.close();
    }
}
