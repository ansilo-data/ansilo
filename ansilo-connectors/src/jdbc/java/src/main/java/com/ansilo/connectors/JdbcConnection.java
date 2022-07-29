package com.ansilo.connectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import com.ansilo.connectors.query.JdbcParameter;
import com.ansilo.connectors.query.JdbcPreparedQuery;

/**
 * The JDBC Connection wrapper class.
 * 
 * This acts an entrypoint called from our rust code to initialise the JDBC connection.
 *
 * TODO: Test
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
    public JdbcConnection(String jdbcUrl, Properties jdbcProps) throws SQLException {
        // TODO: logging
        this(DriverManager.getConnection(jdbcUrl, jdbcProps));
    }

    /**
     * Initialises a new JDBC connection
     */
    public JdbcConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * Prepares the supplied query.
     * 
     * @param query
     * @param params
     * @return
     */
    public JdbcPreparedQuery prepare(String query, List<JdbcParameter> parameters)
            throws SQLException {
        var statement = this.connection.prepareStatement(query);

        return new JdbcPreparedQuery(statement, parameters);
    }

    /**
     * Returns whether this connection is currently within a transaction.
     */
    public boolean isInTransaction() throws SQLException {
        return this.connection.getAutoCommit() == false;
    }

    /**
     * Starts a new transaction
     */
    public void beginTransaction() throws SQLException {
        this.connection.setAutoCommit(false);
    }

    /**
     * Rolls back the current transaction
     */
    public void rollBackTransaction() throws SQLException {
        this.connection.rollback();
    }

    /**
     * Commits the current transaction
     */
    public void commitTransaction() throws SQLException {
        this.connection.commit();
    }
    
    /**
     * Checks if the connection is valid
     * 
     * @throws SQLException
     */
    public boolean isValid(int timeoutSeconds) throws SQLException {
        return this.connection.isValid(timeoutSeconds);
    }

    /**
     * Checks if the connection is closed
     * 
     * @throws SQLException
     */
    public boolean isClosed() throws SQLException {
        return this.connection.isClosed();
    }

    /**
     * Closes the connection
     * 
     * @throws SQLException
     */
    public void close() throws SQLException {
        this.connection.close();
    }
}
