package com.ansilo.connectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.ansilo.connectors.data.JdbcDataType;
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
    public JdbcPreparedQuery prepare(String query, List<Integer> parameterTypes)
            throws SQLException {
        var jdbcTypes = new ArrayList<JdbcDataType>();

        for (var typeId : parameterTypes) {
            jdbcTypes.add(JdbcDataType.createFromTypeId(typeId));
        }

        var statement = this.connection.prepareStatement(query);

        return new JdbcPreparedQuery(statement, jdbcTypes);
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
