package com.ansilo.connectors.params;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.ansilo.connectors.data.JdbcDataType;

/**
 * Wrapper class for holding a JDBC statement parameter
 */
public class JdbcParameter {
    /**
     * The data type of the parameter
     */
    private JdbcDataType dataType;

    /**
     * The value of the parameter
     */
    private Object value;

    /**
     * Creates a new parameter
     */
    public JdbcParameter(JdbcDataType dataType, Object value) {
        this.dataType = dataType;
        this.value = value;
    }

    /**
     * Creates a new parameter from the supplied type id
     * 
     * @throws SQLException
     */
    public static JdbcParameter create(int dataTypeId, Object value) throws SQLException {
        return new JdbcParameter(JdbcDataType.createFromTypeId(dataTypeId), value);
    }

    /**
     * Binds the parameter to the supplied statement
     * 
     * @throws SQLException
     */
    public void bindTo(PreparedStatement statement, int index) throws SQLException {
        this.dataType.bindParam(statement, index, this.value);
    }
}
