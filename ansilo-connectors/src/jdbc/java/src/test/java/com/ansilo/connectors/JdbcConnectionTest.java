package com.ansilo.connectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ansilo.connectors.data.Int32DataType;
import com.ansilo.connectors.data.JdbcDataType;
import com.ansilo.connectors.data.VarcharDataType;

public class JdbcConnectionTest {
    private Connection innerConnection;
    private JdbcConnection connection;

    @BeforeEach
    void setUp() {
        this.innerConnection = mock(Connection.class);
        this.connection = new JdbcConnection(innerConnection);
    }

    @Test
    void testPrepareStatement() throws Exception {
        var query = "EXAMPLE QUERY";
        var params = new ArrayList<Integer>();
        params.add(JdbcDataType.TYPE_INTEGER);
        params.add(JdbcDataType.TYPE_VARCHAR);

        var mockStatement = mock(PreparedStatement.class);
        when(this.innerConnection.prepareStatement(query)).thenReturn(mockStatement);

        var statement = this.connection.prepare(query, params);

        verify(this.innerConnection, times(1)).prepareStatement(query);
        assertArrayEquals(params.toArray(),
                statement.getParameterTypes().stream().map(i -> i.getTypeId()).toArray());
        assertEquals(mockStatement, statement.getPreparedStatement());
    }

    @Test
    void testClose() throws Exception {
        this.connection.close();

        verify(this.innerConnection, times(1)).close();
    }
}
