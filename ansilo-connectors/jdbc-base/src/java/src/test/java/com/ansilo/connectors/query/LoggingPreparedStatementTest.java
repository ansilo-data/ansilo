package com.ansilo.connectors.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LoggingPreparedStatementTest {
    private PreparedStatement inner;
    private LoggingPreparedStatement statement;

    @BeforeEach
    void setUp() throws SQLException {
        this.inner = mock(PreparedStatement.class);
        this.statement = new LoggingPreparedStatement(this.inner);
    }

    @Test
    void testGetInner() throws Exception {
        assertEquals(this.inner, this.statement.getInner());
    }

    @Test
    void testGetLoggedParamsWithoutParams() throws Exception {
        assertArrayEquals(new LoggedParam[0], this.statement.getLoggedParams().toArray());
    }

    @Test
    void testGetLoggedParamsWithSetParam() throws Exception {
        this.statement.setInt(1, 1234);
        assertArrayEquals(new LoggedParam[] {new LoggedParam(1, "setInt", 1234)},
                this.statement.getLoggedParams().toArray());
    }

    @Test
    void testGetLoggedParamsWithMultipleSetParam() throws Exception {
        this.statement.setInt(1, 1234);
        this.statement.setString(2, "ABC");
        assertArrayEquals(
                new LoggedParam[] {new LoggedParam(1, "setInt", 1234),
                        new LoggedParam(2, "setString", "ABC")},
                this.statement.getLoggedParams().toArray());
    }

    @Test
    void testGetLoggedParamsAfterClear() throws Exception {
        this.statement.setInt(1, 1234);
        this.statement.clearLoggedParams();
        assertArrayEquals(new LoggedParam[0], this.statement.getLoggedParams().toArray());
    }
}
