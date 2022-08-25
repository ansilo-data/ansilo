package com.ansilo.connectors.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ansilo.connectors.data.Int32DataType;
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.data.VarcharDataType;
import com.ansilo.connectors.result.JdbcResultSet;

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
        assertArrayEquals(new LoggedParam[] {new LoggedParam(1, DataType.TYPE_INT32, 1234)},
                this.statement.getLoggedParams().toArray());
    }

    @Test
    void testGetLoggedParamsWithMultipleSetParam() throws Exception {
        this.statement.setInt(1, 1234);
        this.statement.setString(2, "ABC");
        assertArrayEquals(
                new LoggedParam[] {new LoggedParam(1, DataType.TYPE_INT32, 1234),
                        new LoggedParam(2, DataType.TYPE_UTF8_STRING, "ABC")},
                this.statement.getLoggedParams().toArray());
    }

    @Test
    void testGetLoggedParamsAfterClear() throws Exception {
        this.statement.setInt(1, 1234);
        this.statement.clearLoggedParams();
        assertArrayEquals(new LoggedParam[0], this.statement.getLoggedParams().toArray());
    }
}
