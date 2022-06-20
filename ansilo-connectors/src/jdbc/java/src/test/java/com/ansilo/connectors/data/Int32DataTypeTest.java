package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Int32DataTypeTest extends DataTypeTest {
    private Int32DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new Int32DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getInt(0)).thenReturn(0);
        when(this.resultSet.wasNull()).thenReturn(true);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte)0);
        verify(this.byteBuffer, times(1)).putInt(0);
    }

    @Test
    void testZero() throws Exception {
        when(this.resultSet.getInt(0)).thenReturn(0);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte)1);
        verify(this.byteBuffer, times(1)).putInt(0);
    }

    @Test
    void testSomeInt() throws Exception {
        when(this.resultSet.getInt(0)).thenReturn(12345);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte)1);
        verify(this.byteBuffer, times(1)).putInt(12345);
    }

    @Test
    void testBindParam() throws Exception {
        this.dataType.bindParam(this.preparedStatement, 1, 123);

        verify(this.preparedStatement, times(1)).setInt(1, 123);
    }

    @Test
    void testBindParamNull() throws Exception {
        this.dataType.bindParam(this.preparedStatement, 1, null);

        verify(this.preparedStatement, times(1)).setNull(1, Types.INTEGER);
    }
}
