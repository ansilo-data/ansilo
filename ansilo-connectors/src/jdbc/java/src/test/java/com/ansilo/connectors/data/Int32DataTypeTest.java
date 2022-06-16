package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
}
