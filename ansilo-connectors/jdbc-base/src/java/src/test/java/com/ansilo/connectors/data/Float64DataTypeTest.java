package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Float64DataTypeTest extends DataTypeTest {
    private Float64DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new Float64DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getDouble(0)).thenReturn((double)123.45);
        when(this.resultSet.wasNull()).thenReturn(true);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).putDouble((double)123.45);
    }

    @Test
    void testZero() throws Exception {
        when(this.resultSet.getDouble(0)).thenReturn((double)0);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putDouble(0);
    }

    @Test
    void testSomeDouble() throws Exception {
        when(this.resultSet.getDouble(0)).thenReturn((double)123.45);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putDouble((double)123.45);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(15);
        buff.put((byte)1);
        buff.putDouble((double)123.45);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setDouble(1, (double)123.45);
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte)0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.DOUBLE);
    }
}
