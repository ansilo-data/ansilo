package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UInt32DataTypeTest extends DataTypeTest {
    private UInt32DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new UInt32DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getUInt32(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).put((byte) 123);
    }

    @Test
    void testWriteInt() throws Exception {
        when(this.mapping.getUInt32(this.resultSet, 0)).thenReturn((long) 12345);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putInt((int) 12345);
    }

    @Test
    void testWriteIntUnsignedConversion() throws Exception {
        when(this.mapping.getUInt32(this.resultSet, 0)).thenReturn(((long) Integer.MAX_VALUE) + 10);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putInt(Integer.MIN_VALUE + 9);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.putInt((int) 12345);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt32(this.preparedStatement, 1, (long) 12345);
    }

    @Test
    void testBindParamUnsignedConversion() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.putInt((int) -1234);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt32(this.preparedStatement, 1,
                ((long) Integer.MAX_VALUE) * 2 - 1232);
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte) 0);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindNull(this.preparedStatement, 1,
                this.dataType.getTypeId());
    }
}
