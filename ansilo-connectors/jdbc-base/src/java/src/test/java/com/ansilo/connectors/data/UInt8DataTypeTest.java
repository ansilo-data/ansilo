package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UInt8DataTypeTest extends DataTypeTest {
    private UInt8DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new UInt8DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getUInt8(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).put((byte) 123);
    }

    @Test
    void testWriteByte() throws Exception {
        when(this.mapping.getUInt8(this.resultSet, 0)).thenReturn((short) 123);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).put((byte) 123);
    }

    @Test
    void testWriteByteUnsignedConversion() throws Exception {
        when(this.mapping.getUInt8(this.resultSet, 0)).thenReturn((short) 250);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).put((byte) -6);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(2);
        buff.put((byte) 1);
        buff.put((byte) 111);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt8(this.preparedStatement, 1, (short) 111);
    }

    @Test
    void testBindParamUnsignedConversion() throws Exception {
        var buff = ByteBuffer.allocate(2);
        buff.put((byte) 1);
        buff.put((byte) -8);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt8(this.preparedStatement, 1, (short) 248);
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
