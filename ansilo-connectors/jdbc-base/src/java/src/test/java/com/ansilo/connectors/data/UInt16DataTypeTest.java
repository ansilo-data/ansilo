package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UInt16DataTypeTest extends DataTypeTest {
    private UInt16DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new UInt16DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getUInt16(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).put((byte) 123);
    }

    @Test
    void testWriteShort() throws Exception {
        when(this.mapping.getUInt16(this.resultSet, 0)).thenReturn((int) 12345);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putShort((short) 12345);
    }

    @Test
    void testWriteShortUnsignedConversion() throws Exception {
        when(this.mapping.getUInt16(this.resultSet, 0)).thenReturn((int) 54321);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putShort((short) -11215);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(3);
        buff.put((byte) 1);
        buff.putShort((short) 12345);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt16(this.preparedStatement, 1, (short) 12345);
    }

    @Test
    void testBindParamUnsignedConversion() throws Exception {
        var buff = ByteBuffer.allocate(3);
        buff.put((byte) 1);
        buff.putShort((short) -1234);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt16(this.preparedStatement, 1, (int) 64302);
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
