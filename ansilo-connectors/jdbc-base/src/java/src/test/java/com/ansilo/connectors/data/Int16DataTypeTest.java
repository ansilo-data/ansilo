package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Int16DataTypeTest extends DataTypeTest {
    private Int16DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new Int16DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getInt16(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).putShort((short) 123);
    }

    @Test
    void testWriteByte() throws Exception {
        when(this.mapping.getInt16(this.resultSet, 0)).thenReturn((short) 123);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putShort((short) 123);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.putShort((short) 111);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindInt16(this.preparedStatement, 1, (short) 111);
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
