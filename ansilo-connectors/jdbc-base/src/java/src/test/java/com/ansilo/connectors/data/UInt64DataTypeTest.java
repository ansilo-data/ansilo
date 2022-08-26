package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UInt64DataTypeTest extends DataTypeTest {
    private UInt64DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new UInt64DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getUInt64(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).put((byte) 123);
    }

    @Test
    void testWriteLong() throws Exception {
        when(this.mapping.getUInt64(this.resultSet, 0)).thenReturn(new BigInteger("12345"));

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putLong((int) 12345);
    }

    @Test
    void testWriteLongUnsignedConversion() throws Exception {
        when(this.mapping.getUInt64(this.resultSet, 0))
                .thenReturn(new BigInteger("9223372036854775808"));

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putLong(-9223372036854775808L);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(9);
        buff.put((byte) 1);
        buff.putLong(1234);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt64(this.preparedStatement, 1,
                new BigInteger("1234"));
    }

    @Test
    void testBindParamUnsignedConversion() throws Exception {
        var buff = ByteBuffer.allocate(9);
        buff.put((byte) 1);
        buff.putLong(-9223372036854775808L);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUInt64(this.preparedStatement, 1,
                new BigInteger("9223372036854775808"));
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
