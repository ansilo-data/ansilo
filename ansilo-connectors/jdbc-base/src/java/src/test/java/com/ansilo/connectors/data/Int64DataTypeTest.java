package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Int64DataTypeTest extends DataTypeTest {
    private Int64DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new Int64DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getLong(0)).thenReturn((long)123);
        when(this.resultSet.wasNull()).thenReturn(true);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).putLong((long)123);
    }

    @Test
    void testWriteByte() throws Exception {
        when(this.resultSet.getLong(0)).thenReturn((long)123);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putLong((long)123);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(15);
        buff.put((byte)1);
        buff.putLong((long)111);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setLong(1, (long)111);
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte)0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.BIGINT);
    }
}
