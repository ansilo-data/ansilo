package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimeDataTypeTest extends DataTypeTest {
    private TimeDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new TimeDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getTime(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
    }

    @Test
    void testWriteTime() throws Exception {
        when(this.mapping.getTime(this.resultSet, 0)).thenReturn(LocalTime.of(12, 34, 56, 12345));

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).put((byte) 12);
        verify(this.byteBuffer, times(1)).put((byte) 34);
        verify(this.byteBuffer, times(1)).put((byte) 56);
        verify(this.byteBuffer, times(1)).putInt(12345);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(8);
        buff.put((byte) 1);
        buff.put((byte) 23);
        buff.put((byte) 59);
        buff.put((byte) 58);
        buff.putInt(987654000);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindTime(this.preparedStatement, 1,
                LocalTime.parse("23:59:58.987654"));
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
