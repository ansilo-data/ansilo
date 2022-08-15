package com.ansilo.connectors.data;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Types;
import java.time.Instant;
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
        when(this.resultSet.getTime(0)).thenReturn(null);
        when(this.resultSet.wasNull()).thenReturn(true);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
    }

    @Test
    void testWriteTime() throws Exception {
        var time = mock(Time.class);
        when(time.toLocalTime()).thenReturn(LocalTime.of(12, 34, 56, 12345));
        when(this.resultSet.getTime(0)).thenReturn(time);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

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
        buff.putInt(987654);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setTime(1, Time.valueOf(LocalTime.parse("23:59:58.987654")));
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte) 0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.TIME);
    }
}
