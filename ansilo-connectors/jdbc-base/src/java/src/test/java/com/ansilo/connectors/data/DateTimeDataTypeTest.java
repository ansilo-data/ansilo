package com.ansilo.connectors.data;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DateTimeDataTypeTest extends DataTypeTest {
    private DateTimeDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new DateTimeDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getTimestamp(0)).thenReturn(null);
        when(this.resultSet.wasNull()).thenReturn(true);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
    }

    @Test
    void testWriteDateTime() throws Exception {
        var time = mock(Timestamp.class);
        when(time.toLocalDateTime()).thenReturn(LocalDateTime.of(2020, 8, 25, 12, 34, 56, 12345));
        when(this.resultSet.getTimestamp(0)).thenReturn(time);
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putInt(2020);
        verify(this.byteBuffer, times(1)).put((byte) 8);
        verify(this.byteBuffer, times(1)).put((byte) 25);
        verify(this.byteBuffer, times(1)).put((byte) 12);
        verify(this.byteBuffer, times(1)).put((byte) 34);
        verify(this.byteBuffer, times(1)).put((byte) 56);
        verify(this.byteBuffer, times(1)).putInt(12345);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(15);
        buff.put((byte) 1);
        buff.putInt(2000);
        buff.put((byte) 6);
        buff.put((byte) 9);
        buff.put((byte) 23);
        buff.put((byte) 59);
        buff.put((byte) 58);
        buff.putInt(987654321);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setTimestamp(1,
                Timestamp.valueOf(LocalDateTime.parse("2000-06-09T23:59:58.987654321")));
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte) 0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.TIMESTAMP);
    }
}
