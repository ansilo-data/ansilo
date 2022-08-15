package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Types;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DateDataTypeTest extends DataTypeTest {
    private DateDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new DateDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getDate(0)).thenReturn(null);
        when(this.resultSet.wasNull()).thenReturn(true);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).putInt(0);
    }

    @Test
    void testWriteDate() throws Exception {
        when(this.resultSet.getDate(0)).thenReturn(Date.valueOf("2020-08-23"));
        when(this.resultSet.wasNull()).thenReturn(false);

        this.dataType.writeToByteBuffer(this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putInt(2020);
        verify(this.byteBuffer, times(1)).put((byte) 8);
        verify(this.byteBuffer, times(1)).put((byte) 23);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(7);
        buff.put((byte)1);
        buff.putInt(2018);
        buff.put((byte)12);
        buff.put((byte)27);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setDate(1, Date.valueOf("2018-12-27"));
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte)0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.DATE);
    }
}
