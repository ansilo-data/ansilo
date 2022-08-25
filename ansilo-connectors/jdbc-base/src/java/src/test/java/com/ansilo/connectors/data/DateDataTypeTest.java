package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.time.LocalDate;
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
        when(this.mapping.getDate(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).putInt(0);
    }

    @Test
    void testWriteDate() throws Exception {
        when(this.mapping.getDate(this.resultSet, 0)).thenReturn(LocalDate.parse("2020-08-23"));

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putInt(2020);
        verify(this.byteBuffer, times(1)).put((byte) 8);
        verify(this.byteBuffer, times(1)).put((byte) 23);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(7);
        buff.put((byte) 1);
        buff.putInt(2018);
        buff.put((byte) 12);
        buff.put((byte) 27);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindDate(this.preparedStatement, 1,
                LocalDate.parse("2018-12-27"));
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
