package com.ansilo.connectors.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DateTimeWithTzDataTypeTest extends DataTypeTest {
    private DateTimeWithTzDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new DateTimeWithTzDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getDateTimeWithTz(this.resultSet, 0)).thenReturn(null);

        assertNull(this.dataType.getStream(this.mapping, this.resultSet, 0));
    }

    @Test
    void testWriteDateTimeWithTz() throws Exception {
        when(this.mapping.getDateTimeWithTz(this.resultSet, 0))
                .thenReturn(ZonedDateTime.parse("2020-01-02T02:03:04.123456789Z"));

        var stream = this.dataType.getStream(this.mapping, this.resultSet, 0);
        var buff = ByteBuffer.allocate(16);

        buff.putInt(2020);
        buff.put((byte) 1);
        buff.put((byte) 2);
        buff.put((byte) 2);
        buff.put((byte) 3);
        buff.put((byte) 4);
        buff.putInt(123456789);
        buff.put(StandardCharsets.UTF_8.encode("UTC"));

        assertArrayEquals(buff.array(), stream.readAllBytes());
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(17);
        buff.put((byte) 1);
        buff.putInt(2000);
        buff.put((byte) 6);
        buff.put((byte) 9);
        buff.put((byte) 23);
        buff.put((byte) 59);
        buff.put((byte) 58);
        buff.putInt(987654321);
        buff.put(StandardCharsets.UTF_8.encode("UTC"));
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindDateTimeWithTz(this.preparedStatement, 1, ZonedDateTime
                .parse("2000-06-09T23:59:58.987654321Z").withZoneSameInstant(ZoneId.of("UTC")));
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
