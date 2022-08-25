package com.ansilo.connectors.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DecimalDataTypeTest extends DataTypeTest {
    private DecimalDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new DecimalDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getDecimal(this.resultSet, 0)).thenReturn(null);

        assertNull(this.dataType.getStream(this.mapping, this.resultSet, 0));
    }

    @Test
    void testZero() throws Exception {
        when(this.mapping.getDecimal(this.resultSet, 0)).thenReturn(BigDecimal.ZERO);

        InputStream stream = this.dataType.getStream(this.mapping, this.resultSet, 0);

        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("0"));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testLargeNumber() throws Exception {
        when(this.mapping.getDecimal(this.resultSet, 0))
                .thenReturn(new BigDecimal("123456789.12345678"));

        InputStream stream = this.dataType.getStream(this.mapping, this.resultSet, 0);

        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("123456789.12345678"));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(4);
        buff.put((byte) 1);
        buff.put(StandardCharsets.UTF_8.encode("1.1"));
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindDecimal(this.preparedStatement, 1,
                new BigDecimal("1.1"));
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

    private byte[] toByteArray(ByteBuffer data) {
        var buf = new byte[data.limit()];
        data.get(buf);
        return buf;
    }
}
