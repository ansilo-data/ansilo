package com.ansilo.connectors.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NVarcharDataTypeTest extends DataTypeTest {
    private Utf8StringDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new Utf8StringDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getNString(0)).thenReturn(null);

        assertNull(this.dataType.getStream(this.resultSet, 0));
    }

    @Test
    void testEmptyString() throws Exception {
        when(this.resultSet.getNString(0)).thenReturn("");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);
        assertArrayEquals(new byte[0], stream.readAllBytes());
    }

    @Test
    void testString() throws Exception {
        when(this.resultSet.getNString(0)).thenReturn("abc");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);

        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("abc"));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testUnicodeString() throws Exception {
        when(this.resultSet.getNString(0)).thenReturn("🥑🥑🥑");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);
        
        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("🥑🥑🥑"));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.put(StandardCharsets.UTF_8.encode("TEST"));
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNString(1, "TEST");
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte) 0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.NVARCHAR);
    }

    private byte[] toByteArray(ByteBuffer data) {
        var buf = new byte[data.limit()];
        data.get(buf);
        return buf;
    }
}
