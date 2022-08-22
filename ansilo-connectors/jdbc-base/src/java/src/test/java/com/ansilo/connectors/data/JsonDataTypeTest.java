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
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.gson.JsonPrimitive;

public class JsonDataTypeTest extends DataTypeTest {
    private JsonDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new JsonDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.resultSet.getObject(0)).thenReturn(null);

        assertNull(this.dataType.getStream(this.resultSet, 0));
    }

    @Test
    void testEmptyString() throws Exception {
        when(this.resultSet.getObject(0)).thenReturn("");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);

        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("\"\""));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testString() throws Exception {
        when(this.resultSet.getObject(0)).thenReturn("abc");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);

        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("\"abc\""));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testObject() throws Exception {
        when(this.resultSet.getObject(0)).thenReturn(Map.of("foo", "bar"));

        InputStream stream = this.dataType.getStream(this.resultSet, 0);

        var expected = this.toByteArray(StandardCharsets.UTF_8.encode("{\"foo\":\"bar\"}"));
        assertArrayEquals(expected, stream.readAllBytes());
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(7);
        buff.put((byte) 1);
        buff.put(StandardCharsets.UTF_8.encode("\"TEST\""));
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setObject(1, new JsonPrimitive("TEST"));
    }

    @Test
    void testBindParamNull() throws Exception {
        var buff = ByteBuffer.allocate(1);
        buff.put((byte) 0);
        buff.rewind();
        this.dataType.bindParam(this.preparedStatement, 1, buff);

        verify(this.preparedStatement, times(1)).setNull(1, Types.JAVA_OBJECT);
    }

    private byte[] toByteArray(ByteBuffer data) {
        var buf = new byte[data.limit()];
        data.get(buf);
        return buf;
    }
}
