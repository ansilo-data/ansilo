package com.ansilo.connectors.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class BinaryDataTypeTest extends DataTypeTest {
    private BinaryDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new BinaryDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getBinary(this.resultSet, 0)).thenReturn(null);

        assertNull(this.dataType.getStream(this.mapping, this.resultSet, 0));
    }

    @Test
    void testEmptyBinaryStream() throws Exception {
        when(this.mapping.getBinary(this.resultSet, 0))
                .thenReturn(ByteArrayInputStream.nullInputStream());

        InputStream stream = this.dataType.getStream(this.mapping, this.resultSet, 0);
        assertArrayEquals(new byte[0], stream.readAllBytes());
    }

    @Test
    void testBinaryStreamWithData() throws Exception {
        when(this.mapping.getBinary(this.resultSet, 0))
                .thenReturn(new ByteArrayInputStream(new byte[] {1, 2, 3}));

        InputStream stream = this.dataType.getStream(this.mapping, this.resultSet, 0);

        assertArrayEquals(new byte[] {1, 2, 3}, stream.readAllBytes());
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(4);
        buff.put((byte) 1);
        buff.put(new byte[] {4, 5, 6});
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        var actual = ArgumentCaptor.forClass(ByteArrayInputStream.class);
        verify(this.mapping, times(1)).bindBinary(eq(this.preparedStatement), eq(1),
                actual.capture());

        assertArrayEquals(new byte[] {4, 5, 6}, actual.getAllValues().get(0).readAllBytes());
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
