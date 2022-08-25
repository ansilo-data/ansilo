package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Float32DataTypeTest extends DataTypeTest {
    private Float32DataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new Float32DataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getFloat32(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).putFloat((float) 123.45);
    }

    @Test
    void testZero() throws Exception {
        when(this.mapping.getFloat32(this.resultSet, 0)).thenReturn((float) 0);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putFloat(0);
    }

    @Test
    void testSomeFloat() throws Exception {
        when(this.mapping.getFloat32(this.resultSet, 0)).thenReturn((float) 123.45);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putFloat((float) 123.45);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.putFloat((float) 123.45);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindFloat32(this.preparedStatement, 1, (float) 123.45);
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
