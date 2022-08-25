package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BoolDataTypeTest extends DataTypeTest {
    private BoolDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new BoolDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getBool(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).put((byte) 1);
    }

    @Test
    void testFalse() throws Exception {
        when(this.mapping.getBool(this.resultSet, 0)).thenReturn(false);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).put((byte) 0);
    }

    @Test
    void testTrue() throws Exception {
        when(this.mapping.getBool(this.resultSet, 0)).thenReturn(true);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(2)).put((byte) 1);
    }

    @Test
    void testBindParamTrue() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.put((byte) 1);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindBool(this.preparedStatement, 1, true);
    }

    @Test
    void testBindParamFalse() throws Exception {
        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1);
        buff.put((byte) 0);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindBool(this.preparedStatement, 1, false);
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
