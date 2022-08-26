package com.ansilo.connectors.data;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UuidDataTypeTest extends DataTypeTest {
    private UuidDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new UuidDataType();
    }

    @Test
    void testHandlesNullValue() throws Exception {
        when(this.mapping.getUuid(this.resultSet, 0)).thenReturn(null);

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 0);
        verify(this.byteBuffer, times(0)).put((byte) 123);
    }

    @Test
    void testWriteUuid() throws Exception {
        when(this.mapping.getUuid(this.resultSet, 0))
                .thenReturn(UUID.fromString("0b51d600-420c-47db-803c-992a4422b7d1"));

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(1)).putLong(815668304127215579L);
        verify(this.byteBuffer, times(1)).putLong(-9206315131441334319L);
    }

    @Test
    void testWriteUuidZero() throws Exception {
        when(this.mapping.getUuid(this.resultSet, 0))
                .thenReturn(UUID.fromString("00000000-0000-0000-0000-000000000000"));

        this.dataType.writeToByteBuffer(this.mapping, this.byteBuffer, this.resultSet, 0);

        verify(this.byteBuffer, times(1)).put((byte) 1);
        verify(this.byteBuffer, times(2)).putLong(0L);
    }

    @Test
    void testBindParam() throws Exception {
        var buff = ByteBuffer.allocate(17);
        buff.put((byte) 1);
        buff.putLong(815668304127215579L);
        buff.putLong(-9206315131441334319L);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUuid(this.preparedStatement, 1,
                UUID.fromString("0b51d600-420c-47db-803c-992a4422b7d1"));
    }

    @Test
    void testBindParamZero() throws Exception {
        var buff = ByteBuffer.allocate(17);
        buff.put((byte) 1);
        buff.putLong(0);
        buff.putLong(0);
        buff.rewind();
        this.dataType.bindParam(this.mapping, this.preparedStatement, 1, buff);

        verify(this.mapping, times(1)).bindUuid(this.preparedStatement, 1,
                UUID.fromString("00000000-0000-0000-0000-000000000000"));
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
