package com.ansilo.connectors.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NVarcharDataTypeTest extends DataTypeTest {
    private NVarcharDataType dataType;

    @BeforeEach
    void setUp() {
        super.setUp();
        this.dataType = new NVarcharDataType();
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
        assertArrayEquals(stream.readAllBytes(), new byte[0]);
    }

    @Test
    void testString() throws Exception {
        when(this.resultSet.getNString(0)).thenReturn("abc");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);
        assertArrayEquals(stream.readAllBytes(), StandardCharsets.UTF_8.encode("abc").array());
    }

    @Test
    void testUnicodeString() throws Exception {
        when(this.resultSet.getNString(0)).thenReturn("🥑🥑🥑");

        InputStream stream = this.dataType.getStream(this.resultSet, 0);
        assertArrayEquals(stream.readAllBytes(), StandardCharsets.UTF_8.encode("🥑🥑🥑").array());
    }

    @Test
    void testBindParam() throws Exception {
        this.dataType.bindParam(this.preparedStatement, 1, "TEST");

        verify(this.preparedStatement, times(1)).setNString(1, "TEST");
    }

    @Test
    void testBindParamNull() throws Exception {
        this.dataType.bindParam(this.preparedStatement, 1, null);

        verify(this.preparedStatement, times(1)).setNull(1, Types.NVARCHAR);
    }
}
