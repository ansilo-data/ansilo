package com.ansilo.connectors.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import com.ansilo.connectors.data.Int32DataType;
import com.ansilo.connectors.data.DataType;

public class JdbcParameterTest {
    @Test
    void testCreateDynamic() throws Exception {
        var dataType = new Int32DataType();
        var param = JdbcParameter.createDynamic(1, dataType);

        assertEquals(1, param.getIndex());
        assertEquals(dataType, param.getDataType());
        assertEquals(false, param.isConstant());
        assertEquals(null, param.getConstantValueBuffer());
    }

    @Test
    void testCreateDynamicFromDataTypeId() throws Exception {
        var param = JdbcParameter.createDynamic(1, DataType.TYPE_INT32);

        assertEquals(1, param.getIndex());
        assertInstanceOf(Int32DataType.class, param.getDataType());
        assertEquals(false, param.isConstant());
        assertEquals(null, param.getConstantValueBuffer());
    }

    @Test
    void testCreateConstant() throws Exception {
        var dataType = new Int32DataType();
        var buff = ByteBuffer.allocate(10);
        var param = JdbcParameter.createConstant(1, dataType, buff);

        assertEquals(1, param.getIndex());
        assertEquals(dataType, param.getDataType());
        assertEquals(true, param.isConstant());
        assertEquals(buff, param.getConstantValueBuffer());
    }

    @Test
    void testCreateConstantFromDataTypeId() throws Exception {
        var buff = ByteBuffer.allocate(10);
        var param = JdbcParameter.createConstant(1, DataType.TYPE_INT32, buff);

        assertEquals(1, param.getIndex());
        assertInstanceOf(Int32DataType.class, param.getDataType());
        assertEquals(true, param.isConstant());
        assertEquals(buff, param.getConstantValueBuffer());
    }

    @Test
    void testCreateConstantCopied() throws Exception {
        var dataType = new Int32DataType();
        var buff = ByteBuffer.wrap(new byte[] {1, 2, 3});
        var param = JdbcParameter.createConstantCopied(1, dataType, buff);

        assertEquals(1, param.getIndex());
        assertEquals(dataType, param.getDataType());
        assertEquals(true, param.isConstant());
        assertNotEquals(buff, param.getConstantValueBuffer());
        assertArrayEquals(new byte[] {1, 2, 3}, param.getConstantValueBuffer().array());
    }

    @Test
    void testCreateConstantCopiedFromDataTypeId() throws Exception {
        var buff = ByteBuffer.wrap(new byte[] {1, 2, 3});
        var param = JdbcParameter.createConstantCopied(1, DataType.TYPE_INT32, buff);

        assertEquals(1, param.getIndex());
        assertInstanceOf(Int32DataType.class, param.getDataType());
        assertEquals(true, param.isConstant());
        assertNotEquals(buff, param.getConstantValueBuffer());
        assertArrayEquals(new byte[] {1, 2, 3}, param.getConstantValueBuffer().array());
    }
}
