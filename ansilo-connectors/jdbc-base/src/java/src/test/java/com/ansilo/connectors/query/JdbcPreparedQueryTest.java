package com.ansilo.connectors.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ansilo.connectors.data.Int32DataType;
import com.ansilo.connectors.data.VarcharDataType;
import com.ansilo.connectors.result.JdbcResultSet;

public class JdbcPreparedQueryTest {
    private PreparedStatement innerStatement;
    private ResultSet mockResultSet;
    private ResultSetMetaData mockResultSetMetadata;
    private List<JdbcParameter> innerParams;
    private JdbcPreparedQuery preparedQuery;

    @BeforeEach
    void setUp() throws SQLException {
        this.innerStatement = mock(PreparedStatement.class);
        this.mockResultSet = mock(ResultSet.class);
        this.mockResultSetMetadata = mock(ResultSetMetaData.class);
        this.innerParams = new ArrayList<>();

        when(this.innerStatement.execute()).thenReturn(true);
        when(this.innerStatement.getResultSet()).thenReturn(this.mockResultSet);
        when(this.mockResultSet.getMetaData()).thenReturn(this.mockResultSetMetadata);
    }

    private void initPreparedQuery() {
        this.preparedQuery = new JdbcPreparedQuery(this.innerStatement, this.innerParams);
    }

    @Test
    void writeWithNoParametersThrows() throws Exception {
        this.initPreparedQuery();
        assertThrows(SQLException.class,
                () -> this.preparedQuery.write(ByteBuffer.wrap(new byte[] {1})));
    }

    @Test
    void writeInt() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(5);
        buff.put((byte) 1); // not null
        buff.putInt(123); // val
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(5, wrote);
        verify(this.innerStatement, times(1)).setInt(1, 123);
    }

    @Test
    void writeIntNull() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(1);
        buff.put((byte) 0); // null
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(1, wrote);
        verify(this.innerStatement, times(1)).setNull(1, Types.INTEGER);
    }

    @Test
    void writeVarchar() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new VarcharDataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(6);
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("abc"));
        buff.put(this.lengthToByte(0)); // eof
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(6, wrote);
        verify(this.innerStatement, times(1)).setString(1, "abc");
    }

    @Test
    void writeMultipleInts() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.innerParams.add(JdbcParameter.createDynamic(2, new Int32DataType()));
        this.innerParams.add(JdbcParameter.createDynamic(3, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(15);
        buff.put((byte) 1); // not null
        buff.putInt(123); // val
        buff.put((byte) 1); // not null
        buff.putInt(456); // val
        buff.put((byte) 1); // not null
        buff.putInt(789); // val
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(15, wrote);
        verify(this.innerStatement, times(1)).setInt(1, 123);
        verify(this.innerStatement, times(1)).setInt(2, 456);
        verify(this.innerStatement, times(1)).setInt(3, 789);
    }

    @Test
    void writeMultipleVarchar() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new VarcharDataType()));
        this.innerParams.add(JdbcParameter.createDynamic(2, new VarcharDataType()));
        this.innerParams.add(JdbcParameter.createDynamic(3, new VarcharDataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(18);
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("abc"));
        buff.put(this.lengthToByte(0)); // eof
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("def"));
        buff.put(this.lengthToByte(0)); // eof
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("ghi"));
        buff.put(this.lengthToByte(0)); // eof
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(18, wrote);
        verify(this.innerStatement, times(1)).setString(1, "abc");
        verify(this.innerStatement, times(1)).setString(2, "def");
        verify(this.innerStatement, times(1)).setString(3, "ghi");
    }

    @Test
    void writeIntThenVarchar() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.innerParams.add(JdbcParameter.createDynamic(2, new VarcharDataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(11);
        buff.put((byte) 1); // not null
        buff.putInt(123); // value
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("abc"));
        buff.put(this.lengthToByte(0)); // eof
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(11, wrote);
        verify(this.innerStatement, times(1)).setInt(1, 123);
        verify(this.innerStatement, times(1)).setString(2, "abc");
    }

    @Test
    void writePartialInt() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(5);
        buff.put((byte) 1); // not null
        buff.putInt(123); // value
        buff.rewind();

        for (int i = 0; i < buff.limit(); i++) {
            var wrote = this.preparedQuery.write(buff.slice(i, 1));
            assertEquals(1, wrote);
        }

        verify(this.innerStatement, times(1)).setInt(1, 123);
    }

    @Test
    void writePartialVarchar() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new VarcharDataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(12);
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("abc"));
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("def"));
        buff.put(this.lengthToByte(1)); // length
        buff.put(StandardCharsets.UTF_8.encode("g"));
        buff.put(this.lengthToByte(0)); // eof
        buff.rewind();

        for (int i = 0; i < buff.limit(); i++) {
            var wrote = this.preparedQuery.write(buff.slice(i, 1));
            assertEquals(1, wrote);
        }

        verify(this.innerStatement, times(1)).setString(1, "abcdefg");
    }

    @Test
    void writeIntWithVarcharAndMixedNulls() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new VarcharDataType()));
        this.innerParams.add(JdbcParameter.createDynamic(2, new Int32DataType()));
        this.innerParams.add(JdbcParameter.createDynamic(3, new VarcharDataType()));
        this.innerParams.add(JdbcParameter.createDynamic(4, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(13);
        buff.put((byte) 1); // not null
        buff.put(this.lengthToByte(3)); // length
        buff.put(StandardCharsets.UTF_8.encode("abc"));
        buff.put(this.lengthToByte(0)); // eof
        buff.put((byte) 0); // null
        buff.put((byte) 0); // null
        buff.put((byte) 1); // not null
        buff.putInt(123);
        buff.rewind();

        for (int i = 0; i < buff.limit(); i++) {
            var wrote = this.preparedQuery.write(buff.slice(i, 1));
            assertEquals(1, wrote);
        }

        verify(this.innerStatement, times(1)).setString(1, "abc");
        verify(this.innerStatement, times(1)).setNull(2, Types.INTEGER);
        verify(this.innerStatement, times(1)).setNull(3, Types.VARCHAR);
        verify(this.innerStatement, times(1)).setInt(4, 123);
    }

    @Test
    void executeWithoutParams() throws Exception {
        this.initPreparedQuery();
        var resultSet = this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).execute();
        verify(this.innerStatement, times(1)).getResultSet();
        assertInstanceOf(JdbcResultSet.class, resultSet);
    }

    @Test
    void executeWithoutWritingParamsThrows() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.initPreparedQuery();

        assertThrows(SQLException.class, () -> {
            this.preparedQuery.execute();
        });
        verify(this.innerStatement, times(0)).execute();
        verify(this.innerStatement, times(0)).getResultSet();
    }

    @Test
    void executeWithPartialParamThrows() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.innerParams.add(JdbcParameter.createDynamic(2, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(5);
        buff.put((byte) 1); // not null
        buff.putInt(123); // val
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(5, wrote);
        verify(this.innerStatement, times(1)).setInt(1, 123);

        assertThrows(SQLException.class, () -> {
            this.preparedQuery.execute();
        });
        verify(this.innerStatement, times(0)).execute();
        verify(this.innerStatement, times(0)).getResultSet();
    }

    @Test
    void executeWithFullParamsSucceeds() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.initPreparedQuery();

        var buff = this.newByteBuffer(5);
        buff.put((byte) 1); // not null
        buff.putInt(123); // val
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(5, wrote);
        verify(this.innerStatement, times(1)).setInt(1, 123);

        this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).execute();
        verify(this.innerStatement, times(1)).getResultSet();
    }

    @Test
    void executeMultipleWithRestart() throws Exception {
        this.innerParams.add(JdbcParameter.createDynamic(1, new Int32DataType()));
        this.initPreparedQuery();

        for (var _i : new byte[] {1, 2, 3}) {
            var buff = this.newByteBuffer(5);
            buff.put((byte) 1); // not null
            buff.putInt(123); // val
            buff.rewind();

            var wrote = this.preparedQuery.write(buff);

            assertEquals(5, wrote);

            this.preparedQuery.execute();
            this.preparedQuery.restart();;
        }

        verify(this.innerStatement, times(3)).setInt(1, 123);
        verify(this.innerStatement, times(3)).execute();
        verify(this.innerStatement, times(3)).getResultSet();
    }

    @Test
    void writeConstantParam() throws Exception {
        var buff = this.newByteBuffer(5);
        buff.put((byte) 1); // not null
        buff.putInt(123); // val

        this.innerParams.add(JdbcParameter.createConstant(1, new Int32DataType(), buff));
        this.initPreparedQuery();

        // should only bind after execute
        verify(this.innerStatement, times(0)).setInt(1, 123);

        this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).setInt(1, 123);

        // should only bind constants once
        this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).setInt(1, 123);
    }

    @Test
    void writeConstantAndDynamicParams() throws Exception {
        var buff1 = this.newByteBuffer(5);
        buff1.put((byte) 1); // not null
        buff1.putInt(123); // val

        var buff3 = this.newByteBuffer(5);
        buff3.put((byte) 1); // not null
        buff3.putInt(789); // val

        this.innerParams.add(JdbcParameter.createConstant(1, new Int32DataType(), buff1));
        this.innerParams.add(JdbcParameter.createDynamic(2, new Int32DataType()));
        this.innerParams.add(JdbcParameter.createConstant(3, new Int32DataType(), buff3));
        this.initPreparedQuery();

        var buff2 = this.newByteBuffer(5);
        buff2.put((byte) 1); // not null
        buff2.putInt(456); // val
        buff2.rewind();

        var wrote = this.preparedQuery.write(buff2);

        assertEquals(5, wrote);

        verify(this.innerStatement, times(1)).setInt(2, 456);
        // should only bind constants after execute
        verify(this.innerStatement, times(0)).setInt(1, 123);
        verify(this.innerStatement, times(0)).setInt(3, 789);

        this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).setInt(1, 123);
        verify(this.innerStatement, times(1)).setInt(2, 456);
        verify(this.innerStatement, times(1)).setInt(3, 789);

        // should only bind constants once but dynamic param again
        buff2 = this.newByteBuffer(5);
        buff2.put((byte) 1); // not null
        buff2.putInt(888); // val
        buff2.rewind();
        wrote = this.preparedQuery.write(buff2);
        assertEquals(5, wrote);
        this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).setInt(1, 123);
        verify(this.innerStatement, times(1)).setInt(2, 888);
        verify(this.innerStatement, times(1)).setInt(3, 789);
    }

    @Test
    void executeNoResultSet() throws Exception {
        this.innerStatement = mock(PreparedStatement.class);
        when(this.innerStatement.execute()).thenReturn(false);

        this.preparedQuery = new JdbcPreparedQuery(this.innerStatement, this.innerParams);

        var resultSet = this.preparedQuery.execute();
        verify(this.innerStatement, times(1)).execute();
        verify(this.innerStatement, times(0)).getResultSet();
        assertInstanceOf(JdbcResultSet.class, resultSet);
    }

    private ByteBuffer newByteBuffer(int capacity) {
        var buff = ByteBuffer.allocate(capacity);
        buff.order(ByteOrder.nativeOrder());
        return buff;
    }

    private byte lengthToByte(int i) {
        return (byte) i;
    }
}
