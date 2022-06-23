package com.ansilo.connectors.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ansilo.connectors.data.Int32DataType;
import com.ansilo.connectors.data.JdbcDataType;
import com.ansilo.connectors.data.VarcharDataType;

public class JdbcPreparedQueryTest {
    private PreparedStatement innerStatement;
    private List<JdbcDataType> innerParamTypes;
    private JdbcPreparedQuery preparedQuery;

    @BeforeEach
    void setUp() throws SQLException {
        this.innerStatement = mock(PreparedStatement.class);
        this.innerParamTypes = new ArrayList<>();
        this.preparedQuery = new JdbcPreparedQuery(this.innerStatement, this.innerParamTypes);
    }

    @Test
    void writeWithNoParametersThrows() throws Exception {
        assertThrows(SQLException.class,
                () -> this.preparedQuery.write(ByteBuffer.wrap(new byte[] {1})));
    }

    @Test
    void writeInt() throws Exception {
        this.innerParamTypes.add(new Int32DataType());

        var buff = ByteBuffer.allocate(5);
        buff.put((byte) 1); // not null
        buff.putInt(123); // val
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(5, wrote);
        verify(this.innerStatement, times(1)).setInt(1, 123);
    }

    @Test
    void writeIntNull() throws Exception {
        this.innerParamTypes.add(new Int32DataType());

        var buff = ByteBuffer.allocate(1);
        buff.put((byte) 0); // null
        buff.rewind();

        var wrote = this.preparedQuery.write(buff);

        assertEquals(1, wrote);
        verify(this.innerStatement, times(1)).setNull(1, Types.INTEGER);
    }

    @Test
    void writeVarchar() throws Exception {
        this.innerParamTypes.add(new VarcharDataType());

        var buff = ByteBuffer.allocate(6);
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
        this.innerParamTypes.add(new Int32DataType());
        this.innerParamTypes.add(new Int32DataType());
        this.innerParamTypes.add(new Int32DataType());

        var buff = ByteBuffer.allocate(15);
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
        this.innerParamTypes.add(new VarcharDataType());
        this.innerParamTypes.add(new VarcharDataType());
        this.innerParamTypes.add(new VarcharDataType());

        var buff = ByteBuffer.allocate(18);
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

    private byte lengthToByte(int i) {
        return (byte) (i - 128);
    }
}
