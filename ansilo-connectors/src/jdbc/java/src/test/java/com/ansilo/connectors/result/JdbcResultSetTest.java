package com.ansilo.connectors.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ansilo.connectors.data.JdbcDataType;

public class JdbcResultSetTest {
    private ResultSet innerResultSet;
    private ResultSetMetaData innerResultSetMetadata;

    @BeforeEach
    void setUp() throws SQLException {
        this.innerResultSet = mock(ResultSet.class);
        this.innerResultSetMetadata = mock(ResultSetMetaData.class);
        when(this.innerResultSet.getMetaData()).thenReturn(this.innerResultSetMetadata);
    }

    @Test
    void testGetRowStructure() throws SQLException {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(2);

        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(this.innerResultSetMetadata.getColumnType(2)).thenReturn(Types.INTEGER);

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var rowStructure = resultSet.getRowStructure();

        assertEquals(2, rowStructure.getCols().size());
        assertEquals(JdbcDataType.TYPE_VARCHAR, rowStructure.getCols().get(0).getDataType().getTypeId());
        assertEquals(JdbcDataType.TYPE_INTEGER, rowStructure.getCols().get(1).getDataType().getTypeId());
    }

    @Test
    void readNoColumns() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(0);

        var buff = ByteBuffer.allocate(1024);
        int read = new JdbcResultSet(this.innerResultSet).read(buff);

        assertEquals(0, read);
    }

    @Test
    void readNoRows() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(false);

        var buff = ByteBuffer.allocate(1024);
        int read = new JdbcResultSet(this.innerResultSet).read(buff);

        assertEquals(0, read);
    }

    @Test
    void readFixedWidthInt() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getInt(1)).thenReturn(123);

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);
        int read = resultSet.read(buff);

        // should read 1 byte (not null) + 4 bytes (int)
        assertEquals(5, read);
        assertEquals(1, buff.get(0));
        assertEquals(123, buff.getInt(1));

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    @Test
    void readVarcharStream() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getString(1)).thenReturn("abc123");

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);
        int read = resultSet.read(buff);

        // should read 1 byte (not null) + 4 bytes (read length) + 6 bytes (utf8 string) + 4 byte (eof marker)
        assertEquals(15, read);
        assertEquals(1, buff.get(0));
        assertEquals(6, buff.getInt(1));
        assertEquals("abc123", StandardCharsets.UTF_8.decode(buff.slice(5, 6)).toString());
        assertEquals(0, buff.getInt(11));

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    @Test
    void testReadFixedWidthNotEnoughBuffer() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getInt(1)).thenReturn(123);

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var tooSmallBuff = ByteBuffer.allocate(3);
        
        assertThrows(SQLException.class, () -> resultSet.read(tooSmallBuff));

        var buff = ByteBuffer.allocate(10);
        int read = resultSet.read(buff);

        // should read 1 byte (not null) + 4 bytes (int)
        assertEquals(5, read);
        assertEquals(1, buff.get(0));
        assertEquals(123, buff.getInt(1));

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    @Test
    void testReadPartialStream() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getString(1)).thenReturn("abc123");

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var partialRead = ByteBuffer.allocate(8);

        // should read partial data
        int read = resultSet.read(partialRead);
        assertEquals(8, read);
        assertEquals(1, partialRead.get(0)); // not null
        assertEquals(3, partialRead.getInt(1)); // read length
        assertEquals("abc", StandardCharsets.UTF_8.decode(partialRead.slice(5, 3)).toString()); // read data


        // should read partial data
        partialRead.rewind();
        read = resultSet.read(partialRead);
        assertEquals(7, read);
        assertEquals(3, partialRead.getInt(0)); // read length
        assertEquals("123", StandardCharsets.UTF_8.decode(partialRead.slice(4, 3)).toString()); // read data

        // end of string
        partialRead.rewind();
        read = resultSet.read(partialRead);
        assertEquals(4, read);
        assertEquals(0, partialRead.getInt(0)); // read length

        // eof
        partialRead.rewind();
        read = resultSet.read(partialRead);
        assertEquals(0, read);
    }

    @Test
    void testMultipleColumns() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(2);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(this.innerResultSetMetadata.getColumnType(2)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getInt(1)).thenReturn(1);
        when(this.innerResultSet.getInt(2)).thenReturn(2);

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);

        // should read to end
        int read = resultSet.read(buff);
        assertEquals(10, read); // 5 bytes * 2
        assertEquals(1, buff.get(0)); // not null
        assertEquals(1, buff.getInt(1)); // int 1
        assertEquals(1, buff.get(5)); // not null
        assertEquals(2, buff.getInt(6)); // int 2

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    @Test
    void testMultipleRows() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(true, true, false);
        when(this.innerResultSet.getInt(1)).thenReturn(1, 2);

        var resultSet = new JdbcResultSet(this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);

        // should read to end
        int read = resultSet.read(buff);
        assertEquals(10, read); // 5 bytes * 2
        assertEquals(1, buff.get(0)); // not null
        assertEquals(1, buff.getInt(1)); // int 1
        assertEquals(1, buff.get(5)); // not null
        assertEquals(2, buff.getInt(6)); // int 2

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }
}