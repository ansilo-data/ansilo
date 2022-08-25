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
import com.ansilo.connectors.data.DataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;

public class JdbcResultSetTest {
    private JdbcDataMapping mapping;
    private ResultSet innerResultSet;
    private ResultSetMetaData innerResultSetMetadata;

    @BeforeEach
    void setUp() throws Exception {
        this.mapping = new JdbcDataMapping();
        this.innerResultSet = mock(ResultSet.class);
        this.innerResultSetMetadata = mock(ResultSetMetaData.class);
        when(this.innerResultSet.getMetaData()).thenReturn(this.innerResultSetMetadata);
    }

    @Test
    void testGetRowStructure() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(2);

        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(this.innerResultSetMetadata.getColumnType(2)).thenReturn(Types.INTEGER);

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
        var rowStructure = resultSet.getRowStructure();

        assertEquals(2, rowStructure.getCols().size());
        assertEquals(DataType.TYPE_UTF8_STRING,
                rowStructure.getCols().get(0).getDataType().getTypeId());
        assertEquals(DataType.TYPE_INT32, rowStructure.getCols().get(1).getDataType().getTypeId());
    }

    @Test
    void testGetRowStructureNoResultSet() throws Exception {
        var resultSet = new JdbcResultSet(this.mapping, null);
        var rowStructure = resultSet.getRowStructure();

        assertEquals(0, rowStructure.getCols().size());
    }

    @Test
    void readNoColumns() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(0);

        var buff = ByteBuffer.allocate(1024);
        int read = new JdbcResultSet(this.mapping, this.innerResultSet).read(buff);

        assertEquals(0, read);
    }

    @Test
    void readNoResultSet() throws Exception {
        var buff = ByteBuffer.allocate(1024);
        int read = new JdbcResultSet(this.mapping, null).read(buff);

        assertEquals(0, read);
    }

    @Test
    void readNoRows() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(false);

        var buff = ByteBuffer.allocate(1024);
        int read = new JdbcResultSet(this.mapping, this.innerResultSet).read(buff);

        assertEquals(0, read);
    }

    @Test
    void readFixedWidthInt() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getInt(1)).thenReturn(123);

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
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
        when(this.innerResultSet.getNString(1)).thenReturn("abc123");

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);
        int read = resultSet.read(buff);

        // should read 1 byte (not null) + 1 byte (read length) + 6 bytes (utf8 string) + 1 byte
        // (eof marker)
        assertEquals(9, read);
        assertEquals(1, buff.get(0));
        assertEquals(6, this.byteToLength(buff.get(1)));
        assertEquals("abc123", StandardCharsets.UTF_8.decode(buff.slice(2, 6)).toString());
        assertEquals(0, this.byteToLength(buff.get(8)));

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

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
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
        when(this.innerResultSet.getNString(1)).thenReturn("abc123");

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
        var partialRead = ByteBuffer.allocate(5);

        // should read partial data
        int read = resultSet.read(partialRead);
        assertEquals(5, read);
        assertEquals(1, partialRead.get(0)); // not null
        assertEquals(3, this.byteToLength(partialRead.get(1))); // read length
        assertEquals("abc", StandardCharsets.UTF_8.decode(partialRead.slice(2, 3)).toString()); // read
                                                                                                // data


        // should read partial data
        partialRead.rewind();
        read = resultSet.read(partialRead);
        assertEquals(4, read);
        assertEquals(3, this.byteToLength(partialRead.get(0))); // read length
        assertEquals("123", StandardCharsets.UTF_8.decode(partialRead.slice(1, 3)).toString()); // read
                                                                                                // data

        // end of string
        partialRead.rewind();
        read = resultSet.read(partialRead);
        assertEquals(1, read);
        assertEquals(0, this.byteToLength(partialRead.get(0))); // read length

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

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
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

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
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
    void testIntsWithNulls() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        when(this.innerResultSet.next()).thenReturn(true, true, true, true, false);
        when(this.innerResultSet.getInt(1)).thenReturn(1, 0, 0, 2);
        when(this.innerResultSet.wasNull()).thenReturn(false, true, true, false);

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);

        // should read to end
        int read = resultSet.read(buff);
        assertEquals(12, read); // 5 bytes * 2 + 1 bytes * 2 (nulls)
        assertEquals(1, buff.get(0)); // not null
        assertEquals(1, buff.getInt(1)); // int 1
        assertEquals(0, buff.get(5)); // null
        assertEquals(0, buff.get(6)); // null
        assertEquals(1, buff.get(7)); // not null
        assertEquals(2, buff.getInt(8)); // int 2

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    @Test
    void readStreamsWithNulls() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);

        when(this.innerResultSet.next()).thenReturn(true, true, true, false);
        when(this.innerResultSet.getNString(1)).thenReturn("abc", null, "123");
        when(this.innerResultSet.wasNull()).thenReturn(false, true, false);

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
        var buff = ByteBuffer.allocate(1024);
        int read = resultSet.read(buff);

        // should read 1 byte (not null) + 1 byte (read length) + 3 bytes (utf8 string) + 1 byte
        // (eof marker)
        // should read 1 byte (not null)
        // should read 1 byte (not null) + 1 byte (read length) + 3 bytes (utf8 string) + 1 byte
        // (eof marker)
        assertEquals(13, read);
        assertEquals(1, buff.get(0));
        assertEquals(3, this.byteToLength(buff.get(1)));
        assertEquals("abc", StandardCharsets.UTF_8.decode(buff.slice(2, 3)).toString());
        assertEquals(0, this.byteToLength(buff.get(5)));
        assertEquals(0, buff.get(6));
        assertEquals(1, buff.get(7));
        assertEquals(3, this.byteToLength(buff.get(8)));
        assertEquals("123", StandardCharsets.UTF_8.decode(buff.slice(9, 3)).toString());
        assertEquals(0, this.byteToLength(buff.get(12)));

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    @Test
    void readLongVarchar() throws Exception {
        when(this.innerResultSetMetadata.getColumnCount()).thenReturn(1);
        when(this.innerResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);

        when(this.innerResultSet.next()).thenReturn(true, false);
        when(this.innerResultSet.getNString(1)).thenReturn("a".repeat(600));

        var resultSet = new JdbcResultSet(this.mapping, this.innerResultSet);
        var buff = ByteBuffer.allocate(1000);
        int read = resultSet.read(buff);
        buff.rewind();
        assertEquals(605, read);
        assertEquals(1, buff.get());
        assertEquals(255, this.byteToLength(buff.get()));
        assertEquals("a".repeat(255),
                StandardCharsets.UTF_8.decode(this.readBytes(buff, 255)).toString());
        assertEquals(255, this.byteToLength(buff.get()));
        assertEquals("a".repeat(255),
                StandardCharsets.UTF_8.decode(this.readBytes(buff, 255)).toString());
        assertEquals(90, this.byteToLength(buff.get()));
        assertEquals("a".repeat(90),
                StandardCharsets.UTF_8.decode(this.readBytes(buff, 90)).toString());
        assertEquals(0, this.byteToLength(buff.get()));

        // eof
        buff.rewind();
        read = resultSet.read(buff);
        assertEquals(0, read);
    }

    private Integer byteToLength(byte b) {
        return Byte.toUnsignedInt(b);
    }

    private ByteBuffer readBytes(ByteBuffer buff, int len) {
        var ret = new byte[len];
        buff.get(ret, 0, len);
        return ByteBuffer.wrap(ret);
    }
}
