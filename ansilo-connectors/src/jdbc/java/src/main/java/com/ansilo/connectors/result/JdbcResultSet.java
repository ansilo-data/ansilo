package com.ansilo.connectors.result;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.ansilo.connectors.data.JdbcDataType;
import com.ansilo.connectors.data.JdbcFixedSizeDataType;
import com.ansilo.connectors.data.JdbcStreamDataType;

/***
 * The JDBC result set wrapper class.
 * 
 * This exposes a result set in the interface expected by our rust code. For efficiency we translate
 * the JDBC result-set into a binary stream format that is written two a buffer managed by rust.
 */
public class JdbcResultSet {
    /**
     * The inner JDBC result set
     */
    private ResultSet resultSet;

    /**
     * The array of data types for each column in the result set.
     */
    private JdbcDataType[] dataTypes;

    /**
     * The position of the result set cursor
     */
    private int rowIndex = 0;

    /**
     * The position of the column within the current row
     */
    private int columnIndex = 0;

    /**
     * Whether we are finished reading from the result set
     */
    private boolean lastRow = false;

    /**
     * The byte stream for reading the data in the current position.
     */
    private InputStream currentStream = null;

    /**
     * Minimum number of bytes required to read the next value
     */
    private Integer requireAtLeastBytes;

    /**
     * Internal read buffer for copying input streams into the byte buffer.
     */
    private byte[] readBuff;

    /**
     * Initialises the result set wrapper
     * 
     * @param resultSet
     * @throws SQLException
     */
    public JdbcResultSet(ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        this.dataTypes = this.getDataTypes();
        this.readBuff = new byte[255];
    }

    /**
     * Gets the row structure of the result set.
     * 
     * @throws SQLException
     */
    public JdbcRowStructure getRowStructure() throws SQLException {
        var metadata = this.resultSet.getMetaData();
        List<JdbcRowColumnInfo> cols = new ArrayList<>();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            cols.add(new JdbcRowColumnInfo(metadata.getColumnName(i + 1), this.dataTypes[i]));
        }

        return new JdbcRowStructure(cols);
    }

    /**
     * Reads the next portion of the result set into the supplied byte buffer.
     * 
     * This uses an instance-level mutable read buffer so this method is NOT THREAD SAFE.
     * 
     * @param buff
     * @throws Exception
     */
    public int read(ByteBuffer buff) throws Exception {
        // We are transfering data within the name process across JNI
        // just use native-endianess
        // We will take care of endianess during serialisation when
        // transferring to remote systems.
        buff.order(ByteOrder.nativeOrder());

        int originalRemaining = buff.remaining();

        // Advance to first row
        if (this.rowIndex == 0 && !this.nextRow()) {
            // If zero results...
            return 0;
        }

        // Edge case, if no columns
        if (this.dataTypes.length == 0) {
            return 0;
        }

        this.requireAtLeastBytes = null;

        // Tight loop for reading data from JDBC (performance sensitive)
        while (true) {
            if (this.columnIndex == this.dataTypes.length) {
                if (this.lastRow || !this.nextRow()) {
                    break;
                }
            }

            if (!buff.hasRemaining()) {
                break;
            }

            var dataType = this.dataTypes[this.columnIndex];

            if (dataType instanceof JdbcFixedSizeDataType) {
                var fixedDataType = (JdbcFixedSizeDataType) dataType;
                if (fixedDataType.getFixedSize() <= buff.remaining()) {
                    fixedDataType.writeToByteBuffer(buff, this.resultSet, this.columnIndex + 1);
                    this.columnIndex++;
                } else {
                    this.requireAtLeastBytes = fixedDataType.getFixedSize();
                    break;
                }
            } else if (dataType instanceof JdbcStreamDataType) {
                var streamDataType = (JdbcStreamDataType) dataType;

                if (this.currentStream == null) {
                    this.currentStream =
                            streamDataType.getStream(this.resultSet, this.columnIndex + 1);

                    // The first byte indicates if the value is null or present
                    buff.put(this.currentStream == null ? (byte) 0 : 1);
                }

                if (this.currentStream == null) {
                    this.columnIndex++;
                }

                // For streaming data, we frame each read with the length (a byte) of that read
                while (this.currentStream != null && buff.remaining() >= 2) {
                    // Calculate maximum read length as remaining bytes - sizeof(byte) header
                    int len = Math.min(255, buff.remaining() - 1);
                    int read = this.currentStream.read(this.readBuff, 0, len);

                    if (read <= 0) {
                        // Write 0 read length which signals EOF
                        buff.put((byte)0);
                        this.currentStream.close();
                        this.currentStream = null;
                        this.columnIndex++;
                        break;
                    } else {
                        // Write the actual read length
                        buff.put((byte)read);
                        // Copy the read buffer into the
                        buff.put(this.readBuff, 0, read);

                        // Require at least 2 bytes to read the header frame + 1 byte of read
                        // data
                        // (assume the buffer will be far larger as that would be terribly
                        // inefficient)
                        this.requireAtLeastBytes = 2;
                    }
                }

                if (buff.remaining() < 2) {
                    break;
                }
            } else {
                throw new SQLException(
                        String.format("Unknown data type class %s", dataType.getClass().getName()));
            }
        }

        var read = originalRemaining - buff.remaining();

        if (read == 0 && this.requireAtLeastBytes != null) {
            throw new SQLException(
                    String.format("At least %d bytes are required to read the next value",
                            this.requireAtLeastBytes));
        }

        return read;
    }

    private boolean nextRow() throws SQLException {
        var res = this.resultSet.next();

        if (res) {
            this.rowIndex++;
            this.columnIndex = 0;
        } else {
            this.lastRow = true;
        }

        return res;
    }

    private JdbcDataType[] getDataTypes() throws SQLException {
        var metadata = this.resultSet.getMetaData();
        JdbcDataType[] dataTypes = new JdbcDataType[metadata.getColumnCount()];

        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = JdbcDataType.createFromJdbcType(metadata.getColumnType(i + 1));
        }

        return dataTypes;
    }
}
