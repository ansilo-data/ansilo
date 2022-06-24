package com.ansilo.connectors.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import com.ansilo.connectors.data.JdbcDataType;
import com.ansilo.connectors.data.JdbcFixedSizeDataType;
import com.ansilo.connectors.data.JdbcStreamDataType;
import com.ansilo.connectors.result.JdbcResultSet;

/**
 * Wrapper class for the JDBC prepared statement class
 */
public class JdbcPreparedQuery {
    /**
     * The actual JDBC statement
     */
    private PreparedStatement preparedStatement;

    /**
     * The type of query parameters
     */
    private List<JdbcDataType> parameterTypes;

    /**
     * The index of the current param
     */
    private int paramIndex = 0;

    /**
     * Local buffer used to buffer partial parameter data
     */
    private ByteArrayOutputStream localBuffer = null;

    /**
     * The length of the next chunk in the stream
     */
    private Integer streamChunkLength = null;

    /**
     * Creates a new prepared query
     */
    public JdbcPreparedQuery(PreparedStatement preparedStatement,
            List<JdbcDataType> parameterTypes) {
        this.preparedStatement = preparedStatement;
        this.parameterTypes = parameterTypes;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public List<JdbcDataType> getParameterTypes() {
        return parameterTypes;
    }

    /**
     * Parses the supplied buff as query parameters and binds them to the query
     */
    public int write(ByteBuffer buff) throws Exception {
        // We are transfering data within the name process across JNI
        // just use native-endianess
        // We will take care of endianess during serialisation when
        // transferring to remote systems.
        buff.order(ByteOrder.nativeOrder());

        var originalPosition = buff.position();

        while (this.getLocalBuffer().size() + buff.remaining() > 0) {

            if (this.paramIndex >= this.parameterTypes.size()) {
                throw new SQLException("Unexpected data after finished writing query parameters");
            }

            var paramType = this.parameterTypes.get(this.paramIndex);

            var isNull = localBuffer.size() > 0 ? false : (buff.get(buff.position()) == 0);

            if (isNull) {
                paramType.bindParam(this.preparedStatement, this.paramIndex + 1, buff);
                this.paramIndex++;
                continue;
            }

            if (paramType instanceof JdbcFixedSizeDataType) {
                var fixedType = (JdbcFixedSizeDataType) paramType;
                var localBuffer = this.getLocalBuffer();

                if (localBuffer.size() == 0 && buff.remaining() >= fixedType.getFixedSize()) {
                    // If no buffered data, read from the buffer directly
                    fixedType.bindParam(this.preparedStatement, this.paramIndex + 1, buff);
                } else if (localBuffer.size() >= fixedType.getFixedSize()) {
                    // If buffer contains full parameter, we read from it directly
                    var tmpBuff =
                            ByteBuffer.wrap(localBuffer.toByteArray(), 0, fixedType.getFixedSize());
                    tmpBuff.order(ByteOrder.nativeOrder());
                    fixedType.bindParam(this.preparedStatement, this.paramIndex + 1, tmpBuff);
                    this.resetLocalBuffer();
                } else if (buff.remaining() > 0) {
                    // Consume the not null flag byte
                    if (this.localBuffer.size() == 0) {
                        localBuffer.write(new byte[] {buff.get()});
                    }

                    // Write the partial data to the local buffer
                    this.saveToBuffer(buff, Math.min(fixedType.getFixedSize(), buff.remaining()));
                    continue;
                } else {
                    break;
                }

            } else if (paramType instanceof JdbcStreamDataType) {
                var streamType = (JdbcStreamDataType) paramType;
                var localBuffer = this.getLocalBuffer();

                // Read chunk length
                if (this.streamChunkLength == null) {
                    // Consume the not null flag byte
                    if (this.localBuffer.size() == 0) {
                        localBuffer.write(new byte[] {buff.get()});
                    }

                    if (buff.remaining() == 0) {
                        break;
                    }

                    this.streamChunkLength = Byte.toUnsignedInt(buff.get());
                }

                // Copy chunk to local buffer
                if (this.streamChunkLength > 0) {
                    if (buff.remaining() == 0) {
                        break;
                    }

                    int chunkLength = Math.min(this.streamChunkLength, buff.remaining());
                    this.saveToBuffer(buff, chunkLength);
                    this.streamChunkLength = this.streamChunkLength - chunkLength;

                    // If chunk finished move to next chunk
                    if (this.streamChunkLength == 0) {
                        this.streamChunkLength = null;
                    }

                    continue;
                }

                // Chunk length == 0 => EOF, we then bind the parameter
                var streamBuff = ByteBuffer.wrap(localBuffer.toByteArray());
                streamBuff.order(ByteOrder.nativeOrder());
                streamType.bindParam(this.preparedStatement, this.paramIndex + 1, streamBuff);
                this.resetLocalBuffer();
                this.streamChunkLength = null;
            }

            this.paramIndex++;
        }

        return buff.position() - originalPosition;
    }

    /**
     * Executes the query and resets the paraemter index to zero.
     * 
     * @return
     * @throws SQLException
     */
    public JdbcResultSet execute() throws SQLException {
        if (this.paramIndex != this.parameterTypes.size()) {
            throw new SQLException(
                    "Cannot execute query until all parameter data has been written");
        }

        var resultSet = new JdbcResultSet(this.preparedStatement.executeQuery());

        // Reset parameter index for next execution
        this.paramIndex = 0;

        return resultSet;
    }

    private void resetLocalBuffer() throws IOException {
        this.localBuffer.reset();
    }

    private ByteArrayOutputStream getLocalBuffer() {
        if (this.localBuffer == null) {
            this.localBuffer = new ByteArrayOutputStream();
        }

        return this.localBuffer;
    }

    private void saveToBuffer(ByteBuffer buff, int length) throws IOException {
        var localBuffer = this.getLocalBuffer();

        var tmpArr = new byte[length];
        buff.get(tmpArr, 0, length);
        localBuffer.write(tmpArr);
    }
}
