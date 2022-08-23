package com.ansilo.connectors.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import com.ansilo.connectors.data.JdbcFixedSizeDataType;
import com.ansilo.connectors.data.JdbcStreamDataType;
import com.ansilo.connectors.result.JdbcResultSet;

/**
 * Wrapper class for the JDBC prepared statement class
 */
public class JdbcPreparedQuery {
    /**
     * The inner JDBC statement We wrap it in a LoggingPreparedStatement to facilate capturing of
     * query params.
     */
    private LoggingPreparedStatement preparedStatement;

    /**
     * The list of all query paramaters
     */
    private List<JdbcParameter> parameters;

    /**
     * The list of all constant query paramaters
     */
    private List<JdbcParameter> constantParameters;

    /**
     * The list of all dynamic query parameters
     */
    private List<JdbcParameter> dynamicParameters;

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
     * Whether constant query parameters have been bound.
     */
    private boolean boundConstantParams = false;

    /**
     * Creates a new prepared query
     */
    public JdbcPreparedQuery(PreparedStatement preparedStatement, List<JdbcParameter> parameters) {
        this.preparedStatement = new LoggingPreparedStatement(preparedStatement);
        this.parameters = parameters;
        this.dynamicParameters = parameters.stream().filter(i -> !i.isConstant()).toList();
        this.constantParameters = parameters.stream().filter(i -> i.isConstant()).toList();
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement.getInner();
    }

    public List<JdbcParameter> getParameters() {
        return parameters;
    }

    /**
     * Parses the supplied buff as query parameters and binds them to the query
     */
    public int write(ByteBuffer buff) throws Exception {
        // We are transfering data within the name process across JNI
        // just use native-endianess
        //
        // This is fine if we assume the reader of the buffer is on the same host.
        // In the current version this assumption hosts as postgres is run in the same container
        // In future versions perhaps we have to revise this assumption if we start supporting
        // running postgres on another host.
        buff.order(ByteOrder.nativeOrder());

        var originalPosition = buff.position();

        while (this.getLocalBuffer().size() + buff.remaining() > 0) {

            if (this.paramIndex >= this.dynamicParameters.size()) {
                throw new SQLException("Unexpected data after finished writing query parameters");
            }

            var param = this.dynamicParameters.get(this.paramIndex);
            var paramType = param.getDataType();

            var isNull = localBuffer.size() > 0 ? false : (buff.get(buff.position()) == 0);

            if (isNull) {
                paramType.bindParam(this.preparedStatement, param.getIndex(), buff);
                this.paramIndex++;
                continue;
            }

            if (paramType instanceof JdbcFixedSizeDataType) {
                var fixedType = (JdbcFixedSizeDataType) paramType;
                var localBuffer = this.getLocalBuffer();

                if (localBuffer.size() == 0 && buff.remaining() >= fixedType.getFixedSize()) {
                    // If no buffered data, read from the buffer directly
                    fixedType.bindParam(this.preparedStatement, param.getIndex(), buff);
                } else if (localBuffer.size() >= fixedType.getFixedSize()) {
                    // If buffer contains full parameter, we read from it directly
                    var tmpBuff =
                            ByteBuffer.wrap(localBuffer.toByteArray(), 0, fixedType.getFixedSize());
                    tmpBuff.order(ByteOrder.nativeOrder());
                    fixedType.bindParam(this.preparedStatement, param.getIndex(), tmpBuff);
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
                streamType.bindParam(this.preparedStatement, param.getIndex(), streamBuff);
                this.resetLocalBuffer();
                this.streamChunkLength = null;
            }

            this.paramIndex++;
        }

        return buff.position() - originalPosition;
    }

    /**
     * Resets the parameter index to zero such that new query parameters can be written.
     * 
     * @return
     * @throws SQLException
     */
    public void restart() throws SQLException {
        // Reset parameter index for next execution
        this.paramIndex = 0;
        this.preparedStatement.clearLoggedParams();
    }

    /**
     * Executes the query and resets the paraemter index to zero.
     * 
     * @return
     * @throws SQLException
     */
    public JdbcResultSet execute() throws SQLException {
        if (this.paramIndex != this.dynamicParameters.size()) {
            throw new SQLException(
                    "Cannot execute query until all parameter data has been written");
        }

        if (!this.boundConstantParams) {
            this.bindConstantParameters();
        }

        var hasResultSet = this.preparedStatement.execute();

        var resultSet =
                new JdbcResultSet(hasResultSet ? this.preparedStatement.getResultSet() : null);

        return resultSet;
    }

    /**
     * Gets the logged query parameters for this execution.
     */
    public List<LoggedParam> getLoggedParams() {
        return this.preparedStatement.getLoggedParams();
    }

    private void bindConstantParameters() throws SQLException {
        for (var param : this.constantParameters) {
            var buff = param.getConstantValueBuffer();
            buff.order(ByteOrder.nativeOrder());
            buff.rewind();
            param.getDataType().bindParam(this.preparedStatement, param.getIndex(), buff);
        }

        this.boundConstantParams = true;
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
