package com.ansilo.connectors.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import com.ansilo.connectors.data.FixedSizeDataType;
import com.ansilo.connectors.data.StreamDataType;
import com.ansilo.connectors.mapping.JdbcDataMapping;
import com.ansilo.connectors.result.JdbcResultSet;
import com.google.gson.GsonBuilder;

/**
 * Wrapper class for the JDBC prepared statement class
 */
public class JdbcPreparedQuery {
    /**
     * The data mapping for this query.
     */
    protected JdbcDataMapping mapping;

    /**
     * The inner JDBC statement We wrap it in a LoggingPreparedStatement to facilate capturing of
     * query params.
     */
    protected LoggingPreparedStatement preparedStatement;

    /**
     * The list of all query paramaters
     */
    protected List<JdbcParameter> parameters;

    /**
     * The list of all constant query paramaters
     */
    protected List<JdbcParameter> constantParameters;

    /**
     * The list of all dynamic query parameters
     */
    protected List<JdbcParameter> dynamicParameters;

    /**
     * The index of the current param
     */
    protected int paramIndex = 0;

    /**
     * Whether the current statement is batched
     */
    protected boolean isBatched = false;

    /**
     * Local buffer used to buffer partial parameter data
     */
    protected ByteArrayOutputStream localBuffer = null;

    /**
     * The length of the next chunk in the stream
     */
    protected Integer streamChunkLength = null;

    /**
     * Whether constant query parameters have been bound.
     */
    protected boolean boundConstantParams = false;

    /**
     * Creates a new prepared query
     */
    public JdbcPreparedQuery(JdbcDataMapping mapping, PreparedStatement preparedStatement,
            List<JdbcParameter> parameters) {
        this.mapping = mapping;
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
        buff.order(ByteOrder.BIG_ENDIAN);

        var originalPosition = buff.position();

        while (this.getLocalBuffer().size() + buff.remaining() > 0) {

            if (this.paramIndex >= this.dynamicParameters.size()) {
                throw new SQLException("Unexpected data after finished writing query parameters");
            }

            var param = this.dynamicParameters.get(this.paramIndex);
            var paramType = param.getDataType();

            var isNull = localBuffer.size() > 0 ? false : (buff.get(buff.position()) == 0);

            if (isNull) {
                paramType.bindParam(this.mapping, this.preparedStatement, param.getIndex(), buff);
                this.paramIndex++;
                continue;
            }

            if (paramType instanceof FixedSizeDataType) {
                var fixedType = (FixedSizeDataType) paramType;
                var localBuffer = this.getLocalBuffer();

                if (localBuffer.size() == 0 && buff.remaining() >= fixedType.getFixedSize()) {
                    // If no buffered data, read from the buffer directly
                    fixedType.bindParam(this.mapping, this.preparedStatement, param.getIndex(),
                            buff);
                } else if (localBuffer.size() >= fixedType.getFixedSize()) {
                    // If buffer contains full parameter, we read from it directly
                    var tmpBuff =
                            ByteBuffer.wrap(localBuffer.toByteArray(), 0, fixedType.getFixedSize());
                    tmpBuff.order(ByteOrder.BIG_ENDIAN);
                    fixedType.bindParam(this.mapping, this.preparedStatement, param.getIndex(),
                            tmpBuff);
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

            } else if (paramType instanceof StreamDataType) {
                var streamType = (StreamDataType) paramType;
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
                streamBuff.order(ByteOrder.BIG_ENDIAN);
                streamType.bindParam(this.mapping, this.preparedStatement, param.getIndex(),
                        streamBuff);
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
        this.isBatched = false;
    }

    /**
     * Executes the query and returns the result set.
     * 
     * @return
     * @throws SQLException
     */
    public JdbcResultSet executeQuery() throws Exception {
        if (this.isBatched) {
            throw new SQLException("Returning result sets is not supported on batched query");
        }

        this.beforeExecute();
        var hasResultSet = this.preparedStatement.execute();

        var resultSet =
                this.newResultSet(hasResultSet ? this.preparedStatement.getResultSet() : null);

        return resultSet;
    }

    /**
     * Executes the query and returns the number of affected rows.
     * 
     * @return
     * @throws SQLException
     */
    public Long executeModify() throws Exception {
        this.beforeExecute();
        if (this.isBatched) {
            var counts = this.preparedStatement.executeBatch();
            long total = 0;

            for (var count : counts) {
                total += count;
            }

            return total;
        } else {
            var hasResultSet = this.preparedStatement.execute();

            return this.getAffectedRows(hasResultSet);
        }
    }

    /**
     * Adds the query to the current batch.
     * 
     * @return
     * @throws SQLException
     */
    public void addBatch() throws Exception {
        this.beforeExecute();
        this.preparedStatement.addBatch();
        this.isBatched = true;
        this.paramIndex = 0;
    }

    protected Long getAffectedRows(boolean hasResultSet) throws SQLException {
        if (hasResultSet) {
            return null;
        }

        try {
            return this.getPreparedStatement().getLargeUpdateCount();
        } catch (UnsupportedOperationException _e) {
            return (long) this.preparedStatement.getUpdateCount();
        }
    }

    protected void beforeExecute() throws Exception {
        if (this.paramIndex != this.dynamicParameters.size()
                && !(this.isBatched && this.paramIndex == 0)) {
            throw new SQLException(
                    "Cannot execute query until all parameter data has been written");
        }

        if (!this.boundConstantParams) {
            this.bindConstantParameters();
        }
    }

    protected JdbcResultSet newResultSet(ResultSet innerResultSet) throws Exception {
        return new JdbcResultSet(this.mapping, innerResultSet);
    }

    /**
     * Gets the logged query parameters for this execution.
     */
    public List<LoggedParam> getLoggedParams() {
        return this.preparedStatement.getLoggedParams();
    }

    /**
     * Gets the logged query parameters as a JSON string.
     * 
     * This format is far more efficient to send over the JNI barrier.
     */
    public String getLoggedParamsAsJson() {
        var gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(this.preparedStatement.getLoggedParams().stream().map(i -> i.toString())
                .collect(Collectors.toList()));
    }

    private void bindConstantParameters() throws Exception {
        for (var param : this.constantParameters) {
            var dataType = param.getDataType();

            var buff = param.getConstantValueBuffer();
            buff.order(ByteOrder.BIG_ENDIAN);
            buff.rewind();

            if (dataType instanceof FixedSizeDataType) {
                dataType.bindParam(this.mapping, this.preparedStatement, param.getIndex(), buff);
            } else if (dataType instanceof StreamDataType) {
                // Read stream into local buffer
                var streamData = new ByteArrayOutputStream();

                // Read not-null byte
                byte notNull = buff.get();
                streamData.write(new byte[] {notNull});

                while (notNull > 0) {
                    byte length = buff.get();

                    // Check for EOF
                    if (length == 0) {
                        break;
                    }

                    // Write chunk to buffer
                    var tmpArr = new byte[length];
                    buff.get(tmpArr, 0, length);
                    streamData.write(tmpArr);
                }

                var streamBuff = ByteBuffer.wrap(streamData.toByteArray());
                streamBuff.order(ByteOrder.BIG_ENDIAN);
                dataType.bindParam(this.mapping, this.preparedStatement, param.getIndex(),
                        streamBuff);
            } else {
                throw new SQLException("Unknown data type class: " + dataType.toString());
            }
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
