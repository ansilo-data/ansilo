package com.ansilo.connectors.query;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import com.ansilo.connectors.data.DataType;

/**
 * Wrapper for a PreparedStatment that captures bound query parameters.
 */
public class LoggingPreparedStatement implements PreparedStatement {
    /**
     * The inner prepared query
     */
    private PreparedStatement inner;

    /**
     * Logged query parameters
     */
    private List<LoggedParam> params;

    public LoggingPreparedStatement(PreparedStatement inner) {
        this.inner = inner;
        this.params = new ArrayList<>();
    }

    public PreparedStatement getInner() {
        return this.inner;
    }

    public List<LoggedParam> getLoggedParams() {
        return this.params;
    }

    public void clearLoggedParams() {
        this.params.clear();
    }

    @Override
    public void addBatch(String arg0) throws SQLException {
        this.inner.addBatch(arg0);
    }

    @Override
    public void cancel() throws SQLException {
        this.inner.cancel();
    }

    @Override
    public void clearBatch() throws SQLException {
        this.inner.clearBatch();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.inner.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        this.inner.close();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.inner.closeOnCompletion();
    }

    @Override
    public boolean execute(String arg0) throws SQLException {
        return this.inner.execute(arg0);
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        return this.inner.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        return this.inner.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        return this.inner.execute(arg0, arg1);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return this.inner.executeBatch();
    }

    @Override
    public ResultSet executeQuery(String arg0) throws SQLException {
        return this.inner.executeQuery(arg0);
    }

    @Override
    public int executeUpdate(String arg0) throws SQLException {
        return this.inner.executeUpdate(arg0);
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        return this.inner.executeUpdate(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        return this.inner.executeUpdate(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        return this.inner.executeUpdate(arg0, arg1);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.inner.getConnection();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.inner.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.inner.getFetchSize();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return this.inner.getGeneratedKeys();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return this.inner.getMaxFieldSize();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.inner.getMaxRows();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return this.inner.getMoreResults();
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        return this.inner.getMoreResults(arg0);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.inner.getQueryTimeout();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.inner.getResultSet();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.inner.getResultSetConcurrency();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.inner.getResultSetHoldability();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.inner.getResultSetType();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return this.inner.getUpdateCount();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.inner.getWarnings();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.inner.isCloseOnCompletion();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.inner.isClosed();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.inner.isPoolable();
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        this.inner.setCursorName(arg0);
    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        this.inner.setEscapeProcessing(arg0);
    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        this.inner.setFetchDirection(arg0);
    }

    @Override
    public void setFetchSize(int arg0) throws SQLException {
        this.inner.setFetchSize(arg0);
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        this.inner.setMaxFieldSize(arg0);
    }

    @Override
    public void setMaxRows(int arg0) throws SQLException {
        this.inner.setMaxRows(arg0);
    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        this.inner.setPoolable(arg0);
    }

    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        this.inner.setQueryTimeout(arg0);
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return this.inner.isWrapperFor(arg0);
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return this.inner.unwrap(arg0);
    }

    @Override
    public void addBatch() throws SQLException {
        this.inner.addBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
        this.inner.clearParameters();
    }

    @Override
    public boolean execute() throws SQLException {
        return this.inner.execute();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return this.inner.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return this.inner.executeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.inner.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return this.inner.getParameterMetaData();
    }

    @Override
    public void setArray(int arg0, Array arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_ARRAY, arg1));
        this.inner.setArray(arg0, arg1);
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setAsciiStream(arg0, arg1);
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_NUMERIC, arg1));
        this.inner.setBigDecimal(arg0, arg1);
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BINARY, arg1));
        this.inner.setBinaryStream(arg0, arg1);
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BINARY, arg1));
        this.inner.setBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BINARY, arg1));
        this.inner.setBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void setBlob(int arg0, Blob arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BLOB, arg1));
        this.inner.setBlob(arg0, arg1);
    }

    @Override
    public void setBlob(int arg0, InputStream arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BLOB, arg1));
        this.inner.setBlob(arg0, arg1);
    }

    @Override
    public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BLOB, arg1));
        this.inner.setBlob(arg0, arg1, arg2);
    }

    @Override
    public void setBoolean(int arg0, boolean arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BOOLEAN, arg1));
        this.inner.setBoolean(arg0, arg1);
    }

    @Override
    public void setByte(int arg0, byte arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BINARY, arg1));
        this.inner.setByte(arg0, arg1);
    }

    @Override
    public void setBytes(int arg0, byte[] arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BINARY, arg1));
        this.inner.setBytes(arg0, arg1);
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setCharacterStream(arg0, arg1);
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void setClob(int arg0, Clob arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_CLOB, arg1));
        this.inner.setClob(arg0, arg1);
    }

    @Override
    public void setClob(int arg0, Reader arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_CLOB, arg1));
        this.inner.setClob(arg0, arg1);
    }

    @Override
    public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_CLOB, arg1));
        this.inner.setClob(arg0, arg1, arg2);
    }

    @Override
    public void setDate(int arg0, Date arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_DATE, arg1));
        this.inner.setDate(arg0, arg1);
    }

    @Override
    public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_DATE, arg1));
        this.inner.setDate(arg0, arg1, arg2);
    }

    @Override
    public void setDouble(int arg0, double arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_FLOAT64, arg1));
        this.inner.setDouble(arg0, arg1);
    }

    @Override
    public void setFloat(int arg0, float arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_FLOAT32, arg1));
        this.inner.setFloat(arg0, arg1);
    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_INT32, arg1));
        this.inner.setInt(arg0, arg1);
    }

    @Override
    public void setLong(int arg0, long arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_INT64, arg1));
        this.inner.setLong(arg0, arg1);
    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_UTF8_STRING, arg1));
        this.inner.setNCharacterStream(arg0, arg1);
    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_UTF8_STRING, arg1));
        this.inner.setNCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void setNClob(int arg0, NClob arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_NCLOB, arg1));
        this.inner.setNClob(arg0, arg1);
    }

    @Override
    public void setNClob(int arg0, Reader arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_NCLOB, arg1));
        this.inner.setNClob(arg0, arg1);
    }

    @Override
    public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_NCLOB, arg1));
        this.inner.setNClob(arg0, arg1, arg2);
    }

    @Override
    public void setNString(int arg0, String arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_UTF8_STRING, arg1));
        this.inner.setNString(arg0, arg1);
    }

    @Override
    public void setNull(int arg0, int arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_NULL, arg1));
        this.inner.setNull(arg0, arg1);
    }

    @Override
    public void setNull(int arg0, int arg1, String arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_NULL, arg1));
        this.inner.setNull(arg0, arg1, arg2);
    }

    @Override
    public void setObject(int arg0, Object arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_JAVA_OBJECT, arg1));
        this.inner.setObject(arg0, arg1);
    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_JAVA_OBJECT, arg1));
        this.inner.setObject(arg0, arg1, arg2);
    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_JAVA_OBJECT, arg1));
        this.inner.setObject(arg0, arg1, arg2, arg3);
    }

    @Override
    public void setRef(int arg0, Ref arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setRef(arg0, arg1);
    }

    @Override
    public void setRowId(int arg0, RowId arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_VARCHAR, arg1));
        this.inner.setRowId(arg0, arg1);
    }

    @Override
    public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_SQLXML, arg1));
        this.inner.setSQLXML(arg0, arg1);
    }

    @Override
    public void setShort(int arg0, short arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_INT16, arg1));
        this.inner.setShort(arg0, arg1);
    }

    @Override
    public void setString(int arg0, String arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_UTF8_STRING, arg1));
        this.inner.setString(arg0, arg1);
    }

    @Override
    public void setTime(int arg0, Time arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_TIME, arg1));
        this.inner.setTime(arg0, arg1);
    }

    @Override
    public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_TIME, arg1));
        this.inner.setTime(arg0, arg1, arg2);
    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_DATE_TIME, arg1));
        this.inner.setTimestamp(arg0, arg1);
    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_DATE_TIME, arg1));
        this.inner.setTimestamp(arg0, arg1, arg2);
    }

    @Override
    public void setURL(int arg0, URL arg1) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_UTF8_STRING, arg1));
        this.inner.setURL(arg0, arg1);
    }

    @Override
    public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        this.params.add(new LoggedParam(arg0, DataType.TYPE_BINARY, arg1));
        this.inner.setUnicodeStream(arg0, arg1, arg2);
    }
}
