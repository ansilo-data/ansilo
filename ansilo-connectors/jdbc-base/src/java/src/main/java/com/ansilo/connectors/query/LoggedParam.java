package com.ansilo.connectors.query;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Logged query parameter
 */
public class LoggedParam {
    private int index;
    private String jdbcMethod;
    private Object value;

    public LoggedParam(int index, String jdbcMethod, Object value) {
        this.index = index;
        this.jdbcMethod = jdbcMethod;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public String getJdbcMethod() {
        return jdbcMethod;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + index;
        result = prime * result + ((jdbcMethod == null) ? 0 : jdbcMethod.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LoggedParam other = (LoggedParam) obj;
        if (index != other.index)
            return false;
        if (jdbcMethod == null) {
            if (other.jdbcMethod != null)
                return false;
        } else if (!jdbcMethod.equals(other.jdbcMethod))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "LoggedParam [index=" + index + ", method=" + this.jdbcMethod + ", value="
                + this.valueToString() + "]";
    }

    private String valueToString() {
        if (value == null) {
            return "null";
        }

        if (value instanceof InputStream) {
            return value.getClass().getName();
        }

        return value.toString();
    }
}
