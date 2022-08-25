package com.ansilo.connectors.query;

import com.ansilo.connectors.data.DataType;

/**
 * Logged query parameter
 */
public class LoggedParam {
    private int index;
    private int jdbcType;
    private Object value;

    public LoggedParam(int index, int jdbcType, Object value) {
        this.index = index;
        this.jdbcType = jdbcType;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + index;
        result = prime * result + jdbcType;
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
        if (jdbcType != other.jdbcType)
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
        return "LoggedParam [index=" + index + ", type=" + DataType.typeName(jdbcType)
                + ", value=" + value + "]";
    }
}
