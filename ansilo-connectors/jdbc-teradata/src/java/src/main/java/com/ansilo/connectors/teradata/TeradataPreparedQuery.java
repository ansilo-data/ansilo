package com.ansilo.connectors.teradata;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import com.ansilo.connectors.mapping.JdbcDataMapping;
import com.ansilo.connectors.query.JdbcParameter;
import com.ansilo.connectors.query.JdbcPreparedQuery;

public class TeradataPreparedQuery extends JdbcPreparedQuery {

    private String query;

    public TeradataPreparedQuery(JdbcDataMapping mapping, PreparedStatement preparedStatement,
            List<JdbcParameter> parameters, String query) {
        super(mapping, preparedStatement, parameters);
        this.query = query;
    }

    // Calculate affected rows for multi-statement requests (MSR)
    @Override
    protected Long getAffectedRows(boolean hasResultSet) throws SQLException {
        if (!this.query.contains(";")) {
            return super.getAffectedRows(hasResultSet);
        }

        // I haven't been able to get a reliable count from MSR so just return null for now
        return null;

        // if (hasResultSet) {
        // return null;
        // }

        // long count = this.preparedStatement.getUpdateCount();

        // if (count == -1) {
        // return null;
        // }

        // while (this.preparedStatement.getMoreResults()) {
        // var resCount = this.preparedStatement.getUpdateCount();

        // if (resCount == -1) {
        // return null;
        // }

        // count += resCount;
        // }

        // return count;
    }
}
