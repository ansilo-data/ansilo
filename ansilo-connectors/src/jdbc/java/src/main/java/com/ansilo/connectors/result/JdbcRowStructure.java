package com.ansilo.connectors.result;

import java.util.List;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * The JDBC row structure wrapper class
 */
public class JdbcRowStructure {
    /**
     * The list of columns in the row
     */
    private List<JdbcRowColumnInfo> cols;

    /**
     * Initialises the row structure
     */
    public JdbcRowStructure(List<JdbcRowColumnInfo> cols) {
        this.cols = cols;
    }

    public List<JdbcRowColumnInfo> getCols() {
        return cols;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this.cols);
    }
}
