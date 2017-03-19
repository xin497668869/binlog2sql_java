package com.seewo.binlog2sql;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class Table {
    private String dbName;
    private String tableName;
    private List<String> columns;

    public Table(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
