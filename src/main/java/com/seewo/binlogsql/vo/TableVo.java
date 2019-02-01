package com.seewo.binlogsql.vo;

import lombok.Data;

import java.util.List;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
@Data
public class TableVo {
    private String           dbName;
    private String           tableName;
    private List<ColumnVo>     columns;

    public TableVo(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }
}
