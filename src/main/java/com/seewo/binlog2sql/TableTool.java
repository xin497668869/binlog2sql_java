package com.seewo.binlog2sql;

import com.seewo.vo.DbInfoVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class TableTool {


    public static final Logger LOGGER = LoggerFactory.getLogger(TableTool.class);

    public static Map<Long, Table> tableInfoMap = new HashMap<>();

    public static Table getTableInfo(Long tableId) {
        if (tableInfoMap.containsKey(tableId)) {
            return tableInfoMap.get(tableId);
        }
        throw new RuntimeException("出现异常, 没找到tableId");
    }


    public static void setTableInfo(DbInfoVo dbInfoVo, String dbName, Long tableId, String tableName) {
        if (tableInfoMap.containsKey(tableId)) {
            return;
        }
        String url = "jdbc:mysql://" + dbInfoVo.getHost() + ":" + dbInfoVo.getPort() + "/" + dbName + "?characterEncoding=UTF-8&autoReconnect=true";

        try (Connection connection = DriverManager.getConnection(url, dbInfoVo.getUsername(), dbInfoVo.getPassword())) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(dbName, null, tableName, null);
            Table table = new Table(dbName, tableName);
            List<String> columnNames = new ArrayList<>();
            while (columns.next()) {
                String column = columns.getString("COLUMN_NAME");
//                String type = columns.getString("TYPE_NAME");
                columnNames.add(column);
            }
            table.setColumns(columnNames);
            tableInfoMap.put(tableId, table);
        } catch (Exception e) {
            LOGGER.error("获取connection失败", e);
            throw new RuntimeException(e);
        }
    }


}
