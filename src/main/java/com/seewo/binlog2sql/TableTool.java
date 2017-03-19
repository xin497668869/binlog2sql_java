package com.seewo.binlog2sql;

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

    public static final String ip = "localhost";
    public static final String dbName = "seewo_remote";
    public static final String url = "jdbc:mysql://" + ip + ":3306/" + dbName + "?characterEncoding=UTF-8&autoReconnect=true&useSSL=true&serverTimezone=UTC";
    public static final String username = "root";
    public static final String password = "root";

    public static final Logger LOGGER = LoggerFactory.getLogger(TableTool.class);

    public static Map<Long, Table> tableInfoMap = new HashMap<>();

    public static Table getTableInfo(Long tableId) {
        if (tableInfoMap.containsKey(tableId)) {
            return tableInfoMap.get(tableId);
        }
        throw new RuntimeException("出现异常, 没找到tableId");
    }



    public static void setTableInfo(String dbName, Long tableId, String tableName) {
        if (tableInfoMap.containsKey(tableId)) {
            return;
        }
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
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
