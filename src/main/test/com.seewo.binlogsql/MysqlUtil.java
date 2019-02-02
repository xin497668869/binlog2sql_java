package com.seewo.binlogsql;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author linxixin@cvte.com
 * @since 1.0
 */
@Slf4j
public class MysqlUtil {

    public static final String ORG_MYSQL_URL = "jdbc:mysql://localhost:3306/test2?user=root&password=root&useUnicode=true&characterEncoding=UTF8";

    public static List<Map<String, Object>> queryByFile(String fileName) throws Exception {
        String sql = new String(IOUtils.resourceToByteArray(fileName, ClassLoader.getSystemClassLoader()));
        return query(sql);
    }

    public static Integer insertOrUpdateByFile(String fileName) throws Exception {
        String sql = new String(IOUtils.resourceToByteArray(fileName, ClassLoader.getSystemClassLoader()));
        return insertOrUpdate(sql);
    }

    public static Integer insertOrUpdate(String sql) throws Exception {
        return executeSql(sql, connection -> {
            try (Statement statement = connection.createStatement()) {
                return statement.executeUpdate(sql);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }, ORG_MYSQL_URL);
    }

    public static List<Map<String, Object>> query(String sql) throws Exception {
        return executeSql(sql, connection -> {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {

                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnCount = resultSetMetaData.getColumnCount();
                String[] columnNames = new String[columnCount + 1];
                for (int i = 1; i <= columnCount; i++) {
                    columnNames[i] = resultSetMetaData.getColumnName(i);
                }

                List<Map<String, Object>> resultList = new ArrayList<>();
                Map<String, Object> resultMap = new HashMap<>();
                resultSet.beforeFirst();
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        resultMap.put(columnNames[i], resultSet.getObject(i));
                    }
                    resultList.add(resultMap);
                }
                log.info("成功查询数据库，查得数据：" + resultList);
                return resultList;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }, ORG_MYSQL_URL);
    }

    /**
     * 查询SQL
     *
     * @param url
     * @param sql 查询语句
     * @return 数据集合
     */
    private static <T> T executeSql(String sql, Function<Connection, T> execute, String url) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        log.info("成功加载驱动");

        log.info("成功获取连接");


        try (java.sql.Connection connection = DriverManager.getConnection(url)) {
            if (execute == null) {
                return null;
            }
            return execute.apply(connection);

        } catch (Throwable t) {
            // TODO 处理异常
            t.printStackTrace();
            return null;
        }
    }
}
