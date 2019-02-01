package com.seewo.binlogsql;


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
public class MysqlUtil {

    public static Integer insertOrUpdate(String sql) throws Exception {
        return executeSql(sql, connection -> {
            try (Statement statement = connection.createStatement()) {
                return statement.executeUpdate(sql);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
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
                System.out.println("成功查询数据库，查得数据：" + resultList);
                return resultList;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
    }

    /**
     * 查询SQL
     *
     * @param sql 查询语句
     * @return 数据集合
     */
    public static <T> T executeSql(String sql, Function<Connection, T> execute) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("成功加载驱动");

        String url = "jdbc:mysql://localhost:3306/test2?user=root&password=root&useUnicode=true&characterEncoding=UTF8";

        System.out.println("成功获取连接");


        try (java.sql.Connection connection = DriverManager.getConnection(url)) {
            return execute.apply(connection);

        } catch (Throwable t) {
            // TODO 处理异常
            t.printStackTrace();
            return null;
        }
    }
}
