package com.seewo;

import com.google.code.or.OpenReplicator;
import com.seewo.binlog2sql.MyBinlogParser;
import com.seewo.binlog2sql.eventhandle.DeleteHandle;
import com.seewo.binlog2sql.eventhandle.InsertHandle;
import com.seewo.binlog2sql.eventhandle.TableMapHandle;
import com.seewo.binlog2sql.eventhandle.UpdateHandle;
import com.seewo.vo.DbInfoVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.google.code.or.common.util.MySQLConstants.DELETE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.DELETE_ROWS_EVENT_V2;
import static com.google.code.or.common.util.MySQLConstants.TABLE_MAP_EVENT;
import static com.google.code.or.common.util.MySQLConstants.UPDATE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.UPDATE_ROWS_EVENT_V2;
import static com.google.code.or.common.util.MySQLConstants.WRITE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.WRITE_ROWS_EVENT_V2;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class Binlog2Sql {

    public static final Logger LOGGER = LoggerFactory.getLogger(Binlog2Sql.class);


    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            LOGGER.error("没找到jdbc类, 无法使用MYSQL类");
        }
    }

    public static String getFirstBinLogName(OpenReplicator or) {
        String url = "jdbc:mysql://" + or.getHost() + ":" + or.getPort() + "/mysql";
        try (Connection conn = DriverManager.getConnection(url, or.getUser(), or.getPassword()); Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("show master logs;");
            while (resultSet.next()) {
                String logName = resultSet.getString("Log_name");
                return logName;
            }
        } catch (SQLException e) {
            LOGGER.error("获取binlogName失败", e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        DbInfoVo dbInfoVo = new DbInfoVo();
        dbInfoVo.setHost("localhost");
        dbInfoVo.setPort(3306);
        dbInfoVo.setUsername("root");
        dbInfoVo.setPassword("root");
        getRollBackSql(dbInfoVo);
    }

    private static List<String> getRollBackSql(DbInfoVo dbInfoVo) throws Exception {
        final OpenReplicator or = new OpenReplicator();
        or.setUser(dbInfoVo.getUsername());
        or.setPassword(dbInfoVo.getPassword());
        or.setHost(dbInfoVo.getHost());
        or.setPort(dbInfoVo.getPort());
        or.setServerId(1);
//        or.setBinlogPosition(4);
//        or.setbinlo(4);
//        or.setBinlogFileName("mysql-bin.000001");
        or.setBinlogFileName(getFirstBinLogName(or));
        boolean isTurn = true; //是否反sql
        MyBinlogParser parser = new MyBinlogParser();

        or.start(transport -> {

            or.setBinlogEventListener(parser.getBinlogEventListener(isTurn));
            parser.setTransport(transport);
            parser.setBinlogFileName(or.getBinlogFileName());
            parser.setBinlogFileName(or.getBinlogFileName());

            parser.registerHandle(new InsertHandle(), WRITE_ROWS_EVENT, WRITE_ROWS_EVENT_V2);
            parser.registerHandle(new DeleteHandle(), DELETE_ROWS_EVENT, DELETE_ROWS_EVENT_V2);
            parser.registerHandle(new UpdateHandle(), UPDATE_ROWS_EVENT, UPDATE_ROWS_EVENT_V2);
            parser.registerHandle(new TableMapHandle(dbInfoVo), TABLE_MAP_EVENT);
            return parser;
        });
        return parser.getSqls();
    }

}
