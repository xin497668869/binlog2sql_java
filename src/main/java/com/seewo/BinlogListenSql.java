package com.seewo;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.seewo.binlog2sql.MyBinlogParser;
import com.seewo.binlog2sql.handler.DeleteHandle;
import com.seewo.binlog2sql.handler.InsertHandle;
import com.seewo.binlog2sql.handler.TableMapHandle;
import com.seewo.binlog2sql.handler.UpdateHandle;
import com.seewo.vo.DbInfoVo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */

public class BinlogListenSql {

    protected static final Logger LOGGER = LogManager.getLogger(BinlogListenSql.class);

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            LOGGER.error("没找到jdbc类, 无法使用MYSQL类");
        }
    }

    public static String getFirstBinLogName(DbInfoVo dbInfoVo) {
        String url = "jdbc:mysql://" + dbInfoVo.getHost() + ":" + dbInfoVo.getPort() + "/mysql";
        try (Connection conn = DriverManager.getConnection(url, dbInfoVo.getUsername(), dbInfoVo.getPassword()); Statement statement = conn.createStatement()) {
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

    public static List<String> getRollBackSql(DbInfoVo dbInfoVo) throws Exception {
        MyBinlogParser myBinlogParser = new MyBinlogParser();

        final BinaryLogClient or = new BinaryLogClient(dbInfoVo.getHost(),
                                                       dbInfoVo.getPort(),
                                                       dbInfoVo.getUsername(),
                                                       dbInfoVo.getPassword());
        or.setServerId(1);

        or.setBinlogFilename(getFirstBinLogName(dbInfoVo));
        myBinlogParser.registerHandle(new InsertHandle(), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS, EventType.PRE_GA_WRITE_ROWS);
        myBinlogParser.registerHandle(new DeleteHandle(), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS, EventType.PRE_GA_DELETE_ROWS);
        myBinlogParser.registerHandle(new UpdateHandle(), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS, EventType.PRE_GA_UPDATE_ROWS);
        myBinlogParser.registerHandle(new TableMapHandle(dbInfoVo), EventType.TABLE_MAP);

        or.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                myBinlogParser.handle(event);
            }
        });
        or.connect();
        return Collections.emptyList();
    }

}
