package com.seewo.binlogsql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.seewo.binlogsql.handler.DeleteHandle;
import com.seewo.binlogsql.handler.InsertHandle;
import com.seewo.binlogsql.handler.TableMapHandle;
import com.seewo.binlogsql.handler.UpdateHandle;
import com.seewo.binlogsql.vo.DbInfoVo;
import com.seewo.binlogsql.vo.EventFilterVo;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
@Slf4j
public class BinlogListenSql {


    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("没找到jdbc类, 无法使用MYSQL类");
        }
    }

    public static String getFirstBinLogName(DbInfoVo dbInfoVo) {
        String url = "jdbc:mysql://" + dbInfoVo.getHost() + ":" + dbInfoVo.getPort() + "/mysql";
        try (Connection conn = DriverManager.getConnection(url, dbInfoVo.getUsername(), dbInfoVo.getPassword()); Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("show master logs;");
            while (resultSet.next()) {
                return resultSet.getString("Log_name");
            }
        } catch (SQLException e) {
            log.error("获取binlogName失败", e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        log.error("#############");
        DbInfoVo dbInfoVo = new DbInfoVo();
        dbInfoVo.setHost("localhost");
        dbInfoVo.setPort(3306);
        dbInfoVo.setUsername("root");
        dbInfoVo.setPassword("root");
        getRollBackSql(dbInfoVo, new EventFilterVo());
    }

    public static MyBinlogParser getRollBackSql(DbInfoVo dbInfoVo, EventFilterVo eventFilterVo) throws Exception {
        MyBinlogParser myBinlogParser = new MyBinlogParser();

        final BinaryLogClient or = new BinaryLogClient(dbInfoVo.getHost(),
                                                       dbInfoVo.getPort(),
                                                       dbInfoVo.getUsername(),
                                                       dbInfoVo.getPassword());
        or.setServerId(1);

        or.setBinlogFilename(getFirstBinLogName(dbInfoVo));
        myBinlogParser.registerHandle(new InsertHandle(eventFilterVo), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS, EventType.PRE_GA_WRITE_ROWS);
        myBinlogParser.registerHandle(new DeleteHandle(eventFilterVo), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS, EventType.PRE_GA_DELETE_ROWS);
        myBinlogParser.registerHandle(new UpdateHandle(eventFilterVo), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS, EventType.PRE_GA_UPDATE_ROWS);
        myBinlogParser.registerHandle(new TableMapHandle(dbInfoVo), EventType.TABLE_MAP);
        or.registerEventListener(event -> {
            if (eventFilterVo.getStartTime() > event.getHeader().getTimestamp()) {
                log.info(event.getHeader().getTimestamp() + "  "+eventFilterVo.getStartTime());
                return;
            }
            myBinlogParser.handle(event);
        });


//        or.setBlocking(false);
        new Thread(() -> {
            try {
                or.connect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        return myBinlogParser;
    }

}
