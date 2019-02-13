package com.seewo.binlogsql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.seewo.binlogsql.handler.DeleteHandle;
import com.seewo.binlogsql.handler.InsertHandle;
import com.seewo.binlogsql.handler.TableMapHandle;
import com.seewo.binlogsql.handler.UpdateHandle;
import com.seewo.binlogsql.vo.DbInfoVo;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
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
@Accessors(chain = true)
public class BinlogListenSql {
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("没找到jdbc类, 无法使用MYSQL类");
        }
    }

    @Getter
    private BinlogParser    binlogParser = new BinlogParser();
    private DbInfoVo        dbInfoVo;
    @Setter
    private Filter          filter       = new Filter() {
    };
    private BinaryLogClient binaryLogClient;

    public BinlogListenSql(DbInfoVo dbInfoVo) {
        this.dbInfoVo = dbInfoVo;
    }

    private String getFirstBinLogName() {
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

    private void initBinlogParser() {

        binlogParser.registerHandle(new InsertHandle(filter), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS, EventType.PRE_GA_WRITE_ROWS);
        binlogParser.registerHandle(new DeleteHandle(filter), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS, EventType.PRE_GA_DELETE_ROWS);
        binlogParser.registerHandle(new UpdateHandle(filter), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS, EventType.PRE_GA_UPDATE_ROWS);
        binlogParser.registerHandle(new TableMapHandle(dbInfoVo), EventType.TABLE_MAP);
    }

    public BinlogListenSql connectAndListen() {
        initBinlogParser();

        binaryLogClient = new BinaryLogClient(dbInfoVo.getHost(),
                                              dbInfoVo.getPort(),
                                              dbInfoVo.getUsername(),
                                              dbInfoVo.getPassword());
        binaryLogClient.setServerId(1);
        binaryLogClient.setBinlogFilename(getFirstBinLogName());

        binaryLogClient.registerEventListener(event -> {
            if (!filter.filter(event)) {
                return;
            }
            binlogParser.handle(event);
        });

        new Thread(() -> {
            try {
                binaryLogClient.connect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        return this;
    }

    public void close() {
        try {
            if (binaryLogClient != null) {
                binaryLogClient.disconnect();
                binaryLogClient = null;
            }
        } catch (IOException e) {
            log.error("关闭失败", e);
        }
    }

}
