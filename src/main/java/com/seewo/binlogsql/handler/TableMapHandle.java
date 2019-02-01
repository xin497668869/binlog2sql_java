package com.seewo.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.seewo.binlogsql.vo.DbInfoVo;

import java.util.Collections;
import java.util.List;

import static com.seewo.binlogsql.tool.TableTool.setTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class TableMapHandle implements BinlogEventHandle {
    private DbInfoVo dbInfoVo;

    public TableMapHandle(DbInfoVo dbInfoVo) {
        this.dbInfoVo = dbInfoVo;
    }


    @Override
    public List<String> handle(Event event, boolean isTurn) {
        TableMapEventData queryEventData = event.getData();
        setTableInfo(dbInfoVo, queryEventData);
        return Collections.emptyList();
    }

}
