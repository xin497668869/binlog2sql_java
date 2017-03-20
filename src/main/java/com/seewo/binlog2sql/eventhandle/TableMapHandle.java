package com.seewo.binlog2sql.eventhandle;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.seewo.vo.DbInfoVo;

import java.util.Collections;
import java.util.List;

import static com.seewo.binlog2sql.TableTool.setTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class TableMapHandle implements BinlogEventHandle{
    private DbInfoVo dbInfoVo;
    public TableMapHandle(DbInfoVo dbInfoVo) {
        this.dbInfoVo = dbInfoVo;
    }

    @Override
    public List<String> handle(BinlogEventV4 event, boolean isTurn) {
        TableMapEvent tableMapEvent = (TableMapEvent) event;
        setTableInfo(dbInfoVo,tableMapEvent.getDatabaseName().toString(), tableMapEvent.getTableId(), tableMapEvent.getTableName().toString());
        return Collections.emptyList();
    }

}
