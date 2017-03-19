package com.seewo.binlog2sql.eventhandle;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Row;
import com.seewo.binlog2sql.Table;

import java.util.ArrayList;
import java.util.List;

import static com.google.code.or.common.util.MySQLConstants.WRITE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.WRITE_ROWS_EVENT_V2;
import static com.seewo.binlog2sql.SqlGenerateTool.deleteSql;
import static com.seewo.binlog2sql.SqlGenerateTool.formatTimestamp;
import static com.seewo.binlog2sql.SqlGenerateTool.insertSql;
import static com.seewo.binlog2sql.TableTool.getTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */

public class InsertHandle implements BinlogEventHandle{

    @Override
    public List<String> handle(BinlogEventV4 event, boolean isTurn) {
        List<String> sqls = new ArrayList<>();
        Table tableInfo = null;
        List<Row> rows = null;
        BinlogEventV4Header eventHeader = event.getHeader();
        if (eventHeader.getEventType() == WRITE_ROWS_EVENT_V2) {
            WriteRowsEventV2 writeRowsEventV2 = (WriteRowsEventV2) event;
            rows = writeRowsEventV2.getRows();
            tableInfo = getTableInfo(writeRowsEventV2.getTableId());
        }
        if (eventHeader.getEventType() == WRITE_ROWS_EVENT) {
            WriteRowsEvent writeRowsEvent = (WriteRowsEvent) event;
            rows = writeRowsEvent.getRows();
            tableInfo = getTableInfo(writeRowsEvent.getTableId());
        }
        String comment = String.format("start %s end %s time %s"
                , eventHeader.getPosition()
                , eventHeader.getNextPosition()
                , formatTimestamp.format(eventHeader.getTimestamp()));
        if (isTurn) {
            sqls.addAll(deleteSql(tableInfo, rows,comment));

        } else {
            sqls.addAll(insertSql(tableInfo, rows,comment));
        }

        return sqls;
    }


}
