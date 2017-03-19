package com.seewo.binlog2sql.eventhandle;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.common.glossary.Row;
import com.seewo.binlog2sql.Table;

import java.util.ArrayList;
import java.util.List;

import static com.google.code.or.common.util.MySQLConstants.DELETE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.DELETE_ROWS_EVENT_V2;
import static com.seewo.binlog2sql.SqlGenerateTool.deleteSql;
import static com.seewo.binlog2sql.SqlGenerateTool.formatTimestamp;
import static com.seewo.binlog2sql.SqlGenerateTool.insertSql;
import static com.seewo.binlog2sql.TableTool.getTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class DeleteHandle implements BinlogEventHandle {

    @Override
    public List<String> handle(BinlogEventV4 event, boolean isTurn) {
        List<String> sqls = new ArrayList<>();
        Table tableInfo = null;
        List<Row> rows = null;
        BinlogEventV4Header eventHeader = event.getHeader();

        if (eventHeader.getEventType() == DELETE_ROWS_EVENT) {
            DeleteRowsEvent deleteRowsEvent = (DeleteRowsEvent) event;
            tableInfo = getTableInfo(deleteRowsEvent.getTableId());
            rows = deleteRowsEvent.getRows();
        } else if (eventHeader.getEventType() == DELETE_ROWS_EVENT_V2) {
            DeleteRowsEventV2 deleteRowsEventV2 = (DeleteRowsEventV2) event;
            tableInfo = getTableInfo(deleteRowsEventV2.getTableId());
            rows = deleteRowsEventV2.getRows();
        }

        String comment = String.format("start %s end %s time %s"
                , eventHeader.getPosition()
                , eventHeader.getNextPosition()
                , formatTimestamp.format(eventHeader.getTimestamp()));

        if (isTurn) {
            sqls.addAll(insertSql(tableInfo, rows, comment));
        } else {
            sqls.addAll(deleteSql(tableInfo, rows, comment));
        }
        return sqls;
    }

}
