package com.seewo.binlog2sql.eventhandle;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.seewo.binlog2sql.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.code.or.common.util.MySQLConstants.UPDATE_ROWS_EVENT;
import static com.google.code.or.common.util.MySQLConstants.UPDATE_ROWS_EVENT_V2;
import static com.seewo.binlog2sql.SqlGenerateTool.formatTimestamp;
import static com.seewo.binlog2sql.SqlGenerateTool.updateSql;
import static com.seewo.binlog2sql.TableTool.getTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class UpdateHandle implements BinlogEventHandle {

    @Override
    public List<String> handle(BinlogEventV4 event, boolean isTurn) {

        List<String> sqls = new ArrayList<>();
        Table tableInfo = null;
        List<Pair<Row>> rowPairs = null;
        BinlogEventV4Header eventHeader = event.getHeader();

        if (eventHeader.getEventType() == UPDATE_ROWS_EVENT) {
            UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) event;
            rowPairs = updateRowsEvent.getRows();
            tableInfo = getTableInfo(updateRowsEvent.getTableId());
        } else if (eventHeader.getEventType() == UPDATE_ROWS_EVENT_V2) {
            UpdateRowsEventV2 updateRowsEventV2 = (UpdateRowsEventV2) event;
            rowPairs = updateRowsEventV2.getRows();
            tableInfo = getTableInfo(updateRowsEventV2.getTableId());
        }
        String comment = String.format("start %s end %s time %s"
                , eventHeader.getPosition()
                , eventHeader.getNextPosition()
                , formatTimestamp.format(eventHeader.getTimestamp()));

        if (isTurn) {
            sqls.addAll(updateSql(tableInfo, rowPairs, comment));
        } else {
            List<Pair<Row>> reversedPairs = rowPairs.stream()
                                                    .map(rowPair -> new Pair<>(rowPair.getAfter(), rowPair.getBefore()))
                                                    .collect(Collectors.toList());
            sqls.addAll(updateSql(tableInfo, reversedPairs, comment));
        }
        return sqls;
    }


}
