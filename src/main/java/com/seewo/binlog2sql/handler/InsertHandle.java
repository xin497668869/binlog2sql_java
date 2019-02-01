package com.seewo.binlog2sql.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.seewo.binlog2sql.BinlogEventHandle;
import com.seewo.vo.ColumnItemDataVo;
import com.seewo.vo.ColumnVo;
import com.seewo.vo.RowVo;
import com.seewo.vo.TableVo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.seewo.binlog2sql.SqlGenerateTool.deleteSql;
import static com.seewo.binlog2sql.SqlGenerateTool.formatTimestamp;
import static com.seewo.binlog2sql.SqlGenerateTool.insertSql;
import static com.seewo.binlog2sql.TableTool.getTableInfo;


/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */

public class InsertHandle implements BinlogEventHandle {


    @Override
    public List<String> handle(Event event, boolean isTurn) {
        WriteRowsEventData writeRowsEventV2 = event.getData();

        TableVo tableVoInfo = getTableInfo(writeRowsEventV2.getTableId());
        EventHeaderV4 eventHeader = event.getHeader();
        List<RowVo> rows = changeToRowVo(tableVoInfo, writeRowsEventV2);
        String comment = String.format("start %s end %s time %s"
                , eventHeader.getPosition()
                , eventHeader.getNextPosition()
                , formatTimestamp.format(eventHeader.getTimestamp()));
        List<String> sqls = new ArrayList<>();
        if (isTurn) {
            sqls.addAll(deleteSql(tableVoInfo, rows, comment));
        } else {
            sqls.addAll(insertSql(tableVoInfo, rows, comment));
        }

        return sqls;
    }

    private List<RowVo> changeToRowVo(TableVo tableVo, WriteRowsEventData writeRowsEventData) {
        List<RowVo> rowVos = new ArrayList<>();

        for (Serializable[] row : writeRowsEventData.getRows()) {
            RowVo rowVo = new RowVo();
            List<ColumnItemDataVo> columnItemData = new ArrayList<>();
            rowVo.setValue(columnItemData);
            for (int i = 0; i < row.length; i++) {
                ColumnVo columnVo = tableVo.getColumns().get(i);
                columnItemData.add(new ColumnItemDataVo(row[i], columnVo));
            }
            rowVos.add(rowVo);
        }
        return rowVos;
    }


}
