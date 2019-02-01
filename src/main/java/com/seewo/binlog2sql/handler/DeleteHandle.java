package com.seewo.binlog2sql.handler;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.seewo.binlog2sql.BinlogEventHandle;
import com.seewo.vo.RowVo;
import com.seewo.vo.TableVo;

import java.util.List;

import static com.seewo.binlog2sql.SqlGenerateTool.changeToRowVo;
import static com.seewo.binlog2sql.SqlGenerateTool.deleteSql;
import static com.seewo.binlog2sql.SqlGenerateTool.getComment;
import static com.seewo.binlog2sql.SqlGenerateTool.insertSql;
import static com.seewo.binlog2sql.TableTool.getTableInfo;

/**
 * @author linxixin@cvte.com
 * @version 1.0
 * @description
 */
public class DeleteHandle implements BinlogEventHandle {

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        DeleteRowsEventData deleteRowsEventData = event.getData();
        TableVo tableVoInfo = getTableInfo(deleteRowsEventData.getTableId());
        List<RowVo> rows = changeToRowVo(tableVoInfo, deleteRowsEventData.getRows());

        if (isTurn) {
            return insertSql(tableVoInfo, rows, getComment(event.getHeader()));
        } else {
            return deleteSql(tableVoInfo, rows, getComment(event.getHeader()));
        }
    }

}
